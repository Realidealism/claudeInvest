"""RS (relative strength vs TAIEX) probe — zero-rebuild.

Extends the existing score_panel in memory (never writes it back) with RS-derived
columns, then answers two questions before any panel rebuild is attempted:

  Q1 (degeneracy): is a continuous RS deviation cell rank-identical to the existing
      distance cell within each daily cross-section?  Theory says yes in log space
      (the market term is a per-day constant), so a continuous RS cell would add
      literally nothing to a decile-spread system.
  Q2 (event edge): do RS Donchian breakout events (path-dependent, therefore NOT
      killed by the constant-shift argument) carry forward-return excess over the
      same-day universe, uniformly across years, without merely re-firing on the
      same days as the existing price Donchian cell?

Usage:
    python -m analysis._score_rs_probe [max_stocks]
"""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from analysis.donchian import calculate_donchian
from analysis.indicators import rolling_std, sma
from db.connection import get_cursor

PANEL = "tmp/score_panel.parquet"
DEAD = "tmp/dead_fish_panel.parquet"

# Two candidate windows for Donchian-on-RS.  123 = Lucas (alpha-max on price),
# 233 = Fibonacci (the one production actually runs on price).
DON_PARAMS = (("rs123", 123, 61), ("rs233", 233, 144))

Z_PERIOD = 233
Z_STD_WIN = 252

PANEL_COLS = [
    "date",
    "stock_id",
    "close",
    "total_long",
    "fwd_5",
    "fwd_20",
    "fwd_60",
    "距離_p233_long",
    "Donchian_long",
]
HORIZONS = ("fwd_5", "fwd_20", "fwd_60")


def _load_taiex() -> pd.Series:
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT trade_date, close_price FROM tw.index_prices "
            "WHERE index_id = 'TAIEX' AND close_price IS NOT NULL "
            "ORDER BY trade_date"
        )
        rows = cur.fetchall()
    s = pd.Series(
        [float(r["close_price"]) for r in rows],
        index=pd.to_datetime([r["trade_date"] for r in rows]),
        name="taiex",
    )
    return s


def _zscore(values: np.ndarray, period: int) -> np.ndarray:
    """Same shape as the production distance cell: (x - SMA) / rolling_std(x - SMA)."""
    ma = np.asarray(sma(values, period), dtype=np.float64)
    dev = values - ma
    std = np.asarray(rolling_std(dev, Z_STD_WIN), dtype=np.float64)
    with np.errstate(invalid="ignore", divide="ignore"):
        z = np.where(std > 0, dev / std, np.nan)
    return z


def _per_stock(df: pd.DataFrame) -> pd.DataFrame:
    """df is one stock, sorted by date, with close + taiex."""
    close = df["close"].to_numpy(dtype=np.float64)
    rs = close / df["taiex"].to_numpy(dtype=np.float64)
    out = {"date": df["date"].to_numpy(), "stock_id": df["stock_id"].to_numpy()}

    # Q1 controls: same z-score recipe on price, on RS, and on log RS.
    out["z_price"] = _zscore(close, Z_PERIOD)
    out["z_rs"] = _zscore(rs, Z_PERIOD)
    out["z_logrs"] = _zscore(np.log(rs), Z_PERIOD)
    out["z_logprice"] = _zscore(np.log(close), Z_PERIOD)

    # Q2: Donchian on the RS series.  calculate_donchian wants high/low/close;
    # RS has no intraday range, so the series stands in for all three.
    rs32 = rs.astype(np.float32)
    for label, entry, exit_ in DON_PARAMS:
        res = calculate_donchian(rs32, rs32, rs32, entry_length=entry, exit_length=exit_)
        out[f"{label}_up"] = np.asarray(res.entry_long, dtype=bool)
        out[f"{label}_down"] = np.asarray(res.entry_short, dtype=bool)

    return pd.DataFrame(out)


def _excess(panel: pd.DataFrame, mask: pd.Series, h: str) -> tuple[float, int]:
    """Mean (stock fwd - same-day universe mean fwd) over rows where mask is True."""
    sub = panel.loc[mask & panel[h].notna(), ["date", h, f"{h}_mkt"]]
    if sub.empty:
        return float("nan"), 0
    return float((sub[h] - sub[f"{h}_mkt"]).mean() * 100), len(sub)


def main() -> None:
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0

    print("loading panel ...", file=sys.stderr)
    panel = pd.read_parquet(PANEL, columns=PANEL_COLS)
    panel["date"] = pd.to_datetime(panel["date"])
    if limit:
        keep = panel["stock_id"].drop_duplicates().head(limit)
        panel = panel[panel["stock_id"].isin(keep)]

    taiex = _load_taiex()
    panel["taiex"] = panel["date"].map(taiex)
    missing = panel["taiex"].isna().sum()
    print(f"panel rows={len(panel):,}  taiex-missing rows={missing:,}", file=sys.stderr)
    panel = panel[panel["taiex"].notna()]

    print("computing RS columns per stock ...", file=sys.stderr)
    parts = [
        _per_stock(g.sort_values("date"))
        for _, g in panel.groupby("stock_id", sort=False)
    ]
    rs = pd.concat(parts, ignore_index=True)
    panel = panel.merge(rs, on=["date", "stock_id"], how="left")

    dead = pd.read_parquet(DEAD)
    dead["date"] = pd.to_datetime(dead["date"])
    panel = panel.merge(dead, on=["date", "stock_id"], how="left")
    panel["is_dead_fish"] = panel["is_dead_fish"].fillna(False)
    panel = panel[~panel["is_dead_fish"]]
    print(f"~dead rows={len(panel):,}", file=sys.stderr)

    panel["year"] = panel["date"].dt.year
    for h in HORIZONS:
        panel[f"{h}_mkt"] = panel.groupby("date")[h].transform("mean")

    # ---------------- Q1: is a continuous RS cell degenerate? ----------------
    print("\n=== Q1  continuous RS vs existing distance cell (daily cross-section) ===")
    print("Spearman per day, then averaged.  ~1.0 => RS adds nothing to a decile system.")
    pairs = [
        ("z_logrs", "z_logprice", "log RS dev  vs  log price dev   (theory: identity)"),
        ("z_rs", "z_price", "RS dev      vs  price dev       (linear, as a cell would be built)"),
        ("z_rs", "距離_p233_long", "RS dev      vs  production 距離_p233 (clipped pts)"),
    ]
    for a, b, label in pairs:
        rhos = []
        for _, g in panel.groupby("date"):
            sub = g[[a, b]].dropna()
            if len(sub) < 30 or sub[a].nunique() < 5 or sub[b].nunique() < 5:
                continue
            rho = sub[a].rank().corr(sub[b].rank())
            if not np.isnan(rho):
                rhos.append(rho)
        arr = np.array(rhos)
        if arr.size == 0:
            print(f"  {label}: no day had enough cross-section (smoke run?)")
            continue
        print(f"  {label}: mean rho={arr.mean():.4f}  median={np.median(arr):.4f}  "
              f"min={arr.min():.4f}  (n_days={len(arr)})")

    # ---------------- Q2: do RS Donchian events carry excess? ----------------
    print("\n=== Q2  RS Donchian event cohort excess (pp vs same-day ~dead universe) ===")
    for label, entry, _ in DON_PARAMS:
        for side in ("up", "down"):
            col = f"{label}_{side}"
            mask = panel[col].fillna(False)
            row = [f"  {col:<12} (entry={entry:>3}d)"]
            for h in HORIZONS:
                ex, n = _excess(panel, mask, h)
                row.append(f"{h}={ex:+.3f}pp (n={n:,})")
            print("  ".join(row))

    print("\n--- per-year excess, H=20 ---")
    years = sorted(panel["year"].unique())
    hdr = "  " + "".join(f"{y:>9}" for y in years)
    print(f"  {'cell':<12}{hdr}")
    for label, _, _ in DON_PARAMS:
        for side in ("up", "down"):
            col = f"{label}_{side}"
            cells = []
            for y in years:
                m = panel["year"].eq(y) & panel[col].fillna(False)
                ex, n = _excess(panel, m, "fwd_20")
                cells.append("      n/a" if n < 50 else f"{ex:>+9.2f}")
            print(f"  {col:<12}  " + "".join(cells))

    print("\n--- per-year excess, H=60 ---")
    for label, _, _ in DON_PARAMS:
        for side in ("up", "down"):
            col = f"{label}_{side}"
            cells = []
            for y in years:
                m = panel["year"].eq(y) & panel[col].fillna(False)
                ex, n = _excess(panel, m, "fwd_60")
                cells.append("      n/a" if n < 50 else f"{ex:>+9.2f}")
            print(f"  {col:<12}  " + "".join(cells))

    # ---------------- Q3: redundancy with the existing price Donchian ----------------
    print("\n=== Q3  overlap with existing price Donchian_long cell ===")
    print("Donchian_long != 0 means the price-breakout event window is active (0-5d).")
    for label, _, _ in DON_PARAMS:
        for side in ("up", "down"):
            col = f"{label}_{side}"
            m = panel[col].fillna(False)
            n = int(m.sum())
            if n == 0:
                continue
            active = panel.loc[m, "Donchian_long"].ne(0).mean()
            rate = m.mean()
            print(f"  {col:<12} trigger_rate={rate * 100:5.2f}%  "
                  f"overlap_with_price_Donchian={active * 100:5.1f}%  (n={n:,})")

    # ---- Q4: does the excess survive once price-Donchian days are removed? ----
    # This is the test that killed the 爆量長黑 candidate (2026-07-07): a cohort
    # edge that lives entirely on days another cell already fires is redundant,
    # no matter how clean the headline number looks.
    print("\n=== Q4  residual cohort: RS event AND price Donchian window inactive ===")
    quiet = panel["Donchian_long"].eq(0)
    for label, _, _ in DON_PARAMS:
        for side in ("up", "down"):
            col = f"{label}_{side}"
            mask = panel[col].fillna(False) & quiet
            row = [f"  {col + '|quiet':<18}"]
            for h in HORIZONS:
                ex, n = _excess(panel, mask, h)
                row.append(f"{h}={ex:+.3f}pp (n={n:,})")
            print("  ".join(row))

    print("\n--- residual per-year excess, H=20 ---")
    print(f"  {'cell':<18}" + "".join(f"{y:>9}" for y in years))
    for label, _, _ in DON_PARAMS:
        for side in ("up", "down"):
            col = f"{label}_{side}"
            cells = []
            for y in years:
                m = panel["year"].eq(y) & panel[col].fillna(False) & quiet
                ex, n = _excess(panel, m, "fwd_20")
                cells.append("      n/a" if n < 50 else f"{ex:>+9.2f}")
            print(f"  {col + '|quiet':<18}" + "".join(cells))

    print("\n--- residual per-year excess, H=60 ---")
    for label, _, _ in DON_PARAMS:
        for side in ("up", "down"):
            col = f"{label}_{side}"
            cells = []
            for y in years:
                m = panel["year"].eq(y) & panel[col].fillna(False) & quiet
                ex, n = _excess(panel, m, "fwd_60")
                cells.append("      n/a" if n < 50 else f"{ex:>+9.2f}")
            print(f"  {col + '|quiet':<18}" + "".join(cells))


    # ---- Q5: is there room to move?  The C1a failure mode (2026-07-07) was a real
    # cohort edge that could not shift decile spread, because the cohort already sat
    # in the bottom decile under the existing cells.  A new cell only pays if its
    # cohort currently lives in deciles the cell would relocate them out of.
    print("\n=== Q5  where does the residual cohort already sit in total_long deciles? ===")
    panel["dec"] = panel.groupby("date")["total_long"].transform(
        lambda s: pd.qcut(s.rank(method="first"), 10, labels=False, duplicates="drop")
    )
    base = panel["dec"].value_counts(normalize=True).sort_index()
    print("  universe baseline share per decile (D1..D10): "
          + " ".join(f"{base.get(i, 0) * 100:4.1f}%" for i in range(10)))
    for label, _, _ in DON_PARAMS:
        for side in ("up", "down"):
            col = f"{label}_{side}"
            sub = panel[panel[col].fillna(False) & quiet]
            if len(sub) < 100:
                continue
            share = sub["dec"].value_counts(normalize=True).sort_index()
            print(f"  {col + '|quiet':<18}"
                  + " ".join(f"{share.get(i, 0) * 100:4.1f}%" for i in range(10)))

    # The decisive number: excess measured against the cohort's OWN decile, not the
    # whole universe.  vs-universe only restates "D1 is bad"; vs-peers asks whether
    # the RS event separates stocks the existing cells already scored the same.
    print("\n--- residual cohort excess vs SAME-DAY SAME-DECILE peers (H=20 / H=60) ---")
    for h in ("fwd_20", "fwd_60"):
        panel[f"{h}_dec"] = panel.groupby(["date", "dec"])[h].transform("mean")
    for label, _, _ in DON_PARAMS:
        for side in ("up", "down"):
            col = f"{label}_{side}"
            m0 = panel[col].fillna(False) & quiet
            if int(m0.sum()) < 100:
                continue
            for h in ("fwd_20", "fwd_60"):
                cells = []
                for d in range(10):
                    sub = panel.loc[
                        m0 & panel["dec"].eq(d) & panel[h].notna(), [h, f"{h}_dec"]
                    ]
                    if len(sub) < 50:
                        cells.append("    n/a")
                    else:
                        cells.append(f"{(sub[h] - sub[f'{h}_dec']).mean() * 100:>+7.2f}")
                allsub = panel.loc[m0 & panel[h].notna(), [h, f"{h}_dec"]]
                tot = (allsub[h] - allsub[f"{h}_dec"]).mean() * 100
                print(f"  {col + '|quiet':<18}{h:<7}" + "".join(cells) + f"   ALL={tot:+.3f}")


if __name__ == "__main__":
    main()
