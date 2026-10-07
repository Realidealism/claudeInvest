"""Volatility compression / level probe — zero-rebuild, pre-registered kill gates.

Two hypotheses, deliberately kept apart because they are not the same claim:

  H1  volatility LEVEL: low-vol stocks out-earn high-vol peers (the low-vol anomaly).
      Has a natural sign, so it could be a ScoreBoard cell.
  H2  volatility COMPRESSION (the TradingView squeeze): predicts |move|, not move.
      A return-ranking board has no way to use an unsigned forecast, so H2 is tested
      against abs(fwd) as well -- if it only lives there, it belongs in the signal
      factory (entry timing / sizing), not in a cell.

Gates, fixed before looking at any number (learned the expensive way on the RS probe):
  - collinearity with the existing knot (MA-convergence) mechanism: squeeze may just be
    knot in different clothes
  - excess measured vs SAME-DAY SAME-DECILE peers, never vs the universe mean
  - n printed on every cell; no extreme cell gets quoted without its n
  - windows/thresholds swept here, not borrowed from elsewhere

Usage:
    python -m analysis._score_vol_probe [max_stocks]
"""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from analysis.indicators import sma
from db.connection import get_cursor  # noqa: F401  (kept: probe may need TAIEX later)

PANEL = "tmp/score_panel.parquet"
DEAD = "tmp/dead_fish_panel.parquet"

VOL_WINS = (20, 60)          # realized-vol estimation windows
PCTL_LOOKBACK = 252          # own-history window the current vol is ranked against
SQUEEZE_THRESH = (0.05, 0.10, 0.20)  # "vol is in the lowest X of its own history"

HORIZONS = ("fwd_5", "fwd_20", "fwd_60")
BANDS = {"D1-D3": (0, 2), "D4-D6": (3, 5), "D7-D8": (6, 7), "D9-D10": (8, 9)}

PANEL_COLS = ["date", "stock_id", "close", "total_long", "fwd_5", "fwd_20", "fwd_60"]


def _rolling_pctl(x: np.ndarray, win: int) -> np.ndarray:
    """Rank of the current value within its own trailing `win` observations, in [0,1]."""
    s = pd.Series(x)
    return s.rolling(win, min_periods=win // 2).apply(
        lambda w: (w[:-1] < w[-1]).mean(), raw=True
    ).to_numpy()


def _per_stock(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"].to_numpy(dtype=np.float64)
    out = {"date": df["date"].to_numpy(), "stock_id": df["stock_id"].to_numpy()}

    logret = np.diff(np.log(np.maximum(close, 1e-9)), prepend=np.nan)
    for w in VOL_WINS:
        vol = pd.Series(logret).rolling(w, min_periods=w // 2).std().to_numpy()
        out[f"vol{w}"] = vol                          # H1: level
        out[f"volp{w}"] = _rolling_pctl(vol, PCTL_LOOKBACK)  # H2: compression vs own history

    # Bollinger bandwidth -- the classic squeeze measure, and the one form of this idea
    # already half-present in the repo (analysis/close.py computes boll but nothing reads it).
    for w in VOL_WINS:
        ma = np.asarray(sma(close, w), dtype=np.float64)
        sd = pd.Series(close).rolling(w, min_periods=w // 2).std().to_numpy()
        with np.errstate(invalid="ignore", divide="ignore"):
            bw = np.where(ma > 0, 2 * sd / ma, np.nan)
        out[f"bw{w}"] = bw
        out[f"bwp{w}"] = _rolling_pctl(bw, PCTL_LOOKBACK)

    # knot proxy: how tightly the medium/long SMAs are bunched, normalised by price.
    # This is the existing mechanism a squeeze cell would be competing with.
    mas = np.vstack([np.asarray(sma(close, p), dtype=np.float64) for p in (21, 55, 144)])
    with np.errstate(invalid="ignore", divide="ignore"):
        out["knot_prox"] = np.where(
            close > 0, (mas.max(axis=0) - mas.min(axis=0)) / close, np.nan
        )
    return pd.DataFrame(out)


def _within(panel: pd.DataFrame, mask: pd.Series, h: str) -> tuple[float, int]:
    sub = panel.loc[mask & panel[h].notna(), [h, f"{h}_dec"]]
    if len(sub) < 50:
        return float("nan"), len(sub)
    return float((sub[h] - sub[f"{h}_dec"]).mean() * 100), len(sub)


def _fmt(ex: float, n: int) -> str:
    return f"    n/a (n={n:>4})" if np.isnan(ex) else f"{ex:>+7.2f} (n={n // 1000:>3}k)"


def main() -> None:
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0

    panel = pd.read_parquet(PANEL, columns=PANEL_COLS)
    panel["date"] = pd.to_datetime(panel["date"])
    if limit:
        keep = panel["stock_id"].drop_duplicates().head(limit)
        panel = panel[panel["stock_id"].isin(keep)]

    print("computing volatility columns ...", file=sys.stderr)
    vol = pd.concat(
        [_per_stock(g.sort_values("date")) for _, g in panel.groupby("stock_id", sort=False)],
        ignore_index=True,
    )
    panel = panel.merge(vol, on=["date", "stock_id"], how="left")

    dead = pd.read_parquet(DEAD)
    dead["date"] = pd.to_datetime(dead["date"])
    panel = panel.merge(dead, on=["date", "stock_id"], how="left")
    panel["is_dead_fish"] = panel["is_dead_fish"].fillna(False)
    panel = panel[~panel["is_dead_fish"]]
    print(f"~dead rows={len(panel):,}", file=sys.stderr)

    panel["year"] = panel["date"].dt.year
    panel["dec"] = panel.groupby("date")["total_long"].transform(
        lambda s: pd.qcut(s.rank(method="first"), 10, labels=False, duplicates="drop")
    )
    for h in HORIZONS:
        panel[f"{h}_dec"] = panel.groupby(["date", "dec"])[h].transform("mean")
        panel[f"abs_{h}"] = panel[h].abs()
        panel[f"abs_{h}_dec"] = panel.groupby(["date", "dec"])[f"abs_{h}"].transform("mean")

    # ---------------- Gate 0: is a squeeze just the knot? ----------------
    print("\n=== Gate 0  squeeze vs existing knot mechanism (daily cross-section Spearman) ===")
    print("knot_prox = (max-min of SMA 21/55/144) / close.  Low = MAs bunched = knot.")
    for a in ("bwp20", "bwp60", "volp20", "volp60", "bw20", "vol20"):
        rhos = []
        for _, g in panel.groupby("date"):
            sub = g[[a, "knot_prox"]].dropna()
            if len(sub) < 30:
                continue
            r = sub[a].rank().corr(sub["knot_prox"].rank())
            if not np.isnan(r):
                rhos.append(r)
        arr = np.array(rhos)
        if arr.size:
            print(f"  {a:>7} vs knot_prox: mean rho={arr.mean():+.4f}  "
                  f"median={np.median(arr):+.4f}  (n_days={len(arr)})")

    # ---------------- H1: volatility LEVEL, cross-sectional ----------------
    print("\n=== H1  volatility LEVEL: within-decile excess by cross-sectional vol quintile ===")
    print("Q1 = lowest vol that day.  Positive Q1 / negative Q5 => low-vol anomaly survives")
    print("the existing cells and could be a signed cell.")
    for v in ("vol20", "vol60"):
        panel[f"{v}_q"] = panel.groupby("date")[v].transform(
            lambda s: pd.qcut(s.rank(method="first"), 5, labels=False, duplicates="drop")
        )
        for h in HORIZONS:
            cells = []
            for q in range(5):
                ex, n = _within(panel, panel[f"{v}_q"].eq(q), h)
                cells.append(_fmt(ex, n))
            print(f"  {v:>6} {h:<7} Q1..Q5: " + "".join(cells))

    # ---------------- H2: COMPRESSION event, signed and unsigned ----------------
    print("\n=== H2  compression event: does it predict RETURN (signed) at all? ===")
    print("within-decile excess of fwd return, by score band.  n on every cell.")
    for base in ("bwp20", "bwp60", "volp20", "volp60"):
        for t in SQUEEZE_THRESH:
            m = panel[base].lt(t)
            for h in HORIZONS:
                cells = []
                for lo, hi in BANDS.values():
                    ex, n = _within(panel, m & panel["dec"].between(lo, hi), h)
                    cells.append(_fmt(ex, n))
                print(f"  {base:>6} <{t:.2f} {h:<7} trig={m.mean() * 100:5.2f}%  "
                      + "".join(cells))

    print("\n=== H2b  compression event: does it predict |RETURN| (unsigned)? ===")
    print("within-decile excess of abs(fwd).  This is what a squeeze is actually claiming.")
    for base in ("bwp20", "bwp60", "volp20", "volp60"):
        for t in SQUEEZE_THRESH:
            m = panel[base].lt(t)
            row = [f"  {base:>6} <{t:.2f} trig={m.mean() * 100:5.2f}%"]
            for h in HORIZONS:
                sub = panel.loc[m & panel[f"abs_{h}"].notna(),
                                [f"abs_{h}", f"abs_{h}_dec"]]
                if len(sub) < 50:
                    row.append(f"{h}: n/a")
                    continue
                ex = (sub[f"abs_{h}"] - sub[f"abs_{h}_dec"]).mean() * 100
                row.append(f"{h}={ex:+.3f}pp (n={len(sub):,})")
            print("  ".join(row))

    print("\n--- H2b per-year, bwp60 < 0.10, abs(fwd_20) ---")
    m = panel["bwp60"].lt(0.10)
    years = sorted(panel["year"].unique())
    for y in years:
        sub = panel.loc[m & panel["year"].eq(y) & panel["abs_fwd_20"].notna(),
                        ["abs_fwd_20", "abs_fwd_20_dec"]]
        if len(sub) < 50:
            print(f"  {y}:      n/a (n={len(sub)})")
            continue
        ex = (sub["abs_fwd_20"] - sub["abs_fwd_20_dec"]).mean() * 100
        print(f"  {y}: {ex:+7.3f}pp (n={len(sub):,})")

    # ---- H1 follow-up: is the high-vol premium a skew artifact? ----
    # Decile spread is a MEAN.  High-vol returns are right-skewed, so a vol cell could
    # flatter a mean-based metric while lowering win rate and deepening the worst loss
    # -- which would violate the project's stated goals outright.  Median and win-rate
    # answer that; if the monotonicity dies on medians, the mean effect is lottery skew.
    print("\n=== H1 follow-up  mean vs MEDIAN vs win-rate, by cross-sectional vol quintile ===")
    print("All figures are vs same-day same-decile peers.  If median/win-rate go flat or")
    print("reverse while the mean stays monotone, the mean is being carried by rare winners.")
    for v in ("vol20", "vol60"):
        for h in HORIZONS:
            panel[f"{h}_dec_med"] = panel.groupby(["date", "dec"])[h].transform("median")
            rows_mean, rows_med, rows_win = [], [], []
            for q in range(5):
                sub = panel.loc[
                    panel[f"{v}_q"].eq(q) & panel[h].notna(),
                    [h, f"{h}_dec", f"{h}_dec_med"],
                ]
                rows_mean.append(f"{(sub[h] - sub[f'{h}_dec']).mean() * 100:>+7.2f}")
                rows_med.append(f"{(sub[h] - sub[f'{h}_dec_med']).median() * 100:>+7.2f}")
                rows_win.append(f"{sub[h].gt(sub[f'{h}_dec_med']).mean() * 100:>6.1f}%")
            print(f"  {v} {h:<7} mean  Q1..Q5: " + "".join(rows_mean))
            print(f"  {v} {h:<7} median      : " + "".join(rows_med))
            print(f"  {v} {h:<7} beat-peer % : " + "".join(rows_win))

    print("\n=== H1 per-year: within-decile MEAN excess, Q5 (highest vol) and Q1 (lowest) ===")
    for v in ("vol60",):
        for h in ("fwd_20", "fwd_60"):
            for q, tag in ((0, "Q1"), (4, "Q5")):
                cells = []
                for y in years:
                    ex, n = _within(panel, panel[f"{v}_q"].eq(q) & panel["year"].eq(y), h)
                    cells.append("     n/a" if np.isnan(ex) else f"{ex:>+8.2f}")
                print(f"  {v} {h:<7} {tag}: " + "".join(cells))
    print("  years: " + "".join(f"{y:>8}" for y in years))


if __name__ == "__main__":
    main()
