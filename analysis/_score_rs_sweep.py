"""RS-breakdown window sweep — which N in "RS makes a new N-day low" carries damage?

The ScoreBoard cell idea died (see _score_rs_probe.py / memory).  What survived was a
conditional: stocks the score system rates highly *while* their RS is breaking down
underperform their same-score peers badly.  That belongs in the signal factory as a
long-side filter/defense, not in the additive ScoreBoard.

Before writing any such rule, the window has to be earned rather than borrowed: 123 was
inherited from the *price* Donchian research and there is no reason the RS series shares
its optimum.  This sweeps N and reports, per decile band of the existing score:

  - within-decile excess (vs same-day same-decile peers) at H=20 / H=60
  - trigger rate and cohort size
  - per-year sign consistency for the high-score band

Pick a plateau, not a lone peak.  A monotone curve means check the time distribution.

Usage:
    python -m analysis._score_rs_sweep [max_stocks]
"""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from analysis.donchian import calculate_donchian
from db.connection import get_cursor

PANEL = "tmp/score_panel.parquet"
DEAD = "tmp/dead_fish_panel.parquet"

WINDOWS = (34, 55, 89, 123, 144, 199, 233, 322, 377)
HORIZONS = ("fwd_20", "fwd_60")

# Decile index is 0-based: 0..2 = D1-D3 (bottom), 7..9 = D8-D10 (what the signal
# factory actually buys from).
BANDS = {"D1-D3": (0, 2), "D4-D6": (3, 5), "D7-D8": (6, 7), "D9-D10": (8, 9)}

PANEL_COLS = ["date", "stock_id", "close", "total_long", "fwd_20", "fwd_60"]


def _load_taiex() -> pd.Series:
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT trade_date, close_price FROM tw.index_prices "
            "WHERE index_id = 'TAIEX' AND close_price IS NOT NULL "
            "ORDER BY trade_date"
        )
        rows = cur.fetchall()
    return pd.Series(
        [float(r["close_price"]) for r in rows],
        index=pd.to_datetime([r["trade_date"] for r in rows]),
    )


def _per_stock(df: pd.DataFrame) -> pd.DataFrame:
    rs = (df["close"].to_numpy(dtype=np.float64)
          / df["taiex"].to_numpy(dtype=np.float64)).astype(np.float32)
    out = {"date": df["date"].to_numpy(), "stock_id": df["stock_id"].to_numpy()}
    for n in WINDOWS:
        res = calculate_donchian(rs, rs, rs, entry_length=n, exit_length=max(2, n // 2))
        out[f"lo{n}"] = np.asarray(res.entry_short, dtype=bool)
    return pd.DataFrame(out)


def _within(panel: pd.DataFrame, mask: pd.Series, h: str) -> tuple[float, int]:
    sub = panel.loc[mask & panel[h].notna(), [h, f"{h}_dec"]]
    if len(sub) < 50:
        return float("nan"), len(sub)
    return float((sub[h] - sub[f"{h}_dec"]).mean() * 100), len(sub)


def main() -> None:
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0

    panel = pd.read_parquet(PANEL, columns=PANEL_COLS)
    panel["date"] = pd.to_datetime(panel["date"])
    if limit:
        keep = panel["stock_id"].drop_duplicates().head(limit)
        panel = panel[panel["stock_id"].isin(keep)]
    panel["taiex"] = panel["date"].map(_load_taiex())
    panel = panel[panel["taiex"].notna()]

    print(f"computing RS Donchian for {len(WINDOWS)} windows ...", file=sys.stderr)
    rs = pd.concat(
        [_per_stock(g.sort_values("date")) for _, g in panel.groupby("stock_id", sort=False)],
        ignore_index=True,
    )
    panel = panel.merge(rs, on=["date", "stock_id"], how="left")

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

    print("\n=== RS new-N-day-low: within-decile excess by score band (pp) ===")
    print("Negative = the RS-breakdown stock underperforms peers the score rated the same.")
    for h in HORIZONS:
        print(f"\n--- {h} ---")
        head = f"  {'N':>4} {'trig%':>6} " + "".join(f"{b:>18}" for b in BANDS)
        print(head)
        for n in WINDOWS:
            m = panel[f"lo{n}"].fillna(False)
            cells = []
            for lo, hi in BANDS.values():
                mb = m & panel["dec"].between(lo, hi)
                ex, cnt = _within(panel, mb, h)
                cells.append("               n/a" if np.isnan(ex) else f"{ex:>+9.2f} (n={cnt // 1000:>3}k)")
            print(f"  {n:>4} {m.mean() * 100:>5.2f}% " + "".join(cells))

    print("\n=== per-year within-decile excess, high-score band D7-D10 ===")
    hi_band = panel["dec"].between(6, 9)
    years = sorted(panel["year"].unique())
    for h in HORIZONS:
        print(f"\n--- {h} ---")
        print(f"  {'N':>4}" + "".join(f"{y:>8}" for y in years) + f"{'neg/tot':>10}")
        for n in WINDOWS:
            m = panel[f"lo{n}"].fillna(False) & hi_band
            cells, neg, tot = [], 0, 0
            for y in years:
                ex, cnt = _within(panel, m & panel["year"].eq(y), h)
                if np.isnan(ex):
                    cells.append("     n/a")
                    continue
                cells.append(f"{ex:>+8.2f}")
                tot += 1
                neg += ex < 0
            print(f"  {n:>4}" + "".join(cells) + f"{neg:>6}/{tot}")


if __name__ == "__main__":
    main()
