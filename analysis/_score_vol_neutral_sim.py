"""Simulate a volatility-neutralised ScoreBoard on the frozen panel — zero rebuild.

The audit (_score_skew_audit.py) showed the board leans high-vol: Spearman(total_long,
vol) = +0.23 on 94.5% of days, D10 sits at the 62nd volatility percentile vs D1 at the
37th.  And within a decile, high vol buys a positive MEAN but a negative MEDIAN and a
45% win rate.  So part of the headline decile spread is variance compensation collected
in the mean -- paid for in win rate and depth of loss, i.e. two of the three stated
project goals.

This prices that trade.  A synthetic continuous cell hands points to the low-vol side of
each day's cross-section (same shape as the 距離 cells: a centred, capped, continuous
score), and the board is re-ranked with it at several caps.  For each cap we report what
gets worse (mean spread) and what gets better (median spread, win rate, tail loss).

Nothing here writes the panel or score.py -- it is pure column arithmetic on the frozen
panel, the same technique _score_reweight.py uses.

Usage:
    python -m analysis._score_vol_neutral_sim [max_stocks]
"""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd

PANEL = "tmp/score_panel.parquet"
DEAD = "tmp/dead_fish_panel.parquet"

VOL_WIN = 60
CAPS = (0, 5, 10, 15, 20, 30)   # 0 = current production board
HORIZONS = ("fwd_20", "fwd_60")


def _decile(s: pd.Series) -> pd.Series:
    return pd.qcut(s.rank(method="first"), 10, labels=False, duplicates="drop")


def main() -> None:
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0

    panel = pd.read_parquet(
        PANEL, columns=["date", "stock_id", "close", "total_long", "fwd_20", "fwd_60"]
    )
    panel["date"] = pd.to_datetime(panel["date"])
    if limit:
        keep = panel["stock_id"].drop_duplicates().head(limit)
        panel = panel[panel["stock_id"].isin(keep)]

    print("computing realized vol ...", file=sys.stderr)
    parts = []
    for _, g in panel.groupby("stock_id", sort=False):
        g = g.sort_values("date")
        c = g["close"].to_numpy(dtype=np.float64)
        lr = np.diff(np.log(np.maximum(c, 1e-9)), prepend=np.nan)
        parts.append(pd.DataFrame({
            "date": g["date"].to_numpy(),
            "stock_id": g["stock_id"].to_numpy(),
            "vol": pd.Series(lr).rolling(VOL_WIN, min_periods=VOL_WIN // 2).std().to_numpy(),
        }))
    panel = panel.merge(pd.concat(parts, ignore_index=True), on=["date", "stock_id"], how="left")

    dead = pd.read_parquet(DEAD)
    dead["date"] = pd.to_datetime(dead["date"])
    panel = panel.merge(dead, on=["date", "stock_id"], how="left")
    panel["is_dead_fish"] = panel["is_dead_fish"].fillna(False)
    panel = panel[~panel["is_dead_fish"] & panel["vol"].notna()]
    print(f"~dead rows={len(panel):,}", file=sys.stderr)

    panel["year"] = panel["date"].dt.year
    # Low vol gets +1, high vol gets -1, linear in the day's volatility rank.
    panel["volr"] = panel.groupby("date")["vol"].rank(pct=True)
    panel["volpts_unit"] = (0.5 - panel["volr"]) * 2.0
    panel["mkt_med"] = panel.groupby("date")[HORIZONS[0]].transform("median")
    for h in HORIZONS:
        panel[f"{h}_mktmed"] = panel.groupby("date")[h].transform("median")

    print("\n=== volatility-neutral board: what you give up vs what you buy ===")
    print("cap = points the synthetic low-vol cell can add/subtract (距離 cells use ±10).")
    print("mean spread = the metric every adoption decision has ever been made on.")
    print("median spread / D10 win-rate / D10 worst-5% = what the three goals actually care about.\n")

    rows = []
    for cap in CAPS:
        p = panel
        score = p["total_long"] + cap * p["volpts_unit"]
        dec = score.groupby(p["date"]).transform(_decile)
        top, bot = dec.eq(9), dec.eq(0)
        rec: dict[str, object] = {"cap": cap}
        for h in HORIZONS:
            ok = p[h].notna()
            d10 = p.loc[top & ok, h]
            d1 = p.loc[bot & ok, h]
            rec[f"{h}_mean"] = (d10.mean() - d1.mean()) * 100
            rec[f"{h}_med"] = (d10.median() - d1.median()) * 100
            rec[f"{h}_win"] = (
                p.loc[top & ok, h] > p.loc[top & ok, f"{h}_mktmed"]
            ).mean() * 100
            rec[f"{h}_p05"] = d10.quantile(0.05) * 100
        rec["d10_volr"] = p.loc[top, "volr"].mean()
        rec["d1_volr"] = p.loc[bot, "volr"].mean()
        rows.append(rec)

    df = pd.DataFrame(rows)
    for h in HORIZONS:
        print(f"--- {h} ---")
        print(f"  {'cap':>4} {'mean spread':>12} {'median spread':>14} "
              f"{'D10 win%':>9} {'D10 worst-5%':>13} {'D10 vol pctl':>13}")
        for _, r in df.iterrows():
            print(f"  {int(r['cap']):>4} {r[f'{h}_mean']:>11.3f}% {r[f'{h}_med']:>13.3f}% "
                  f"{r[f'{h}_win']:>8.1f}% {r[f'{h}_p05']:>12.2f}% {r['d10_volr']:>12.3f}")
        print()

    print("=== per-year mean spread (fwd_60), to check the trade is uniform ===")
    years = sorted(panel["year"].unique())
    print(f"  {'cap':>4}" + "".join(f"{y:>8}" for y in years))
    for cap in CAPS:
        score = panel["total_long"] + cap * panel["volpts_unit"]
        dec = score.groupby(panel["date"]).transform(_decile)
        cells = []
        for y in years:
            m = panel["year"].eq(y) & panel["fwd_60"].notna()
            d10 = panel.loc[m & dec.eq(9), "fwd_60"]
            d1 = panel.loc[m & dec.eq(0), "fwd_60"]
            if len(d10) < 100 or len(d1) < 100:
                cells.append("     n/a")
            else:
                cells.append(f"{(d10.mean() - d1.mean()) * 100:>+8.2f}")
        print(f"  {cap:>4}" + "".join(cells))


if __name__ == "__main__":
    main()
