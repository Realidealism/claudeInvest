"""Calibrate MU/SIGMA against the production denominator, and verify the cell reproduces
the simulated trade-off before any of it is hardcoded into score.py.

The sims divided by the median volatility of the ~dead-filtered panel.  Production divides
by tw.market_vol, whose median is taken over the full active universe -- a different
constant.  A per-day constant cannot change the cross-sectional ORDER, so the ranking is
untouched, but it does shift MU, and MU/SIGMA are about to be frozen into score.py.  So
they get re-measured here, against exactly what production will divide by, and the k=15
trade-off is re-checked on that basis.

If the numbers here do not match _score_vol_neutral_sim3 at k=15, the cell being built is
not the cell that was simulated, and the constants must not be shipped.

Usage:
    python -m analysis._score_vol_calibrate
"""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from analysis.volatility import calculate_parkinson_vol
from db.connection import get_cursor

PANEL = "tmp/score_panel.parquet"
DEAD = "tmp/dead_fish_panel.parquet"
K = 15.0
CLIP_SIGMA = 2.5


def main() -> None:
    panel = pd.read_parquet(
        PANEL, columns=["date", "stock_id", "total_long", "fwd_20", "fwd_60"]
    )
    panel["date"] = pd.to_datetime(panel["date"])

    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT p.stock_id, p.trade_date, p.high_price, p.low_price
            FROM tw.daily_prices p
            JOIN tw.stocks s ON s.stock_id = p.stock_id
            WHERE s.is_active = TRUE AND s.security_type = 'STOCK'
              AND s.market IN ('TWSE', 'TPEx')
              AND p.high_price IS NOT NULL AND p.low_price IS NOT NULL
              AND p.low_price > 0
            ORDER BY p.stock_id, p.trade_date
        """)
        px = pd.DataFrame(cur.fetchall())
        cur.execute("SELECT trade_date, median_vol FROM tw.market_vol ORDER BY trade_date")
        mv = pd.DataFrame(cur.fetchall())

    px["trade_date"] = pd.to_datetime(px["trade_date"])
    for c in ("high_price", "low_price"):
        px[c] = pd.to_numeric(px[c], errors="coerce")
    parts = []
    for _, g in px.groupby("stock_id", sort=False):
        g = g.sort_values("trade_date")
        parts.append(pd.DataFrame({
            "date": g["trade_date"].to_numpy(),
            "stock_id": g["stock_id"].to_numpy(),
            "vol": calculate_parkinson_vol(
                g["high_price"].to_numpy(), g["low_price"].to_numpy()
            ),
        }))
    vol = pd.concat(parts, ignore_index=True)

    mv["date"] = pd.to_datetime(mv["trade_date"])
    mv["median_vol"] = mv["median_vol"].astype(float)

    panel = panel.merge(vol, on=["date", "stock_id"], how="left")
    panel = panel.merge(mv[["date", "median_vol"]], on="date", how="left")

    dead = pd.read_parquet(DEAD)
    dead["date"] = pd.to_datetime(dead["date"])
    panel = panel.merge(dead, on=["date", "stock_id"], how="left")
    panel["is_dead_fish"] = panel["is_dead_fish"].fillna(False)
    panel = panel[~panel["is_dead_fish"]]

    usable = panel["vol"].gt(0) & panel["median_vol"].gt(0) & np.isfinite(panel["vol"])
    print(f"~dead rows={len(panel):,}  with valid vol={usable.sum():,} "
          f"({usable.mean() * 100:.1f}%)", file=sys.stderr)
    panel = panel[usable]
    panel["year"] = panel["date"].dt.year

    lrv = np.log(panel["vol"] / panel["median_vol"])
    MU, SIGMA = float(lrv.mean()), float(lrv.std())
    yr = lrv.groupby(panel["year"]).agg(["mean", "std"])

    print("\n=== constants to freeze into score.py (production denominator) ===")
    print(f"  MU    = {MU:+.4f}")
    print(f"  SIGMA = {SIGMA:.4f}")
    print("  per-year MU : " + " ".join(f"{v:+.2f}" for v in yr["mean"]))
    print("  per-year SIG: " + " ".join(f"{v:.2f}" for v in yr["std"]))
    print(f"  MU drift / SIGMA = {(yr['mean'].max() - yr['mean'].min()) / SIGMA:.2f}"
          "   (<0.2 = safe to hardcode)")

    z = -(lrv - MU) / SIGMA
    pts = (z * K).clip(-CLIP_SIGMA * K, CLIP_SIGMA * K)
    print(f"\n  pts: std={pts.std():.2f}  min={pts.min():.1f}  max={pts.max():.1f}  "
          f"clipped={100 * (pts.abs() >= CLIP_SIGMA * K - 1e-9).mean():.2f}%")

    def _dec(s):
        return pd.qcut(s.rank(method="first"), 10, labels=False, duplicates="drop")

    print(f"\n=== verify: does k={K:.0f} reproduce the simulated trade-off? ===")
    for h in ("fwd_20", "fwd_60"):
        panel[f"{h}_mm"] = panel.groupby("date")[h].transform("median")
        print(f"\n--- {h} ---")
        print(f"  {'k':>4}{'mean spread':>13}{'median spread':>15}{'D10 win%':>10}"
              f"{'D10 worst5%':>13}")
        for k in (0.0, K):
            p = (z * k).clip(-CLIP_SIGMA * k, CLIP_SIGMA * k) if k else 0.0
            dec = (panel["total_long"] + p).groupby(panel["date"]).transform(_dec)
            ok = panel[h].notna()
            d10 = panel.loc[dec.eq(9) & ok, h]
            d1 = panel.loc[dec.eq(0) & ok, h]
            win = (d10 > panel.loc[dec.eq(9) & ok, f"{h}_mm"]).mean() * 100
            print(f"  {k:>4.0f}{(d10.mean() - d1.mean()) * 100:>12.3f}%"
                  f"{(d10.median() - d1.median()) * 100:>14.3f}%{win:>9.1f}%"
                  f"{d10.quantile(0.05) * 100:>12.2f}%")

    print("\n=== per-year median decile spread (the k=15 anchor: every year positive) ===")
    years = sorted(panel["year"].unique())
    print(f"  {'k':>4}" + "".join(f"{y:>8}" for y in years))
    for k in (0.0, K):
        p = (z * k).clip(-CLIP_SIGMA * k, CLIP_SIGMA * k) if k else 0.0
        dec = (panel["total_long"] + p).groupby(panel["date"]).transform(_dec)
        cells = []
        for y in years:
            m = panel["year"].eq(y) & panel["fwd_60"].notna()
            d10 = panel.loc[m & dec.eq(9), "fwd_60"]
            d1 = panel.loc[m & dec.eq(0), "fwd_60"]
            cells.append("     n/a" if len(d10) < 100 or len(d1) < 100
                         else f"{(d10.median() - d1.median()) * 100:>+8.2f}")
        print(f"  {k:>4.0f}" + "".join(cells))


if __name__ == "__main__":
    main()
