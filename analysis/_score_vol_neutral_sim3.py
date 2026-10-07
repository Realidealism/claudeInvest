"""Final calibration of the low-volatility cell, on the window the sweep actually earned.

k=12 was derived on cc60 (a borrowed window).  The sweep picked Parkinson-233 instead --
peak of a real plateau (233/377/610 within 0.13pp), better than close-to-close at every
window, MU drift 0.10 sigma.  So k has to be re-derived here; nothing carries over.

Stopping rule for k (non-arbitrary): the smallest k that pulls the top decile's volatility
percentile back to ~0.50.  Below that the board is still variance-tilted; above it, the
board is actively betting on low volatility, which is a different wager than neutralising.

Also prices the liquidity side-effect the sweep exposed: low-vol names trade thinner
(Spearman(vol, log turnover) = +0.20), so the cell drags the top decile toward less liquid
stocks, a cost no backtest without slippage will show.

Usage:
    python -m analysis._score_vol_neutral_sim3 [max_stocks]
"""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from db.connection import get_cursor

PANEL = "tmp/score_panel.parquet"
DEAD = "tmp/dead_fish_panel.parquet"

VOL_WIN = 233                       # earned by _score_vol_window_sweep, not borrowed
KS = (0, 10, 11, 12, 13, 14, 15, 16)
HORIZONS = ("fwd_20", "fwd_60")
_PARK_C = 1.0 / (4.0 * np.log(2.0))


def _decile(s: pd.Series) -> pd.Series:
    return pd.qcut(s.rank(method="first"), 10, labels=False, duplicates="drop")


def main() -> None:
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0

    panel = pd.read_parquet(
        PANEL, columns=["date", "stock_id", "total_long", "fwd_20", "fwd_60"]
    )
    panel["date"] = pd.to_datetime(panel["date"])
    if limit:
        keep = panel["stock_id"].drop_duplicates().head(limit)
        panel = panel[panel["stock_id"].isin(keep)]

    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT stock_id, trade_date, high_price, low_price, turnover "
            "FROM tw.daily_prices "
            "WHERE high_price IS NOT NULL AND low_price IS NOT NULL AND low_price > 0 "
            "ORDER BY stock_id, trade_date"
        )
        rows = cur.fetchall()
    o = pd.DataFrame(rows).rename(columns={"trade_date": "date"})
    o["date"] = pd.to_datetime(o["date"])
    for c in ("high_price", "low_price", "turnover"):
        o[c] = pd.to_numeric(o[c], errors="coerce")
    o["hl2"] = np.log(o["high_price"] / o["low_price"]) ** 2
    o["vol"] = np.sqrt(
        _PARK_C
        * o.groupby("stock_id", sort=False)["hl2"].transform(
            lambda s: s.rolling(VOL_WIN, min_periods=VOL_WIN // 2).mean()
        )
    )
    panel = panel.merge(o[["date", "stock_id", "vol", "turnover"]],
                        on=["date", "stock_id"], how="left")

    dead = pd.read_parquet(DEAD)
    dead["date"] = pd.to_datetime(dead["date"])
    panel = panel.merge(dead, on=["date", "stock_id"], how="left")
    panel["is_dead_fish"] = panel["is_dead_fish"].fillna(False)
    panel = panel[~panel["is_dead_fish"] & panel["vol"].gt(0)]
    panel["year"] = panel["date"].dt.year
    print(f"~dead rows={len(panel):,}", file=sys.stderr)

    panel["volr"] = panel.groupby("date")["vol"].rank(pct=True)
    panel["tnr"] = panel.groupby("date")["turnover"].rank(pct=True)
    lrv = np.log(panel["vol"] / panel.groupby("date")["vol"].transform("median"))
    MU, SIGMA = float(lrv.mean()), float(lrv.std())
    print(f"MU={MU:+.4f}  SIGMA={SIGMA:.4f}", file=sys.stderr)
    z = -(lrv - MU) / SIGMA

    for h in HORIZONS:
        panel[f"{h}_mktmed"] = panel.groupby("date")[h].transform("median")

    print(f"\n=== Parkinson-{VOL_WIN} low-vol cell: calibrating k ===")
    print(f"pts = clip(z * k, +-2.5k),  z = -(log(vol/median_vol) - {MU:+.4f}) / {SIGMA:.4f}")
    print("stopping rule: smallest k with D10 vol-pctl ~ 0.50 (neutral, not reversed)\n")

    for h in HORIZONS:
        print(f"--- {h} ---")
        print(f"  {'k':>4}{'mean spread':>13}{'median spread':>15}{'D10 win%':>10}"
              f"{'D10 worst5%':>13}{'D10 vol-pctl':>14}{'D10 turnover-pctl':>19}")
        for k in KS:
            pts = (z * k).clip(-2.5 * k, 2.5 * k) if k else pd.Series(0.0, index=z.index)
            dec = (panel["total_long"] + pts).groupby(panel["date"]).transform(_decile)
            ok = panel[h].notna()
            d10 = panel.loc[dec.eq(9) & ok, h]
            d1 = panel.loc[dec.eq(0) & ok, h]
            win = (d10 > panel.loc[dec.eq(9) & ok, f"{h}_mktmed"]).mean() * 100
            print(f"  {k:>4}{(d10.mean() - d1.mean()) * 100:>12.3f}%"
                  f"{(d10.median() - d1.median()) * 100:>14.3f}%{win:>9.1f}%"
                  f"{d10.quantile(0.05) * 100:>12.2f}%"
                  f"{panel.loc[dec.eq(9), 'volr'].mean():>14.3f}"
                  f"{panel.loc[dec.eq(9), 'tnr'].mean():>19.3f}")
        print()

    print("=== per-year, at the neutralising k (fwd_60) ===")
    years = sorted(panel["year"].unique())
    print(f"  {'k':>4}{'metric':>8}" + "".join(f"{y:>8}" for y in years))
    for k in (0, 12, 13, 14, 15):
        pts = (z * k).clip(-2.5 * k, 2.5 * k) if k else pd.Series(0.0, index=z.index)
        dec = (panel["total_long"] + pts).groupby(panel["date"]).transform(_decile)
        for metric in ("mean", "median"):
            cells = []
            for y in years:
                m = panel["year"].eq(y) & panel["fwd_60"].notna()
                d10 = panel.loc[m & dec.eq(9), "fwd_60"]
                d1 = panel.loc[m & dec.eq(0), "fwd_60"]
                if len(d10) < 100 or len(d1) < 100:
                    cells.append("     n/a")
                elif metric == "mean":
                    cells.append(f"{(d10.mean() - d1.mean()) * 100:>+8.2f}")
                else:
                    cells.append(f"{(d10.median() - d1.median()) * 100:>+8.2f}")
            print(f"  {k:>4}{metric:>8}" + "".join(cells))


if __name__ == "__main__":
    main()
