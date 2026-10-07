"""Re-simulate the low-vol cell using the formula that can actually be built.

Sim v1 scored the cell as a linear function of the day's volatility RANK, which is not
implementable: ScoreBoard.evaluate(data, i) sees one stock at a time (score.py:223) and
production fans it out across processes (daily_snapshot.py:485), so no cell can see its
peers.  What IS implementable, via the same per-day global-series hook the breadth cell
uses (_load_breadth_trends, score.py:667), is

    relvol = vol60(stock) / vol60(market)
    pts    = clip(-(log(relvol) - MU) / SIGMA * 3, -cap, +cap)

Dividing by a same-day constant leaves the cross-sectional ORDER identical to the rank
version, but the POINT DISTRIBUTION differs (z-shaped, not uniform) -- and the board adds
points up, so distribution matters.  This reruns the trade-off with the real formula, and
checks whether fixed MU/SIGMA constants stay valid across years.

Two candidate denominators, both a per-day constant (so both preserve rank identically):
  taiex  -- vol of the TAIEX index itself; no new table needed
  median -- cross-sectional median stock vol that day; tracks dispersion better but needs
            a new daily series to be persisted for production

Usage:
    python -m analysis._score_vol_neutral_sim2 [max_stocks]
"""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from db.connection import get_cursor

PANEL = "tmp/score_panel.parquet"
DEAD = "tmp/dead_fish_panel.parquet"

VOL_WIN = 60
CAPS = (0, 5, 10, 15, 20, 30)
HORIZONS = ("fwd_20", "fwd_60")


def _decile(s: pd.Series) -> pd.Series:
    return pd.qcut(s.rank(method="first"), 10, labels=False, duplicates="drop")


def _taiex_vol() -> pd.Series:
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT trade_date, close_price FROM tw.index_prices "
            "WHERE index_id = 'TAIEX' AND close_price IS NOT NULL ORDER BY trade_date"
        )
        rows = cur.fetchall()
    s = pd.Series(
        [float(r["close_price"]) for r in rows],
        index=pd.to_datetime([r["trade_date"] for r in rows]),
    )
    lr = np.log(s).diff()
    return lr.rolling(VOL_WIN, min_periods=VOL_WIN // 2).std()


def main() -> None:
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0

    panel = pd.read_parquet(
        PANEL, columns=["date", "stock_id", "close", "total_long", "fwd_20", "fwd_60"]
    )
    panel["date"] = pd.to_datetime(panel["date"])
    if limit:
        keep = panel["stock_id"].drop_duplicates().head(limit)
        panel = panel[panel["stock_id"].isin(keep)]

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
    panel = panel[~panel["is_dead_fish"] & panel["vol"].gt(0)]
    panel["year"] = panel["date"].dt.year
    print(f"~dead rows={len(panel):,}", file=sys.stderr)

    panel["mkt_taiex"] = panel["date"].map(_taiex_vol())
    panel["mkt_median"] = panel.groupby("date")["vol"].transform("median")

    for h in HORIZONS:
        panel[f"{h}_mktmed"] = panel.groupby("date")[h].transform("median")

    for den in ("taiex", "median"):
        d = panel[f"mkt_{den}"]
        panel[f"lrv_{den}"] = np.log(panel["vol"] / d)

    print("\n=== are fixed MU/SIGMA constants stable across years? ===")
    print("(if the cross-sectional spread of log rel-vol drifts, hardcoded constants rot)")
    for den in ("taiex", "median"):
        col = f"lrv_{den}"
        g = panel.groupby("year")[col].agg(["mean", "std"])
        print(f"  {den:>6} MU per year : " + " ".join(f"{v:+.2f}" for v in g["mean"]))
        print(f"  {den:>6} SIGMA per yr: " + " ".join(f"{v:.2f}" for v in g["std"]))
        print(f"  {den:>6} pooled MU={panel[col].mean():+.4f}  SIGMA={panel[col].std():.4f}")

    # The strength knob is the MULTIPLIER k, not the cap: z has unit sigma, so clip(z*3,
    # +-cap) saturates the moment cap exceeds ~3 sigma and every larger cap is identical.
    # Sweep k, clip at 2.5 sigma, and print std(pts) so this is comparable with sim v1
    # (whose uniform rank points had std = 0.577 * cap).
    print("\n=== trade-off with the IMPLEMENTABLE formula, sweeping the multiplier k ===")
    print("pts = clip(z * k, +-2.5k)   z = -(log(vol/mkt_vol) - MU)/SIGMA   (low vol => +)")
    panel["volr"] = panel.groupby("date")["vol"].rank(pct=True)
    for den in ("taiex", "median"):
        mu = float(panel[f"lrv_{den}"].mean())
        sig = float(panel[f"lrv_{den}"].std())
        z = -(panel[f"lrv_{den}"] - mu) / sig
        for h in HORIZONS:
            print(f"\n--- denominator={den}  {h} ---")
            print(f"  {'k':>4} {'std(pts)':>9} {'mean spread':>12} {'median spread':>14} "
                  f"{'D10 win%':>9} {'D10 worst-5%':>13} {'D10 vol pctl':>13}")
            for k in (0, 3, 6, 9, 12, 18):
                pts = (z * k).clip(-2.5 * k, 2.5 * k) if k else pd.Series(0.0, index=z.index)
                score = panel["total_long"] + pts
                dec = pd.Series(score).groupby(panel["date"]).transform(_decile)
                ok = panel[h].notna()
                d10 = panel.loc[dec.eq(9) & ok, h]
                d1 = panel.loc[dec.eq(0) & ok, h]
                win = (panel.loc[dec.eq(9) & ok, h]
                       > panel.loc[dec.eq(9) & ok, f"{h}_mktmed"]).mean() * 100
                print(f"  {k:>4} {pts.std():>9.2f} {(d10.mean() - d1.mean()) * 100:>11.3f}% "
                      f"{(d10.median() - d1.median()) * 100:>13.3f}% {win:>8.1f}% "
                      f"{d10.quantile(0.05) * 100:>12.2f}% "
                      f"{panel.loc[dec.eq(9), 'volr'].mean():>12.3f}")


if __name__ == "__main__":
    main()
