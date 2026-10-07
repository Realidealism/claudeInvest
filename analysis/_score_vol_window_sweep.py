"""Earn the volatility window instead of borrowing it.

VOL_WIN=60 was picked because the board's headline horizon is H=60 and 60 is a round
number -- exactly the "borrowed parameter" mistake this project just wrote into its own
rules.  This sweeps Fibonacci windows (the family the SMA periods already use) and two
estimators, and scores each on what the low-vol cell is actually meant to buy:

  median-spread  = (Q1 median excess) - (Q5 median excess)   <- the reliable effect
  win-spread     = (Q1 beat%) - (Q5 beat%)                   <- the win-rate goal
  mean-spread    = (Q5 mean excess) - (Q1 mean excess)       <- the right-tail effect,
                                                                shown only for contrast

All excesses are vs same-day same-total_long-decile peers, so they measure what the cell
would add on top of the 21 cells already in the board.  MU/SIGMA drift across years is
reported too: a window whose constants rot cannot be hardcoded.

Estimators:
  cc   close-to-close rolling std of log returns (what the sims used)
  park Parkinson high-low range estimator -- strictly more efficient per observation,
       and immune to the close-to-close gap noise that dominates thin Taiwan names

Usage:
    python -m analysis._score_vol_window_sweep [max_stocks]
"""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from db.connection import get_cursor

PANEL = "tmp/score_panel.parquet"
DEAD = "tmp/dead_fish_panel.parquet"

# Fibonacci, same family as the SMA periods (3,5,8,13,21,34,55,89,144,233,377).
# 20/60 kept as the incumbents so the sweep can be read against what the sims assumed.
WINDOWS = (13, 21, 34, 55, 89, 144, 233, 377, 610)
HORIZONS = ("fwd_20", "fwd_60")
_PARK_C = 1.0 / (4.0 * np.log(2.0))


def _load_ohlc() -> pd.DataFrame:
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT stock_id, trade_date, high_price, low_price, close_price, turnover "
            "FROM tw.daily_prices "
            "WHERE close_price IS NOT NULL AND high_price IS NOT NULL "
            "AND low_price IS NOT NULL AND low_price > 0 "
            "ORDER BY stock_id, trade_date"
        )
        rows = cur.fetchall()
    df = pd.DataFrame(rows)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    for c in ("high_price", "low_price", "close_price", "turnover"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.rename(columns={"trade_date": "date"})


def main() -> None:
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0

    panel = pd.read_parquet(
        PANEL, columns=["date", "stock_id", "total_long", "fwd_20", "fwd_60"]
    )
    panel["date"] = pd.to_datetime(panel["date"])
    if limit:
        keep = panel["stock_id"].drop_duplicates().head(limit)
        panel = panel[panel["stock_id"].isin(keep)]

    print("loading OHLC ...", file=sys.stderr)
    ohlc = _load_ohlc()
    ohlc = ohlc[ohlc["stock_id"].isin(panel["stock_id"].unique())]
    print(f"ohlc rows={len(ohlc):,}", file=sys.stderr)

    ohlc = ohlc.sort_values(["stock_id", "date"])
    g = ohlc.groupby("stock_id", sort=False)
    ohlc["lr"] = np.log(ohlc["close_price"]) - np.log(g["close_price"].shift(1))
    ohlc["hl2"] = np.log(ohlc["high_price"] / ohlc["low_price"]) ** 2

    print("computing vol columns ...", file=sys.stderr)
    g = ohlc.groupby("stock_id", sort=False)
    for w in WINDOWS:
        ohlc[f"cc{w}"] = g["lr"].transform(
            lambda s, w=w: s.rolling(w, min_periods=w // 2).std()
        )
        ohlc[f"park{w}"] = np.sqrt(
            _PARK_C
            * g["hl2"].transform(lambda s, w=w: s.rolling(w, min_periods=w // 2).mean())
        )

    cols = ["date", "stock_id", "turnover"] + [
        f"{e}{w}" for w in WINDOWS for e in ("cc", "park")
    ]
    panel = panel.merge(ohlc[cols], on=["date", "stock_id"], how="left")

    dead = pd.read_parquet(DEAD)
    dead["date"] = pd.to_datetime(dead["date"])
    panel = panel.merge(dead, on=["date", "stock_id"], how="left")
    panel["is_dead_fish"] = panel["is_dead_fish"].fillna(False)
    panel = panel[~panel["is_dead_fish"]]
    panel["year"] = panel["date"].dt.year
    print(f"~dead rows={len(panel):,}", file=sys.stderr)

    panel["dec"] = panel.groupby("date")["total_long"].transform(
        lambda s: pd.qcut(s.rank(method="first"), 10, labels=False, duplicates="drop")
    )
    for h in HORIZONS:
        panel[f"{h}_dm"] = panel.groupby(["date", "dec"])[h].transform("mean")
        panel[f"{h}_dmed"] = panel.groupby(["date", "dec"])[h].transform("median")

    for h in HORIZONS:
        print(f"\n=== {h}: which volatility window separates the RELIABLE effect best? ===")
        print("  median-spread / win-spread: bigger = low-vol beats high-vol more reliably")
        print("  mean-spread: the right-tail effect, shown only for contrast (bigger = more skew)")
        print(f"  {'est':>5}{'win':>5}{'med-spread':>12}{'win-spread':>12}"
              f"{'mean-spread':>13}{'MU drift/SIG':>14}{'cover%':>8}")
        for est in ("cc", "park"):
            for w in WINDOWS:
                col = f"{est}{w}"
                sub = panel[panel[col].gt(0) & panel[h].notna()].copy()
                if len(sub) < 10000:
                    continue
                q = sub.groupby("date")[col].transform(
                    lambda s: pd.qcut(s.rank(method="first"), 5, labels=False, duplicates="drop")
                )
                lo, hi = q.eq(0), q.eq(4)
                med_q1 = (sub.loc[lo, h] - sub.loc[lo, f"{h}_dmed"]).median() * 100
                med_q5 = (sub.loc[hi, h] - sub.loc[hi, f"{h}_dmed"]).median() * 100
                win_q1 = (sub.loc[lo, h] > sub.loc[lo, f"{h}_dmed"]).mean() * 100
                win_q5 = (sub.loc[hi, h] > sub.loc[hi, f"{h}_dmed"]).mean() * 100
                mean_q1 = (sub.loc[lo, h] - sub.loc[lo, f"{h}_dm"]).mean() * 100
                mean_q5 = (sub.loc[hi, h] - sub.loc[hi, f"{h}_dm"]).mean() * 100

                # constants that would be hardcoded: log(vol / same-day median vol)
                lrv = np.log(sub[col] / sub.groupby("date")[col].transform("median"))
                yr_mu = lrv.groupby(sub["year"]).mean()
                drift = (yr_mu.max() - yr_mu.min()) / lrv.std()
                cover = panel[col].gt(0).mean() * 100
                print(f"  {est:>5}{w:>5}{med_q1 - med_q5:>+11.3f}%{win_q1 - win_q5:>+11.2f}pp"
                      f"{mean_q5 - mean_q1:>+12.3f}%{drift:>13.2f}{cover:>7.1f}%")

    # Is a long-window volatility cell secretly a size/liquidity tilt?  If low vol just
    # means "big liquid stock", the cell is not neutralising variance, it is buying beta
    # to the large-cap end -- a different bet, and one the user must be told about.
    print("\n=== is this just a size/liquidity tilt?  Spearman(vol, log turnover) per day ===")
    panel["ltn"] = np.log(panel["turnover"].where(panel["turnover"] > 0))
    for est in ("cc", "park"):
        for w in (21, 89, 233, 610):
            col = f"{est}{w}"
            rs = []
            for _, gg in panel.groupby("date"):
                sub = gg[[col, "ltn"]].dropna()
                if len(sub) < 30:
                    continue
                r = sub[col].rank().corr(sub["ltn"].rank())
                if not np.isnan(r):
                    rs.append(r)
            print(f"  {est}{w:<4} vs log-turnover: mean rho={np.mean(rs):+.4f}")

    print("\n=== per-year median-spread (fwd_60), top window candidates ===")
    years = sorted(panel["year"].unique())
    print(f"  {'cell':<10}" + "".join(f"{y:>8}" for y in years))
    for est, w in (("cc", 89), ("park", 233), ("park", 377), ("park", 610)):
        col = f"{est}{w}"
        cells = []
        for y in years:
            sub = panel[panel[col].gt(0) & panel["fwd_60"].notna() & panel["year"].eq(y)]
            if len(sub) < 1000:
                cells.append("     n/a")
                continue
            q = sub.groupby("date")[col].transform(
                lambda s: pd.qcut(s.rank(method="first"), 5, labels=False, duplicates="drop")
            )
            m1 = (sub.loc[q.eq(0), "fwd_60"] - sub.loc[q.eq(0), "fwd_60_dmed"]).median() * 100
            m5 = (sub.loc[q.eq(4), "fwd_60"] - sub.loc[q.eq(4), "fwd_60_dmed"]).median() * 100
            cells.append(f"{m1 - m5:>+8.2f}")
        print(f"  {est}{w:<8}" + "".join(cells))


if __name__ == "__main__":
    main()
