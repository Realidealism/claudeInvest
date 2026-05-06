"""Ad-hoc Stage C: per-category ablation using top-bottom decile spread.

For each of 18 (category × timeframe) cells, subtract the cell's contribution
from total_long, re-rank into deciles daily, compute the top-1−bottom-1
decile spread of forward returns, and report Δspread = baseline − ablated
across full / bull / bear regimes.

Δ > 0 → removing the cell hurts the spread (cell contributes)
Δ < 0 → removing the cell improves the spread (cell drags)
|Δ| small → cell is noise

Usage:
    python -m analysis._score_ablation
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

PANEL = Path("tmp/score_panel.parquet")
HORIZONS = (5, 20, 60)
MIN_DAILY_N = 30
CATEGORIES = ["扣抵", "排列", "大盤", "波浪", "洪量", "OBV", "MACD", "Donchian", "距離"]
TIMEFRAMES = ["short", "medium", "long"]

BULL_YEARS = {2017, 2018, 2019, 2020, 2023}
BEAR_YEARS = {2021, 2022}


def daily_spread_series(df: pd.DataFrame, score_col: str, fwd_col: str) -> pd.Series:
    """For each date, return (decile-10 mean − decile-1 mean) of fwd_col.
    Stocks ranked daily by score_col."""
    sub = df.dropna(subset=[score_col, fwd_col])
    rank_pct = sub.groupby("date")[score_col].rank(method="first", pct=True)
    decile = (rank_pct * 10).clip(upper=9.9999).astype(int) + 1
    g = sub.assign(decile=decile).groupby(["date", "decile"])[fwd_col].mean()
    per_day = g.unstack().dropna(subset=[1, 10])
    return per_day[10] - per_day[1]


def regime_mean(spread_series: pd.Series, dates: set) -> float:
    s = spread_series[spread_series.index.isin(dates)]
    return s.mean()


def main():
    sys.stdout.reconfigure(encoding="utf-8")

    df = pd.read_parquet(PANEL)
    df["date"] = pd.to_datetime(df["date"])
    print(f"Loaded panel: {df.shape[0]:,} rows", file=sys.stderr)

    # Build regime date sets (filtered to days with enough samples)
    date_n = df.groupby("date").size()
    valid_dates = date_n[date_n >= MIN_DAILY_N].index
    full_d = set(valid_dates)
    bull_d = {d for d in valid_dates if d.year in BULL_YEARS}
    bear_d = {d for d in valid_dates if d.year in BEAR_YEARS}
    regimes = [("full", full_d), ("bull", bull_d), ("bear", bear_d)]
    print(f"  full={len(full_d)} days, bull={len(bull_d)}, bear={len(bear_d)}",
          file=sys.stderr)

    cat_cols = [f"{c}_{tf}" for c in CATEGORIES for tf in TIMEFRAMES]

    # ── Baseline ────────────────────────────────────────
    print("\n" + "=" * 70)
    print("Baseline top-bottom decile spread (mean of daily)")
    print("=" * 70)
    print(f"{'horizon':>8s}  {'full':>10s}  {'bull':>10s}  {'bear':>10s}")
    print("-" * 70)
    base_spread = {}  # (h, regime) -> mean spread (in %)
    for h in HORIZONS:
        spread_s = daily_spread_series(df, "total_long", f"fwd_{h}")
        line = f"{h:>8d}"
        for regime, dates in regimes:
            m = regime_mean(spread_s, dates) * 100
            base_spread[(h, regime)] = m
            line += f"  {m:>+9.3f}%"
        print(line)

    # ── Ablation ────────────────────────────────────────
    t0 = time.time()
    results = {}  # (cat_col, h, regime) -> ablated spread (%)
    for k, cat_col in enumerate(cat_cols):
        df["_ablated"] = df["total_long"] - df[cat_col]
        for h in HORIZONS:
            spread_s = daily_spread_series(df, "_ablated", f"fwd_{h}")
            for regime, dates in regimes:
                results[(cat_col, h, regime)] = regime_mean(spread_s, dates) * 100
        print(f"  ablated {k+1}/{len(cat_cols)}: {cat_col}  "
              f"({time.time() - t0:.0f}s)", file=sys.stderr)
    df.drop(columns=["_ablated"], inplace=True)

    # ── Print Δ tables (one per horizon) ────────────────
    for h in HORIZONS:
        print("\n" + "=" * 70)
        print(f"Δspread = baseline − ablated, H={h} days  (Δ>0: cell helps)")
        print("=" * 70)
        rows = []
        for cat_col in cat_cols:
            r = {"cell": cat_col}
            for regime, _ in regimes:
                r[regime] = base_spread[(h, regime)] - results[(cat_col, h, regime)]
            rows.append(r)
        out = pd.DataFrame(rows).sort_values("full", ascending=False)
        print(f"  {'cell':<14s}  {'full_Δ':>9s}  {'bull_Δ':>9s}  {'bear_Δ':>9s}")
        print("  " + "-" * 56)
        for _, r in out.iterrows():
            print(f"  {r['cell']:<14s}  "
                  f"{r['full']:>+8.3f}%  "
                  f"{r['bull']:>+8.3f}%  "
                  f"{r['bear']:>+8.3f}%")


if __name__ == "__main__":
    main()
