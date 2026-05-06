"""Ad-hoc: compare top-bottom decile spread with vs without knot rescue.

Reads tmp/score_panel.parquet rebuilt after knot-rescue was added to
ScoreBoard.evaluate(). The panel's `total_long` is post-rescue;
sum(cat_tf_cols) per row reproduces pre-rescue total. Compares both as
score columns and reports H=5/20/60 spread across full/bull/bear regimes.

Usage:
    python -m analysis._score_knot_ablation
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PANEL = Path("tmp/score_panel.parquet")
HORIZONS = (5, 20, 60)
MIN_DAILY_N = 30
CATEGORIES = ["扣抵", "排列", "大盤", "波浪", "洪量", "OBV", "MACD", "Donchian"]
TIMEFRAMES = ["short", "medium", "long"]

BULL_YEARS = {2017, 2018, 2019, 2020, 2023}
BEAR_YEARS = {2021, 2022}


def daily_spread_series(df: pd.DataFrame, score_col: str, fwd_col: str) -> pd.Series:
    sub = df.dropna(subset=[score_col, fwd_col])
    rank_pct = sub.groupby("date")[score_col].rank(method="first", pct=True)
    decile = (rank_pct * 10).clip(upper=9.9999).astype(int) + 1
    g = sub.assign(decile=decile).groupby(["date", "decile"])[fwd_col].mean()
    per_day = g.unstack().dropna(subset=[1, 10])
    return per_day[10] - per_day[1]


def regime_mean(spread_series: pd.Series, dates: set) -> float:
    s = spread_series[spread_series.index.isin(dates)]
    return s.mean()


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    df = pd.read_parquet(PANEL)
    df["date"] = pd.to_datetime(df["date"])
    print(f"Loaded panel: {df.shape[0]:,} rows", file=sys.stderr)

    # Pre-rescue score = sum of (category × timeframe) cell aggregates
    cat_cols = [f"{c}_{tf}" for c in CATEGORIES for tf in TIMEFRAMES
                if f"{c}_{tf}" in df.columns]
    df["total_long_alone"] = df[cat_cols].sum(axis=1)
    df["rescue_delta"] = df["total_long"] - df["total_long_alone"]

    # Sanity: rescue is positive on the long side (only floor active in panel
    # except 5-day knot cap which is rare). short side has its own rescue,
    # not stored in panel since panel only records long-side details.
    n_rescued = (df["rescue_delta"] != 0).sum()
    print(f"Rows with non-zero rescue: {n_rescued:,} / {len(df):,} "
          f"({100 * n_rescued / len(df):.2f}%)", file=sys.stderr)
    if n_rescued:
        print(f"  Δ mean (on rescued rows): {df.loc[df['rescue_delta'] != 0, 'rescue_delta'].mean():+.2f}",
              file=sys.stderr)
        print(f"  Δ range: {df['rescue_delta'].min():+.0f} ~ {df['rescue_delta'].max():+.0f}",
              file=sys.stderr)

    # Regime date sets
    date_n = df.groupby("date").size()
    valid_dates = date_n[date_n >= MIN_DAILY_N].index
    full_d = set(valid_dates)
    bull_d = {d for d in valid_dates if d.year in BULL_YEARS}
    bear_d = {d for d in valid_dates if d.year in BEAR_YEARS}
    regimes = [("full", full_d), ("bull", bull_d), ("bear", bear_d)]
    print(f"  full={len(full_d)} days, bull={len(bull_d)}, bear={len(bear_d)}",
          file=sys.stderr)

    print("\n" + "=" * 78)
    print("Top-bottom decile spread (mean of daily, %)")
    print("=" * 78)
    print(f"{'horizon':>4s}  {'regime':>6s}  {'no-rescue':>10s}  "
          f"{'rescued':>10s}  {'Δ (rescued − base)':>22s}")
    print("-" * 78)
    for h in HORIZONS:
        s_alone = daily_spread_series(df, "total_long_alone", f"fwd_{h}")
        s_resc = daily_spread_series(df, "total_long", f"fwd_{h}")
        for regime, dates in regimes:
            m_alone = regime_mean(s_alone, dates) * 100
            m_resc = regime_mean(s_resc, dates) * 100
            delta = m_resc - m_alone
            print(f"{h:>4d}  {regime:>6s}  "
                  f"{m_alone:>+9.3f}%  {m_resc:>+9.3f}%  "
                  f"{delta:>+19.4f}pp")


if __name__ == "__main__":
    main()
