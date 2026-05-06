"""Ad-hoc: Wave event-window category ablation.

Mirror of _score_macd_ablation.py / _score_obv_ablation.py for the 波浪 cat.
Wave events are added only to the medium ScoreCard, so ablation focuses on
the single 波浪_medium cell (other scope columns are 0).

Usage:
    python -m analysis._score_wave_ablation
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PANEL = Path("tmp/score_panel.parquet")
HORIZONS = (5, 20, 60)
MIN_DAILY_N = 30

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

    wave_cols = [c for c in ["波浪_short", "波浪_medium", "波浪_long"] if c in df.columns]
    if not wave_cols:
        print("ERROR: no 波浪_* columns in panel", file=sys.stderr)
        sys.exit(1)
    print(f"Wave cells found: {wave_cols}", file=sys.stderr)

    print("\n" + "=" * 70)
    print("Per-cell Wave distribution (long-side points triggered)")
    print("=" * 70)
    print(f"{'cell':<14s}  {'mean':>8s}  {'std':>6s}  "
          f"{'min':>5s}  {'max':>5s}  {'%nonzero':>8s}")
    for c in wave_cols:
        s = df[c]
        nz = (s != 0).mean() * 100
        print(f"{c:<14s}  {s.mean():>+8.3f}  {s.std():>6.2f}  "
              f"{s.min():>+5.0f}  {s.max():>+5.0f}  {nz:>7.2f}%")

    df["score_base"] = df["total_long"]
    df["score_no_wave"] = df["total_long"] - df[wave_cols].sum(axis=1)
    for c in wave_cols:
        df[f"score_no_{c}"] = df["total_long"] - df[c]

    date_n = df.groupby("date").size()
    valid_dates = date_n[date_n >= MIN_DAILY_N].index
    full_d = set(valid_dates)
    bull_d = {d for d in valid_dates if d.year in BULL_YEARS}
    bear_d = {d for d in valid_dates if d.year in BEAR_YEARS}
    regimes = [("full", full_d), ("bull", bull_d), ("bear", bear_d)]

    print("\n" + "=" * 80)
    print("Group ablation: all wave cells together")
    print("=" * 80)
    print(f"{'horizon':>4s}  {'regime':>6s}  {'with-wave':>10s}  "
          f"{'no-wave':>10s}  {'Δ (with−no)':>14s}")
    print("-" * 80)
    for h in HORIZONS:
        s_base = daily_spread_series(df, "score_base", f"fwd_{h}")
        s_no = daily_spread_series(df, "score_no_wave", f"fwd_{h}")
        for regime, dates in regimes:
            m_base = regime_mean(s_base, dates) * 100
            m_no = regime_mean(s_no, dates) * 100
            delta = m_base - m_no
            print(f"{h:>4d}  {regime:>6s}  "
                  f"{m_base:>+9.3f}%  {m_no:>+9.3f}%  "
                  f"{delta:>+11.4f}pp")

    print("\n" + "=" * 80)
    print("Per-cell ablation")
    print("=" * 80)
    print(f"{'horizon':>4s}  {'regime':>6s}  ", end="")
    for c in wave_cols:
        print(f"{c:>14s}  ", end="")
    print()
    print("-" * 80)
    base_cache = {}
    for h in HORIZONS:
        s_base = daily_spread_series(df, "score_base", f"fwd_{h}")
        for regime, dates in regimes:
            base_cache[(h, regime)] = regime_mean(s_base, dates) * 100
    for h in HORIZONS:
        for regime, dates in regimes:
            m_base = base_cache[(h, regime)]
            print(f"{h:>4d}  {regime:>6s}  ", end="")
            for c in wave_cols:
                s_ab = daily_spread_series(df, f"score_no_{c}", f"fwd_{h}")
                m_ab = regime_mean(s_ab, dates) * 100
                delta = m_base - m_ab
                print(f"{delta:>+12.4f}pp  ", end="")
            print()


if __name__ == "__main__":
    main()
