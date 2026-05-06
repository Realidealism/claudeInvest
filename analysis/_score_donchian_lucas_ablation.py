"""Ad-hoc: Donchian event-form per-cell ablation (works for any window choice).

Round 2 (Lucas 18/47/123) was採用 long-only after subset trap analysis.
Round 3 testing Fibonacci-shifted 21/55/144 across all three scopes.

Note: Donchian entry events have a subset relation:
  entry_long_long ⊂ entry_long_medium ⊂ entry_long_short
A 123-day high implies 47-day and 18-day highs. So three cells fire
simultaneously on major breakouts → graduated tier weighting (small +5,
medium +10, big +15) — possibly feature, possibly共線. Ablation tells.

Usage:
    python -m analysis._score_donchian_lucas_ablation
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

    cols = [c for c in ["Donchian_short", "Donchian_medium", "Donchian_long"] if c in df.columns]
    if not cols:
        print("ERROR: no Donchian_* columns. Did you rebuild panel?", file=sys.stderr)
        sys.exit(1)
    print(f"Donchian cells found: {cols}", file=sys.stderr)

    print("\n" + "=" * 70)
    print("Per-cell Donchian distribution")
    print("=" * 70)
    print(f"{'cell':<18s}  {'mean':>8s}  {'std':>6s}  "
          f"{'min':>5s}  {'max':>5s}  {'%nonzero':>8s}")
    for c in cols:
        s = df[c]
        nz = (s != 0).mean() * 100
        print(f"{c:<18s}  {s.mean():>+8.3f}  {s.std():>6.2f}  "
              f"{s.min():>+5.0f}  {s.max():>+5.0f}  {nz:>7.2f}%")

    df["score_base"] = df["total_long"]
    df["score_no_don"] = df["total_long"] - df[cols].sum(axis=1)
    for c in cols:
        df[f"score_no_{c}"] = df["total_long"] - df[c]

    date_n = df.groupby("date").size()
    valid_dates = date_n[date_n >= MIN_DAILY_N].index
    full_d = set(valid_dates)
    bull_d = {d for d in valid_dates if d.year in BULL_YEARS}
    bear_d = {d for d in valid_dates if d.year in BEAR_YEARS}
    regimes = [("full", full_d), ("bull", bull_d), ("bear", bear_d)]

    print("\n" + "=" * 80)
    print("Group ablation: all 3 Donchian cells")
    print("=" * 80)
    print(f"{'horizon':>4s}  {'regime':>6s}  {'with-don':>10s}  "
          f"{'no-don':>10s}  {'Δ (with−no)':>14s}")
    print("-" * 80)
    for h in HORIZONS:
        s_base = daily_spread_series(df, "score_base", f"fwd_{h}")
        s_no = daily_spread_series(df, "score_no_don", f"fwd_{h}")
        for regime, dates in regimes:
            m_base = regime_mean(s_base, dates) * 100
            m_no = regime_mean(s_no, dates) * 100
            delta = m_base - m_no
            print(f"{h:>4d}  {regime:>6s}  "
                  f"{m_base:>+9.3f}%  {m_no:>+9.3f}%  "
                  f"{delta:>+11.4f}pp")

    print("\n" + "=" * 80)
    print("Per-cell ablation (Δ = with-cell − without-cell)")
    print("=" * 80)
    print(f"{'horizon':>4s}  {'regime':>6s}  ", end="")
    for c in cols:
        print(f"{c:>16s}  ", end="")
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
            for c in cols:
                s_ab = daily_spread_series(df, f"score_no_{c}", f"fwd_{h}")
                m_ab = regime_mean(s_ab, dates) * 100
                delta = m_base - m_ab
                print(f"{delta:>+14.4f}pp  ", end="")
            print()


if __name__ == "__main__":
    main()
