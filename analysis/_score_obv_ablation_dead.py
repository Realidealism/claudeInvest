"""Ad-hoc: OBV ablation side-by-side full universe vs ~dead_fish filter.

Joins tmp/score_panel.parquet with tmp/dead_fish_panel.parquet to mark
each row as dead/alive, then runs per-cell ablation under both views.

Usage:
    python -m analysis._score_obv_ablation_dead
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PANEL = Path("tmp/score_panel.parquet")
DEAD = Path("tmp/dead_fish_panel.parquet")
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


def run_ablation(df: pd.DataFrame, label: str) -> None:
    obv_cols = ["OBV_short", "OBV_medium", "OBV_long"]
    df = df.copy()
    df["score_base"] = df["total_long"]
    df["score_no_obv"] = df["total_long"] - df[obv_cols].sum(axis=1)
    for c in obv_cols:
        df[f"score_no_{c}"] = df["total_long"] - df[c]

    date_n = df.groupby("date").size()
    valid_dates = date_n[date_n >= MIN_DAILY_N].index
    full_d = set(valid_dates)
    bull_d = {d for d in valid_dates if d.year in BULL_YEARS}
    bear_d = {d for d in valid_dates if d.year in BEAR_YEARS}
    regimes = [("full", full_d), ("bull", bull_d), ("bear", bear_d)]

    print("\n" + "=" * 80)
    print(f"[{label}]  rows: {len(df):,}  valid days: {len(valid_dates):,}")
    print("=" * 80)

    print("\nGroup ablation: all 3 OBV cells together")
    print(f"{'horizon':>4s}  {'regime':>6s}  {'with-OBV':>10s}  "
          f"{'no-OBV':>10s}  {'Δ (with−no)':>14s}")
    print("-" * 60)
    for h in HORIZONS:
        s_base = daily_spread_series(df, "score_base", f"fwd_{h}")
        s_no = daily_spread_series(df, "score_no_obv", f"fwd_{h}")
        for regime, dates in regimes:
            m_base = regime_mean(s_base, dates) * 100
            m_no = regime_mean(s_no, dates) * 100
            delta = m_base - m_no
            print(f"{h:>4d}  {regime:>6s}  "
                  f"{m_base:>+9.3f}%  {m_no:>+9.3f}%  {delta:>+11.4f}pp")

    print("\nPer-cell ablation: Δ = with-cell − without-cell  (positive = cell helps)")
    print(f"{'horizon':>4s}  {'regime':>6s}  ", end="")
    for c in obv_cols:
        print(f"{c:>14s}  ", end="")
    print()
    print("-" * 70)
    base_cache = {}
    for h in HORIZONS:
        s_base = daily_spread_series(df, "score_base", f"fwd_{h}")
        for regime, dates in regimes:
            base_cache[(h, regime)] = regime_mean(s_base, dates) * 100
    for h in HORIZONS:
        for regime, dates in regimes:
            m_base = base_cache[(h, regime)]
            print(f"{h:>4d}  {regime:>6s}  ", end="")
            for c in obv_cols:
                s_ab = daily_spread_series(df, f"score_no_{c}", f"fwd_{h}")
                m_ab = regime_mean(s_ab, dates) * 100
                delta = m_base - m_ab
                print(f"{delta:>+12.4f}pp  ", end="")
            print()


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    panel = pd.read_parquet(PANEL)
    panel["date"] = pd.to_datetime(panel["date"])

    dead = pd.read_parquet(DEAD)
    dead["date"] = pd.to_datetime(dead["date"])

    merged = panel.merge(dead, on=["date", "stock_id"], how="left")
    merged["is_dead_fish"] = merged["is_dead_fish"].fillna(False)

    n_total = len(merged)
    n_dead = merged["is_dead_fish"].sum()
    print(f"Joined panel: {n_total:,} rows  ({n_dead:,} = {n_dead/n_total*100:.1f}% dead)",
          file=sys.stderr)

    run_ablation(merged, "FULL universe (incl. dead)")
    run_ablation(merged[~merged["is_dead_fish"]], "~DEAD filter (alive only)")


if __name__ == "__main__":
    main()
