"""Ad-hoc: re-derive C dampened multipliers under ~dead filter.

Strategy: back-calculate raw cell values by dividing current panel cells
by their current WEIGHT_MULTIPLIERS, then run train/test derivation on
~dead-filtered raw panel. Avoids needing to rebuild panel with mult=1.0.

Output: new multiplier dict to paste into score.py WEIGHT_MULTIPLIERS.
Plus test-period spread comparison.

Usage:
    python -m analysis._score_train_test_weights_dead
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

PANEL = Path("tmp/score_panel.parquet")
DEAD = Path("tmp/dead_fish_panel.parquet")
HORIZONS = (5, 20, 60)
MIN_DAILY_N = 30

CATEGORIES = ["扣抵", "排列", "大盤", "波浪", "洪量", "OBV", "MACD", "Donchian"]
TIMEFRAMES = ["short", "medium", "long"]

TRAIN_END = pd.Timestamp("2020-12-31")
BULL_YEARS = {2017, 2018, 2019, 2020, 2023}
BEAR_YEARS = {2021, 2022}

# Current production multipliers (from score.py WEIGHT_MULTIPLIERS as of 2026-05-13)
CURRENT_MULTIPLIERS = {
    ("扣抵", "medium"): 1.185,
    ("扣抵", "long"): 1.099,
    ("排列", "medium"): 0.969,
    ("排列", "long"): 1.341,
    ("大盤", "long"): 1.096,
    ("洪量", "short"): 0.650,
    ("洪量", "medium"): 0.924,
    ("洪量", "long"): 1.041,
    ("MACD", "short"): 0.878,
    ("MACD", "medium"): 1.013,
    ("OBV", "short"): 0.815,
    ("OBV", "medium"): 1.033,
    ("OBV", "long"): 0.938,
    ("波浪", "short"): 0.912,
    ("波浪", "medium"): 1.036,
    ("波浪", "long"): 1.131,
}


def daily_spread_series(df: pd.DataFrame, score_col: str, fwd_col: str) -> pd.Series:
    sub = df.dropna(subset=[score_col, fwd_col])
    rank_pct = sub.groupby("date")[score_col].rank(method="first", pct=True)
    decile = (rank_pct * 10).clip(upper=9.9999).astype(int) + 1
    g = sub.assign(decile=decile).groupby(["date", "decile"])[fwd_col].mean()
    per_day = g.unstack().dropna(subset=[1, 10])
    return per_day[10] - per_day[1]


def regime_mean(series: pd.Series, dates: set) -> float:
    return series[series.index.isin(dates)].mean()


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    df = pd.read_parquet(PANEL)
    df["date"] = pd.to_datetime(df["date"])

    dead = pd.read_parquet(DEAD)
    dead["date"] = pd.to_datetime(dead["date"])
    df = df.merge(dead, on=["date", "stock_id"], how="left")
    df["is_dead_fish"] = df["is_dead_fish"].fillna(False)
    print(f"Loaded panel: {len(df):,} rows", file=sys.stderr)

    # Filter ~dead
    df = df[~df["is_dead_fish"]].copy()
    print(f"After ~dead filter: {len(df):,} rows", file=sys.stderr)

    # Back-calculate raw cell values (divide by current multiplier)
    cat_tf_cols = []
    for cat in CATEGORIES:
        for tf in TIMEFRAMES:
            col = f"{cat}_{tf}"
            if col not in df.columns:
                continue
            cat_tf_cols.append(col)
            m = CURRENT_MULTIPLIERS.get((cat, tf), 1.0)
            if abs(m - 1.0) > 1e-6:
                df[col] = df[col] / m
    print(f"Active cells: {len(cat_tf_cols)}", file=sys.stderr)
    print("Back-calculated raw cell values", file=sys.stderr)

    # Pre-rescue total = sum of raw cells (NB: distance cells already have
    # weight 1.0, no back-calc needed)
    df["pre_rescue_raw"] = df[cat_tf_cols].sum(axis=1)

    # Add distance cells if present
    dist_cols = [c for c in df.columns if c.startswith("距離_")]
    if dist_cols:
        df["pre_rescue_raw"] = df["pre_rescue_raw"] + df[dist_cols].sum(axis=1)
        all_cols_with_dist = cat_tf_cols + dist_cols
    else:
        all_cols_with_dist = cat_tf_cols

    # Train/test split
    train_df = df[df["date"] <= TRAIN_END].copy()
    test_df = df[df["date"] > TRAIN_END].copy()
    print(f"Train (~dead, 2017-2020): {len(train_df):,} rows", file=sys.stderr)
    print(f"Test (~dead, 2021-2026): {len(test_df):,} rows", file=sys.stderr)

    # Per-cell train Δ at H=60 full → new multiplier
    print("\n" + "=" * 80)
    print("~DEAD train period per-cell H=60 full Δ and proposed multipliers")
    print("=" * 80)
    print(f"{'cell':<18s}  {'train Δ':>9s}  {'A1 raw mult':>11s}  "
          f"{'C dampened':>11s}  {'old C':>8s}  {'change':>9s}")
    print("-" * 80)

    train_base = daily_spread_series(train_df, "pre_rescue_raw", "fwd_60")
    base_full = train_base.mean() * 100

    new_multipliers_raw = {}
    new_multipliers_c = {}
    for c in all_cols_with_dist:
        train_df["_ablate"] = train_df["pre_rescue_raw"] - train_df[c]
        s_ab = daily_spread_series(train_df, "_ablate", "fwd_60")
        delta = (base_full / 100 - s_ab.mean()) * 100  # pp
        m_raw = float(np.clip(1.0 + 5.0 * delta, 0.3, 2.0))
        m_c = (m_raw + 1.0) / 2.0
        new_multipliers_raw[c] = m_raw
        new_multipliers_c[c] = m_c

        # Compare with old C dampened
        cat = c.rsplit("_", 1)[0]
        tf = c.rsplit("_", 1)[1]
        old_c = CURRENT_MULTIPLIERS.get((cat, tf), 1.0)
        change = m_c - old_c
        print(f"{c:<18s}  {delta:>+8.4f}  {m_raw:>10.3f}×  "
              f"{m_c:>10.3f}×  {old_c:>7.3f}×  {change:>+8.3f}")
    train_df.drop(columns=["_ablate"], inplace=True)

    # Synthesize new totals
    df["new_total_raw"] = sum(df[c] * new_multipliers_raw[c] for c in all_cols_with_dist)
    df["new_total_c"] = sum(df[c] * new_multipliers_c[c] for c in all_cols_with_dist)
    df["old_total"] = sum(df[c] * CURRENT_MULTIPLIERS.get(
        (c.rsplit("_", 1)[0], c.rsplit("_", 1)[1]), 1.0
    ) for c in all_cols_with_dist)

    test_df = df[df["date"] > TRAIN_END]
    test_date_n = test_df.groupby("date").size()
    valid_dates = test_date_n[test_date_n >= MIN_DAILY_N].index
    full_d = set(valid_dates)
    bull_d = {d for d in valid_dates if d.year in BULL_YEARS}
    bear_d = {d for d in valid_dates if d.year in BEAR_YEARS}
    regimes = [("full", full_d), ("bull", bull_d), ("bear", bear_d)]

    print("\n" + "=" * 90)
    print("Test-period (~dead, 2021-2026) spread: old(current C) vs new(A1 raw) vs new(C dampened)")
    print("=" * 90)
    print(f"{'H':>3s}  {'regime':>6s}  {'OLD C':>10s}  {'NEW A1':>10s}  {'ΔA1':>10s}  "
          f"{'NEW C':>10s}  {'ΔC':>10s}")
    print("-" * 90)
    for h in HORIZONS:
        s_old = daily_spread_series(test_df, "old_total", f"fwd_{h}")
        s_a1 = daily_spread_series(test_df, "new_total_raw", f"fwd_{h}")
        s_c = daily_spread_series(test_df, "new_total_c", f"fwd_{h}")
        for regime, dates in regimes:
            m_old = regime_mean(s_old, dates) * 100
            m_a1 = regime_mean(s_a1, dates) * 100
            m_c = regime_mean(s_c, dates) * 100
            print(f"{h:>3d}  {regime:>6s}  {m_old:>+9.3f}%  "
                  f"{m_a1:>+9.3f}%  {m_a1-m_old:>+9.4f}pp  "
                  f"{m_c:>+9.3f}%  {m_c-m_old:>+9.4f}pp")

    # Output multiplier dict for paste
    print("\n" + "=" * 80)
    print("PROPOSED WEIGHT_MULTIPLIERS (C dampened, ~dead-derived)")
    print("=" * 80)
    print("WEIGHT_MULTIPLIERS = {")
    for c in all_cols_with_dist:
        cat, tf = c.rsplit("_", 1)
        m_c = new_multipliers_c[c]
        m_raw = new_multipliers_raw[c]
        if abs(m_c - 1.0) > 0.02:  # show only non-trivial
            print(f'    ("{cat}", "{tf}"): {m_c:.3f},  # A1 raw {m_raw:.3f}')
    print("}")


if __name__ == "__main__":
    main()
