"""Ad-hoc: out-of-sample weight optimization via train/test split.

Step 1: split panel into train (2017-2020) and test (2021-2026).
Step 2: compute per-cell H=60 full Δ on train only.
Step 3: derive multipliers from train Δ via clip(1 + 5×Δ, 0.3, 2.0).
Step 4: synthesize new total_long = Σ (cell × multiplier) (skip rescue).
Step 5: compare test-period spread (old vs new) at H=5/20/60 × full/bull/bear.

Memory 2026-05-04 lesson: in-sample weight rebalance (B2) failed —
排列_long boost ±10→±15 made total H=60 full spread worse. Train/test
split addresses overfitting risk by isolating fit from evaluation.

Usage:
    python -m analysis._score_train_test_weights
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

PANEL = Path("tmp/score_panel.parquet")
HORIZONS = (5, 20, 60)
MIN_DAILY_N = 30

CATEGORIES = ["扣抵", "排列", "大盤", "波浪", "洪量", "OBV", "MACD", "Donchian"]
TIMEFRAMES = ["short", "medium", "long"]

TRAIN_END = pd.Timestamp("2020-12-31")
BULL_YEARS = {2017, 2018, 2019, 2020, 2023}
BEAR_YEARS = {2021, 2022}


def daily_spread_series(df: pd.DataFrame, score_col: str, fwd_col: str) -> pd.Series:
    sub = df.dropna(subset=[score_col, fwd_col])
    rank_pct = sub.groupby("date")[score_col].rank(method="first", pct=True)
    decile = (rank_pct * 10).clip(upper=9.9999).astype(int) + 1
    g = sub.assign(decile=decile).groupby(["date", "decile"])[fwd_col].mean()
    per_day = g.unstack().dropna(subset=[1, 10])
    return per_day[10] - per_day[1]


def regime_mean(series: pd.Series, dates: set) -> float:
    s = series[series.index.isin(dates)]
    return s.mean()


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    df = pd.read_parquet(PANEL)
    df["date"] = pd.to_datetime(df["date"])
    print(f"Loaded panel: {df.shape[0]:,} rows", file=sys.stderr)

    # Available cell columns (skip ones not in current panel)
    cat_tf_cols = [f"{c}_{tf}" for c in CATEGORIES for tf in TIMEFRAMES
                   if f"{c}_{tf}" in df.columns]
    print(f"Active cells in panel: {len(cat_tf_cols)}", file=sys.stderr)

    # Pre-rescue total_long = sum of cell aggregates
    df["pre_rescue"] = df[cat_tf_cols].sum(axis=1)

    # ── Step 1: train/test split ─────────────────────────────
    train_df = df[df["date"] <= TRAIN_END].copy()
    test_df = df[df["date"] > TRAIN_END].copy()
    print(f"Train: {len(train_df):,} rows ({train_df['date'].min().date()} ~ "
          f"{train_df['date'].max().date()})", file=sys.stderr)
    print(f"Test:  {len(test_df):,} rows ({test_df['date'].min().date()} ~ "
          f"{test_df['date'].max().date()})", file=sys.stderr)

    # ── Step 2: per-cell H=60 full Δ on train ────────────────
    print("\n" + "=" * 80)
    print("Train-period per-cell H=60 full Δ (and resulting multiplier)")
    print("=" * 80)
    print(f"{'cell':<18s}  {'train Δ':>9s}  {'multiplier':>10s}")
    print("-" * 50)

    train_base = daily_spread_series(train_df, "pre_rescue", "fwd_60")
    base_full = train_base.mean() * 100

    multipliers = {}
    for c in cat_tf_cols:
        train_df["_ablate"] = train_df["pre_rescue"] - train_df[c]
        s_ab = daily_spread_series(train_df, "_ablate", "fwd_60")
        delta = (base_full / 100 - s_ab.mean()) * 100  # in pp
        m = float(np.clip(1.0 + 5.0 * delta, 0.3, 2.0))
        multipliers[c] = m
        if abs(delta) > 0.005 or abs(m - 1.0) > 0.05:  # show only non-trivial
            print(f"{c:<18s}  {delta:>+8.4f}  {m:>9.3f}×")
    train_df.drop(columns=["_ablate"], inplace=True)

    # ── Step 3: synthesize new totals ────────────────────────
    df["new_total"] = sum(df[c] * multipliers[c] for c in cat_tf_cols)

    # New max_possible (theoretical, for normalization reference)
    # Estimate via std-bounded spread from observed range per cell
    new_max_estimate = sum(abs(df[c]).max() * multipliers[c] for c in cat_tf_cols)
    old_max_estimate = sum(abs(df[c]).max() for c in cat_tf_cols)
    print(f"\nNew weight range (max abs sum, rough): "
          f"old={old_max_estimate:.0f} → new={new_max_estimate:.0f}")

    # Refresh train_df / test_df to include new_total
    train_df = df[df["date"] <= TRAIN_END]
    test_df = df[df["date"] > TRAIN_END]

    # ── Step 4: test-period spread comparison ────────────────
    test_date_n = test_df.groupby("date").size()
    valid_dates = test_date_n[test_date_n >= MIN_DAILY_N].index
    full_d = set(valid_dates)
    bull_d = {d for d in valid_dates if d.year in BULL_YEARS}
    bear_d = {d for d in valid_dates if d.year in BEAR_YEARS}
    regimes = [("full", full_d), ("bull", bull_d), ("bear", bear_d)]

    print("\n" + "=" * 90)
    print("Test-period (2021-2026) spread comparison: old vs new weights")
    print("=" * 90)
    print(f"{'horizon':>4s}  {'regime':>6s}  {'old':>10s}  {'new':>10s}  "
          f"{'Δ (new-old)':>15s}  {'verdict':>10s}")
    print("-" * 90)
    for h in HORIZONS:
        s_old = daily_spread_series(test_df, "pre_rescue", f"fwd_{h}")
        s_new = daily_spread_series(test_df, "new_total", f"fwd_{h}")
        for regime, dates in regimes:
            m_old = regime_mean(s_old, dates) * 100
            m_new = regime_mean(s_new, dates) * 100
            delta = m_new - m_old
            verdict = "✓" if delta > 0.01 else ("✗" if delta < -0.01 else "≈")
            print(f"{h:>4d}  {regime:>6s}  {m_old:>+9.3f}%  {m_new:>+9.3f}%  "
                  f"{delta:>+12.4f}pp  {verdict:>9s}")

    # ── Step 4b: volume-family floor variants ────────────────
    # User concern: 洪量_short 0.30× trim is too aggressive on volume.
    # Try variants that floor volume cells (洪量, OBV) at higher levels.
    print("\n" + "=" * 90)
    print("Volume-family floor variants (test period only)")
    print("=" * 90)

    def variant_multipliers(adjustments: dict) -> dict:
        m2 = dict(multipliers)
        for k, v in adjustments.items():
            m2[k] = v
        return m2

    variants = {
        "A1 baseline (train multipliers as-is)": multipliers,
        "A2 floor 洪量 at 0.70×": variant_multipliers({
            "洪量_short": max(0.70, multipliers.get("洪量_short", 1.0)),
            "洪量_medium": max(0.70, multipliers.get("洪量_medium", 1.0)),
            "洪量_long": max(0.70, multipliers.get("洪量_long", 1.0)),
        }),
        "A3 洪量_short kept at 1.00×": variant_multipliers({
            "洪量_short": 1.00,
        }),
        "A4 floor 洪量+OBV at 0.70×": variant_multipliers({
            "洪量_short": max(0.70, multipliers.get("洪量_short", 1.0)),
            "洪量_medium": max(0.70, multipliers.get("洪量_medium", 1.0)),
            "洪量_long": max(0.70, multipliers.get("洪量_long", 1.0)),
            "OBV_short": max(0.70, multipliers.get("OBV_short", 1.0)),
            "OBV_medium": max(0.70, multipliers.get("OBV_medium", 1.0)),
            "OBV_long": max(0.70, multipliers.get("OBV_long", 1.0)),
        }),
        "A5 洪量 全部 1.00× (不動)": variant_multipliers({
            "洪量_short": 1.00, "洪量_medium": 1.00, "洪量_long": 1.00,
        }),
    }

    print(f"{'variant':<42s}  {'H=60 full':>10s}  {'H=60 bull':>10s}  {'H=60 bear':>10s}")
    print("-" * 90)

    s_old_60 = daily_spread_series(test_df, "pre_rescue", "fwd_60")
    base_60_full = regime_mean(s_old_60, full_d) * 100
    base_60_bull = regime_mean(s_old_60, bull_d) * 100
    base_60_bear = regime_mean(s_old_60, bear_d) * 100
    print(f"{'baseline (old weights)':<42s}  "
          f"{base_60_full:>+9.3f}%  {base_60_bull:>+9.3f}%  {base_60_bear:>+9.3f}%")

    for label, m in variants.items():
        df["_var"] = sum(df[c] * m[c] for c in cat_tf_cols)
        v_test = df[df["date"] > TRAIN_END]
        s = daily_spread_series(v_test, "_var", "fwd_60")
        m60 = regime_mean(s, full_d) * 100
        m60b = regime_mean(s, bull_d) * 100
        m60d = regime_mean(s, bear_d) * 100
        print(f"{label:<42s}  {m60:>+9.3f}%  {m60b:>+9.3f}%  {m60d:>+9.3f}%")
    df.drop(columns=["_var"], inplace=True, errors="ignore")

    # ── Step 5: train-period sanity (should improve, by construction) ─
    print("\n" + "=" * 90)
    print("Train-period (2017-2020) spread sanity (in-sample, should improve)")
    print("=" * 90)
    print(f"{'horizon':>4s}  {'regime':>6s}  {'old':>10s}  {'new':>10s}  "
          f"{'Δ (new-old)':>15s}")
    print("-" * 80)
    train_date_n = train_df.groupby("date").size()
    train_valid = train_date_n[train_date_n >= MIN_DAILY_N].index
    train_full = set(train_valid)
    train_bull = {d for d in train_valid if d.year in BULL_YEARS}
    train_bear = {d for d in train_valid if d.year in BEAR_YEARS}
    train_regimes = [("full", train_full), ("bull", train_bull), ("bear", train_bear)]

    for h in HORIZONS:
        s_old = daily_spread_series(train_df, "pre_rescue", f"fwd_{h}")
        s_new = daily_spread_series(train_df, "new_total", f"fwd_{h}")
        for regime, dates in train_regimes:
            m_old = regime_mean(s_old, dates) * 100
            m_new = regime_mean(s_new, dates) * 100
            delta = m_new - m_old
            print(f"{h:>4d}  {regime:>6s}  {m_old:>+9.3f}%  {m_new:>+9.3f}%  "
                  f"{delta:>+12.4f}pp")


if __name__ == "__main__":
    main()
