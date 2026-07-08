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
# Cell columns are discovered from the panel at runtime, not hardcoded — the
# 距離 category is stored per-period (距離_p55…p377), so a fixed category list
# drifts out of sync with _score_panel.py and KeyErrors. See _score_reweight.py.
EXCLUDE_COLS = {"date", "stock_id", "close", "total_long"}

def regime_from_breadth(dates) -> dict[str, set]:
    """Classify dates into 7 regime buckets using daily breadth trend data.

    long_trend  ∈ {+1,+2,+3} → bull_L; {-1,-2,-3} → bear_L; {0} → neutral_L
    medium_trend∈ {+1,+2,+3} → bull_M; {-1,-2,-3} → bear_M; {0} → neutral_M

    Returns dict with keys: bull_L, bear_L, neutral_L, bull_M, bear_M, neutral_M
    Dates without breadth data are excluded from all buckets and counted.
    """
    import sys as _sys
    from analysis.score import _load_breadth_trends

    breadth = _load_breadth_trends()
    # Normalise breadth keys to pandas Timestamp for consistent comparison
    breadth_norm: dict = {}
    for k, v in breadth.items():
        breadth_norm[pd.Timestamp(k)] = v

    buckets: dict[str, set] = {
        "bull_L": set(), "bear_L": set(), "neutral_L": set(),
        "bull_M": set(), "bear_M": set(), "neutral_M": set(),
    }
    n_missing = 0
    for d in dates:
        ts = pd.Timestamp(d)
        bt = breadth_norm.get(ts)
        if bt is None:
            n_missing += 1
            continue
        lt = bt.get("long_trend", 0)
        mt = bt.get("medium_trend", 0)
        if lt > 0:
            buckets["bull_L"].add(ts)
        elif lt < 0:
            buckets["bear_L"].add(ts)
        else:
            buckets["neutral_L"].add(ts)
        if mt > 0:
            buckets["bull_M"].add(ts)
        elif mt < 0:
            buckets["bear_M"].add(ts)
        else:
            buckets["neutral_M"].add(ts)

    if n_missing > 0:
        print(
            f"[regime_from_breadth] {n_missing} dates have no breadth label "
            f"(excluded from all buckets)",
            file=_sys.stderr,
        )
    return buckets


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

    # Breadth-based regime split (replaces year-based BULL/BEAR_YEARS)
    rb = regime_from_breadth(valid_dates)
    bull_L = rb["bull_L"] & full_d
    bear_L = rb["bear_L"] & full_d
    neutral_L = rb["neutral_L"] & full_d
    bull_M = rb["bull_M"] & full_d
    bear_M = rb["bear_M"] & full_d
    neutral_M = rb["neutral_M"] & full_d

    regimes = [
        ("full", full_d),
        ("bull_L", bull_L), ("bear_L", bear_L), ("neutral_L", neutral_L),
        ("bull_M", bull_M), ("bear_M", bear_M), ("neutral_M", neutral_M),
    ]
    print(
        f"  full={len(full_d)} | "
        f"bull_L={len(bull_L)} bear_L={len(bear_L)} neutral_L={len(neutral_L)} | "
        f"bull_M={len(bull_M)} bear_M={len(bear_M)} neutral_M={len(neutral_M)}",
        file=sys.stderr,
    )

    cat_cols = [
        c for c in df.columns
        if c not in EXCLUDE_COLS and not c.startswith("fwd_") and (df[c] != 0).any()
    ]
    print(f"Active cells: {len(cat_cols)}", file=sys.stderr)

    regime_names = [name for name, _ in regimes]

    # ── Baseline ────────────────────────────────────────
    print("\n" + "=" * 110)
    print("Baseline top-bottom decile spread (mean of daily)")
    print("=" * 110)
    hdr = f"{'horizon':>8s}" + "".join(f"  {n:>10s}" for n in regime_names)
    print(hdr)
    print("-" * 110)
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

    # ── Per-year ΔIC table ───────────────────────────────
    all_years = sorted({d.year for d in full_d})
    print("\n" + "=" * 70)
    print("Per-year baseline spread (full universe, mean of daily, %)")
    print("=" * 70)
    print(f"  {'year':>4s}  {'n_days':>6s}" + "".join(f"  H{h:>3d}%" for h in HORIZONS))
    print("  " + "-" * 50)
    year_spread: dict[tuple, float] = {}
    for h in HORIZONS:
        spread_s = daily_spread_series(df, "total_long", f"fwd_{h}")
        for yr in all_years:
            yr_dates = {d for d in full_d if d.year == yr}
            year_spread[(yr, h)] = regime_mean(spread_s, yr_dates) * 100
    for yr in all_years:
        n_yr = len({d for d in full_d if d.year == yr})
        vals = "".join(f"  {year_spread[(yr, h)]:>+7.3f}%" for h in HORIZONS)
        print(f"  {yr:>4d}  {n_yr:>6d}{vals}")

    # ── Print Δ tables (one per horizon) ────────────────
    for h in HORIZONS:
        print("\n" + "=" * 110)
        print(f"Δspread = baseline − ablated, H={h} days  (Δ>0: cell helps)")
        print("=" * 110)
        rows = []
        for cat_col in cat_cols:
            r = {"cell": cat_col}
            for regime, _ in regimes:
                r[regime] = base_spread[(h, regime)] - results[(cat_col, h, regime)]
            rows.append(r)
        out = pd.DataFrame(rows).sort_values("full", ascending=False)
        col_hdr = f"  {'cell':<14s}" + "".join(f"  {n+'_Δ':>11s}" for n in regime_names)
        print(col_hdr)
        print("  " + "-" * (14 + 13 * len(regime_names)))
        for _, r in out.iterrows():
            vals = "".join(f"  {r[n]:>+10.3f}%" for n in regime_names)
            print(f"  {r['cell']:<14s}{vals}")


if __name__ == "__main__":
    main()
