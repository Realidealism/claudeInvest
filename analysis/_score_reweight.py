"""Reweight cells by column arithmetic — no 27-min panel rebuild.

Boolean cells (all categories except 距離) score pts = points × trigger
(analysis/score.py _eval_side), so their stored contribution column
{cat}_{tf} is exactly linear in the weight: rescale the column, re-add the
delta to total_long. Exact on non-knot-rescue bars (99.96% of the production
panel, measured 2026-07-07); knot-rescue bars are approximated to the SAME
degree the accepted _score_ablation.py (total_long - col) already is.

距離_* cells are continuous cap-clipped (score.py _add_distance_scope:
column = clip(z×3, ±cap), weight IS the cap), so scaling is NOT linear:
    scale == 0   → subtract column (exact, same as boolean ablation)
    0 < scale <1 → clip(column, ±cap×scale) (exact: clip∘clip = tighter clip)
    scale > 1    → IMPOSSIBLE from the panel (clipped bars lost the raw z);
                   hard error — rebuild via _score_panel with the new cap.

Universe: ~dead filtered (skill rule: 所有 ablation 用 ~dead 過濾), via
tmp/dead_fish_panel.parquet left-merge (missing dates treated alive, same as
_score_all_ablation_dead.py). --full-universe for reference numbers only.

Scope:
    ✓ reweight existing cells / ablation (scale=0) — column already in panel.
    ✗ brand-new cells or signal-form (trend↔event) changes — no column exists,
      MUST rebuild via `python -m analysis._score_panel`.

scale = new_weight / old_weight. Examples:
    boolean ±5 → ±15 base points   → scale 3.0
    multiplier 1.341 → 2.0         → scale ~1.49
    距離 cap 10 → 5                → scale 0.5 (clip path)

Usage:
    python -m analysis._score_reweight "排列_long=1.5,洪量_medium=0"
    python -m analysis._score_reweight "距離_p233_long=0.5"
    python -m analysis._score_reweight "排列_long=1.5" --full-universe
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from analysis._score_ablation import (
    HORIZONS,
    MIN_DAILY_N,
    daily_spread_series,
    regime_from_breadth,
    regime_mean,
)

DEAD_PANEL = Path("tmp/dead_fish_panel.parquet")
_DISTANCE_CAP = 10.0  # mirrors score.py _DISTANCE_CAP; 距離 column = clip(z×3, ±cap)


def parse_changes(spec: str) -> dict[str, float]:
    """'col=scale,col=scale' -> {col: scale}."""
    changes: dict[str, float] = {}
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            sys.exit(f"bad change '{part}': expected col=scale")
        col, scale = part.split("=", 1)
        changes[col.strip()] = float(scale)
    if not changes:
        sys.exit("no changes parsed")
    return changes


def reweighted_total(df: pd.DataFrame, changes: dict[str, float]) -> pd.Series:
    """total_long with each changed cell's column contribution replaced."""
    total = df["total_long"].copy()
    for col, scale in changes.items():
        if col not in df.columns:
            sys.exit(
                f"column '{col}' not in panel — is it a brand-new cell? "
                f"Those have no column and MUST be rebuilt via _score_panel."
            )
        if col.startswith("距離") and scale not in (0.0, 1.0):
            # Continuous cap-clipped cell: column = clip(z×3, ±cap).
            if scale > 1.0:
                sys.exit(
                    f"'{col}' is a continuous cap cell; cap INCREASE cannot be "
                    f"derived from the panel (clipped bars lost the raw z). "
                    f"Change the cap in score.py and rebuild via _score_panel."
                )
            new_cap = _DISTANCE_CAP * scale
            total = total - df[col] + df[col].clip(-new_cap, new_cap)
        else:
            total = total + df[col] * (scale - 1.0)
    return total


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("changes", help="'col=scale,col=scale' (scale=new/old weight)")
    ap.add_argument("--panel", default="tmp/score_panel.parquet")
    ap.add_argument("--full-universe", action="store_true",
                    help="skip ~dead filter (reference only, NOT for adoption)")
    args = ap.parse_args()

    changes = parse_changes(args.changes)

    df = pd.read_parquet(Path(args.panel))
    df["date"] = pd.to_datetime(df["date"])
    n_all = len(df)

    if args.full_universe:
        universe = "FULL (reference only — adoption decisions need ~dead)"
    else:
        if not DEAD_PANEL.exists():
            sys.exit(
                f"{DEAD_PANEL} missing — build it first: "
                f"python -m analysis._build_dead_fish_panel (~1 min), "
                f"or pass --full-universe for reference numbers."
            )
        dead = pd.read_parquet(DEAD_PANEL)
        dead["date"] = pd.to_datetime(dead["date"])
        df = df.merge(dead, on=["date", "stock_id"], how="left")
        df["is_dead_fish"] = df["is_dead_fish"].fillna(False)
        df = df[~df["is_dead_fish"]]
        universe = f"~dead ({len(df):,}/{n_all:,} rows)"

    print(f"Universe: {universe}", file=sys.stderr)
    print(f"Changes: {changes}", file=sys.stderr)

    df = df.copy()
    df["_reweighted"] = reweighted_total(df, changes)

    date_n = df.groupby("date").size()
    valid_dates = date_n[date_n >= MIN_DAILY_N].index
    full_d = set(valid_dates)

    rb = regime_from_breadth(valid_dates)
    bull_L = rb["bull_L"] & full_d
    bear_L = rb["bear_L"] & full_d
    neutral_L = rb["neutral_L"] & full_d
    bull_M = rb["bull_M"] & full_d
    bear_M = rb["bear_M"] & full_d
    neutral_M = rb["neutral_M"] & full_d

    # long-scale regimes used for verdict; medium listed for reference
    long_regimes = [
        ("full", full_d),
        ("bull_L", bull_L), ("bear_L", bear_L), ("neutral_L", neutral_L),
    ]
    med_regimes = [
        ("bull_M", bull_M), ("bear_M", bear_M), ("neutral_M", neutral_M),
    ]
    all_regimes = long_regimes + med_regimes

    print("\n" + "=" * 80)
    print(f"Decile spread (mean of daily, %) — baseline vs reweighted [{universe.split(' ')[0]}]")
    print("=" * 80)
    print(f"{'H':>3s} {'regime':>9s}  {'baseline':>10s}  {'reweight':>10s}  {'Δ':>9s}")
    print("-" * 80)

    # verdict_deltas: full + bull_L + bear_L only (neutral excluded — noisy)
    verdict_deltas: list[float] = []
    all_deltas: list[float] = []
    for h in HORIZONS:
        base_s = daily_spread_series(df, "total_long", f"fwd_{h}")
        new_s = daily_spread_series(df, "_reweighted", f"fwd_{h}")
        for regime, dates in all_regimes:
            b = regime_mean(base_s, dates) * 100
            n = regime_mean(new_s, dates) * 100
            d = n - b
            all_deltas.append(d)
            if regime in ("full", "bull_L", "bear_L"):
                verdict_deltas.append(d)
            note = "  [ref]" if regime in ("bull_M", "bear_M", "neutral_M", "neutral_L") else ""
            print(f"{h:>3d} {regime:>9s}  {b:>+9.3f}%  {n:>+9.3f}%  {d:>+8.3f}%{note}")
        print("-" * 80)

    # ── Per-year Δ table ────────────────────────────────────────────────────
    all_years = sorted({d.year for d in full_d})
    print("\n" + "=" * 70)
    print("Per-year Δ (reweight − baseline, %, ~dead universe)")
    print("=" * 70)
    print(f"  {'year':>4s}  {'n_days':>6s}" + "".join(f"  H{h:>3d}Δ%" for h in HORIZONS))
    print("  " + "-" * 50)
    for h in HORIZONS:
        base_s = daily_spread_series(df, "total_long", f"fwd_{h}")
        new_s = daily_spread_series(df, "_reweighted", f"fwd_{h}")
        # cache per-year delta
        if h == HORIZONS[0]:
            yr_delta: dict[tuple, float] = {}
        for yr in all_years:
            yr_dates = {d for d in full_d if d.year == yr}
            yr_delta[(yr, h)] = (regime_mean(new_s, yr_dates) - regime_mean(base_s, yr_dates)) * 100
    for yr in all_years:
        n_yr = len({d for d in full_d if d.year == yr})
        vals = "".join(f"  {yr_delta[(yr, h)]:>+7.3f}%" for h in HORIZONS)
        print(f"  {yr:>4d}  {n_yr:>6d}{vals}")

    # Adoption verdict — based on full + bull_L + bear_L only
    if all(d == 0 for d in all_deltas):
        verdict = "NO-OP — 所有 Δ 為 0（scale=1 或欄全零）"
    elif all(d >= 0 for d in verdict_deltas):
        if all(d >= 0 for d in all_deltas):
            verdict = "CLEAN WIN — 全 H×regime Δ≥0，直接採用候選"
        else:
            verdict = "CLEAN WIN (long) — full+bull_L+bear_L 皆正（neutral/M 桶有負，供參考）"
    else:
        verdict = "DRAG — full 或 bull_L/bear_L 桶有負，拒絕或找中間 weight"
    print(f"\nverdict: {verdict}")
    print("note: knot-rescue 列為近似（同 _score_ablation.py）；新 cell/換形式/距離 cap 調升須 rebuild")
    print("note: medium 桶標 [ref]，不進 verdict 判準")


if __name__ == "__main__":
    main()
