"""Ad-hoc: compare two score panels under ~dead filter.

Reads current tmp/score_panel.parquet vs a baseline copy, computes
overall H=60/20/5 full+bull+bear spreads + per-cell Δ for the cell of
interest, plus group ablation. Used after experimental weight changes.

Usage:
    python -m analysis._score_compare_panels <baseline_path> <label>
    e.g. python -m analysis._score_compare_panels tmp/score_panel.bak_baseline "扣抵_long ±5 vs ±15"
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from analysis._score_ablation import regime_from_breadth

CURRENT = Path("tmp/score_panel.parquet")
DEAD = Path("tmp/dead_fish_panel.parquet")
HORIZONS = (5, 20, 60)
MIN_DAILY_N = 30


def daily_spread_series(df: pd.DataFrame, score_col: str, fwd_col: str) -> pd.Series:
    sub = df.dropna(subset=[score_col, fwd_col])
    rank_pct = sub.groupby("date")[score_col].rank(method="first", pct=True)
    decile = (rank_pct * 10).clip(upper=9.9999).astype(int) + 1
    g = sub.assign(decile=decile).groupby(["date", "decile"])[fwd_col].mean()
    per_day = g.unstack().dropna(subset=[1, 10])
    return per_day[10] - per_day[1]


def regime_mean(s: pd.Series, dates: set) -> float:
    return s[s.index.isin(dates)].mean()


def compute_summary(df: pd.DataFrame, cells_of_interest: list[str]) -> dict:
    date_n = df.groupby("date").size()
    valid = date_n[date_n >= MIN_DAILY_N].index
    full = set(valid)

    rb = regime_from_breadth(valid)
    bull_L = rb["bull_L"] & full
    bear_L = rb["bear_L"] & full
    neutral_L = rb["neutral_L"] & full
    bull_M = rb["bull_M"] & full
    bear_M = rb["bear_M"] & full
    neutral_M = rb["neutral_M"] & full

    regimes = [
        ("full", full),
        ("bull_L", bull_L), ("bear_L", bear_L), ("neutral_L", neutral_L),
        ("bull_M", bull_M), ("bear_M", bear_M), ("neutral_M", neutral_M),
    ]

    df = df.copy()
    df["score_base"] = df["total_long"]

    out = {"base": {}, "cells": {}, "base_daily": {}}
    for h in HORIZONS:
        s_base = daily_spread_series(df, "score_base", f"fwd_{h}")
        out["base_daily"][h] = s_base
        for r_name, r_dates in regimes:
            out["base"][(h, r_name)] = regime_mean(s_base, r_dates) * 100

    for cell in cells_of_interest:
        if cell not in df.columns:
            continue
        df[f"no_{cell}"] = df["total_long"] - df[cell]
        for h in HORIZONS:
            s_ab = daily_spread_series(df, f"no_{cell}", f"fwd_{h}")
            for r_name, r_dates in regimes:
                m_base = out["base"][(h, r_name)]
                m_ab = regime_mean(s_ab, r_dates) * 100
                out["cells"][(cell, h, r_name)] = m_base - m_ab
        df.drop(columns=[f"no_{cell}"], inplace=True)

    return out


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    if len(sys.argv) < 3:
        print("usage: _score_compare_panels <baseline_path> <label> [<cells_csv>]",
              file=sys.stderr)
        sys.exit(1)
    base_path = Path(sys.argv[1])
    label = sys.argv[2]
    cells = sys.argv[3].split(",") if len(sys.argv) > 3 else [
        "扣抵_long", "扣抵_medium", "排列_long", "排列_medium",
        "洪量_short", "洪量_medium", "洪量_long",
    ]

    dead = pd.read_parquet(DEAD)
    dead["date"] = pd.to_datetime(dead["date"])

    panels = [("baseline", base_path), ("current", CURRENT)]
    summaries = {}
    for name, path in panels:
        panel = pd.read_parquet(path)
        panel["date"] = pd.to_datetime(panel["date"])
        merged = panel.merge(dead, on=["date", "stock_id"], how="left")
        merged["is_dead_fish"] = merged["is_dead_fish"].fillna(False)
        alive = merged[~merged["is_dead_fish"]]
        print(f"[{name}] {len(alive):,} alive rows", file=sys.stderr)
        summaries[name] = compute_summary(alive, cells)

    print(f"\n===  {label}  (~dead universe)  ===")

    regime_order = ("full", "bull_L", "bear_L", "neutral_L", "bull_M", "bear_M", "neutral_M")
    print("\n[OVERALL spread H × regime]  baseline → current  Δ pp")
    print(f"{'H':>3s}  {'regime':>9s}  {'baseline':>10s}  {'current':>10s}  {'Δ pp':>10s}")
    for h in HORIZONS:
        for r in regime_order:
            b = summaries["baseline"]["base"][(h, r)]
            c = summaries["current"]["base"][(h, r)]
            print(f"{h:>3d}  {r:>9s}  {b:>+9.4f}%  {c:>+9.4f}%  {c-b:>+9.4f}pp")

    print("\n[PER-YEAR OVERALL Δ]  current − baseline (pp; dates aligned by intersection)")
    diff_by_h = {
        h: (summaries["current"]["base_daily"][h]
            - summaries["baseline"]["base_daily"][h]).dropna() * 100
        for h in HORIZONS
    }
    all_years = sorted({d.year for s in diff_by_h.values() for d in s.index})
    print(f"{'year':>4s}  {'n_days':>6s}" + "".join(f"  {'H' + str(h) + 'Δ':>9s}" for h in HORIZONS))
    for yr in all_years:
        n_yr = int((diff_by_h[HORIZONS[0]].index.year == yr).sum())
        vals = ""
        for h in HORIZONS:
            s_yr = diff_by_h[h][diff_by_h[h].index.year == yr]
            vals += f"  {s_yr.mean():>+8.4f}" if len(s_yr) else f"  {'n/a':>8s}"
        print(f"{yr:>4d}  {n_yr:>6d}{vals}")

    print("\n[PER-CELL Δ]  baseline → current  ΔΔ pp")
    for cell in cells:
        print(f"\n  {cell}")
        print(f"  {'H':>3s}  {'regime':>9s}  {'baseline':>10s}  {'current':>10s}  {'ΔΔ pp':>10s}")
        for h in HORIZONS:
            for r in regime_order:
                b = summaries["baseline"]["cells"].get((cell, h, r), 0.0)
                c = summaries["current"]["cells"].get((cell, h, r), 0.0)
                print(f"  {h:>3d}  {r:>9s}  {b:>+9.4f}pp  {c:>+9.4f}pp  {c-b:>+9.4f}pp")


if __name__ == "__main__":
    main()
