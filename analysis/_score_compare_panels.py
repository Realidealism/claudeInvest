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

CURRENT = Path("tmp/score_panel.parquet")
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


def regime_mean(s: pd.Series, dates: set) -> float:
    return s[s.index.isin(dates)].mean()


def compute_summary(df: pd.DataFrame, cells_of_interest: list[str]) -> dict:
    date_n = df.groupby("date").size()
    valid = date_n[date_n >= MIN_DAILY_N].index
    full = set(valid)
    bull = {d for d in valid if d.year in BULL_YEARS}
    bear = {d for d in valid if d.year in BEAR_YEARS}
    regimes = [("full", full), ("bull", bull), ("bear", bear)]

    df = df.copy()
    df["score_base"] = df["total_long"]

    out = {"base": {}, "cells": {}}
    for h in HORIZONS:
        s_base = daily_spread_series(df, "score_base", f"fwd_{h}")
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

    print("\n[OVERALL spread H × regime]  baseline → current  Δ pp")
    print(f"{'H':>3s}  {'regime':>6s}  {'baseline':>10s}  {'current':>10s}  {'Δ pp':>10s}")
    for h in HORIZONS:
        for r in ("full", "bull", "bear"):
            b = summaries["baseline"]["base"][(h, r)]
            c = summaries["current"]["base"][(h, r)]
            print(f"{h:>3d}  {r:>6s}  {b:>+9.4f}%  {c:>+9.4f}%  {c-b:>+9.4f}pp")

    print("\n[PER-CELL Δ]  baseline → current  ΔΔ pp")
    for cell in cells:
        print(f"\n  {cell}")
        print(f"  {'H':>3s}  {'regime':>6s}  {'baseline':>10s}  {'current':>10s}  {'ΔΔ pp':>10s}")
        for h in HORIZONS:
            for r in ("full", "bull", "bear"):
                b = summaries["baseline"]["cells"].get((cell, h, r), 0.0)
                c = summaries["current"]["cells"].get((cell, h, r), 0.0)
                print(f"  {h:>3d}  {r:>6s}  {b:>+9.4f}pp  {c:>+9.4f}pp  {c-b:>+9.4f}pp")


if __name__ == "__main__":
    main()
