"""Ad-hoc: full-cell ablation comparing full universe vs ~dead_fish filter.

Generalizes _score_obv_ablation_dead.py to every category × timeframe
cell present in the panel (non-zero coverage). Reveals which cells'
verdicts shift after death-fish filtering.

Usage:
    python -m analysis._score_all_ablation_dead
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

EXCLUDE_COLS = {"date", "stock_id", "close", "total_long", "is_dead_fish"}


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


def compute_cell_deltas(df: pd.DataFrame, cells: list[str]) -> dict:
    """Returns {(cell, h, regime): delta_pp} for every cell + regime."""
    date_n = df.groupby("date").size()
    valid_dates = date_n[date_n >= MIN_DAILY_N].index
    full_d = set(valid_dates)
    bull_d = {d for d in valid_dates if d.year in BULL_YEARS}
    bear_d = {d for d in valid_dates if d.year in BEAR_YEARS}
    regimes = [("full", full_d), ("bull", bull_d), ("bear", bear_d)]

    df = df.copy()
    df["score_base"] = df["total_long"]
    base_cache = {}
    for h in HORIZONS:
        s_base = daily_spread_series(df, "score_base", f"fwd_{h}")
        for r_name, r_dates in regimes:
            base_cache[(h, r_name)] = regime_mean(s_base, r_dates) * 100

    out = {}
    for cell in cells:
        df[f"score_no_{cell}"] = df["total_long"] - df[cell]
        for h in HORIZONS:
            s_ab = daily_spread_series(df, f"score_no_{cell}", f"fwd_{h}")
            for r_name, r_dates in regimes:
                m_base = base_cache[(h, r_name)]
                m_ab = regime_mean(s_ab, r_dates) * 100
                out[(cell, h, r_name)] = m_base - m_ab
        df.drop(columns=[f"score_no_{cell}"], inplace=True)
    return out


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    panel = pd.read_parquet(PANEL)
    panel["date"] = pd.to_datetime(panel["date"])

    dead = pd.read_parquet(DEAD)
    dead["date"] = pd.to_datetime(dead["date"])

    merged = panel.merge(dead, on=["date", "stock_id"], how="left")
    merged["is_dead_fish"] = merged["is_dead_fish"].fillna(False)

    # Discover active cells (non-zero in panel)
    cells = []
    for c in merged.columns:
        if c in EXCLUDE_COLS or c.startswith("fwd_"):
            continue
        if (merged[c] != 0).any():
            cells.append(c)
    print(f"Active cells: {len(cells)}", file=sys.stderr)

    full = compute_cell_deltas(merged, cells)
    print("Computed FULL universe deltas", file=sys.stderr)

    alive = compute_cell_deltas(merged[~merged["is_dead_fish"]], cells)
    print("Computed ~DEAD deltas", file=sys.stderr)

    # Side-by-side table per horizon × regime
    for h in HORIZONS:
        for r in ("full", "bull", "bear"):
            print(f"\n===  H={h} {r}  (Δspread per cell, pp; +/- pp = sign-flip)  ===")
            print(f"{'cell':<22s}  {'full':>9s}  {'~dead':>9s}  {'Δ':>9s}  flag")
            print("-" * 65)
            rows = []
            for c in cells:
                f = full.get((c, h, r), 0.0)
                a = alive.get((c, h, r), 0.0)
                rows.append((c, f, a, a - f))
            # Sort by descending ~dead Δ
            rows.sort(key=lambda x: -x[2])
            for c, f, a, d in rows:
                flag = ""
                if f < -0.005 and a > 0.005:
                    flag = "REVIVE"
                elif f > 0.005 and a < -0.005:
                    flag = "DROP?"
                elif abs(a - f) > 0.03:
                    flag = "SHIFT"
                print(f"{c:<22s}  {f:>+8.4f}  {a:>+8.4f}  {d:>+8.4f}  {flag}")


if __name__ == "__main__":
    main()
