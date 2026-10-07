"""Adoption verdict for the 波動 (low-volatility) cell, under the 2026-07-11 skew gate.

Baseline is taken as `total_long - 波動_long` on the rebuilt panel, not as a comparison
against the old panel.  That is deliberate: a rebuild-vs-rebuild diff carries 64.8% row
drift (the null-rebuild baseline measured 2026-07-07), which swamps effects this size.
Frozen-panel column arithmetic is exact -- the cell is continuous and additive, so
subtracting its column reproduces the board without it, bit for bit.

Reports the standard grid (H x regime decile spread, per-year) plus the three metrics the
skew gate added, because this cell is expected to LOSE mean spread and win everywhere else.
That is the whole trade; the gate exists so it can be seen rather than hidden.

Usage:
    python -m analysis._score_vol_verdict
"""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from analysis._score_ablation import regime_from_breadth

PANEL = "tmp/score_panel.parquet"
DEAD = "tmp/dead_fish_panel.parquet"
CELL = "波動_long"
HS = (5, 20, 60)
REGIMES = ("full", "bull_L", "bear_L")


def _dec(s: pd.Series) -> pd.Series:
    return pd.qcut(s.rank(method="first"), 10, labels=False, duplicates="drop")


def _stats(panel: pd.DataFrame, score: pd.Series, h: str, mask: pd.Series) -> dict:
    sub = panel[mask & panel[h].notna()]
    if len(sub) < 1000:
        return {}
    dec = score[sub.index].groupby(sub["date"]).transform(_dec)
    d10 = sub.loc[dec.eq(9), h]
    d1 = sub.loc[dec.eq(0), h]
    med = sub.loc[dec.eq(9), f"{h}_mm"]
    return {
        "mean": (d10.mean() - d1.mean()) * 100,
        "median": (d10.median() - d1.median()) * 100,
        "win": (d10 > med).mean() * 100,
        "p05": d10.quantile(0.05) * 100,
    }


def main() -> None:
    panel = pd.read_parquet(PANEL)
    panel["date"] = pd.to_datetime(panel["date"])
    if CELL not in panel.columns:
        sys.exit(f"panel has no {CELL} column -- wrong panel?")

    dead = pd.read_parquet(DEAD)
    dead["date"] = pd.to_datetime(dead["date"])
    panel = panel.merge(dead, on=["date", "stock_id"], how="left")
    panel["is_dead_fish"] = panel["is_dead_fish"].fillna(False)
    panel = panel[~panel["is_dead_fish"]].reset_index(drop=True)
    panel["year"] = panel["date"].dt.year
    for h in HS:
        panel[f"fwd_{h}_mm"] = panel.groupby("date")[f"fwd_{h}"].transform("median")

    print(f"~dead rows={len(panel):,}  cell nonzero={100 * panel[CELL].ne(0).mean():.1f}%  "
          f"pts std={panel[CELL].std():.2f}  range=[{panel[CELL].min():.1f}, "
          f"{panel[CELL].max():.1f}]", file=sys.stderr)

    base = panel["total_long"] - panel[CELL]
    new = panel["total_long"]

    # regime_from_breadth normalises its buckets to pd.Timestamp, so match on the
    # Timestamp column, not on .dt.date -- the two never compare equal.
    reg = regime_from_breadth(panel["date"].unique())
    masks = {"full": pd.Series(True, index=panel.index)}
    for r in ("bull_L", "bear_L"):
        masks[r] = panel["date"].isin(reg.get(r, set()))
        print(f"  regime {r}: {masks[r].sum():,} rows", file=sys.stderr)

    print("\n=== H x regime: baseline (no cell) -> with cell ===")
    print("  mean spread is EXPECTED to fall; the gate is median / win / tail.\n")
    for metric, label in (("mean", "mean spread"), ("median", "median spread"),
                          ("win", "D10 win%"), ("p05", "D10 worst-5%")):
        print(f"--- {label} ---")
        print(f"  {'H':>3}" + "".join(f"{r:>26}" for r in REGIMES))
        for h in HS:
            cells = []
            for r in REGIMES:
                b = _stats(panel, base, f"fwd_{h}", masks[r])
                n = _stats(panel, new, f"fwd_{h}", masks[r])
                if not b or not n:
                    cells.append(f"{'n/a':>26}")
                    continue
                cells.append(f"{b[metric]:>9.3f} ->{n[metric]:>8.3f} "
                             f"({n[metric] - b[metric]:+.3f})".rjust(26))
            print(f"  {h:>3}" + "".join(cells))
        print()

    print("=== per-year delta (with cell - baseline), fwd_60 ===")
    years = sorted(panel["year"].unique())
    print(f"  {'metric':>8}" + "".join(f"{y:>8}" for y in years))
    for metric in ("mean", "median"):
        cells = []
        for y in years:
            m = panel["year"].eq(y)
            b = _stats(panel, base, "fwd_60", m)
            n = _stats(panel, new, "fwd_60", m)
            cells.append("     n/a" if not b or not n
                         else f"{n[metric] - b[metric]:>+8.2f}")
        print(f"  {metric:>8}" + "".join(cells))

    print("\n=== absolute per-year median decile spread (the k=15 anchor) ===")
    print(f"  {'':>8}" + "".join(f"{y:>8}" for y in years))
    for tag, sc in (("baseline", base), ("with cell", new)):
        cells = []
        for y in years:
            s = _stats(panel, sc, "fwd_60", panel["year"].eq(y))
            cells.append("     n/a" if not s else f"{s['median']:>+8.2f}")
        print(f"  {tag:>9}" + "".join(cells))


if __name__ == "__main__":
    main()
