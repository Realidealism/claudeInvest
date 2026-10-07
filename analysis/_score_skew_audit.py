"""Skew audit of the production ScoreBoard — is the mean-based decile spread being
paid in right-tail lottery returns rather than in reliable outperformance?

Motivated by the 2026-07-11 volatility probe: within a score decile, volatility sorts
MEAN forward return positively but MEDIAN forward return negatively (win rate 54.4% at
the low-vol end vs 45.1% at the high-vol end).  Decile spread is a mean, so any cell
that happens to tilt toward high-vol names is rewarded by the metric while degrading
win rate and max loss -- two of the project's three stated goals.

This asks how far that has already happened:
  A. does total_long itself lean toward high-volatility stocks?
  B. which individual cells load on volatility?
  C. for each cell, is the bull/bear cohort's edge visible in the MEDIAN and the win
     rate, or only in the MEAN?  Mean-only = the cell is selling lottery tickets.

Zero rebuild; reads the frozen production panel and never writes it.

Usage:
    python -m analysis._score_skew_audit [max_stocks]
"""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd

PANEL = "tmp/score_panel.parquet"
DEAD = "tmp/dead_fish_panel.parquet"

VOL_WIN = 60
H = "fwd_60"
NON_CELL = {"date", "stock_id", "close", "total_long", "fwd_5", "fwd_20", "fwd_60"}


def main() -> None:
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0

    panel = pd.read_parquet(PANEL)
    panel["date"] = pd.to_datetime(panel["date"])
    if limit:
        keep = panel["stock_id"].drop_duplicates().head(limit)
        panel = panel[panel["stock_id"].isin(keep)]

    cells = [c for c in panel.columns if c not in NON_CELL]

    print("computing realized vol ...", file=sys.stderr)
    parts = []
    for sid, g in panel.groupby("stock_id", sort=False):
        g = g.sort_values("date")
        c = g["close"].to_numpy(dtype=np.float64)
        lr = np.diff(np.log(np.maximum(c, 1e-9)), prepend=np.nan)
        parts.append(
            pd.DataFrame({
                "date": g["date"].to_numpy(),
                "stock_id": g["stock_id"].to_numpy(),
                "vol": pd.Series(lr).rolling(VOL_WIN, min_periods=VOL_WIN // 2).std().to_numpy(),
            })
        )
    panel = panel.merge(pd.concat(parts, ignore_index=True), on=["date", "stock_id"], how="left")

    dead = pd.read_parquet(DEAD)
    dead["date"] = pd.to_datetime(dead["date"])
    panel = panel.merge(dead, on=["date", "stock_id"], how="left")
    panel["is_dead_fish"] = panel["is_dead_fish"].fillna(False)
    panel = panel[~panel["is_dead_fish"] & panel["vol"].notna()]
    print(f"~dead rows={len(panel):,}  cells={len(cells)}", file=sys.stderr)

    panel["fwd_mkt"] = panel.groupby("date")[H].transform("mean")
    panel["fwd_med"] = panel.groupby("date")[H].transform("median")

    # ---- A: does the score itself lean toward high-vol names? ----
    print("\n=== A  cross-sectional Spearman(total_long, volatility), per day ===")
    rhos = []
    for _, g in panel.groupby("date"):
        sub = g[["total_long", "vol"]].dropna()
        if len(sub) < 30:
            continue
        r = sub["total_long"].rank().corr(sub["vol"].rank())
        if not np.isnan(r):
            rhos.append(r)
    arr = np.array(rhos)
    print(f"  mean rho={arr.mean():+.4f}  median={np.median(arr):+.4f}  "
          f"pos-days={100 * (arr > 0).mean():.1f}%  (n_days={len(arr)})")

    panel["dec"] = panel.groupby("date")["total_long"].transform(
        lambda s: pd.qcut(s.rank(method="first"), 10, labels=False, duplicates="drop")
    )
    panel["volr"] = panel.groupby("date")["vol"].transform(
        lambda s: s.rank(pct=True)
    )
    print("\n  mean volatility percentile-rank of each score decile (0.50 = neutral):")
    band = panel.groupby("dec")["volr"].mean()
    print("    " + "  ".join(f"D{i + 1}={band.get(i, np.nan):.3f}" for i in range(10)))

    # ---- B: which cells load on volatility? ----
    print("\n=== B  cross-sectional Spearman(cell points, volatility) ===")
    print("  positive = the cell hands its bullish points to high-vol names")
    loads = {}
    for c in cells:
        if panel[c].abs().sum() == 0:
            continue
        rs = []
        for _, g in panel.groupby("date"):
            sub = g[[c, "vol"]].dropna()
            if len(sub) < 30 or sub[c].nunique() < 3:
                continue
            r = sub[c].rank().corr(sub["vol"].rank())
            if not np.isnan(r):
                rs.append(r)
        if rs:
            loads[c] = float(np.mean(rs))
    for c, v in sorted(loads.items(), key=lambda kv: -abs(kv[1])):
        flag = "  <-- loads on vol" if abs(v) > 0.15 else ""
        print(f"  {c:<22} rho={v:+.4f}{flag}")

    # ---- C: is each cell's edge in the mean only, or also the median? ----
    print(f"\n=== C  per-cell cohort edge: MEAN vs MEDIAN vs win-rate ({H}, vs same-day universe) ===")
    print("  mean>0 but median<=0  =>  the cell's edge is right-tail skew, not reliability")
    print(f"  {'cell':<22}{'side':<6}{'mean':>8}{'median':>9}{'beat%':>8}{'n':>10}  verdict")
    for c in cells:
        if panel[c].abs().sum() == 0:
            continue
        for side, m in (("bull", panel[c] > 0), ("bear", panel[c] < 0)):
            sub = panel.loc[m & panel[H].notna(), [H, "fwd_mkt", "fwd_med"]]
            if len(sub) < 500:
                continue
            mean_ex = (sub[H] - sub["fwd_mkt"]).mean() * 100
            med_ex = (sub[H] - sub["fwd_med"]).median() * 100
            beat = sub[H].gt(sub["fwd_med"]).mean() * 100
            if side == "bull":
                bad = mean_ex > 0 and (med_ex <= 0 or beat < 50)
            else:
                bad = mean_ex < 0 and (med_ex >= 0 or beat > 50)
            verdict = "SKEW" if bad else ""
            print(f"  {c:<22}{side:<6}{mean_ex:>+8.2f}{med_ex:>+9.2f}{beat:>7.1f}%"
                  f"{len(sub):>10,}  {verdict}")


if __name__ == "__main__":
    main()
