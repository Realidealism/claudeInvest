"""IS/OOS robustness validation + sensitivity grid.

Stage A — IS/OOS split (the gold standard):
    1. Run the 17-variant ablation suite on IN-SAMPLE only (2017-2020)
    2. Identify the best gate set by IS Sharpe
    3. Apply that gate set to OUT-OF-SAMPLE (2021 -> 2024-09)
    4. Compare OOS Sharpe vs IS Sharpe — generalization test
    5. As a control, also run the "current production" gate (F6+F7+F8)
       on OOS so we can compare honest performance.

Stage B — Sensitivity grid:
    For the production gate (F6+F7+F8), sweep top_k × min_score_floor and
    print the matrix. A smooth plateau means robust; a sharp single peak
    means cherry-picked.

All backtests share the same data load to keep runtime <30 min.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import pandas as pd

from hermit_stock.backtest.ablation import run_ablation_suite
from hermit_stock.backtest.engine import (
    BacktestConfig,
    build_adj_close_table,
    run_backtest,
)
from hermit_stock.backtest.metrics import compute_metrics
from hermit_stock.data.adapters import db_adapter

IS_START = date(2017, 1, 1)
IS_END = date(2020, 12, 31)
OOS_START = date(2021, 1, 1)
OOS_END = date(2024, 9, 30)
FULL_START = date(2017, 1, 1)
FULL_END = date(2024, 9, 30)
TOP_K = 10
FLOOR = 3
PRODUCTION_GATE = frozenset({"F6", "F7", "F8"})


def _run(cfg: BacktestConfig, *, metas, quarterly, monthly, adj, bench) -> dict[str, object]:
    res = run_backtest(
        cfg, metas=metas, quarterly_by_ticker=quarterly,
        monthly_by_ticker=monthly, adj_close=adj,
    )
    m = compute_metrics(res.nav, bench)
    return {
        "label": cfg.label,
        "cumret_pct": round(m.cumulative_return * 100, 2),
        "ann_pct": round(m.annual_return * 100, 2),
        "sharpe": round(m.sharpe, 2),
        "mdd_pct": round(m.max_drawdown * 100, 2),
        "alpha_pct": round((m.alpha or 0) * 100, 2),
    }


def main() -> None:
    print("loading universe ...")
    metas = db_adapter.load_active_stocks()
    tl = [m.ticker for m in metas]
    quarterly = db_adapter.load_all_quarterly_reports(tl)
    monthly = db_adapter.load_all_monthly_revenue(tl)
    prices = db_adapter.load_all_daily_prices(tl)
    divs = db_adapter.load_all_dividends(tl)
    reds = db_adapter.load_all_capital_reductions(tl)
    adj = build_adj_close_table(quarterly, prices, divs, reds)
    bench_pairs = db_adapter.load_index_close("TAIEX")
    bench = pd.Series({d: c for d, c in bench_pairs})
    bench.index = pd.to_datetime(bench.index)
    print(f"loaded: q={len(quarterly)} m={len(monthly)} adj={adj.shape}\n")

    # ---------------- STAGE A: IS ablation ----------------
    print("=" * 60)
    print("STAGE A.1 — IS ablation (2017-2020, 17 variants)")
    print("=" * 60)
    is_base = BacktestConfig(
        start=IS_START, end=IS_END, top_k=TOP_K, min_score_floor=FLOOR,
        label="IS_main",
    )
    is_results = run_ablation_suite(
        is_base,
        metas=metas, quarterly_by_ticker=quarterly, monthly_by_ticker=monthly,
        adj_close=adj,
    )

    is_rows: list[dict[str, object]] = []
    for label, res in is_results.items():
        m = compute_metrics(res.nav, bench)
        is_rows.append({
            "variant": label,
            "is_cumret": round(m.cumulative_return * 100, 2),
            "is_sharpe": round(m.sharpe, 2),
            "is_mdd": round(m.max_drawdown * 100, 2),
        })
    is_df = pd.DataFrame(is_rows).sort_values("is_sharpe", ascending=False)
    print("Top 5 by IS Sharpe:")
    print(is_df.head(5).to_string(index=False))
    print()

    best_is_label = is_df.iloc[0]["variant"]
    print(f"Best IS variant: {best_is_label}")

    # Reconstruct the best variant's enabled_rules from its label
    # (drop_F1 -> remove F1, only_F1 -> only F1, main -> all 8)
    all_rules = frozenset({"F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"})
    best_label_str = str(best_is_label)
    if best_label_str.startswith("drop_"):
        rule = best_label_str.replace("drop_", "")
        is_best_enabled = all_rules - {rule}
        is_best_floor = FLOOR
    elif best_label_str.startswith("only_"):
        rule = best_label_str.replace("only_", "")
        is_best_enabled = frozenset({rule})
        is_best_floor = 1
    else:
        is_best_enabled = all_rules
        is_best_floor = FLOOR

    # ---------------- STAGE A.2: OOS test ----------------
    print()
    print("=" * 60)
    print("STAGE A.2 — apply IS best to OOS (2021-2024)")
    print("=" * 60)
    rows: list[dict[str, object]] = []

    # IS best applied to OOS
    cfg_oos_isbest = BacktestConfig(
        start=OOS_START, end=OOS_END, top_k=TOP_K, min_score_floor=is_best_floor,
        enabled_rules=is_best_enabled,
        label=f"OOS_apply_IS_best({best_is_label})",
    )
    rows.append(_run(cfg_oos_isbest, metas=metas, quarterly=quarterly,
                     monthly=monthly, adj=adj, bench=bench))

    # Production gate (F6+F7+F8) on OOS — sanity check
    cfg_oos_prod = BacktestConfig(
        start=OOS_START, end=OOS_END, top_k=TOP_K, min_score_floor=FLOOR,
        gate_rules=PRODUCTION_GATE,
        label="OOS_production_gate_F6F7F8",
    )
    rows.append(_run(cfg_oos_prod, metas=metas, quarterly=quarterly,
                     monthly=monthly, adj=adj, bench=bench))

    # Production gate also on IS for direct comparison
    cfg_is_prod = BacktestConfig(
        start=IS_START, end=IS_END, top_k=TOP_K, min_score_floor=FLOOR,
        gate_rules=PRODUCTION_GATE,
        label="IS_production_gate_F6F7F8",
    )
    rows.append(_run(cfg_is_prod, metas=metas, quarterly=quarterly,
                     monthly=monthly, adj=adj, bench=bench))

    # Production on full period for reference
    cfg_full_prod = BacktestConfig(
        start=FULL_START, end=FULL_END, top_k=TOP_K, min_score_floor=FLOOR,
        gate_rules=PRODUCTION_GATE,
        label="FULL_production_gate_F6F7F8",
    )
    rows.append(_run(cfg_full_prod, metas=metas, quarterly=quarterly,
                     monthly=monthly, adj=adj, bench=bench))

    is_oos_df = pd.DataFrame(rows)
    print(is_oos_df.to_string(index=False))
    print()

    # ---------------- STAGE B: Sensitivity grid ----------------
    print()
    print("=" * 60)
    print("STAGE B — sensitivity (top_k × floor on full period, gate F6+F7+F8)")
    print("=" * 60)
    grid_rows: list[dict[str, object]] = []
    for top_k in (5, 10, 15):
        for floor in (2, 3, 4):
            cfg = BacktestConfig(
                start=FULL_START, end=FULL_END,
                top_k=top_k, min_score_floor=floor,
                gate_rules=PRODUCTION_GATE,
                label=f"k{top_k}_f{floor}",
            )
            r = _run(cfg, metas=metas, quarterly=quarterly,
                     monthly=monthly, adj=adj, bench=bench)
            r["top_k"] = top_k
            r["floor"] = floor
            grid_rows.append(r)
            print(f"  k={top_k} floor={floor}: cumret={r['cumret_pct']}% sharpe={r['sharpe']}")

    grid_df = pd.DataFrame(grid_rows)
    print("\nSensitivity grid (cumret %):")
    pivot_cum = grid_df.pivot(index="floor", columns="top_k", values="cumret_pct")
    print(pivot_cum.to_string())
    print("\nSensitivity grid (Sharpe):")
    pivot_sh = grid_df.pivot(index="floor", columns="top_k", values="sharpe")
    print(pivot_sh.to_string())

    # Save
    is_df.to_csv("backtest_out_compare/is_ablation.csv", index=False)
    is_oos_df.to_csv("backtest_out_compare/is_oos_comparison.csv", index=False)
    grid_df.to_csv("backtest_out_compare/sensitivity_grid.csv", index=False)
    print("\ndone — outputs in backtest_out_compare/")


if __name__ == "__main__":
    main()
