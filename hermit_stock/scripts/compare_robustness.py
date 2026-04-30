"""Compare four backtest variants for robustness check:

  A: baseline       — active-only universe, no liquidity filter (current "best")
  B: +delisted      — include delisted stocks (eliminates survivorship bias)
  C: +liquidity     — active-only, min 60d avg turnover ≥ 5000 萬
  D: +both          — delisted + liquidity (most realistic)

All 4 use the empirical-best strategy: gate F6+F7+F8, floor=3, top_k=10.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from hermit_stock.backtest.engine import (
    BacktestConfig,
    build_adj_close_table,
    run_backtest,
)
from hermit_stock.backtest.metrics import compute_metrics
from hermit_stock.data.adapters import db_adapter

START = date(2017, 1, 1)
END = date(2024, 9, 30)
TOP_K = 10
FLOOR = 3
GATES = frozenset({"F6", "F7", "F8"})
MIN_TURNOVER = 50_000_000.0  # 5000 萬 NTD/day


def _run(
    label: str,
    *,
    metas, quarterly, monthly, adj, turnover_60d, bench,
    use_liquidity: bool,
) -> dict[str, object]:
    cfg = BacktestConfig(
        start=START, end=END, top_k=TOP_K, min_score_floor=FLOOR,
        gate_rules=GATES,
        min_avg_turnover=MIN_TURNOVER if use_liquidity else 0.0,
        label=label,
    )
    print(f"\n=== {label} (universe={len(metas)}, liquidity={use_liquidity}) ===")
    res = run_backtest(
        cfg, metas=metas, quarterly_by_ticker=quarterly,
        monthly_by_ticker=monthly, adj_close=adj,
        turnover_60d=turnover_60d if use_liquidity else None,
    )
    m = compute_metrics(res.nav, bench)
    return {
        "variant": label,
        "n_universe": len(metas),
        "cumret_pct": round(m.cumulative_return * 100, 2),
        "ann_pct": round(m.annual_return * 100, 2),
        "sharpe": round(m.sharpe, 2),
        "mdd_pct": round(m.max_drawdown * 100, 2),
        "alpha_pct": round((m.alpha or 0) * 100, 2),
        "n_trades": len(res.portfolio.trade_log),
    }


def main() -> None:
    bench_pairs = db_adapter.load_index_close("TAIEX")
    bench = pd.Series({d: c for d, c in bench_pairs})
    bench.index = pd.to_datetime(bench.index)

    rows: list[dict[str, object]] = []

    print("=" * 60)
    print("(A) baseline — active-only, no liquidity filter")
    print("=" * 60)
    metas_a = db_adapter.load_active_stocks(include_delisted=False)
    tl_a = [m.ticker for m in metas_a]
    q_a = db_adapter.load_all_quarterly_reports(tl_a)
    m_a = db_adapter.load_all_monthly_revenue(tl_a)
    p_a = db_adapter.load_all_daily_prices(tl_a)
    d_a = db_adapter.load_all_dividends(tl_a)
    r_a = db_adapter.load_all_capital_reductions(tl_a)
    adj_a = build_adj_close_table(q_a, p_a, d_a, r_a)
    turnover_a = db_adapter.load_turnover_table(tl_a).rolling(window=60, min_periods=20).mean()
    print(f"loaded: q={len(q_a)} m={len(m_a)} p={len(p_a)} adj={adj_a.shape}")

    rows.append(_run(
        "(A) active-only", metas=metas_a, quarterly=q_a, monthly=m_a, adj=adj_a,
        turnover_60d=None, bench=bench, use_liquidity=False,
    ))
    rows.append(_run(
        "(C) active+liquidity", metas=metas_a, quarterly=q_a, monthly=m_a, adj=adj_a,
        turnover_60d=turnover_a, bench=bench, use_liquidity=True,
    ))

    print()
    print("=" * 60)
    print("(B,D) including delisted stocks")
    print("=" * 60)
    metas_full = db_adapter.load_active_stocks(include_delisted=True)
    tl_full = [m.ticker for m in metas_full]
    print(f"full universe size: {len(tl_full)} (vs active-only {len(metas_a)})")
    q_full = db_adapter.load_all_quarterly_reports(tl_full)
    m_full = db_adapter.load_all_monthly_revenue(tl_full)
    p_full = db_adapter.load_all_daily_prices(tl_full)
    d_full = db_adapter.load_all_dividends(tl_full)
    r_full = db_adapter.load_all_capital_reductions(tl_full)
    adj_full = build_adj_close_table(q_full, p_full, d_full, r_full)
    turnover_full = (
        db_adapter.load_turnover_table(tl_full).rolling(window=60, min_periods=20).mean()
    )
    print(f"loaded: q={len(q_full)} m={len(m_full)} p={len(p_full)} adj={adj_full.shape}")

    rows.append(_run(
        "(B) include-delisted", metas=metas_full, quarterly=q_full, monthly=m_full,
        adj=adj_full, turnover_60d=None, bench=bench, use_liquidity=False,
    ))
    rows.append(_run(
        "(D) delisted+liquidity", metas=metas_full, quarterly=q_full, monthly=m_full,
        adj=adj_full, turnover_60d=turnover_full, bench=bench, use_liquidity=True,
    ))

    df = pd.DataFrame(rows)
    print()
    print("=" * 60)
    print("FINAL COMPARISON")
    print("=" * 60)
    print(df.to_string(index=False))
    df.to_csv("backtest_out_compare/robustness.csv", index=False)


if __name__ == "__main__":
    main()
