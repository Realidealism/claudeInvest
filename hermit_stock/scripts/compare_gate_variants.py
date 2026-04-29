"""Run 4 gate variants once, share data load, write comparison table.

Variants:
    (a) gate F7+F8, floor=4         — strictness compensation
    (b) gate F6+F7+F8, floor=3      — promote F6 to gate
    (c) gate F2+F6+F7+F8, floor=3   — full growth+momentum gate
    (d) main, floor=3               — baseline (current best with F6 fix)
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


def main() -> None:
    print("loading universe ...")
    metas = db_adapter.load_active_stocks()
    tl = [m.ticker for m in metas]
    quarterly = db_adapter.load_all_quarterly_reports(tl)
    monthly = db_adapter.load_all_monthly_revenue(tl)
    prices = db_adapter.load_all_daily_prices(tl)
    divs = db_adapter.load_all_dividends(tl)
    reds = db_adapter.load_all_capital_reductions(tl)
    print(f"loaded: q={len(quarterly)} m={len(monthly)} p={len(prices)}")
    adj = build_adj_close_table(quarterly, prices, divs, reds)
    print(f"adj_close: {adj.shape}")

    bench_pairs = db_adapter.load_index_close("TAIEX")
    bench = pd.Series({d: c for d, c in bench_pairs})
    bench.index = pd.to_datetime(bench.index)

    variants = [
        BacktestConfig(
            start=START,
            end=END,
            top_k=TOP_K,
            min_score_floor=4,
            gate_rules=frozenset({"F7", "F8"}),
            label="(a) gate_F7F8_floor4",
        ),
        BacktestConfig(
            start=START,
            end=END,
            top_k=TOP_K,
            min_score_floor=3,
            gate_rules=frozenset({"F6", "F7", "F8"}),
            label="(b) gate_F6F7F8_floor3",
        ),
        BacktestConfig(
            start=START,
            end=END,
            top_k=TOP_K,
            min_score_floor=3,
            gate_rules=frozenset({"F2", "F6", "F7", "F8"}),
            label="(c) gate_F2F6F7F8_floor3",
        ),
        BacktestConfig(
            start=START,
            end=END,
            top_k=TOP_K,
            min_score_floor=3,
            label="(d) main_floor3",
        ),
    ]

    rows: list[dict[str, object]] = []
    for cfg in variants:
        print(f"\n=== {cfg.label} ===")
        result = run_backtest(
            cfg,
            metas=metas,
            quarterly_by_ticker=quarterly,
            monthly_by_ticker=monthly,
            adj_close=adj,
        )
        m = compute_metrics(result.nav, bench)
        rows.append(
            {
                "variant": cfg.label,
                "cumret_pct": round(m.cumulative_return * 100, 2),
                "ann_pct": round(m.annual_return * 100, 2),
                "sharpe": round(m.sharpe, 2),
                "mdd_pct": round(m.max_drawdown * 100, 2),
                "alpha_pct": round((m.alpha or 0) * 100, 2),
                "beta": round(m.beta or 0, 2),
                "n_rebal": len(result.rebalance_dates_used),
                "n_trade": len(result.portfolio.trade_log),
            }
        )
        print(rows[-1])

    df = pd.DataFrame(rows)
    print("\n=== SUMMARY ===")
    print(df.to_string(index=False))
    df.to_csv("backtest_out_compare/gate_variants.csv", index=False)
    print("\nwrote backtest_out_compare/gate_variants.csv")


if __name__ == "__main__":
    main()
