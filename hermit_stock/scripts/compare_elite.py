"""Compare baseline vs elite-override on full 2017-2024 backtest.

  A: baseline      — gate F6+F7+F8, floor=3, no elite override
  B: +elite        — same + elite override (rescue F7/F8-only fails when
                     score>=7 + elite quality)
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from hermit_stock.backtest.engine import BacktestConfig, build_adj_close_table, run_backtest
from hermit_stock.backtest.metrics import compute_metrics
from hermit_stock.data.adapters import db_adapter

START = date(2017, 1, 1)
END = date(2024, 9, 30)
GATES = frozenset({"F6", "F7", "F8"})


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
    print(f"loaded: q={len(quarterly)} adj={adj.shape}\n")

    rows: list[dict[str, object]] = []
    for label, override in [("(A) baseline", False), ("(B) +elite override", True)]:
        cfg = BacktestConfig(
            start=START, end=END, top_k=10, min_score_floor=3,
            gate_rules=GATES, elite_override=override, label=label,
        )
        print(f"=== {label} ===")
        res = run_backtest(
            cfg, metas=metas, quarterly_by_ticker=quarterly,
            monthly_by_ticker=monthly, adj_close=adj,
        )
        m = compute_metrics(res.nav, bench)
        rows.append({
            "variant": label,
            "cumret_pct": round(m.cumulative_return * 100, 2),
            "ann_pct": round(m.annual_return * 100, 2),
            "sharpe": round(m.sharpe, 2),
            "mdd_pct": round(m.max_drawdown * 100, 2),
            "alpha_pct": round((m.alpha or 0) * 100, 2),
            "beta": round(m.beta or 0, 2),
            "n_trades": len(res.portfolio.trade_log),
        })
        print(rows[-1])
        print()

    print("=" * 60)
    print(pd.DataFrame(rows).to_string(index=False))


if __name__ == "__main__":
    main()
