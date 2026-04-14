"""
Console text report for backtest results.
"""

from __future__ import annotations

from backtest.trade import BacktestResult
from backtest.data import StockData


def print_report(result: BacktestResult, data: StockData) -> None:
    """Print a formatted backtest report to console."""
    print()
    print("=" * 100)
    print(f"  回測報告: {data.stock_id} {data.stock_name}")
    print("=" * 100)
    print(f"  策略:     {result.strategy_name}")
    print(f"  期間:     {result.start_date} ~ {result.end_date}")
    print(f"  初始資金: {result.initial_capital:,.0f}")
    print()

    # Trade details
    if result.trades:
        print(f"{'#':>3s}  {'方向':>4s}  {'進場日':>12s}  {'進場價':>8s}  "
              f"{'出場日':>12s}  {'出場價':>8s}  {'持有':>4s}  "
              f"{'損益':>10s}  {'報酬率':>7s}  進場原因  →  出場原因")
        print("-" * 120)

        for idx, t in enumerate(result.trades, 1):
            direction = "做多" if t.direction == "long" else "做空"
            pnl_str = f"{t.pnl:>+,.0f}"
            pct_str = f"{t.pnl_pct:>+.2%}"
            entry_r = ", ".join(t.entry_reasons)
            exit_r = ", ".join(t.exit_reasons)

            print(f"{idx:3d}  {direction:>4s}  {str(t.entry_date):>12s}  "
                  f"{t.entry_price:8.2f}  {str(t.exit_date):>12s}  "
                  f"{t.exit_price:8.2f}  {t.holding_days:4d}  "
                  f"{pnl_str:>10s}  {pct_str:>7s}  "
                  f"{entry_r}  →  {exit_r}")

        if any(t.cash_dividends > 0 for t in result.trades):
            print()
            print("  * 含除權息交易:")
            for idx, t in enumerate(result.trades, 1):
                if t.cash_dividends > 0:
                    print(f"    #{idx}: 現金股利 {t.cash_dividends:,.0f}")
    else:
        print("  (無交易)")

    # Summary stats
    print()
    print("=" * 100)
    print("  績效摘要")
    print("=" * 100)
    print(f"  總報酬率:   {result.total_return:>+.2%}")
    print(f"  勝率:       {result.win_rate:.1%} "
          f"({sum(1 for t in result.trades if t.pnl > 0)}/{result.total_trades})")
    print(f"  平均獲利:   {result.avg_win:>+.2%}")
    print(f"  平均虧損:   {result.avg_loss:>+.2%}")
    pf = f"{result.profit_factor:.2f}" if result.profit_factor != float('inf') else "∞"
    print(f"  獲利因子:   {pf}")
    print(f"  最大回撤:   {result.max_drawdown:>+.2%}")
    print(f"  夏普比率:   {result.sharpe_ratio:.2f}")
    print(f"  總交易次數: {result.total_trades}")
    print()
