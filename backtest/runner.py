"""
CLI entry point for backtesting.

Usage:
    python -m backtest --stock 2330
    python -m backtest --stock 2330 --start 2023-01-01 --end 2025-12-31
    python -m backtest --stock 2330 --chart output.png
"""

from __future__ import annotations

import argparse
import sys
import io
from datetime import date

# Ensure UTF-8 output on Windows
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from backtest.data import load_stock_data
from backtest.strategy import (
    Strategy,
    TrailingStopConfig,
    bool_condition,
    threshold_condition,
)
from backtest.engine import run_backtest, InsufficientDataError
from backtest.report import print_report
from backtest.chart import plot_backtest


def example_strategy() -> Strategy:
    """
    Placeholder strategy for testing the framework.

    Uses OBV signal_up/down as entry/exit with SMA(8) trailing stop.
    Replace with real conditions later.
    """
    s = Strategy("OBV 範例策略")

    s.long_entry = [
        bool_condition("OBV買訊", lambda d: d.obv_result.signal_up),
    ]
    s.long_exit = [
        bool_condition("OBV賣訊", lambda d: d.obv_result.signal_down),
    ]
    s.short_entry = [
        bool_condition("OBV賣訊", lambda d: d.obv_result.signal_down),
    ]
    s.short_exit = [
        bool_condition("OBV買訊", lambda d: d.obv_result.signal_up),
    ]
    s.trailing_stop = TrailingStopConfig(
        defense_source=lambda d: d.close_result.ma.sma[8],
    )
    return s


def tip_breakout_strategy() -> Strategy:
    """Tip breakout + pure wave filters."""
    s = Strategy("波浪突破前高")

    s.long_entry = [
        bool_condition("突破前高", lambda d: d.wave_result.tip_breakout_up),
        bool_condition("上升浪", lambda d: d.wave_result.direction),
        bool_condition("波浪中價上", lambda d: d.wave_result.close_cross_wave_d2ma),
        bool_condition("浪量增", lambda d: d.wave_result.wave_volume_up),
    ]
    s.long_exit = [
        bool_condition("跌破前低", lambda d: d.wave_result.tip_breakout_down),
        bool_condition("高低下沉", lambda d: d.wave_result.sink),
    ]
    s.trailing_stop = TrailingStopConfig(
        defense_source=lambda d: d.wave_result.down_price0,
    )
    return s


def sink_reversal_strategy() -> Strategy:
    """Sink reversal + pure wave filters."""
    s = Strategy("下沉反轉")

    s.long_entry = [
        bool_condition("下沉反轉", lambda d: d.wave_result.sink_reversal),
        bool_condition("上升浪", lambda d: d.wave_result.direction),
        bool_condition("波浪中價上", lambda d: d.wave_result.close_cross_wave_d2ma),
        bool_condition("浪量增", lambda d: d.wave_result.wave_volume_up),
    ]
    s.long_exit = [
        bool_condition("再次下沉", lambda d: d.wave_result.sink),
        bool_condition("跌破前低", lambda d: d.wave_result.tip_breakout_down),
    ]
    s.trailing_stop = TrailingStopConfig(
        defense_source=lambda d: d.wave_result.down_price0,
    )
    return s


def tip_trend_strategy() -> Strategy:
    """Tip breakout + wave trend composite filter."""
    s = Strategy("突破前高+趨勢")

    s.long_entry = [
        bool_condition("突破前高", lambda d: d.wave_result.tip_breakout_up),
        bool_condition("上升浪", lambda d: d.wave_result.direction),
        threshold_condition("趨勢正向", lambda d: d.wave_result.wave_trend.composite, ">", 0),
    ]
    s.long_exit = [
        bool_condition("跌破前低", lambda d: d.wave_result.tip_breakout_down),
        bool_condition("高低下沉", lambda d: d.wave_result.sink),
    ]
    s.trailing_stop = TrailingStopConfig(
        defense_source=lambda d: d.wave_result.down_price0,
    )
    return s


def _trend_only(name, trend_name, trend_accessor, entry_th=0.1, exit_th=-0.05):
    """Pure trend-following with asymmetric thresholds: enter when trend >
    entry_th, exit earlier when trend < exit_th."""
    s = Strategy(name)
    s.long_entry = [
        threshold_condition(trend_name, trend_accessor, ">", entry_th),
    ]
    s.long_exit = [
        threshold_condition(trend_name + "轉弱", trend_accessor, "<", exit_th),
    ]
    return s


def trend_short_strategy() -> Strategy:
    return _trend_only("短期趨勢", "短期正向",
                       lambda d: d.wave_result.wave_trend.short)

def trend_medium_strategy() -> Strategy:
    return _trend_only("中期趨勢", "中期正向",
                       lambda d: d.wave_result.wave_trend.medium)

def trend_long_strategy() -> Strategy:
    return _trend_only("長期趨勢", "長期正向",
                       lambda d: d.wave_result.wave_trend.long)

def trend_composite_strategy() -> Strategy:
    return _trend_only("綜合趨勢", "綜合正向",
                       lambda d: d.wave_result.wave_trend.composite)


STRATEGIES = {
    "obv": example_strategy,
    "tip": tip_breakout_strategy,
    "tip_trend": tip_trend_strategy,
    "sink": sink_reversal_strategy,
    "t_short": trend_short_strategy,
    "t_med": trend_medium_strategy,
    "t_long": trend_long_strategy,
    "t_comp": trend_composite_strategy,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Taiwan stock backtester")
    parser.add_argument("--stock", required=True, help="Stock ID (e.g. 2330)")
    parser.add_argument("--strategy", type=str, default=None,
                        help=f"Strategy name ({', '.join(STRATEGIES)}). "
                             "Omit to run all strategies.")
    parser.add_argument("--start", type=date.fromisoformat, default=None,
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=date.fromisoformat, default=None,
                        help="End date (YYYY-MM-DD)")
    parser.add_argument("--capital", type=float, default=1_000_000,
                        help="Initial capital (default: 1000000)")
    parser.add_argument("--shares", type=int, default=1000,
                        help="Shares per trade (default: 1000)")
    parser.add_argument("--chart", type=str, default=None,
                        help="Save chart to file (e.g. output.png)")
    return parser.parse_args()


def main():
    args = parse_args()

    print(f"載入 {args.stock} 資料中...")
    data = load_stock_data(args.stock, args.start, args.end)
    print(f"資料: {data.dates[0]} ~ {data.dates[-1]} ({data.n} 天)")

    if args.strategy:
        if args.strategy not in STRATEGIES:
            print(f"未知策略: {args.strategy}")
            print(f"可用策略: {', '.join(STRATEGIES)}")
            return
        strategies_to_run = [STRATEGIES[args.strategy]]
    else:
        strategies_to_run = list(STRATEGIES.values())

    for strategy_fn in strategies_to_run:
        strategy = strategy_fn()
        print(f"\n策略: {strategy.name}")
        print(f"執行回測中...")

        try:
            result = run_backtest(
                data, strategy,
                capital=args.capital,
                shares_per_trade=args.shares,
            )
        except InsufficientDataError as e:
            print(f"\n⚠ 跳過回測: {e}")
            continue

        print_report(result, data)

        if args.chart:
            chart_path = args.chart
            if len(strategies_to_run) > 1:
                base, ext = chart_path.rsplit(".", 1) if "." in chart_path else (chart_path, "png")
                chart_path = f"{base}_{strategy.name}.{ext}"
            plot_backtest(data, result, save_path=chart_path)


if __name__ == "__main__":
    main()
