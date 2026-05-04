"""Buy (波段多) and Sell (波段空) signal factories.

  buy  : long_entry  = BuyCondition,  long_exit  = BuyFleeSignal
  sell : short_entry = SellCondition, short_exit = SellFleeSignal
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

from analysis.indicators import rolling_highest, rolling_lowest
from signal_backtest.signal import DefenseRule, SignalSet, SignalSpec
from signal_backtest.factories._conditions import (
    buy_condition,
    sell_condition,
    buy_flee_signal,
    sell_flee_signal,
)

if TYPE_CHECKING:
    from backtest.data import StockData


def buy_signal(data: "StockData") -> SignalSpec:
    """波段多 (long-only)."""
    n = data.n
    long_entry = buy_condition(data)
    long_exit = buy_flee_signal(data)
    zero = np.zeros(n, dtype=np.bool_)

    # v43: 洪量規則對 buy 的所有變種都是負向（持倉 26d 太長被殺贏單），不加洪量規則
    # v56: 多方 stagnation 視窗 8d → 13d
    close_hi13 = rolling_highest(data.close, 13)
    at_13d_high = data.close >= close_hi13
    any_high_in_13d = rolling_highest(at_13d_high.astype(np.float32), 13) > 0.5
    stagnant_long = ~any_high_in_13d
    long_defense = [
        DefenseRule(name="停滯13日無新高→8日低",
                    trigger=stagnant_long, source=rolling_lowest(data.low, 8)),
    ]

    return SignalSpec(
        name="buy",
        signals=SignalSet(
            long_entry=long_entry,
            long_exit=long_exit,
            short_entry=zero,
            short_exit=zero,
        ),
        long_defense=long_defense,
    )


def sell_signal(data: "StockData") -> SignalSpec:
    """波段空 (short-only)."""
    n = data.n
    short_entry = sell_condition(data)
    short_exit = sell_flee_signal(data)
    zero = np.zeros(n, dtype=np.bool_)

    flood = data.volume_result.flood
    # v55: stagnation defense
    close_lo13 = rolling_lowest(data.close, 13)
    at_13d_low = data.close <= close_lo13
    any_low_in_8d = rolling_highest(at_13d_low.astype(np.float32), 8) > 0.5
    stagnant_short = ~any_low_in_8d
    short_defense = [
        DefenseRule(name="洪量當日3日高",
                    trigger=flood, source=rolling_highest(data.high, 3)),
        DefenseRule(name="停滯8日無新低→5日高",
                    trigger=stagnant_short, source=rolling_highest(data.high, 5)),
    ]

    return SignalSpec(
        name="sell",
        signals=SignalSet(
            long_entry=zero,
            long_exit=zero,
            short_entry=short_entry,
            short_exit=short_exit,
        ),
        short_defense=short_defense,
        # v38: 做空訊號 floor ratchet 從 13d 拉緊到 8d，左尾風險改善
        short_floor_period=8,
    )
