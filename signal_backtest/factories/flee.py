"""Reversal signal factories — Flee系列.

  buy_flee  : short_entry = BuyFleeSignal,  short_exit = PickCondition
              (多翻空就做空，看到底訊就出場)
  sell_flee : long_entry  = SellFleeSignal, long_exit  = TouchCondition
              (空翻多就做多，看到頭訊就出場)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

from analysis.indicators import rolling_highest, rolling_lowest
from signal_backtest.signal import DefenseRule, SignalSet, SignalSpec
from signal_backtest.factories._conditions import (
    buy_flee_signal,
    sell_flee_signal,
    pick_condition,
    touch_condition,
)

if TYPE_CHECKING:
    from backtest.data import StockData


def buy_flee_factory(data: "StockData") -> SignalSpec:
    """多翻空反轉 (short-only)."""
    n = data.n
    short_entry = buy_flee_signal(data)
    short_exit = pick_condition(data)
    zero = np.zeros(n, dtype=np.bool_)

    # v43: 洪量規則對 buy_flee 邊際負（短側 floor 已 8d，flood 規則只是 redundant），不加
    # v56: buy_flee source HH5 → HH8
    close_lo13 = rolling_lowest(data.close, 13)
    at_13d_low = data.close <= close_lo13
    any_low_in_8d = rolling_highest(at_13d_low.astype(np.float32), 8) > 0.5
    stagnant_short = ~any_low_in_8d
    short_defense = [
        DefenseRule(name="停滯8日無新低→8日高",
                    trigger=stagnant_short, source=rolling_highest(data.high, 8)),
    ]

    return SignalSpec(
        name="buy_flee",
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


def sell_flee_factory(data: "StockData") -> SignalSpec:
    """空翻多反轉 (long-only)."""
    n = data.n
    long_entry = sell_flee_signal(data)
    long_exit = touch_condition(data)
    zero = np.zeros(n, dtype=np.bool_)

    # v43: 洪量規則對 sell_flee 持續負向（持倉 20d 太長被殺贏單），不加
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
        name="sell_flee",
        signals=SignalSet(
            long_entry=long_entry,
            long_exit=long_exit,
            short_entry=zero,
            short_exit=zero,
        ),
        long_defense=long_defense,
    )
