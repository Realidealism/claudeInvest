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
    return SignalSpec(
        name="buy_flee",
        signals=SignalSet(
            long_entry=zero,
            long_exit=zero,
            short_entry=short_entry,
            short_exit=short_exit,
        ),
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
    return SignalSpec(
        name="sell_flee",
        signals=SignalSet(
            long_entry=long_entry,
            long_exit=long_exit,
            short_entry=zero,
            short_exit=zero,
        ),
    )
