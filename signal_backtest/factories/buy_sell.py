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
    return SignalSpec(
        name="buy",
        signals=SignalSet(
            long_entry=long_entry,
            long_exit=long_exit,
            short_entry=zero,
            short_exit=zero,
        ),
    )


def sell_signal(data: "StockData") -> SignalSpec:
    """波段空 (short-only)."""
    n = data.n
    short_entry = sell_condition(data)
    short_exit = sell_flee_signal(data)
    zero = np.zeros(n, dtype=np.bool_)

    flood = data.volume_result.flood
    short_defense = [
        DefenseRule(name="洪量當日3日高",
                    trigger=flood, source=rolling_highest(data.high, 3)),
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
