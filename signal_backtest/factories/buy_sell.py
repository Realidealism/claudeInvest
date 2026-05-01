"""Buy (波段多) and Sell (波段空) signal factories.

  buy  : long_entry  = BuyCondition,  long_exit  = BuyFleeSignal
  sell : short_entry = SellCondition, short_exit = SellFleeSignal
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

from signal_backtest.signal import SignalSet, SignalSpec
from signal_backtest.factories._conditions import (
    buy_condition,
    sell_condition,
    buy_flee_signal,
    sell_flee_signal,
    long_pressure_short_defense_rule,
)

if TYPE_CHECKING:
    from backtest.data import StockData


def buy_signal(data: "StockData") -> SignalSpec:
    """波段多 (long-only)."""
    n = data.n
    long_entry = buy_condition(data)
    long_exit = buy_flee_signal(data)
    zero = np.zeros(n, dtype=np.bool_)

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

    return SignalSpec(
        name="sell",
        signals=SignalSet(
            long_entry=zero,
            long_exit=zero,
            short_entry=short_entry,
            short_exit=short_exit,
        ),
        short_defense=[long_pressure_short_defense_rule(data)],
    )
