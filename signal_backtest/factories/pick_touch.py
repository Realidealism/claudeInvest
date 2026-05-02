"""Pick (抄底) and Touch (摸頭) signal factories.

  pick  : long_entry  = PickCondition,  long_exit  = BuyFleeSignal
  touch : short_entry = TouchCondition, short_exit = SellFleeSignal

Defense rules use Chandelier-style proximity to recent extremes from the
turn-point apparatus is too coarse — kept minimal here, the engine's
floor-ratchet (13-day rolling H/L) handles trailing defense.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

from signal_backtest.signal import SignalSet, SignalSpec
from signal_backtest.factories._conditions import (
    pick_condition,
    touch_condition,
    buy_flee_signal,
    sell_flee_signal,
)

if TYPE_CHECKING:
    from backtest.data import StockData


def pick_signal(data: "StockData") -> SignalSpec:
    """抄底 (long-only)."""
    n = data.n
    long_entry = pick_condition(data)
    long_exit = buy_flee_signal(data)
    zero = np.zeros(n, dtype=np.bool_)

    return SignalSpec(
        name="pick",
        signals=SignalSet(
            long_entry=long_entry,
            long_exit=long_exit,
            short_entry=zero,
            short_exit=zero,
        ),
    )


def touch_signal(data: "StockData") -> SignalSpec:
    """摸頭 (short-only)."""
    n = data.n
    short_entry = touch_condition(data)
    short_exit = sell_flee_signal(data)
    zero = np.zeros(n, dtype=np.bool_)

    return SignalSpec(
        name="touch",
        signals=SignalSet(
            long_entry=zero,
            long_exit=zero,
            short_entry=short_entry,
            short_exit=short_exit,
        ),
        # v38: 做空訊號 floor ratchet 從 13d 拉緊到 8d，左尾風險改善
        short_floor_period=8,
    )
