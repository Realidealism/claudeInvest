"""Pick (抄底) and Touch (摸頭) signal factories.

  pick  : long_entry  = PickCondition,  long_exit  = BuyFleeSignal
  touch : short_entry = TouchCondition, short_exit = SellFleeSignal

Defense (v43 selective flood, after probing all variants):
  Floor-ratchet on intraday H/L (long 13d / short 8d after v38) plus
  signal-specific flood rules where they help:
    pick  — flood_recent5 → LL8 (best long-side gain, +0.06 PF)
    touch — flood (single day) → HH3 (modest +0.01 PF)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

from analysis.indicators import rolling_highest, rolling_lowest
from signal_backtest.signal import DefenseRule, SignalSet, SignalSpec
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
    not_dead_fish = ~data.money_result.dead  # money_level >= 3 (>= 9M turnover)
    long_entry = pick_condition(data) & not_dead_fish
    long_exit = buy_flee_signal(data)
    zero = np.zeros(n, dtype=np.bool_)

    flood = data.volume_result.flood
    flood_recent5 = rolling_highest(flood.astype(np.float32), 5) > 0
    # v56: 多方 stagnation 視窗 8d → 13d（給更多天數）
    close_hi13 = rolling_highest(data.close, 13)
    at_13d_high = data.close >= close_hi13
    any_high_in_13d = rolling_highest(at_13d_high.astype(np.float32), 13) > 0.5
    stagnant_long = ~any_high_in_13d
    long_defense = [
        DefenseRule(name="洪量後5日內8日低",
                    trigger=flood_recent5, source=rolling_lowest(data.low, 8)),
        DefenseRule(name="停滯13日無新高→8日低",
                    trigger=stagnant_long, source=rolling_lowest(data.low, 8)),
    ]

    return SignalSpec(
        name="pick",
        signals=SignalSet(
            long_entry=long_entry,
            long_exit=long_exit,
            short_entry=zero,
            short_exit=zero,
        ),
        long_defense=long_defense,
    )


def touch_signal(data: "StockData") -> SignalSpec:
    """摸頭 (short-only)."""
    n = data.n
    not_dead_fish = ~data.money_result.dead  # money_level >= 3 (>= 9M turnover)
    short_entry = touch_condition(data) & not_dead_fish
    short_exit = sell_flee_signal(data)
    zero = np.zeros(n, dtype=np.bool_)

    flood = data.volume_result.flood
    # v56: touch source HH5 → HH8（更鬆給空頭反彈空間）
    close_lo13 = rolling_lowest(data.close, 13)
    at_13d_low = data.close <= close_lo13
    any_low_in_8d = rolling_highest(at_13d_low.astype(np.float32), 8) > 0.5
    stagnant_short = ~any_low_in_8d
    short_defense = [
        DefenseRule(name="洪量當日3日高",
                    trigger=flood, source=rolling_highest(data.high, 3)),
        DefenseRule(name="停滯8日無新低→8日高",
                    trigger=stagnant_short, source=rolling_highest(data.high, 8)),
    ]

    return SignalSpec(
        name="touch",
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
