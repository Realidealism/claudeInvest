"""Unified position-machine factories with dynamic tier state.

  unified_long  : entry  = pick OR buy OR sell_flee (state-machine resolves tier)
                  exit   = buy_flee (single global exit + defense breach)
                  defense = active tier's rules (dynamic per state machine)

  unified_short : entry  = touch OR sell OR buy_flee (state-machine resolves tier)
                  exit   = sell_flee (single global exit + defense breach)
                  defense = active tier's rules

Tier strictness ordering (list[0] = strictest, list[-1] = loosest):

  LONG:  pick (strict) → sell_flee (medium) → buy (loose)
  SHORT: buy_flee (strict) → touch (medium) → sell (loose)

State machine while holding (engine.run_side_backtest_tiered):
  - current_tier_idx ratchets toward looser; never downgrades
  - When stricter tier signal fires during holding, temp_strict overrides
    for 5 bars then expires and reverts to current_tier
  - Exit is single signal (long: buy_flee, short: sell_flee), tier-agnostic
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

from signal_backtest.signal import (
    DefenseRule, SignalSet, SignalSpec, TierConfig,
)
from signal_backtest.factories._conditions import (
    buy_condition,
    buy_flee_signal,
    pick_condition,
    post_break_blackout,
    sell_condition,
    sell_flee_signal,
    touch_condition,
)
from signal_backtest.factories.pick_touch import pick_signal, touch_signal
from signal_backtest.factories.buy_sell import buy_signal, sell_signal
from signal_backtest.factories.flee import buy_flee_factory, sell_flee_factory

if TYPE_CHECKING:
    from backtest.data import StockData


def unified_long_factory(data: "StockData") -> SignalSpec:
    """統一做多 dynamic-tier：pick/buy/sell_flee 動態 tier，buy_flee 全局出場."""
    n = data.n
    not_dead_fish = ~data.money_result.dead & ~post_break_blackout(data)
    # money_level >= 3 (>= 9M turnover); the second term drops bars whose
    # history spans an unadjusted corporate-action seam (v365)
    pick = pick_condition(data) & not_dead_fish
    buy = buy_condition(data) & not_dead_fish
    sell_flee = sell_flee_signal(data) & not_dead_fish
    long_exit = buy_flee_signal(data)

    pick_spec = pick_signal(data)
    buy_spec = buy_signal(data)
    sell_flee_spec = sell_flee_factory(data)

    # Tier ordering: list[0] = strictest, list[-1] = loosest
    long_tiers = [
        TierConfig(name="pick",       entry=pick,      defense_rules=pick_spec.long_defense),
        TierConfig(name="sell_flee",  entry=sell_flee, defense_rules=sell_flee_spec.long_defense),
        TierConfig(name="buy",        entry=buy,       defense_rules=buy_spec.long_defense),
    ]

    long_entry = pick | buy | sell_flee  # union, for completeness; engine uses tier states

    zero = np.zeros(n, dtype=np.bool_)
    return SignalSpec(
        name="unified_long",
        signals=SignalSet(
            long_entry=long_entry,
            long_exit=long_exit,
            short_entry=zero,
            short_exit=zero,
        ),
        long_tiers=long_tiers,
    )


def unified_short_factory(data: "StockData") -> SignalSpec:
    """統一做空 dynamic-tier：touch/sell/buy_flee 動態 tier，sell_flee 全局出場."""
    n = data.n
    not_dead_fish = ~data.money_result.dead & ~post_break_blackout(data)
    # money_level >= 3 (>= 9M turnover); the second term drops bars whose
    # history spans an unadjusted corporate-action seam (v365)
    touch = touch_condition(data) & not_dead_fish
    sell = sell_condition(data) & not_dead_fish
    buy_flee = buy_flee_signal(data) & not_dead_fish
    short_exit = sell_flee_signal(data)

    touch_spec = touch_signal(data)
    sell_spec = sell_signal(data)
    buy_flee_spec = buy_flee_factory(data)

    short_tiers = [
        TierConfig(name="buy_flee", entry=buy_flee, defense_rules=buy_flee_spec.short_defense),
        TierConfig(name="touch",    entry=touch,    defense_rules=touch_spec.short_defense),
        TierConfig(name="sell",     entry=sell,     defense_rules=sell_spec.short_defense),
    ]

    short_entry = touch | sell | buy_flee

    zero = np.zeros(n, dtype=np.bool_)
    return SignalSpec(
        name="unified_short",
        signals=SignalSet(
            long_entry=zero,
            long_exit=zero,
            short_entry=short_entry,
            short_exit=short_exit,
        ),
        short_tiers=short_tiers,
        # Match short-side floor tightening from v38
        short_floor_period=8,
    )
