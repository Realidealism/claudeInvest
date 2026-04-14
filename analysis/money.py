"""
Money (成交金額) technical analysis — ported from Go CalculateMoney.

Detects "dead fish" (死魚) stocks — those with persistently low
turnover, indicating illiquidity.

Usage:
    from analysis.money import calculate_money
    result = calculate_money(money)
    result.dead_fish          # composite dead-fish flag
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from numpy.typing import NDArray

from analysis.indicators import sma, rolling_count_ge
from analysis.constants import (
    VOLUME_SMA_PERIODS,
    MONEY_LEVEL_THRESHOLDS,
    FISH_DEAD_SHORT_CONFIGS,
    FISH_DEAD_LONG_CONFIGS,
)

F32 = np.float32
F32Array = NDArray[np.float32]
BoolArray = NDArray[np.bool_]
U8Array = NDArray[np.uint8]


@dataclass
class MoneyResult:
    """Complete money analysis result."""
    sma: dict[int, F32Array]
    money_level: U8Array        # 0–11

    fish: BoolArray             # money_level < 2
    dead: BoolArray             # money_level < 3

    fish_short: dict[tuple[int, int], BoolArray]  # short-window rolling counts
    fish_long: dict[tuple[int, int], BoolArray]    # long-window rolling counts
    dead_short: dict[tuple[int, int], BoolArray]
    dead_long: dict[tuple[int, int], BoolArray]

    dead_fish: BoolArray        # composite flag


def calculate_money(money: F32Array) -> MoneyResult:
    """
    Main entry point — equivalent to Go GetCalculateMoney.

    Parameters
    ----------
    money : float32 numpy array of daily turnover (oldest first).
    """
    n = len(money)
    money = money.astype(F32)

    # SMA
    sma_d = {p: sma(money, p) for p in VOLUME_SMA_PERIODS}

    # Money level (based on 8-day SMA)
    money_level = np.full(n, len(MONEY_LEVEL_THRESHOLDS), dtype=np.uint8)
    for i, threshold in reversed(list(enumerate(MONEY_LEVEL_THRESHOLDS))):
        money_level[sma_d[8] < threshold] = i

    # Fish / Dead base flags
    fish = money_level < 2
    dead = money_level < 3

    # Rolling counts
    fish_short = {(w, t): rolling_count_ge(fish, w, t) for w, t in FISH_DEAD_SHORT_CONFIGS}
    fish_long = {(w, t): rolling_count_ge(fish, w, t) for w, t in FISH_DEAD_LONG_CONFIGS}
    dead_short = {(w, t): rolling_count_ge(dead, w, t) for w, t in FISH_DEAD_SHORT_CONFIGS}
    dead_long = {(w, t): rolling_count_ge(dead, w, t) for w, t in FISH_DEAD_LONG_CONFIGS}

    # Composite dead-fish:
    # (any fish_short AND any fish_long) OR (any dead_short AND any dead_long)
    any_fish_short = np.zeros(n, dtype=np.bool_)
    for v in fish_short.values():
        any_fish_short |= v
    any_fish_long = np.zeros(n, dtype=np.bool_)
    for v in fish_long.values():
        any_fish_long |= v

    any_dead_short = np.zeros(n, dtype=np.bool_)
    for v in dead_short.values():
        any_dead_short |= v
    any_dead_long = np.zeros(n, dtype=np.bool_)
    for v in dead_long.values():
        any_dead_long |= v

    dead_fish = (any_fish_short & any_fish_long) | (any_dead_short & any_dead_long)

    return MoneyResult(
        sma=sma_d,
        money_level=money_level,
        fish=fish,
        dead=dead,
        fish_short=fish_short,
        fish_long=fish_long,
        dead_short=dead_short,
        dead_long=dead_long,
        dead_fish=dead_fish,
    )
