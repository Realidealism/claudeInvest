"""MACD-based signal factories.

Production signal:
  macd  — long-timeframe MACD gold/death cross, LONG-ONLY.
          Validated PF 淨 1.14 on 300-stock pure cross test (long side).
          Short side dropped: PF 淨 0.99 (no edge after cost).

Diagnostic factories (kept for re-validation, but not in the main 6+1
production rotation tracked by _compare.py):
  macd_short / macd_medium / macd_long — pure cross both sides
  at each timeframe.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

from signal_backtest.signal import SignalSet, SignalSpec

if TYPE_CHECKING:
    from backtest.data import StockData


def macd_signal(data: "StockData") -> SignalSpec:
    """Production MACD signal — long-only on long-timeframe MACD cross.

    long_entry = MACD long-timeframe gold cross
    long_exit  = MACD long-timeframe death cross
    """
    n = data.n
    m = data.macd.long
    zero = np.zeros(n, dtype=np.bool_)
    return SignalSpec(
        name="macd",
        signals=SignalSet(
            long_entry=m.macd_gold,
            long_exit=m.macd_death,
            short_entry=zero,
            short_exit=zero,
        ),
    )


def macd_short_signal(data: "StockData") -> SignalSpec:
    """Pure short-timeframe MACD cross signal."""
    m = data.macd.short
    return SignalSpec(
        name="macd_short",
        signals=SignalSet(
            long_entry=m.macd_gold,
            long_exit=m.macd_death,
            short_entry=m.macd_death,
            short_exit=m.macd_gold,
        ),
    )


def macd_medium_signal(data: "StockData") -> SignalSpec:
    """Pure medium-timeframe MACD cross signal."""
    m = data.macd.medium
    return SignalSpec(
        name="macd_medium",
        signals=SignalSet(
            long_entry=m.macd_gold,
            long_exit=m.macd_death,
            short_entry=m.macd_death,
            short_exit=m.macd_gold,
        ),
    )


def macd_long_signal(data: "StockData") -> SignalSpec:
    """Pure long-timeframe MACD cross signal."""
    m = data.macd.long
    return SignalSpec(
        name="macd_long",
        signals=SignalSet(
            long_entry=m.macd_gold,
            long_exit=m.macd_death,
            short_entry=m.macd_death,
            short_exit=m.macd_gold,
        ),
    )
