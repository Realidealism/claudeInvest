"""
Signal interface for signal-driven backtesting.

Two kinds of signals are returned per (stock, side):

  • Entry / exit signals — boolean arrays in SignalSet.
  • Defense rules       — list[DefenseRule] per side. Each rule has a
        trigger (bool array) + source price. On trigger days, the source
        value is considered for an update; updates only commit in the
        favorable direction (long: higher; short: lower).

Initial defense at entry is NaN — the position has no defense until a
rule fires for the first time. If the user wants an initial stop, they
should add a rule whose trigger fires on entry days (or always).

A signal factory takes StockData and returns a SignalSpec.
A dummy SMA-cross signal is provided as a smoke test.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, TYPE_CHECKING

import numpy as np
from numpy.typing import NDArray

if TYPE_CHECKING:
    from backtest.data import StockData

F32 = np.float32
F32Array = NDArray[np.float32]
BoolArray = NDArray[np.bool_]


@dataclass
class SignalSet:
    """Four per-day boolean arrays defining when to enter/exit each side."""
    long_entry: BoolArray
    long_exit: BoolArray
    short_entry: BoolArray
    short_exit: BoolArray


@dataclass
class DefenseRule:
    """One signal-driven defense-price rule.

    On bars where trigger[i] is True, source[i] becomes a candidate
    defense price; the engine only commits the update if it is in the
    favorable direction (long: higher; short: lower).
    """
    name: str           # human-readable, used as DefenseEvent reason
    trigger: BoolArray
    source: F32Array


@dataclass
class SignalSpec:
    """Complete signal specification returned by a factory."""
    name: str
    signals: SignalSet
    long_defense: list[DefenseRule] | None = None
    short_defense: list[DefenseRule] | None = None
    # Floor-ratchet lookback window for trailing stop. Engine uses
    # rolling_lowest(low, n) for long and rolling_highest(high, n) for
    # short. Smaller = tighter stop, faster to cut losers but also early
    # cut winners. Default 13 matches engine's historical behavior.
    long_floor_period: int = 13
    short_floor_period: int = 13


SignalFactory = Callable[["StockData"], SignalSpec]


# ── Dummy signal (smoke test only — replace with real signals later) ─────────


def dummy_sma_cross(data: "StockData") -> SignalSpec:
    """
    Toy SMA cross signal used purely to verify the framework runs.

    Long enters when close crosses above 5-day SMA, exits on cross below.
    Short is the mirror image.

    Defense (rule-driven, no daily baseline):
      - Long: when close crosses above 21-day SMA, push defense to SMA21.
      - Short: when close crosses below 21-day SMA, push defense to SMA21.
    """
    sma5 = data.close_result.ma.sma[5]
    sma21 = data.close_result.ma.sma[21]
    close = data.close

    cross_up5 = np.zeros_like(close, dtype=bool)
    cross_dn5 = np.zeros_like(close, dtype=bool)
    cross_up5[1:] = (close[1:] > sma5[1:]) & (close[:-1] <= sma5[:-1])
    cross_dn5[1:] = (close[1:] < sma5[1:]) & (close[:-1] >= sma5[:-1])

    cross_up21 = np.zeros_like(close, dtype=bool)
    cross_dn21 = np.zeros_like(close, dtype=bool)
    cross_up21[1:] = (close[1:] > sma21[1:]) & (close[:-1] <= sma21[:-1])
    cross_dn21[1:] = (close[1:] < sma21[1:]) & (close[:-1] >= sma21[:-1])

    signals = SignalSet(
        long_entry=cross_up5,
        long_exit=cross_dn5,
        short_entry=cross_dn5,
        short_exit=cross_up5,
    )
    long_defense = [
        DefenseRule(name="站上21日均線", trigger=cross_up21, source=sma21),
    ]
    short_defense = [
        DefenseRule(name="跌破21日均線", trigger=cross_dn21, source=sma21),
    ]
    return SignalSpec(
        name="dummy_sma_cross",
        signals=signals,
        long_defense=long_defense,
        short_defense=short_defense,
    )


SIGNAL_FACTORIES: dict[str, SignalFactory] = {
    "dummy": dummy_sma_cross,
}


def _register_factories() -> None:
    """Import factory submodules directly (not via factories package
    __init__) so that callers who only need _conditions don't trigger a
    circular import through this module.
    """
    from signal_backtest.factories.pick_touch import pick_signal, touch_signal
    from signal_backtest.factories.buy_sell import buy_signal, sell_signal
    from signal_backtest.factories.flee import buy_flee_factory, sell_flee_factory
    from signal_backtest.factories.macd import (
        macd_short_signal, macd_medium_signal, macd_long_signal,
    )

    SIGNAL_FACTORIES.update({
        "pick":       pick_signal,
        "touch":      touch_signal,
        "buy":        buy_signal,
        "sell":       sell_signal,
        "buy_flee":   buy_flee_factory,
        "sell_flee":  sell_flee_factory,
        # MACD diagnostic — kept for ad-hoc re-validation, not in production
        "macd_short":  macd_short_signal,
        "macd_medium": macd_medium_signal,
        "macd_long":   macd_long_signal,
    })


_register_factories()
