"""
Strategy definition and condition primitives for backtesting.

A Strategy consists of entry/exit conditions for long and short positions.
Conditions are thin wrappers around analysis module outputs (numpy arrays).

Entry requires ALL conditions true (AND logic).
Exit triggers on ANY condition true (OR logic).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, TYPE_CHECKING
import operator

import numpy as np
from numpy.typing import NDArray

if TYPE_CHECKING:
    from backtest.data import StockData

F32Array = NDArray[np.float32]
BoolArray = NDArray[np.bool_]

_OPS = {
    ">": operator.gt,
    "<": operator.lt,
    ">=": operator.ge,
    "<=": operator.le,
    "==": operator.eq,
}


@dataclass
class Condition:
    """A named boolean condition evaluated per day."""
    name: str
    evaluate: Callable[[StockData, int], bool]


@dataclass
class TrailingStopConfig:
    """Trailing stop (移動停利) configuration."""
    # Returns a float array — the defense price source
    defense_source: Callable[[StockData], F32Array]


class Strategy:
    """
    User-defined trading strategy.

    Example usage:
        s = Strategy("OBV突破+均線多排")
        s.long_entry = [
            bool_condition("OBV買訊", lambda d: d.obv_result.signal_up),
            bool_condition("短排多", lambda d: d.close_result.ma.sort_normal["short"].up),
        ]
        s.long_exit = [
            bool_condition("OBV賣訊", lambda d: d.obv_result.signal_down),
        ]
        s.trailing_stop = TrailingStopConfig(
            defense_source=lambda d: d.close_result.ma.sma[8],
        )
    """

    def __init__(self, name: str):
        self.name = name
        self.long_entry: list[Condition] = []
        self.long_exit: list[Condition] = []
        self.short_entry: list[Condition] = []
        self.short_exit: list[Condition] = []
        self.trailing_stop: TrailingStopConfig | None = None


# ── Convenience factories ──────────────────────────────────────────────────


def bool_condition(
    name: str,
    accessor: Callable[[StockData], BoolArray],
) -> Condition:
    """Wrap a boolean array field as a Condition."""
    def _eval(data: StockData, i: int) -> bool:
        return bool(accessor(data)[i])
    return Condition(name=name, evaluate=_eval)


def threshold_condition(
    name: str,
    accessor: Callable[[StockData], F32Array],
    op: str,
    value: float,
) -> Condition:
    """Compare a float array against a threshold per day."""
    cmp = _OPS[op]

    def _eval(data: StockData, i: int) -> bool:
        return bool(cmp(float(accessor(data)[i]), value))
    return Condition(name=name, evaluate=_eval)


def cross_above(
    name: str,
    a_accessor: Callable[[StockData], F32Array],
    b_accessor: Callable[[StockData], F32Array],
) -> Condition:
    """True on the day array `a` crosses above array `b`."""
    def _eval(data: StockData, i: int) -> bool:
        if i < 1:
            return False
        a = a_accessor(data)
        b = b_accessor(data)
        return float(a[i]) > float(b[i]) and float(a[i - 1]) <= float(b[i - 1])
    return Condition(name=name, evaluate=_eval)


def cross_below(
    name: str,
    a_accessor: Callable[[StockData], F32Array],
    b_accessor: Callable[[StockData], F32Array],
) -> Condition:
    """True on the day array `a` crosses below array `b`."""
    def _eval(data: StockData, i: int) -> bool:
        if i < 1:
            return False
        a = a_accessor(data)
        b = b_accessor(data)
        return float(a[i]) < float(b[i]) and float(a[i - 1]) >= float(b[i - 1])
    return Condition(name=name, evaluate=_eval)
