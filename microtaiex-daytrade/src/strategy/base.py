"""Strategy interface and signal types.

A strategy consumes only CLOSED bars of its declared ``timeframe`` (no
look-ahead) and returns an optional Signal. The Signal is an intent; the risk
manager decides whether and how to turn it into an order.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional

from broker.types import Bar


class SignalType(Enum):
    LONG = "long"     # go / stay long
    SHORT = "short"   # go / stay short
    FLAT = "flat"     # close to flat


@dataclass(frozen=True)
class Signal:
    symbol: str
    type: SignalType
    ts: datetime
    price: float          # reference price (the closing price of the trigger bar)
    reason: str = ""
    strength: float = 0.0


@dataclass
class StrategyContext:
    """Rolling history of closed bars for the strategy's timeframe.

    ``bars`` is oldest..newest and includes the bar currently being handed to
    ``on_bar_close``. Strategies and indicators read from it instead of keeping
    their own buffers.
    """

    bars: List[Bar] = field(default_factory=list)

    def closes(self) -> List[float]:
        return [b.close for b in self.bars]


class Strategy(ABC):
    timeframe: str = "5m"

    @abstractmethod
    def on_bar_close(self, bar: Bar, ctx: StrategyContext) -> Optional[Signal]:
        ...
