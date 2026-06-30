"""Neutral domain types shared across all broker adapters.

These types decouple the upper layers (data/strategy/risk/position) from any
specific broker SDK. Adapters translate broker-native payloads into these.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class Side(Enum):
    BUY = "buy"
    SELL = "sell"


class OpenClose(Enum):
    OPEN = "open"     # new position
    COVER = "cover"   # close existing position
    AUTO = "auto"     # broker decides (net)


class PriceType(Enum):
    LIMIT = "limit"
    MARKET = "market"


class TimeInForce(Enum):
    ROD = "rod"
    IOC = "ioc"
    FOK = "fok"


class ConnectionStatus(Enum):
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    DISCONNECTED = "disconnected"


@dataclass(frozen=True)
class Tick:
    symbol: str
    ts: datetime
    price: float
    volume: int
    bid: Optional[float] = None
    ask: Optional[float] = None


@dataclass(frozen=True)
class Bar:
    symbol: str
    ts: datetime          # bar close timestamp (right edge)
    open: float
    high: float
    low: float
    close: float
    volume: int
    timeframe: str        # e.g. "1m", "5m"


@dataclass(frozen=True)
class OrderRequest:
    symbol: str
    side: Side
    lot: int
    price: Optional[float] = None          # None => market
    price_type: PriceType = PriceType.LIMIT
    tif: TimeInForce = TimeInForce.ROD
    open_close: OpenClose = OpenClose.AUTO


@dataclass(frozen=True)
class OrderResult:
    order_id: str
    accepted: bool
    msg: str = ""
    raw: object = None


@dataclass(frozen=True)
class Trade:
    order_id: str
    symbol: str
    side: Side
    price: float
    lot: int
    ts: datetime
    open_close: OpenClose = OpenClose.AUTO


@dataclass(frozen=True)
class Position:
    symbol: str
    side: Side
    lot: int
    avg_price: float
