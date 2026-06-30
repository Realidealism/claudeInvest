"""SimBroker: a no-network adapter for tests, demos, and backtests.

Implements the full BrokerAdapter contract without any live connection:
- ``feed_tick`` / ``feed_ticks`` push synthetic ticks through ``_emit_tick``.
- ``place_order`` fills immediately and emits a Trade, maintaining a net
  position so the same strategy/risk/position code runs unchanged offline.
- ``get_kbars`` returns pre-loaded historical bars (warmup), default empty.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Iterable, List, Optional

from .base import BrokerAdapter
from .types import (
    Bar,
    ConnectionStatus,
    OpenClose,
    OrderRequest,
    OrderResult,
    Position,
    Side,
    Tick,
    Trade,
)

log = logging.getLogger(__name__)

_EPOCH = datetime(1970, 1, 1)


class SimBroker(BrokerAdapter):
    def __init__(self, symbol: str = "TMFR1") -> None:
        super().__init__()
        self._symbol = symbol
        self._order_seq = 0
        self._last_ts: datetime = _EPOCH
        # net position per symbol: signed lots + average price
        self._net: Dict[str, int] = {}
        self._avg: Dict[str, float] = {}
        self._kbars: Dict[str, List[Bar]] = {}

    # ---- lifecycle ----
    def connect(self) -> None:
        self._emit_connection(ConnectionStatus.CONNECTED)

    def disconnect(self) -> None:
        self._emit_connection(ConnectionStatus.DISCONNECTED)

    def _do_subscribe(self, symbol: str) -> None:
        pass

    def set_mark_time(self, ts: datetime) -> None:
        """Set the timestamp used to stamp fills (backtest drives this per bar)."""
        self._last_ts = ts

    # ---- tick injection ----
    def feed_tick(self, tick: Tick) -> None:
        self._last_ts = tick.ts
        self._emit_tick(tick)

    def feed_ticks(self, ticks: Iterable[Tick]) -> None:
        for tick in ticks:
            self.feed_tick(tick)

    # ---- warmup history ----
    def set_kbars(self, symbol: str, bars: List[Bar]) -> None:
        self._kbars[symbol] = list(bars)

    def get_kbars(self, symbol: str, start: str, end: str) -> List[Bar]:
        return [b for b in self._kbars.get(symbol, []) if start <= b.ts.strftime("%Y%m%d") <= end]

    # ---- orders ----
    def place_order(self, req: OrderRequest) -> OrderResult:
        self._order_seq += 1
        oid = f"SIM{self._order_seq:06d}"
        price = req.price if req.price is not None else self._mark_price(req.symbol)
        trade = Trade(
            order_id=oid,
            symbol=req.symbol,
            side=req.side,
            price=price,
            lot=req.lot,
            ts=self._last_ts,
            open_close=req.open_close,
        )
        self._apply_fill(trade)
        self._emit_trade(trade)
        return OrderResult(order_id=oid, accepted=True, msg="sim filled", raw=trade)

    def update_order(self, order_id, price=None, lot=None) -> OrderResult:
        return OrderResult(order_id=order_id, accepted=True, msg="sim no-op")

    def cancel_order(self, order_id) -> OrderResult:
        return OrderResult(order_id=order_id, accepted=True, msg="sim cancelled")

    # ---- account ----
    def list_positions(self) -> List[Position]:
        out: List[Position] = []
        for sym, net in self._net.items():
            if net == 0:
                continue
            out.append(
                Position(
                    symbol=sym,
                    side=Side.BUY if net > 0 else Side.SELL,
                    lot=abs(net),
                    avg_price=self._avg.get(sym, 0.0),
                )
            )
        return out

    # ---- internals ----
    def _mark_price(self, symbol: str) -> float:
        return self._avg.get(symbol, 0.0)

    def _apply_fill(self, trade: Trade) -> None:
        sym = trade.symbol
        signed = trade.lot if trade.side is Side.BUY else -trade.lot
        old = self._net.get(sym, 0)
        new = old + signed
        old_avg = self._avg.get(sym, 0.0)
        if old == 0 or (old > 0) == (signed > 0):
            # opening or increasing in the same direction -> weighted average
            total = abs(old) + abs(signed)
            self._avg[sym] = (abs(old) * old_avg + abs(signed) * trade.price) / total if total else 0.0
        elif (new > 0) == (old > 0) or new == 0:
            # reducing toward / to flat -> keep average
            self._avg[sym] = old_avg if new != 0 else 0.0
        else:
            # flipped through zero -> new average is the fill price
            self._avg[sym] = trade.price
        self._net[sym] = new
