"""Position lifecycle: FLAT -> PENDING_ENTRY -> HOLDING -> PENDING_EXIT -> FLAT.

Driven by submitted orders (request_*) and fill reports (on_trade). The PENDING
states block duplicate orders: a second entry while an entry is in flight is
rejected, so the strategy/risk loop cannot double-order before a fill lands.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional

from broker.types import OpenClose, OrderRequest, Side, Trade


class PosState(Enum):
    FLAT = "flat"
    PENDING_ENTRY = "pending_entry"
    HOLDING = "holding"
    PENDING_EXIT = "pending_exit"


class PositionStateMachine:
    def __init__(self) -> None:
        self.state = PosState.FLAT
        self.symbol: Optional[str] = None
        self.side: Optional[Side] = None
        self.entry_price: Optional[float] = None
        self.lot: int = 0

    def on_order_submitted(self, req: OrderRequest) -> bool:
        """Register an outgoing order. Returns False if rejected (wrong state)."""
        if req.open_close is OpenClose.OPEN:
            if self.state is PosState.FLAT:
                self.state = PosState.PENDING_ENTRY
                return True
            return False
        # COVER / exit
        if self.state is PosState.HOLDING:
            self.state = PosState.PENDING_EXIT
            return True
        return False

    def on_trade(self, trade: Trade) -> None:
        """Apply a fill report to advance the lifecycle."""
        if self.state is PosState.PENDING_ENTRY:
            self.state = PosState.HOLDING
            self.symbol = trade.symbol
            self.side = trade.side
            self.entry_price = trade.price
            self.lot = trade.lot
        elif self.state is PosState.PENDING_EXIT:
            self._go_flat()

    def is_flat(self) -> bool:
        return self.state is PosState.FLAT

    def is_holding(self) -> bool:
        return self.state is PosState.HOLDING

    def _go_flat(self) -> None:
        self.state = PosState.FLAT
        self.symbol = None
        self.side = None
        self.entry_price = None
        self.lot = 0
