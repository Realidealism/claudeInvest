"""Yuanta Futures COM adapter (alternative, stub).

Same COM/message-pump shape as the Capital SKCOM adapter. Kept as a thin
template so it imports cleanly and can be fleshed out later if the live实盤
emphasis shifts to Yuanta. All live calls raise NotImplementedError.
"""
from __future__ import annotations

import logging
from typing import List, Optional

from .base import BrokerAdapter, ReconnectMixin
from .types import Bar, OrderRequest, OrderResult, Position

log = logging.getLogger(__name__)


class YuantaComAdapter(BrokerAdapter, ReconnectMixin):
    def __init__(self, user_id: str, password: str, **kwargs) -> None:
        super().__init__()
        self._init_reconnect()
        self._id = user_id
        self._pwd = password

    def connect(self) -> None:
        raise NotImplementedError("Yuanta COM adapter not implemented (stub)")

    def disconnect(self) -> None:
        self._stop_watchdog()

    def _reconnect(self) -> None:
        raise NotImplementedError

    def _do_subscribe(self, symbol: str) -> None:
        raise NotImplementedError

    def place_order(self, req: OrderRequest) -> OrderResult:
        raise NotImplementedError

    def update_order(
        self, order_id, price: Optional[float] = None, lot: Optional[int] = None
    ) -> OrderResult:
        raise NotImplementedError

    def cancel_order(self, order_id) -> OrderResult:
        raise NotImplementedError

    def list_positions(self) -> List[Position]:
        raise NotImplementedError

    def get_kbars(self, symbol: str, start: str, end: str) -> List[Bar]:
        raise NotImplementedError
