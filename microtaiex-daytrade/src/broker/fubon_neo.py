"""Fubon Neo adapter (native Python / WebSocket, stub).

Cross-platform, no COM. Kept as a template for a clean greenfield experiment.
All live calls raise NotImplementedError until an account is available.
"""
from __future__ import annotations

import logging
from typing import List, Optional

from .base import BrokerAdapter, ReconnectMixin
from .types import Bar, OrderRequest, OrderResult, Position

log = logging.getLogger(__name__)


class FubonNeoAdapter(BrokerAdapter, ReconnectMixin):
    def __init__(
        self,
        user_id: str,
        password: str,
        cert_path: str,
        cert_pwd: str,
        **kwargs,
    ) -> None:
        super().__init__()
        self._init_reconnect()
        self._id = user_id
        self._pwd = password
        self._cert_path = cert_path
        self._cert_pwd = cert_pwd

    def connect(self) -> None:
        raise NotImplementedError("Fubon Neo adapter not implemented (stub)")

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
