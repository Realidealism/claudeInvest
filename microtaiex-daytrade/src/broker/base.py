"""Broker abstraction layer: the only contract the upper layers depend on.

`BrokerAdapter` defines the minimal interface. `ReconnectMixin` adds a
heartbeat watchdog + exponential backoff + auto re-subscribe shared by both
native-Python and COM-style adapters.
"""
from __future__ import annotations

import logging
import threading
import time
from abc import ABC, abstractmethod
from typing import Callable, List, Optional, Set

from .types import (
    Bar,
    ConnectionStatus,
    OrderRequest,
    OrderResult,
    Position,
    Tick,
    Trade,
)

log = logging.getLogger(__name__)

OnTick = Callable[[Tick], None]
OnTrade = Callable[[Trade], None]
OnConnection = Callable[[ConnectionStatus], None]


class BrokerAdapter(ABC):
    """Single dependency contract for the upper layers.

    Subclasses implement the live integration; callers wire callbacks via the
    ``set_on_*`` setters and never touch a broker SDK directly.
    """

    def __init__(self) -> None:
        self._on_tick: Optional[OnTick] = None
        self._on_trade: Optional[OnTrade] = None
        self._on_connection: Optional[OnConnection] = None
        self._subscribed: Set[str] = set()

    # ---- callback wiring ----
    def set_on_tick(self, cb: OnTick) -> None:
        self._on_tick = cb

    def set_on_trade(self, cb: OnTrade) -> None:
        self._on_trade = cb

    def set_on_connection(self, cb: OnConnection) -> None:
        self._on_connection = cb

    def _emit_tick(self, tick: Tick) -> None:
        if self._on_tick is not None:
            self._on_tick(tick)

    def _emit_trade(self, trade: Trade) -> None:
        if self._on_trade is not None:
            self._on_trade(trade)

    def _emit_connection(self, status: ConnectionStatus) -> None:
        if self._on_connection is not None:
            self._on_connection(status)

    # ---- subscription (tracked for auto re-subscribe on reconnect) ----
    def subscribe(self, symbol: str) -> None:
        self._subscribed.add(symbol)
        self._do_subscribe(symbol)

    @abstractmethod
    def _do_subscribe(self, symbol: str) -> None:
        ...

    # ---- lifecycle ----
    @abstractmethod
    def connect(self) -> None:
        ...

    @abstractmethod
    def disconnect(self) -> None:
        ...

    # ---- orders ----
    @abstractmethod
    def place_order(self, req: OrderRequest) -> OrderResult:
        ...

    @abstractmethod
    def update_order(
        self, order_id: str, price: Optional[float] = None, lot: Optional[int] = None
    ) -> OrderResult:
        ...

    @abstractmethod
    def cancel_order(self, order_id: str) -> OrderResult:
        ...

    # ---- account / history ----
    @abstractmethod
    def list_positions(self) -> List[Position]:
        ...

    @abstractmethod
    def get_kbars(self, symbol: str, start: str, end: str) -> List[Bar]:
        ...


class ReconnectMixin:
    """Heartbeat watchdog + exponential backoff + auto re-subscribe.

    Adapters call ``_init_reconnect`` in ``__init__``, ``_mark_alive`` whenever
    any payload (tick / order report / connection event) arrives, and
    ``_start_watchdog`` / ``_stop_watchdog`` around the connected lifetime.
    Subclasses implement ``_reconnect`` to rebuild their session.
    """

    def _init_reconnect(
        self,
        heartbeat_timeout: float = 30.0,
        backoff_base: float = 1.0,
        backoff_max: float = 60.0,
    ) -> None:
        self._hb_timeout = heartbeat_timeout
        self._backoff_base = backoff_base
        self._backoff_max = backoff_max
        self._last_alive = 0.0
        self._wd_stop = threading.Event()
        self._wd_thread: Optional[threading.Thread] = None

    def _mark_alive(self) -> None:
        self._last_alive = time.monotonic()

    def _start_watchdog(self) -> None:
        self._mark_alive()
        self._wd_stop.clear()
        self._wd_thread = threading.Thread(
            target=self._watchdog_loop, name="reconnect-watchdog", daemon=True
        )
        self._wd_thread.start()

    def _stop_watchdog(self) -> None:
        self._wd_stop.set()

    def _watchdog_loop(self) -> None:
        while not self._wd_stop.wait(1.0):
            if time.monotonic() - self._last_alive > self._hb_timeout:
                self._reconnect_with_backoff()

    def _reconnect_with_backoff(self) -> None:
        backoff = self._backoff_base
        while not self._wd_stop.is_set():
            try:
                self._reconnect()
                self._resubscribe_all()
                self._mark_alive()
                return
            except Exception:  # noqa: BLE001 - keep retrying through any failure
                log.exception("reconnect failed; retrying in %.1fs", backoff)
                if self._wd_stop.wait(backoff):
                    return
                backoff = min(backoff * 2, self._backoff_max)

    def _resubscribe_all(self) -> None:
        for symbol in list(getattr(self, "_subscribed", ())):
            self._do_subscribe(symbol)  # type: ignore[attr-defined]

    def _reconnect(self) -> None:
        raise NotImplementedError
