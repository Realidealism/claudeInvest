"""Thread-safe event bus: a single consumer thread drains a FIFO queue.

COM/broker callbacks run on pump/socket threads and just ``publish`` onto the
queue; the lone consumer dispatches to handlers serially. This serializes all
state mutation (strategy / risk / position) onto one thread -- no asyncio, no
locks in the handlers themselves (plan §12).
"""
from __future__ import annotations

import logging
import queue
import threading
from collections import defaultdict
from typing import Callable, DefaultDict, List

log = logging.getLogger(__name__)

Handler = Callable[[object], None]

_SENTINEL = object()


class EventBus:
    def __init__(self) -> None:
        self._q: "queue.Queue" = queue.Queue()
        self._subs: DefaultDict[str, List[Handler]] = defaultdict(list)
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def subscribe(self, event_type: str, handler: Handler) -> None:
        with self._lock:
            self._subs[event_type].append(handler)

    def publish(self, event_type: str, payload: object) -> None:
        self._q.put((event_type, payload))

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, name="event-bus", daemon=True)
        self._thread.start()

    def wait_idle(self) -> None:
        """Block until every queued event has been processed."""
        self._q.join()

    def stop(self, drain: bool = True, timeout: float = 5.0) -> None:
        if drain:
            self._q.join()
        self._q.put((_SENTINEL, None))
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def _loop(self) -> None:
        while True:
            event_type, payload = self._q.get()
            try:
                if event_type is _SENTINEL:
                    return
                with self._lock:
                    handlers = list(self._subs.get(event_type, ()))
                for handler in handlers:
                    try:
                        handler(payload)
                    except Exception:  # noqa: BLE001 - one bad handler must not kill the bus
                        log.exception("handler for %r failed", event_type)
            finally:
                self._q.task_done()
