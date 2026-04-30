"""WebSocket message dispatcher for esun_marketdata.

The SDK's WebSocketStockClient uses an EventEmitter pattern:

    stock = sdk.websocket_client.stock
    stock.on('connect', on_connect)
    stock.on('message', on_message)   # receives raw JSON string
    stock.on('disconnect', on_disconnect)
    stock.on('error', on_error)
    stock.connect()                    # non-blocking — spawns an internal thread
    stock.subscribe({'channel': 'trades', 'symbol': '2330'})

This module provides a small parser that turns the raw payload into calls to
the store layer. The reconnect loop and thread management live in watcher.py.
"""

import json
import traceback
from datetime import datetime, timezone, timedelta


_TPE_TZ = timezone(timedelta(hours=8))


def _to_int(v):
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _to_float(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _epoch_us_to_dt(us):
    n = _to_int(us)
    if n is None or n <= 0:
        return None
    return datetime.fromtimestamp(n / 1_000_000, tz=_TPE_TZ)


def _pick(d: dict, *keys):
    for k in keys:
        v = d.get(k)
        if v is not None:
            return v
    return None


class MessageDispatcher:
    """Parses raw WS messages and calls the supplied trade/book callbacks."""

    def __init__(self, on_trade, on_book):
        self.on_trade = on_trade
        self.on_book = on_book

    def handle(self, raw):
        """Entry point for stock.on('message', dispatcher.handle)."""
        try:
            if isinstance(raw, (str, bytes, bytearray)):
                msg = json.loads(raw)
            else:
                msg = raw
        except json.JSONDecodeError:
            return

        if not isinstance(msg, dict):
            return

        # Envelope: {"event": "data", "data": {...}}
        # Skip non-data events (auth, pong, heartbeat, ...)
        event = msg.get("event")
        if event != "data":
            return

        payload = msg.get("data")
        if not isinstance(payload, dict):
            return

        channel = payload.get("channel")
        symbol  = payload.get("symbol")
        if not symbol:
            return

        try:
            if channel == "trades":
                self._handle_trade(symbol, payload)
            elif channel in ("books", "aggregates"):
                self._handle_book(symbol, payload)
            # candles/indices channels are not currently wired into the store
        except Exception:
            print("[WS] [ERROR] message handler failed:")
            traceback.print_exc()

    def _handle_trade(self, symbol, payload):
        last_price = _pick(payload, "price", "lastPrice")
        if last_price is None:
            return
        size = _pick(payload, "size", "lastSize")
        total = payload.get("total") or {}
        self.on_trade(
            symbol,
            float(last_price),
            _to_int(size),
            _epoch_us_to_dt(_pick(payload, "time", "at", "lastUpdated")),
            _to_int(total.get("tradeVolume")),
            _to_int(total.get("tradeValue")),
        )

    def _handle_book(self, symbol, payload):
        bids = payload.get("bids") or []
        asks = payload.get("asks") or []
        bid_price = bid_size = ask_price = ask_size = None
        if bids:
            bid_price = _to_float(bids[0].get("price"))
            bid_size  = _to_int(bids[0].get("size"))
        if asks:
            ask_price = _to_float(asks[0].get("price"))
            ask_size  = _to_int(asks[0].get("size"))
        self.on_book(symbol, bid_price, bid_size, ask_price, ask_size)
