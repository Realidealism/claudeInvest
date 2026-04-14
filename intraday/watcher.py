"""WebSocket watcher — the fine layer of the intraday pipeline.

Loads the TW watchlist from portfolio.watchlist, opens a WebSocket stock
session through the shared SDK, subscribes to trades + books for every
symbol, and routes ticks into tw.intraday_quotes via the store helpers.

The esun_marketdata WebSocketClient uses a non-blocking `connect()` — it
spawns its own internal `run_forever` thread and returns once authentication
completes. Our watcher thread therefore has to:

    1. connect() + subscribe()
    2. sleep on a local disconnect flag OR the global stop_event
    3. on wake, tear down the client and decide whether to reconnect

Reconnects use exponential backoff capped at 60s.
"""

import threading
import traceback

from intraday import store
from intraday.esun_ws import MessageDispatcher
from intraday.watchlist import load_tw_watchlist


def _on_trade(symbol, last_price, last_size, last_trade_at, total_volume, total_value):
    try:
        store.upsert_trade(
            stock_id=symbol,
            last_price=last_price,
            last_size=last_size,
            last_trade_at=last_trade_at,
            total_volume=total_volume,
            total_value=total_value,
        )
    except Exception:
        print(f"[WS] [ERROR] upsert_trade {symbol}:")
        traceback.print_exc()


def _on_book(symbol, bid_price, bid_size, ask_price, ask_size):
    try:
        store.upsert_book(
            stock_id=symbol,
            bid_price=bid_price,
            bid_size=bid_size,
            ask_price=ask_price,
            ask_size=ask_size,
        )
    except Exception:
        print(f"[WS] [ERROR] upsert_book {symbol}:")
        traceback.print_exc()


def _run_once(stop_event: threading.Event, sdk, symbols: list[str]) -> None:
    """One connect-subscribe-wait-disconnect cycle."""
    # Each access to sdk.websocket_client returns a fresh factory, so the
    # stock client is fresh per reconnect.
    stock = sdk.websocket_client.stock
    dispatcher = MessageDispatcher(on_trade=_on_trade, on_book=_on_book)
    local_disc = threading.Event()

    def on_connect():
        print(f"[WS] connected, subscribing {len(symbols)} symbols")
        for sym in symbols:
            try:
                stock.subscribe({"channel": "trades", "symbol": sym})
                stock.subscribe({"channel": "books",  "symbol": sym})
            except Exception:
                print(f"[WS] [ERROR] subscribe failed for {sym}:")
                traceback.print_exc()

    def on_disconnect(*args, **kwargs):
        print("[WS] disconnected")
        local_disc.set()

    def on_error(err):
        print(f"[WS] [ERROR] {err}")

    stock.on("connect",    on_connect)
    stock.on("message",    dispatcher.handle)
    stock.on("disconnect", on_disconnect)
    stock.on("error",      on_error)

    try:
        stock.connect()  # returns after auth handshake; SDK thread keeps running
    except Exception:
        print("[WS] [ERROR] connect failed:")
        traceback.print_exc()
        return

    # Wait until either the SDK fires disconnect or we're asked to stop.
    while not stop_event.is_set() and not local_disc.is_set():
        stop_event.wait(1.0)

    try:
        stock.disconnect()
    except Exception:
        pass


def run(stop_event: threading.Event, sdk):
    """Main watcher loop. Reloads the watchlist on every reconnect so symbols
    added while the pipeline is running eventually get picked up."""
    print("[WS] starting")
    backoff = 5

    while not stop_event.is_set():
        try:
            symbols = load_tw_watchlist()
        except Exception:
            print("[WS] [ERROR] failed to load watchlist:")
            traceback.print_exc()
            if stop_event.wait(backoff):
                break
            continue

        if not symbols:
            print("[WS] watchlist is empty, checking again in 60s")
            if stop_event.wait(60):
                break
            continue

        try:
            _run_once(stop_event, sdk, symbols)
        except Exception:
            print("[WS] [ERROR] ws loop crashed:")
            traceback.print_exc()

        if stop_event.is_set():
            break

        print(f"[WS] reconnecting in {backoff}s")
        if stop_event.wait(backoff):
            break
        backoff = min(backoff * 2, 60)

    print("[WS] stopping")
