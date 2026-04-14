"""Intraday real-time quote pipeline (Fugle MarketData API).

Layers:
  - sweeper: REST snapshot every ~20s, full TSE + OTC market
  - watcher: WebSocket subscription for watchlist symbols (tick-level)
  - store:   upsert into tw.intraday_quotes (latest snapshot only)
  - signals: rule engine over the latest snapshot (Phase B skeleton)

Entry point: intraday_update.py at the repo root.
"""
