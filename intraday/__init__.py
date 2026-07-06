"""Intraday real-time quote pipeline (E.Sun / esun_marketdata).

Layers:
  - sweeper: REST snapshot every ~20s, full TSE + OTC market
  - store:   upsert into tw.intraday_quotes (latest snapshot only)

Entry points: intraday_sweep_update.py (sweeper daemon) and
intraday_snapshot.py (ScoreBoard + signal snapshot daemon) at the repo root.
"""
