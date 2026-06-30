"""SQLite persistence for ticks and bars.

Follows the Invest repo's local-SQLite style (sqlite3 + row_factory=Row,
CREATE TABLE IF NOT EXISTS). Timestamps stored as ISO-8601 strings. Bars are
upserted on (symbol, timeframe, ts).
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from broker.types import Bar, Tick

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks (
    symbol TEXT NOT NULL,
    ts     TEXT NOT NULL,
    price  REAL NOT NULL,
    volume INTEGER NOT NULL,
    bid    REAL,
    ask    REAL
);
CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts ON ticks(symbol, ts);

CREATE TABLE IF NOT EXISTS bars (
    symbol    TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    ts        TEXT NOT NULL,
    open      REAL NOT NULL,
    high      REAL NOT NULL,
    low       REAL NOT NULL,
    close     REAL NOT NULL,
    volume    INTEGER NOT NULL,
    PRIMARY KEY (symbol, timeframe, ts)
);
"""


class Store:
    def __init__(self, path: str = "reports/market.db") -> None:
        p = Path(path)
        if p.parent and str(p.parent) not in ("", "."):
            p.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(p))
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def save_tick(self, tick: Tick) -> None:
        self._conn.execute(
            "INSERT INTO ticks (symbol, ts, price, volume, bid, ask) VALUES (?, ?, ?, ?, ?, ?)",
            (tick.symbol, tick.ts.isoformat(), tick.price, tick.volume, tick.bid, tick.ask),
        )
        self._conn.commit()

    def save_bar(self, bar: Bar) -> None:
        self._conn.execute(
            """INSERT INTO bars (symbol, timeframe, ts, open, high, low, close, volume)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(symbol, timeframe, ts) DO UPDATE SET
                   open=excluded.open, high=excluded.high, low=excluded.low,
                   close=excluded.close, volume=excluded.volume""",
            (
                bar.symbol, bar.timeframe, bar.ts.isoformat(),
                bar.open, bar.high, bar.low, bar.close, bar.volume,
            ),
        )
        self._conn.commit()

    def load_bars(self, symbol: str, timeframe: str) -> List[Bar]:
        rows = self._conn.execute(
            "SELECT * FROM bars WHERE symbol=? AND timeframe=? ORDER BY ts",
            (symbol, timeframe),
        ).fetchall()
        return [
            Bar(
                symbol=r["symbol"],
                ts=datetime.fromisoformat(r["ts"]),
                open=r["open"], high=r["high"], low=r["low"],
                close=r["close"], volume=r["volume"], timeframe=r["timeframe"],
            )
            for r in rows
        ]

    def close(self) -> None:
        self._conn.close()
