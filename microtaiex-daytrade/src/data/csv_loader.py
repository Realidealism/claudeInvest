"""Load historical bars from CSV into List[Bar] for backtesting.

Expected columns (header required): ts, open, high, low, close, volume
``ts`` accepts ISO-8601 ("2024-12-18T09:05:00") or "YYYY/MM/DD HH:MM".
"""
from __future__ import annotations

import csv
import glob
from datetime import datetime
from typing import List, Tuple

from broker.types import Bar


def _parse_ts(s: str) -> datetime:
    s = s.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return datetime.fromisoformat(s)  # last resort; raises if unrecognized


def load_bars_csv(path: str, symbol: str = "TMF00", timeframe: str = "5m") -> List[Bar]:
    bars: List[Bar] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            bars.append(Bar(
                symbol=row.get("symbol", symbol),
                ts=_parse_ts(row["ts"]),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=int(float(row["volume"])),
                timeframe=row.get("timeframe", timeframe),
            ))
    bars.sort(key=lambda b: b.ts)
    return bars


def load_bars_glob(pattern: str, symbol: str = "TMF00", timeframe: str = "5m") -> Tuple[List[Bar], List[str]]:
    """Load + merge all CSVs matching ``pattern`` (a single file also works).

    De-duplicates by timestamp, sorts ascending. Returns (bars, matched_files).
    """
    files = glob.glob(pattern) or [pattern]
    seen, bars = set(), []
    for fp in sorted(files):
        for b in load_bars_csv(fp, symbol, timeframe):
            if b.ts in seen:
                continue
            seen.add(b.ts)
            bars.append(b)
    bars.sort(key=lambda b: b.ts)
    return bars, sorted(files)
