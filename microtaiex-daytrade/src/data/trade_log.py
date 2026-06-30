"""Append closed round-trips to a CSV for later review (paper or live).

One row per completed round-trip. Created with a header if the file is new;
otherwise appended, so a multi-session day accumulates in one file.
"""
from __future__ import annotations

import csv
from pathlib import Path

_HEADER = ["entry_ts", "exit_ts", "symbol", "side", "lot",
           "entry_price", "exit_price", "points", "gross_ntd", "signal"]


class TradeLog:
    def __init__(self, path: str, point_value: float = 10.0) -> None:
        self._path = Path(path)
        self._point_value = point_value
        if self._path.parent and str(self._path.parent) not in ("", "."):
            self._path.parent.mkdir(parents=True, exist_ok=True)
        if not self._path.exists():
            with open(self._path, "w", encoding="utf-8", newline="") as f:
                csv.writer(f).writerow(_HEADER)

    def append(self, rt) -> None:
        gross = rt.points * self._point_value * rt.lot
        with open(self._path, "a", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow([
                rt.entry_ts.isoformat() if rt.entry_ts else "",
                rt.exit_ts.isoformat() if rt.exit_ts else "",
                rt.symbol, rt.side.value, rt.lot,
                rt.entry_price, rt.exit_price, rt.points, gross,
                getattr(rt, "reason", ""),
            ])
