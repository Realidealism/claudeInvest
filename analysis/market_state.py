"""
Market trend state — per-day TAIEX-wide breadth trend at short/medium/long
scopes, sourced from the pre-computed tw.market_breadth table.

Used by signal_backtest factories to filter out entries that fight the
overall market direction (e.g. don't bottom-fish in a strongly bearish
market, don't short in a strongly bullish market).

Trend codes (per analysis.market_breadth.TREND_CODE):
   -3 bear_exhausting   -2 strong_bear   -1 bear
    0 neutral
    1 bull               2 strong_bull    3 bull_exhausting

Caches the entire market_breadth history at first call (~2500 rows, KBs).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
from numpy.typing import NDArray

from db.connection import get_cursor

I8Array = NDArray[np.int8]
BoolArray = NDArray[np.bool_]


@dataclass
class MarketState:
    """Per-day trend codes aligned to a stock's date series."""
    short_trend: I8Array
    medium_trend: I8Array
    long_trend: I8Array


_CACHE: dict[str, tuple[int, int, int]] | None = None


def _load_cache() -> dict[str, tuple[int, int, int]]:
    """Load the full market_breadth history once, keyed by ISO date string."""
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT trade_date, short_trend, medium_trend, long_trend "
            "FROM tw.market_breadth ORDER BY trade_date"
        )
        rows = cur.fetchall()
    _CACHE = {
        r["trade_date"].isoformat(): (
            int(r["short_trend"] or 0),
            int(r["medium_trend"] or 0),
            int(r["long_trend"] or 0),
        )
        for r in rows
    }
    return _CACHE


def _to_iso(d) -> str:
    """Accept either date objects or ISO strings."""
    return d if isinstance(d, str) else d.isoformat()


def calculate_market_state(dates) -> MarketState:
    """Align market trend codes to a list of stock dates.

    For dates missing from market_breadth (e.g. pre-2016, or rare gaps),
    use the previous available value (forward fill). Pre-history defaults
    to 0 (neutral) so filters don't block early bars.
    """
    cache = _load_cache()
    n = len(dates)
    short  = np.zeros(n, dtype=np.int8)
    medium = np.zeros(n, dtype=np.int8)
    long_  = np.zeros(n, dtype=np.int8)

    last = (0, 0, 0)
    for i, d in enumerate(dates):
        v = cache.get(_to_iso(d))
        if v is not None:
            last = v
        short[i], medium[i], long_[i] = last

    return MarketState(short_trend=short, medium_trend=medium, long_trend=long_)
