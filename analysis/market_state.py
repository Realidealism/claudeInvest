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

Also exposes the Go MarketArrangement support stack required by the
PercentageHot + OSC defense system in PickCondition / TouchCondition /
BuyCondition / SellCondition:
   PercentageHot     ×3 scope ×2 direction (already in v19 system)
   PercentageDir     ×3 scope ×2 direction (today > yesterday)
   PercentageStatus  ×3 scope (uint8 0-5 ordinal of Up/Flat/Down)

Caches the entire market_breadth history at first call (~2500 rows, KBs).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
from numpy.typing import NDArray

from db.connection import get_cursor
from analysis.market_percentage import calculate_percentage_hots, calculate_pct_status

I8Array = NDArray[np.int8]
U8Array = NDArray[np.uint8]
BoolArray = NDArray[np.bool_]
F32Array = NDArray[np.float32]


@dataclass
class MarketState:
    """Per-day trend codes + percentage hot/direction/status flags,
    aligned to a stock's date series. All arrays share the same length n
    as the input dates list."""
    short_trend: I8Array
    medium_trend: I8Array
    long_trend: I8Array

    short_up_hot: BoolArray
    short_down_hot: BoolArray
    medium_up_hot: BoolArray
    medium_down_hot: BoolArray
    long_up_hot: BoolArray
    long_down_hot: BoolArray

    short_up_dir: BoolArray
    short_down_dir: BoolArray
    medium_up_dir: BoolArray
    medium_down_dir: BoolArray
    long_up_dir: BoolArray
    long_down_dir: BoolArray

    short_pct_status: U8Array     # 0-5 ordinal (Go ShortPercentageStatus)
    medium_pct_status: U8Array
    long_pct_status: U8Array


# Cache the per-date row tuple after the full-history computation.
# Tuple layout (matches the per-date payload in _build_cache):
#   (s_trend, m_trend, l_trend,
#    s_up_hot, s_dn_hot, m_up_hot, m_dn_hot, l_up_hot, l_dn_hot,
#    s_up_dir, s_dn_dir, m_up_dir, m_dn_dir, l_up_dir, l_dn_dir,
#    s_pct_status, m_pct_status, l_pct_status)
_CACHE: dict[str, tuple] | None = None


def _load_cache() -> dict[str, tuple]:
    """Load market_breadth once, derive PercentageHot/Dir/Status over
    full history, return per-date tuple lookup."""
    global _CACHE
    if _CACHE is not None:
        return _CACHE

    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT trade_date, short_trend, medium_trend, long_trend, "
            "       short_up, short_down, medium_up, medium_down, "
            "       long_up, long_down, total_stocks "
            "FROM tw.market_breadth ORDER BY trade_date"
        )
        rows = cur.fetchall()

    n = len(rows)
    dates = [r["trade_date"].isoformat() for r in rows]

    def _pct(num_key: str) -> F32Array:
        arr = np.zeros(n, dtype=np.float32)
        for i, r in enumerate(rows):
            t = r["total_stocks"]
            v = r[num_key]
            if t and v is not None:
                arr[i] = float(v) / float(t)
        return arr

    s_up_pct = _pct("short_up")
    s_dn_pct = _pct("short_down")
    m_up_pct = _pct("medium_up")
    m_dn_pct = _pct("medium_down")
    l_up_pct = _pct("long_up")
    l_dn_pct = _pct("long_down")

    # Flat = neither sort_normal up nor down for that scope, derived as
    # (1 - up_pct - down_pct) and clipped at 0 to absorb rounding.
    s_flat_pct = np.clip(1.0 - s_up_pct - s_dn_pct, 0.0, 1.0).astype(np.float32)
    m_flat_pct = np.clip(1.0 - m_up_pct - m_dn_pct, 0.0, 1.0).astype(np.float32)
    l_flat_pct = np.clip(1.0 - l_up_pct - l_dn_pct, 0.0, 1.0).astype(np.float32)

    pct = calculate_percentage_hots(
        short_up=s_up_pct,
        short_down=s_dn_pct,
        medium_up=m_up_pct,
        medium_down=m_dn_pct,
        long_up=l_up_pct,
        long_down=l_dn_pct,
    )
    s_status = calculate_pct_status(s_up_pct, s_flat_pct, s_dn_pct)
    m_status = calculate_pct_status(m_up_pct, m_flat_pct, m_dn_pct)
    l_status = calculate_pct_status(l_up_pct, l_flat_pct, l_dn_pct)

    cache: dict[str, tuple] = {}
    for i in range(n):
        cache[dates[i]] = (
            int(rows[i]["short_trend"] or 0),
            int(rows[i]["medium_trend"] or 0),
            int(rows[i]["long_trend"] or 0),
            bool(pct["short_up_hot"][i]),
            bool(pct["short_down_hot"][i]),
            bool(pct["medium_up_hot"][i]),
            bool(pct["medium_down_hot"][i]),
            bool(pct["long_up_hot"][i]),
            bool(pct["long_down_hot"][i]),
            bool(pct["short_up_dir"][i]),
            bool(pct["short_down_dir"][i]),
            bool(pct["medium_up_dir"][i]),
            bool(pct["medium_down_dir"][i]),
            bool(pct["long_up_dir"][i]),
            bool(pct["long_down_dir"][i]),
            int(s_status[i]),
            int(m_status[i]),
            int(l_status[i]),
        )
    _CACHE = cache
    return _CACHE


def _to_iso(d) -> str:
    """Accept either date objects or ISO strings."""
    return d if isinstance(d, str) else d.isoformat()


_DEFAULT = (0, 0, 0,                              # trends
            False, False, False, False, False, False,   # hots
            False, False, False, False, False, False,   # dirs
            3, 3, 3)                              # pct_status (3 = neutral)


def calculate_market_state(dates) -> MarketState:
    """Align all market arrays to a list of stock dates.

    For dates missing from market_breadth (rare gaps), forward-fill the
    previous available value. Pre-history defaults match Go zero-init
    (trend=0, hot=False, dir=False, pct_status=3).
    """
    cache = _load_cache()
    n = len(dates)
    short = np.zeros(n, dtype=np.int8)
    medium = np.zeros(n, dtype=np.int8)
    long_ = np.zeros(n, dtype=np.int8)
    s_up_hot = np.zeros(n, dtype=np.bool_)
    s_dn_hot = np.zeros(n, dtype=np.bool_)
    m_up_hot = np.zeros(n, dtype=np.bool_)
    m_dn_hot = np.zeros(n, dtype=np.bool_)
    l_up_hot = np.zeros(n, dtype=np.bool_)
    l_dn_hot = np.zeros(n, dtype=np.bool_)
    s_up_dir = np.zeros(n, dtype=np.bool_)
    s_dn_dir = np.zeros(n, dtype=np.bool_)
    m_up_dir = np.zeros(n, dtype=np.bool_)
    m_dn_dir = np.zeros(n, dtype=np.bool_)
    l_up_dir = np.zeros(n, dtype=np.bool_)
    l_dn_dir = np.zeros(n, dtype=np.bool_)
    s_status = np.full(n, 3, dtype=np.uint8)
    m_status = np.full(n, 3, dtype=np.uint8)
    l_status = np.full(n, 3, dtype=np.uint8)

    last = _DEFAULT
    for i, d in enumerate(dates):
        v = cache.get(_to_iso(d))
        if v is not None:
            last = v
        (short[i], medium[i], long_[i],
         s_up_hot[i], s_dn_hot[i], m_up_hot[i], m_dn_hot[i], l_up_hot[i], l_dn_hot[i],
         s_up_dir[i], s_dn_dir[i], m_up_dir[i], m_dn_dir[i], l_up_dir[i], l_dn_dir[i],
         s_status[i], m_status[i], l_status[i]) = last

    return MarketState(
        short_trend=short, medium_trend=medium, long_trend=long_,
        short_up_hot=s_up_hot, short_down_hot=s_dn_hot,
        medium_up_hot=m_up_hot, medium_down_hot=m_dn_hot,
        long_up_hot=l_up_hot, long_down_hot=l_dn_hot,
        short_up_dir=s_up_dir, short_down_dir=s_dn_dir,
        medium_up_dir=m_up_dir, medium_down_dir=m_dn_dir,
        long_up_dir=l_up_dir, long_down_dir=l_dn_dir,
        short_pct_status=s_status,
        medium_pct_status=m_status,
        long_pct_status=l_status,
    )
