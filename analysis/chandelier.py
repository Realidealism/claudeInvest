"""
Chandelier Exit — volatility-based trailing stop.

Faithful port of Alex Orekhov (everget)'s Pine v6 implementation:
    long_stop  = highest(close, length) - ATR(length) * mult
    short_stop = lowest(close, length)  + ATR(length) * mult

Trailing rule (ratchet): a stop can only move in the direction that would
make it tighter on an active position. Once the previous close leaves that
side of the stop, the ratchet resets.

Direction state machine:
    close > prior_short_stop  →  dir = +1 (long)
    close < prior_long_stop   →  dir = -1 (short)
    otherwise                 →  dir unchanged

No persistence — the engine is stateless, a single call recomputes the full
series from the supplied OHLC arrays. Backtest callers feed the full history
they already have; realtime callers feed the last N bars and read only the
latest row.

Usage:
    from analysis.chandelier import calculate_chandelier, compute_for_stock
    from db.connection import get_cursor

    # Direct: from in-memory arrays
    res = calculate_chandelier(high, low, close, length=21, mult=3.0)
    latest_dir = res.dir[-1]
    latest_long_stop = res.long_stop[-1]

    # Convenience: pull from tw.daily_prices for one ticker
    res = compute_for_stock('2330', end_date=date.today(), lookback=80)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Sequence

import numpy as np
from numpy.typing import NDArray


F64 = np.float64
F64Array = NDArray[np.float64]
I8Array = NDArray[np.int8]
BoolArray = NDArray[np.bool_]


@dataclass
class ChandelierResult:
    """Full output series, one value per input bar.

    All arrays share the length of the input `close`. Bars before enough
    warmup (~length) carry NaN for atr / long_stop / short_stop; dir
    defaults to +1 on those early bars and is meaningless until the stops
    are populated.
    """
    atr:        F64Array        # mult × 1.0 = raw ATR (per-bar volatility)
    long_stop:  F64Array        # trailing long stop
    short_stop: F64Array        # trailing short stop
    dir:        I8Array         # +1 long / -1 short
    flipped:    BoolArray       # True where dir[i] != dir[i-1]


def calculate_chandelier(
    high: Sequence[float] | F64Array,
    low:  Sequence[float] | F64Array,
    close: Sequence[float] | F64Array,
    length: int = 21,
    mult: float = 3.0,
    use_close: bool = True,
) -> ChandelierResult:
    """Compute the Chandelier Exit series for an oldest-first OHLC history.

    length      — ATR period AND highest/lowest lookback window. Default 21.
    mult        — ATR multiplier defining stop distance. Default 3.0.
    use_close   — True: uses highest(close)/lowest(close) as the extremum
                  base. False: uses highest(high)/lowest(low) — the classical
                  Chuck LeBeau formulation, slightly wider stops.
    """
    h = np.asarray(high,  dtype=F64)
    l = np.asarray(low,   dtype=F64)
    c = np.asarray(close, dtype=F64)
    n = len(c)
    if n == 0:
        empty_f = np.empty(0, dtype=F64)
        empty_i = np.empty(0, dtype=np.int8)
        empty_b = np.empty(0, dtype=np.bool_)
        return ChandelierResult(empty_f, empty_f, empty_f, empty_i, empty_b)

    # --- 1) True Range ---
    tr = np.empty(n, dtype=F64)
    tr[0] = h[0] - l[0]
    if n > 1:
        prev_close = c[:-1]
        tr[1:] = np.maximum.reduce([
            h[1:] - l[1:],
            np.abs(h[1:] - prev_close),
            np.abs(l[1:] - prev_close),
        ])

    # --- 2) ATR via Wilder's RMA ---
    # Pine's ta.atr seeds at bar `length-1` with SMA(TR, length), then
    # applies RMA recursion. Earlier bars stay NaN.
    atr = np.full(n, np.nan, dtype=F64)
    if n >= length:
        atr[length - 1] = tr[:length].mean()
        k = length - 1
        for i in range(length, n):
            atr[i] = (atr[i - 1] * k + tr[i]) / length

    # --- 3) Rolling highest / lowest over `length` bars ---
    hi_src = c if use_close else h
    lo_src = c if use_close else l
    hi_roll = np.full(n, np.nan, dtype=F64)
    lo_roll = np.full(n, np.nan, dtype=F64)
    for i in range(n):
        lo_idx = max(0, i - length + 1)
        hi_roll[i] = hi_src[lo_idx:i + 1].max()
        lo_roll[i] = lo_src[lo_idx:i + 1].min()

    # --- 4) Trailing stops + direction state machine ---
    atr_scaled = mult * atr
    long_stop  = np.full(n, np.nan, dtype=F64)
    short_stop = np.full(n, np.nan, dtype=F64)
    dir_arr    = np.ones(n, dtype=np.int8)     # default +1, matches Pine `var int dir = 1`

    # Seed the first valid bar (when ATR is available).
    first = length - 1
    if first < n:
        long_stop[first]  = hi_roll[first] - atr_scaled[first]
        short_stop[first] = lo_roll[first] + atr_scaled[first]

    for i in range(first + 1, n):
        long_new  = hi_roll[i] - atr_scaled[i]
        short_new = lo_roll[i] + atr_scaled[i]

        long_prev  = long_stop[i - 1]
        short_prev = short_stop[i - 1]

        # Ratchet: only tighten when yesterday's close was on the right side.
        long_stop[i]  = max(long_new, long_prev)   if c[i - 1] > long_prev   else long_new
        short_stop[i] = min(short_new, short_prev) if c[i - 1] < short_prev  else short_new

        # Direction flips when today's close crosses YESTERDAY's opposite stop.
        if c[i] > short_prev:
            dir_arr[i] = 1
        elif c[i] < long_prev:
            dir_arr[i] = -1
        else:
            dir_arr[i] = dir_arr[i - 1]

    # Carry dir through the warmup bars (before `first`) as the initial +1.
    # No flip is possible there because stops are NaN.
    flipped = np.zeros(n, dtype=np.bool_)
    if n > 1:
        flipped[1:] = dir_arr[1:] != dir_arr[:-1]

    return ChandelierResult(
        atr=atr,
        long_stop=long_stop,
        short_stop=short_stop,
        dir=dir_arr,
        flipped=flipped,
    )


def compute_for_stock(
    stock_id: str,
    end_date: date | None = None,
    lookback: int = 80,
    length: int = 21,
    mult: float = 3.0,
    use_close: bool = True,
) -> tuple[list[date], ChandelierResult] | None:
    """Convenience wrapper: fetch OHLC from tw.daily_prices and compute.

    Returns (dates_oldest_first, ChandelierResult) or None if no data.
    `lookback` should be generous enough that ATR and the rolling window
    both warm up before the dates of interest (≥ length + ~20 bars).
    """
    from db.connection import get_cursor

    with get_cursor(commit=False) as cur:
        if end_date is None:
            cur.execute(
                """
                SELECT trade_date, high_price, low_price, close_price
                FROM tw.daily_prices
                WHERE stock_id = %s
                  AND close_price IS NOT NULL
                  AND high_price IS NOT NULL
                  AND low_price IS NOT NULL
                ORDER BY trade_date DESC
                LIMIT %s
                """,
                (stock_id, lookback),
            )
        else:
            cur.execute(
                """
                SELECT trade_date, high_price, low_price, close_price
                FROM tw.daily_prices
                WHERE stock_id = %s
                  AND trade_date <= %s
                  AND close_price IS NOT NULL
                  AND high_price IS NOT NULL
                  AND low_price IS NOT NULL
                ORDER BY trade_date DESC
                LIMIT %s
                """,
                (stock_id, end_date, lookback),
            )
        rows = cur.fetchall()

    if not rows:
        return None

    rows.reverse()  # oldest first
    dates = [r["trade_date"] for r in rows]
    h = np.array([float(r["high_price"])  for r in rows], dtype=F64)
    l = np.array([float(r["low_price"])   for r in rows], dtype=F64)
    c = np.array([float(r["close_price"]) for r in rows], dtype=F64)

    res = calculate_chandelier(h, l, c, length=length, mult=mult, use_close=use_close)
    return dates, res
