"""
Donchian Channel Breakout — classic Turtle Trading signal.

Upper / lower bands are rolling highest(high, N) / lowest(low, N). A close
that pierces the prior-bar upper signals a long entry; piercing the prior-
bar lower signals a short entry. Exits use a shorter window M < N for
quicker reaction — the asymmetric "System 1" variant of Richard Dennis's
original Turtle rules.

Reuses rolling_highest / rolling_lowest from analysis.indicators. Those
helpers already power the 2-5 day trigger/creep pattern in analysis.candle;
Donchian is conceptually the same operation at swing-trading timescales
(20 / 55 days).

Usage:
    from analysis.donchian import calculate_donchian, compute_for_stock

    # Direct: from in-memory OHLC arrays
    res = calculate_donchian(high, low, close, entry_length=20, exit_length=10)
    latest_dir = res.direction[-1]        # +1 long, -1 short, 0 flat
    latest_upper = res.upper_entry[-1]    # prior-bar 20-day high

    # Convenience: pull from tw.daily_prices for one ticker
    dates, res = compute_for_stock('2330', lookback=80)

Parameter conventions:
    20 / 10  — Turtle System 1 (fast swing)
    55 / 20  — Turtle System 2 (long swing, big trends)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Sequence

import numpy as np
from numpy.typing import NDArray

from analysis.indicators import rolling_highest, rolling_lowest


F32 = np.float32
F32Array = NDArray[np.float32]
I8Array = NDArray[np.int8]
BoolArray = NDArray[np.bool_]


@dataclass
class DonchianResult:
    """One value per input bar; all arrays share length with `close`.

    upper_entry / lower_entry    prior-bar N-day extremes for entry
    upper_exit  / lower_exit     prior-bar M-day extremes for exit
    entry_long  = close > upper_entry (raw condition, pre-state)
    entry_short = close < lower_entry
    exit_long   = close < lower_exit  (relevant only when in long position)
    exit_short  = close > upper_exit  (relevant only when in short position)
    direction   = +1 in long / -1 in short / 0 flat (state machine output)
    flipped     = True where direction differs from previous bar
    valid       = True after entry_length bars of history have accumulated
    """
    upper_entry: F32Array
    lower_entry: F32Array
    upper_exit:  F32Array
    lower_exit:  F32Array

    entry_long:  BoolArray
    entry_short: BoolArray
    exit_long:   BoolArray
    exit_short:  BoolArray

    direction:   I8Array
    flipped:     BoolArray
    valid:       BoolArray


def _shift1(arr: F32Array) -> F32Array:
    """Shift array by 1 (prior-bar semantics). First element gets its own value
    so downstream comparisons behave like "no prior data yet" once masked by
    the `valid` flag."""
    out = np.roll(arr, 1).copy()
    if len(out) > 0:
        out[0] = arr[0]
    return out


def calculate_donchian(
    high:  Sequence[float] | F32Array,
    low:   Sequence[float] | F32Array,
    close: Sequence[float] | F32Array,
    entry_length: int = 20,
    exit_length:  int = 10,
) -> DonchianResult:
    """Compute Donchian Channel + direction state over an oldest-first OHLC series.

    entry_length    window for entry breakout (default 20 — Turtle System 1)
    exit_length     window for exit breakout  (default 10 — System 1 fast exit)
                    Use 55 / 20 for the slow System 2 variant.
    """
    h = np.asarray(high,  dtype=F32)
    l = np.asarray(low,   dtype=F32)
    c = np.asarray(close, dtype=F32)
    n = len(c)

    if n == 0:
        empty_f = np.empty(0, dtype=F32)
        empty_i = np.empty(0, dtype=np.int8)
        empty_b = np.empty(0, dtype=np.bool_)
        return DonchianResult(
            upper_entry=empty_f, lower_entry=empty_f,
            upper_exit=empty_f,  lower_exit=empty_f,
            entry_long=empty_b,  entry_short=empty_b,
            exit_long=empty_b,   exit_short=empty_b,
            direction=empty_i,   flipped=empty_b, valid=empty_b,
        )

    # --- Rolling extremes ---
    upper_entry = _shift1(rolling_highest(h, entry_length))
    lower_entry = _shift1(rolling_lowest(l,  entry_length))
    upper_exit  = _shift1(rolling_highest(h, exit_length))
    lower_exit  = _shift1(rolling_lowest(l,  exit_length))

    # Valid once enough prior bars have accumulated for entry window.
    valid = np.arange(n) >= entry_length

    # --- Raw breakout conditions ---
    entry_long  = valid & (c > upper_entry)
    entry_short = valid & (c < lower_entry)
    exit_long   = valid & (c < lower_exit)
    exit_short  = valid & (c > upper_exit)

    # --- Direction state machine ---
    # entry conditions take priority over exit conditions: if today both
    # pierces the entry side AND the opposite-direction exit, we treat it
    # as a fresh entry in the new direction (a reversal day).
    direction = np.zeros(n, dtype=np.int8)
    prev = 0
    for i in range(n):
        if entry_long[i]:
            direction[i] = 1
        elif entry_short[i]:
            direction[i] = -1
        elif prev == 1 and exit_long[i]:
            direction[i] = 0
        elif prev == -1 and exit_short[i]:
            direction[i] = 0
        else:
            direction[i] = prev
        prev = direction[i]

    flipped = np.zeros(n, dtype=np.bool_)
    if n > 1:
        flipped[1:] = direction[1:] != direction[:-1]

    return DonchianResult(
        upper_entry=upper_entry,
        lower_entry=lower_entry,
        upper_exit=upper_exit,
        lower_exit=lower_exit,
        entry_long=entry_long,
        entry_short=entry_short,
        exit_long=exit_long,
        exit_short=exit_short,
        direction=direction,
        flipped=flipped,
        valid=valid,
    )


def compute_for_stock(
    stock_id: str,
    end_date: date | None = None,
    lookback: int = 80,
    entry_length: int = 20,
    exit_length:  int = 10,
) -> tuple[list[date], DonchianResult] | None:
    """Fetch OHLC from tw.daily_prices for one ticker and compute Donchian.

    Returns (dates_oldest_first, DonchianResult) or None if no rows.
    lookback should be ≥ entry_length + ~20 to let the bands warm up.
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
    h = np.array([float(r["high_price"])  for r in rows], dtype=F32)
    l = np.array([float(r["low_price"])   for r in rows], dtype=F32)
    c = np.array([float(r["close_price"]) for r in rows], dtype=F32)

    res = calculate_donchian(h, l, c, entry_length=entry_length, exit_length=exit_length)
    return dates, res
