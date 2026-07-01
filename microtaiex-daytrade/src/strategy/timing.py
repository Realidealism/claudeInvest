"""Time-rule gates for the 麻紗 day-trade system (MASHA_FUTURES_SPEC §8).

Two independent, entry-only gates (they never block exits/stops):

  in_tradeable_window(ts) — §8.1: only enter during the productive part of the
    session (麻紗 mostly trades before 10:00, futures to ~11:00). Avoids the
    open-15min wash and the post-12:30 作價 tail.

  is_excluded_bar(ts)     — §8.2: drop the last N 5-min bars before settlement
    close, when 主力做價 distorts the K (結算與現貨加權收盤不同步):
      weekly settlement (any Wed)         -> last 3 bars
      monthly settlement (third Wed)      -> near-month closes 13:30, last 6 bars
      四巫日 (third Fri of 3/6/9/12)       -> 比照週三, last 3 bars

Both are pure time-of-day rules over the TAIFEX session clock (core/clock).
Windows/counts are module constants so they can be swept in backtests.
"""
from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import Sequence

from broker.types import Bar
from core import clock

# §8.1 entry windows per session (start inclusive, end exclusive), local time.
DAY_ENTRY_WINDOW = (time(8, 45), time(11, 0))
NIGHT_ENTRY_WINDOW = (time(15, 0), time(17, 0))

# §8.2 near-month final trading on monthly settlement day is 13:30 (not 13:45).
_SETTLE_CLOSE_NEAR = time(13, 30)

# §8.3 時間波 points: 固定型 (09:20/09:40) + 一小時型 (10:00/11:00) + 對稱型
# (10:35/10:45 = 09:35/09:45 + 1h). Bar close時間到此 → 提高警覺.
TIMEWAVE_POINTS = frozenset({
    time(9, 20), time(9, 40), time(10, 0),
    time(10, 35), time(10, 45), time(11, 0),
})


def in_tradeable_window(ts: datetime) -> bool:
    """True if new entries are allowed at ts (§8.1). Exits are never gated."""
    t = ts.time()
    s = clock.session_of(ts)
    if s is clock.Session.DAY:
        return DAY_ENTRY_WINDOW[0] <= t < DAY_ENTRY_WINDOW[1]
    if s is clock.Session.NIGHT:
        # night window is the first hours from 15:00 (all before midnight)
        return NIGHT_ENTRY_WINDOW[0] <= t < NIGHT_ENTRY_WINDOW[1]
    return False


def _exclusion(ts: datetime):
    """(close_time, n_bars) for settlement proximity on ts's day session, or None."""
    if clock.session_of(ts) is not clock.Session.DAY:
        return None
    d = ts.date()
    if clock.is_settlement_day(d):          # third Wednesday = monthly
        return _SETTLE_CLOSE_NEAR, 6        # near-month settles 13:30, last 6 bars
    if d.weekday() == 2:                    # any other Wednesday = weekly
        return clock.DAY_CLOSE, 3
    if clock.is_quadruple_witching(d):      # third Friday of a quarter month
        return clock.DAY_CLOSE, 3
    return None


def is_excluded_bar(ts: datetime, bar_minutes: int = 5) -> bool:
    """True if ts falls in a settlement-做價 window and should not produce entries."""
    ex = _exclusion(ts)
    if ex is None:
        return False
    close_t, n = ex
    cutoff = (datetime.combine(ts.date(), close_t)
              - timedelta(minutes=n * bar_minutes)).time()
    return ts.time() >= cutoff


# ── §8.3 時間波 有單/沒單 ────────────────────────────────────────────────

def is_timewave_bar(ts: datetime) -> bool:
    """True if the bar closes on a 時間波 point (§8.3)."""
    return ts.time() in TIMEWAVE_POINTS


def no_direction(bars: Sequence[Bar], doji_ratio: float = 0.3) -> bool:
    """§8.3 沒方向: the current bar has a small body vs its range (doji-like) —
    the market showed no commitment at the time point."""
    if not bars:
        return False
    b = bars[-1]
    rng = b.high - b.low
    if rng <= 0:
        return True
    return abs(b.close - b.open) <= doji_ratio * rng
