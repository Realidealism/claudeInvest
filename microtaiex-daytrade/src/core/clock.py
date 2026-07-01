"""Trading clock: sessions, settlement, force-close, and micro-Taiex contracts.

All datetimes are treated as Taipei local (UTC+8) naive; the system is intended
to run in that timezone. Sessions (TAIFEX micro-Taiex / TMF):
- Day   08:45 - 13:45
- Night 15:00 - 05:00 (next calendar day)

Each session force-closes before its own close; positions never carry across a
session boundary (plan §8/§12).

Contract code: ``TMF`` + month letter (Jan=A .. Dec=L) + last digit of the year.
Example: 2024/12 -> ``TMFL4``. Front-month rolls to next month after settlement
(third Wednesday of the month).
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from enum import Enum

MONTH_CODES = "ABCDEFGHIJKL"  # Jan=A .. Dec=L

DAY_OPEN = time(8, 45)
DAY_CLOSE = time(13, 45)
NIGHT_OPEN = time(15, 0)
NIGHT_CLOSE = time(5, 0)   # next day

# Force-close N minutes before each session close.
FORCE_CLOSE_BUFFER_MIN = 1


class Session(Enum):
    DAY = "day"
    NIGHT = "night"
    CLOSED = "closed"


# ---- contract codes ----

def contract_code(year: int, month: int) -> str:
    """Micro-Taiex contract code, e.g. (2024, 12) -> 'TMFL4'."""
    if not 1 <= month <= 12:
        raise ValueError(f"month out of range: {month}")
    return f"TMF{MONTH_CODES[month - 1]}{year % 10}"


def third_wednesday(year: int, month: int) -> date:
    """Settlement date: the third Wednesday of the month."""
    d = date(year, month, 1)
    # weekday(): Mon=0 .. Sun=6; Wednesday=2
    offset = (2 - d.weekday()) % 7
    first_wed = d + timedelta(days=offset)
    return first_wed + timedelta(days=14)


def is_settlement_day(d: date) -> bool:
    return d == third_wednesday(d.year, d.month)


def third_friday(year: int, month: int) -> date:
    """Third Friday of the month (quadruple-witching date for quarter months)."""
    d = date(year, month, 1)
    offset = (4 - d.weekday()) % 7   # weekday(): Mon=0 .. Fri=4
    return d + timedelta(days=offset) + timedelta(days=14)


def is_quadruple_witching(d: date) -> bool:
    """四巫日: index+single-stock futures & options all settle — third Friday of
    Mar/Jun/Sep/Dec."""
    return d.month in (3, 6, 9, 12) and d == third_friday(d.year, d.month)


def front_contract(d: date) -> str:
    """Front-month contract for date ``d``; rolls after settlement day."""
    settle = third_wednesday(d.year, d.month)
    if d <= settle:
        return contract_code(d.year, d.month)
    # rolled to next month
    if d.month == 12:
        return contract_code(d.year + 1, 1)
    return contract_code(d.year, d.month + 1)


# ---- sessions ----

def session_of(dt: datetime) -> Session:
    t = dt.time()
    if DAY_OPEN <= t < DAY_CLOSE:
        return Session.DAY
    # night wraps midnight: [15:00, 24:00) or [00:00, 05:00)
    if t >= NIGHT_OPEN or t < NIGHT_CLOSE:
        return Session.NIGHT
    return Session.CLOSED


def is_trading(dt: datetime) -> bool:
    return session_of(dt) is not Session.CLOSED


def session_close(dt: datetime) -> datetime | None:
    """Absolute close datetime of the session containing ``dt`` (None if closed)."""
    s = session_of(dt)
    if s is Session.DAY:
        return datetime.combine(dt.date(), DAY_CLOSE)
    if s is Session.NIGHT:
        if dt.time() >= NIGHT_OPEN:
            # before midnight -> close is 05:00 next day
            return datetime.combine(dt.date() + timedelta(days=1), NIGHT_CLOSE)
        # after midnight -> close is 05:00 same day
        return datetime.combine(dt.date(), NIGHT_CLOSE)
    return None


def force_close_at(dt: datetime) -> datetime | None:
    """When to force-close the session containing ``dt`` (None if closed)."""
    close = session_close(dt)
    if close is None:
        return None
    return close - timedelta(minutes=FORCE_CLOSE_BUFFER_MIN)


def should_force_close(dt: datetime) -> bool:
    fc = force_close_at(dt)
    return fc is not None and dt >= fc
