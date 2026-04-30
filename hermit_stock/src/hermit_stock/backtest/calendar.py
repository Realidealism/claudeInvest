"""Rebalance-date generator (B3' design).

Event days (publish_date in our estimator):
    monthly revenue : 10th of every month
    Q1 quarterly    : 5/15
    Q2 quarterly    : 8/14
    Q3 quarterly    : 11/14
    Q4 / annual     : 3/31 of next year

For each event day E, the rebalance day = first trading day strictly after E.
This guarantees we wait until the publish_date has fully passed before reading
the data — zero lookahead, simple to reason about.

Duplicate rebalance dates (e.g. Q2 publish 8/14 and August monthly publish
8/10 might both snap to 8/15) are de-duplicated.
"""

from __future__ import annotations

import bisect
from datetime import date

from ..data.publish_date import quarter_publish_date


def monthly_event_days(start: date, end: date) -> list[date]:
    """Monthly revenue publish-date: 10th of each month."""
    out: list[date] = []
    y, m = start.year, start.month
    while date(y, m, 10) <= end:
        d = date(y, m, 10)
        if d >= start:
            out.append(d)
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out


def quarterly_event_days(start: date, end: date) -> list[date]:
    """5/15, 8/14, 11/14, next-year 3/31."""
    out: list[date] = []
    for y in range(start.year - 1, end.year + 2):
        for q in (1, 2, 3, 4):
            d = quarter_publish_date(y, q)
            if start <= d <= end:
                out.append(d)
    return sorted(out)


def event_days(start: date, end: date) -> list[date]:
    """Union of monthly + quarterly events, sorted, deduped."""
    return sorted(set(monthly_event_days(start, end) + quarterly_event_days(start, end)))


def next_trading_day(d: date, trading_days: list[date]) -> date | None:
    """First trading day strictly > d. None if d is past the calendar."""
    idx = bisect.bisect_right(trading_days, d)
    if idx >= len(trading_days):
        return None
    return trading_days[idx]


def rebalance_dates(start: date, end: date, trading_days: list[date]) -> list[date]:
    """Snap each event day to the next trading day, dedupe, clip to [start, end]."""
    raw = event_days(start, end)
    snapped: list[date] = []
    for ev in raw:
        t = next_trading_day(ev, trading_days)
        if t is None or t > end:
            continue
        if t < start:
            continue
        snapped.append(t)
    return sorted(set(snapped))
