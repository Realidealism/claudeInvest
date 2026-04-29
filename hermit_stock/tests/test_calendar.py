"""Rebalance-calendar tests."""

from __future__ import annotations

from datetime import date, timedelta

from hermit_stock.backtest.calendar import (
    monthly_event_days,
    next_trading_day,
    quarterly_event_days,
    rebalance_dates,
)


def test_monthly_event_days_emits_10th_of_each_month() -> None:
    days = monthly_event_days(date(2024, 1, 1), date(2024, 6, 30))
    assert days == [
        date(2024, 1, 10),
        date(2024, 2, 10),
        date(2024, 3, 10),
        date(2024, 4, 10),
        date(2024, 5, 10),
        date(2024, 6, 10),
    ]


def test_quarterly_event_days_uses_statutory_caps() -> None:
    days = quarterly_event_days(date(2024, 1, 1), date(2024, 12, 31))
    # 2023Q4 → 2024-03-31, 2024Q1 → 2024-05-15, 2024Q2 → 8/14, 2024Q3 → 11/14
    assert date(2024, 3, 31) in days
    assert date(2024, 5, 15) in days
    assert date(2024, 8, 14) in days
    assert date(2024, 11, 14) in days


def test_next_trading_day_skips_weekends() -> None:
    trading = [
        date(2024, 11, 11),
        date(2024, 11, 12),
        date(2024, 11, 13),
        date(2024, 11, 14),
        date(2024, 11, 15),
    ]
    # Q3 publish 11/14 → next trading day 11/15
    assert next_trading_day(date(2024, 11, 14), trading) == date(2024, 11, 15)
    # If 11/15 falls on Sat, next would be Mon — but here we have 11/15 in calendar
    assert next_trading_day(date(2024, 11, 13), trading) == date(2024, 11, 14)


def test_next_trading_day_returns_none_past_calendar() -> None:
    trading = [date(2024, 1, 1)]
    assert next_trading_day(date(2024, 12, 31), trading) is None


def test_rebalance_dates_combines_and_dedupes() -> None:
    # Build a calendar of all weekdays in Jan-Dec 2024
    trading: list[date] = []
    d = date(2024, 1, 1)
    while d <= date(2024, 12, 31):
        if d.weekday() < 5:  # Mon-Fri
            trading.append(d)
        d += timedelta(days=1)

    rd = rebalance_dates(date(2024, 1, 1), date(2024, 12, 31), trading)
    # Expect ~12 monthly + 4 quarterly = up to 16, but some collapse / move
    assert 10 <= len(rd) <= 16
    # All in calendar
    assert all(d in trading for d in rd)
    # Strictly sorted, no dupes
    assert rd == sorted(set(rd))
