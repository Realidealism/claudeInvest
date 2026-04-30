"""as_of filtering — the single chokepoint for lookahead-bias defense.

Every consumer that asks "what data was available at time t?" must call these
helpers. Filtering is on publish_date (strictly <= as_of), never on
period_end / year_month / trade_date.
"""

from __future__ import annotations

from datetime import date

from .models import DailyPrice, MonthlyRevenue, QuarterlyReport


def filter_quarterly(reports: list[QuarterlyReport], as_of: date) -> list[QuarterlyReport]:
    return [r for r in reports if r.publish_date <= as_of]


def filter_monthly(monthly: list[MonthlyRevenue], as_of: date) -> list[MonthlyRevenue]:
    return [m for m in monthly if m.publish_date <= as_of]


def filter_prices(prices: list[DailyPrice], as_of: date) -> list[DailyPrice]:
    return [p for p in prices if p.trade_date <= as_of]
