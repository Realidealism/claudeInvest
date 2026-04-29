"""Revenue-related indicators (TTM, YoY, monthly momentum, quarterly momentum)."""

from __future__ import annotations

from decimal import Decimal

from ..data.models import MonthlyRevenue, QuarterlyReport


def ttm_revenue(reports: list[QuarterlyReport]) -> Decimal | None:
    """Sum the latest 4 single-quarter revenues. None if any of the 4 missing."""
    if len(reports) < 4:
        return None
    last4 = reports[-4:]
    if any(r.revenue is None for r in last4):
        return None
    return sum((r.revenue for r in last4 if r.revenue is not None), Decimal(0))


def revenue_yoy_ttm(reports: list[QuarterlyReport]) -> Decimal | None:
    """TTM revenue growth = (latest 4q sum) / (prior 4q sum) - 1."""
    if len(reports) < 8:
        return None
    cur = ttm_revenue(reports)
    prev = ttm_revenue(reports[:-4])
    if cur is None or prev is None or prev == 0:
        return None
    return (cur - prev) / prev


def quarterly_qoq(reports: list[QuarterlyReport]) -> Decimal | None:
    """Latest single-quarter revenue QoQ growth."""
    if len(reports) < 2:
        return None
    cur, prev = reports[-1].revenue, reports[-2].revenue
    if cur is None or prev is None or prev == 0:
        return None
    return (cur - prev) / prev


def quarterly_yoy(reports: list[QuarterlyReport]) -> Decimal | None:
    """Latest single-quarter revenue YoY growth (vs same quarter last year)."""
    if len(reports) < 5:
        return None
    cur, prev = reports[-1].revenue, reports[-5].revenue
    if cur is None or prev is None or prev == 0:
        return None
    return (cur - prev) / prev


def quarterly_yoy_at(reports: list[QuarterlyReport], idx: int) -> Decimal | None:
    """Quarterly YoY at offset idx (negative = from end). Used by F8."""
    if abs(idx) >= len(reports) or len(reports) + idx < 4:
        return None
    cur = reports[idx].revenue
    prev = reports[idx - 4].revenue
    if cur is None or prev is None or prev == 0:
        return None
    return (cur - prev) / prev


def latest_month_yoy(monthly: list[MonthlyRevenue]) -> Decimal | None:
    if not monthly or monthly[-1].yoy is None:
        return None
    return monthly[-1].yoy / Decimal(100)  # DB stores as percent


def cumulative_yoy_ytd(monthly: list[MonthlyRevenue]) -> Decimal | None:
    """YTD cumulative revenue YoY. monthly is ascending; uses latest month's
    year-to-date sum vs same months prior year.
    """
    if not monthly:
        return None
    latest = monthly[-1]
    year, month = latest.year_month.split("-")
    yi, mi = int(year), int(month)

    def ytd_sum(target_year: int) -> Decimal | None:
        s = Decimal(0)
        seen = 0
        for r in monthly:
            ry, rm = r.year_month.split("-")
            if int(ry) == target_year and int(rm) <= mi:
                s += r.revenue
                seen += 1
        return s if seen >= 1 else None

    cur = ytd_sum(yi)
    prev = ytd_sum(yi - 1)
    if cur is None or prev is None or prev == 0:
        return None
    return (cur - prev) / prev


def is_monthly_revenue_12m_high(monthly: list[MonthlyRevenue]) -> bool | None:
    if len(monthly) < 12:
        return None
    last = monthly[-1].revenue
    window = monthly[-12:]
    return all(last >= m.revenue for m in window)
