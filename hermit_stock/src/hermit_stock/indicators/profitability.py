"""Profitability indicators: net income TTM YoY, gross/operating margins."""

from __future__ import annotations

from decimal import Decimal

from ..data.models import QuarterlyReport


def ttm_net_income(reports: list[QuarterlyReport]) -> Decimal | None:
    if len(reports) < 4:
        return None
    last4 = reports[-4:]
    if any(r.net_income is None for r in last4):
        return None
    return sum((r.net_income for r in last4 if r.net_income is not None), Decimal(0))


def net_income_yoy_ttm(reports: list[QuarterlyReport]) -> Decimal | None:
    if len(reports) < 8:
        return None
    cur = ttm_net_income(reports)
    prev = ttm_net_income(reports[:-4])
    if cur is None or prev is None or prev == 0:
        return None
    return (cur - prev) / prev


def annual_gross_margin(reports: list[QuarterlyReport], year: int) -> Decimal | None:
    """Sum gross_profit and revenue across the 4 quarters of `year` and divide."""
    target = [r for r in reports if r.period.startswith(f"{year}Q")]
    if len(target) < 4:
        return None
    if any(r.revenue is None or r.gross_profit is None for r in target):
        return None
    rev = sum((r.revenue for r in target if r.revenue is not None), Decimal(0))
    gp = sum((r.gross_profit for r in target if r.gross_profit is not None), Decimal(0))
    if rev == 0:
        return None
    return gp / rev


def gross_margin_consecutive_rises(
    reports: list[QuarterlyReport], lookback_years: int = 3
) -> int | None:
    """Count consecutive years (most recent first) where annual gross margin
    rose vs the previous year. Returns None if too few full years available.
    """
    if not reports:
        return None
    years = sorted({int(r.period[:4]) for r in reports if r.period.endswith("Q4")})
    if len(years) < 2:
        return None
    margins: list[tuple[int, Decimal]] = []
    for y in years[-lookback_years - 1 :]:
        m = annual_gross_margin(reports, y)
        if m is not None:
            margins.append((y, m))
    if len(margins) < 2:
        return None
    margins.sort(key=lambda x: x[0])
    rises = 0
    for i in range(len(margins) - 1, 0, -1):
        if margins[i][1] > margins[i - 1][1]:
            rises += 1
        else:
            break
    return rises


def operating_margin(report: QuarterlyReport) -> Decimal | None:
    if report.revenue is None or report.operating_income is None or report.revenue == 0:
        return None
    return report.operating_income / report.revenue
