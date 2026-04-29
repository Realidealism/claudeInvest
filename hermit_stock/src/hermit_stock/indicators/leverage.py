"""Leverage indicators: debt ratio."""

from __future__ import annotations

from decimal import Decimal

from ..data.models import QuarterlyReport


def debt_ratio(report: QuarterlyReport) -> Decimal | None:
    if report.total_liabilities is None or report.total_assets is None:
        return None
    if report.total_assets == 0:
        return None
    return report.total_liabilities / report.total_assets


def annual_debt_ratio(reports: list[QuarterlyReport], year: int) -> Decimal | None:
    """Use Q4 balance sheet as year-end snapshot."""
    q4 = next((r for r in reports if r.period == f"{year}Q4"), None)
    if q4 is None:
        return None
    return debt_ratio(q4)


def debt_ratio_consecutive_rises_years(
    reports: list[QuarterlyReport], lookback: int = 4
) -> int | None:
    """Count consecutive year-end debt-ratio rises ending at the latest year."""
    years = sorted({int(r.period[:4]) for r in reports if r.period.endswith("Q4")})
    if len(years) < 2:
        return None
    series: list[tuple[int, Decimal]] = []
    for y in years[-lookback - 1 :]:
        v = annual_debt_ratio(reports, y)
        if v is not None:
            series.append((y, v))
    if len(series) < 2:
        return 0
    series.sort(key=lambda x: x[0])
    rises = 0
    for i in range(len(series) - 1, 0, -1):
        if series[i][1] > series[i - 1][1]:
            rises += 1
        else:
            break
    return rises
