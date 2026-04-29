"""Efficiency indicators: inventory days, AR days."""

from __future__ import annotations

from decimal import Decimal

from ..data.models import QuarterlyReport

DAYS_PER_QUARTER = Decimal(91)


def inventory_days(report: QuarterlyReport) -> Decimal | None:
    """Single-quarter inventory days = inventory / cogs * 91."""
    if report.inventory is None or report.cost_of_revenue is None:
        return None
    if report.cost_of_revenue == 0:
        return None
    return report.inventory / report.cost_of_revenue * DAYS_PER_QUARTER


def inventory_days_series(reports: list[QuarterlyReport]) -> list[Decimal | None]:
    return [inventory_days(r) for r in reports]


def consecutive_rises_at_end(values: list[Decimal | None]) -> int:
    """Count how many consecutive rises end the series (latest first).

    Skips Nones at the tail, then counts rises walking backwards.
    """
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return 0
    rises = 0
    for i in range(len(clean) - 1, 0, -1):
        if clean[i] > clean[i - 1]:
            rises += 1
        else:
            break
    return rises
