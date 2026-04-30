"""Auto-select the appropriate valuation method (PE / PB / PS) per design §8.1.

Phase 2 default: industry value_source defaults to "operations", so most
profitable companies pick PE. Industry YAML override is a Phase 6 feature; the
selector accepts an optional override in the meantime.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from ..data.models import QuarterlyReport
from ..indicators import revenue

ValuationMethod = Literal["PE", "PB", "PS"]
ValueSource = Literal["operations", "assets"]


def _annual_net_income(reports: list[QuarterlyReport], year: int) -> Decimal | None:
    target = [r for r in reports if r.period.startswith(f"{year}Q")]
    if len(target) < 4:
        return None
    if any(r.net_income is None for r in target):
        return None
    return sum((r.net_income for r in target if r.net_income is not None), Decimal(0))


def _consecutive_profitable_years(reports: list[QuarterlyReport]) -> int:
    years = sorted({int(r.period[:4]) for r in reports if r.period.endswith("Q4")})
    consecutive = 0
    for y in reversed(years):
        ni = _annual_net_income(reports, y)
        if ni is None:
            break
        if ni > 0:
            consecutive += 1
        else:
            break
    return consecutive


def _annual_revenue(reports: list[QuarterlyReport], year: int) -> Decimal | None:
    target = [r for r in reports if r.period.startswith(f"{year}Q")]
    if len(target) < 4:
        return None
    if any(r.revenue is None for r in target):
        return None
    return sum((r.revenue for r in target if r.revenue is not None), Decimal(0))


def _three_year_revenue_growth(reports: list[QuarterlyReport]) -> Decimal | None:
    """Annualized revenue growth over the last 3 full years (CAGR)."""
    years = sorted({int(r.period[:4]) for r in reports if r.period.endswith("Q4")})
    if len(years) < 4:
        return None
    end_year = years[-1]
    start_year = end_year - 3
    end_rev = _annual_revenue(reports, end_year)
    start_rev = _annual_revenue(reports, start_year)
    if end_rev is None or start_rev is None or start_rev == 0:
        return None
    ratio = end_rev / start_rev
    # CAGR = ratio^(1/3) - 1; pow on Decimal is fragile, use float here
    cagr = float(ratio) ** (1 / 3) - 1
    return Decimal(str(cagr))


def select_valuation_method(
    reports: list[QuarterlyReport],
    value_source: ValueSource = "operations",
    high_growth_threshold: Decimal = Decimal("0.20"),
) -> ValuationMethod:
    """Phase 2 implementation of design §8.1 selector tree."""
    profitable_years = _consecutive_profitable_years(reports)
    if profitable_years >= 3:
        return "PE" if value_source == "operations" else "PB"
    cagr = _three_year_revenue_growth(reports) or revenue.revenue_yoy_ttm(reports)
    if cagr is not None and cagr > high_growth_threshold:
        return "PS"
    return "PB"
