"""Cash-flow indicators: free cash flow."""

from __future__ import annotations

from decimal import Decimal

from ..data.models import QuarterlyReport


def annual_fcf(reports: list[QuarterlyReport], year: int) -> Decimal | None:
    """Sum the 4 single-quarter FCFs for `year`. Falls back to OCF + CAPEX
    (capex stored as negative number) when free_cash_flow is missing.
    """
    target = [r for r in reports if r.period.startswith(f"{year}Q")]
    if not target:
        return None
    total = Decimal(0)
    used = 0
    for r in target:
        if r.free_cash_flow is not None:
            total += r.free_cash_flow
            used += 1
        elif r.operating_cash_flow is not None and r.capex is not None:
            total += r.operating_cash_flow + r.capex
            used += 1
    if used < 4:
        return None
    return total


def annual_fcf_series(reports: list[QuarterlyReport]) -> list[tuple[int, Decimal]]:
    years = sorted({int(r.period[:4]) for r in reports if r.period.endswith("Q4")})
    out: list[tuple[int, Decimal]] = []
    for y in years:
        v = annual_fcf(reports, y)
        if v is not None:
            out.append((y, v))
    return out


def fcf_consecutive_negative_years(reports: list[QuarterlyReport]) -> int:
    series = annual_fcf_series(reports)
    if not series:
        return 0
    series.sort(key=lambda x: x[0])
    neg = 0
    for _, v in reversed(series):
        if v < 0:
            neg += 1
        else:
            break
    return neg
