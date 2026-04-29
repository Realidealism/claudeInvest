"""Per-share indicators: EPS_TTM, BVPS, SPS."""

from __future__ import annotations

from decimal import Decimal

from ..data.models import QuarterlyReport


def eps_ttm(reports: list[QuarterlyReport]) -> Decimal | None:
    """Sum the latest 4 single-quarter EPS values."""
    if len(reports) < 4:
        return None
    last4 = reports[-4:]
    if any(r.eps is None for r in last4):
        return None
    return sum((r.eps for r in last4 if r.eps is not None), Decimal(0))


def bvps(report: QuarterlyReport) -> Decimal | None:
    if report.book_value_per_share is not None:
        return report.book_value_per_share
    if (
        report.equity_attributable is not None
        and report.shares_outstanding is not None
        and report.shares_outstanding != 0
    ):
        return report.equity_attributable / report.shares_outstanding
    if (
        report.total_equity is not None
        and report.shares_outstanding is not None
        and report.shares_outstanding != 0
    ):
        return report.total_equity / report.shares_outstanding
    return None


def sps_ttm(reports: list[QuarterlyReport]) -> Decimal | None:
    """TTM revenue / latest shares outstanding."""
    if len(reports) < 4:
        return None
    last4 = reports[-4:]
    if any(r.revenue is None for r in last4):
        return None
    revenue_ttm = sum((r.revenue for r in last4 if r.revenue is not None), Decimal(0))
    shares = reports[-1].shares_outstanding
    if shares is None or shares == 0:
        return None
    return revenue_ttm / shares
