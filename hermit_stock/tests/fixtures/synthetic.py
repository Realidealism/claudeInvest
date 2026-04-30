"""In-memory synthetic fixtures for unit tests (no DB required)."""

from __future__ import annotations

from decimal import Decimal

from hermit_stock.data.models import MonthlyRevenue, QuarterlyReport
from hermit_stock.data.publish_date import quarter_period_end, quarter_publish_date


def make_quarter(
    year: int,
    quarter: int,
    *,
    revenue: Decimal | None = None,
    cogs: Decimal | None = None,
    gross_profit: Decimal | None = None,
    net_income: Decimal | None = None,
    eps: Decimal | None = None,
    inventory: Decimal | None = None,
    total_assets: Decimal | None = None,
    total_liabilities: Decimal | None = None,
    total_equity: Decimal | None = None,
    equity_attributable: Decimal | None = None,
    shares_outstanding: Decimal | None = None,
    operating_cash_flow: Decimal | None = None,
    capex: Decimal | None = None,
    free_cash_flow: Decimal | None = None,
    book_value_per_share: Decimal | None = None,
    operating_income: Decimal | None = None,
    ticker: str = "TEST",
) -> QuarterlyReport:
    return QuarterlyReport(
        ticker=ticker,
        period=f"{year}Q{quarter}",
        period_end=quarter_period_end(year, quarter),
        publish_date=quarter_publish_date(year, quarter),
        source="csv",
        revenue=revenue,
        cost_of_revenue=cogs,
        gross_profit=gross_profit,
        operating_income=operating_income,
        net_income=net_income,
        eps=eps,
        inventory=inventory,
        total_assets=total_assets,
        total_liabilities=total_liabilities,
        total_equity=total_equity,
        equity_attributable=equity_attributable,
        shares_outstanding=shares_outstanding,
        operating_cash_flow=operating_cash_flow,
        capex=capex,
        free_cash_flow=free_cash_flow,
        book_value_per_share=book_value_per_share,
    )


def make_monthly(
    year: int,
    month: int,
    revenue: Decimal,
    *,
    yoy: Decimal | None = None,
    mom: Decimal | None = None,
    ticker: str = "TEST",
) -> MonthlyRevenue:
    from hermit_stock.data.publish_date import monthly_publish_date

    ym = f"{year:04d}-{month:02d}"
    return MonthlyRevenue(
        ticker=ticker,
        year_month=ym,
        publish_date=monthly_publish_date(ym),
        source="csv",
        revenue=revenue,
        yoy=yoy,
        mom=mom,
    )


def linear_growth_quarters(
    *,
    start_year: int = 2020,
    n_quarters: int = 12,
    base_revenue: Decimal = Decimal("100_000_000"),
    qoq_growth: Decimal = Decimal("0.05"),
    gross_margin: Decimal = Decimal("0.4"),
    net_margin: Decimal = Decimal("0.2"),
    shares_outstanding: Decimal = Decimal("1_000_000_000"),
) -> list[QuarterlyReport]:
    """Generate `n_quarters` of compounding-growth synthetic reports."""
    reports: list[QuarterlyReport] = []
    rev = base_revenue
    for i in range(n_quarters):
        year = start_year + i // 4
        q = (i % 4) + 1
        gp = rev * gross_margin
        cogs = rev - gp
        ni = rev * net_margin
        eps = ni / shares_outstanding
        reports.append(
            make_quarter(
                year,
                q,
                revenue=rev,
                cogs=cogs,
                gross_profit=gp,
                net_income=ni,
                eps=eps,
                inventory=cogs * Decimal("0.25"),
                total_assets=rev * 4,
                total_liabilities=rev * Decimal("1.6"),
                total_equity=rev * Decimal("2.4"),
                equity_attributable=rev * Decimal("2.4"),
                shares_outstanding=shares_outstanding,
                operating_cash_flow=ni * Decimal("1.1"),
                capex=-(rev * Decimal("0.05")),
                free_cash_flow=ni * Decimal("1.1") - rev * Decimal("0.05"),
            )
        )
        rev *= Decimal(1) + qoq_growth
    return reports
