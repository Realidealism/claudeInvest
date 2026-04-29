"""Pydantic data models for fundamental data.

DB stores monetary values in thousands of NTD (千元). Adapters multiply by 1000
on the way in so all model fields are denominated in NTD. Per-share values
(eps, book_value_per_share) and percentages are stored as-is.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

QuarterlySource = Literal["db", "mops", "finmind", "csv"]
MonthlySource = Literal["db", "mops", "finmind", "csv"]
PriceSource = Literal["db", "twse", "tpex", "finmind"]


class QuarterlyReport(BaseModel):
    """Single-quarter financial report (period_type='Q' from MOPS).

    Merges income_statements + balance_sheets + cash_flows into one row.
    Balance-sheet fields are point-in-time at quarter end (no period_type).
    """

    model_config = ConfigDict(frozen=True)

    ticker: str
    period: str  # "2024Q3"
    period_end: date
    publish_date: date  # estimated by statutory deadline (see publish_date.py)
    source: QuarterlySource = "db"

    # --- Income statement (single-quarter, period_type='Q') ---
    revenue: Decimal | None = None
    cost_of_revenue: Decimal | None = None
    gross_profit: Decimal | None = None
    operating_expenses: Decimal | None = None
    operating_income: Decimal | None = None
    non_operating_income: Decimal | None = None
    pretax_income: Decimal | None = None
    tax_expense: Decimal | None = None
    net_income: Decimal | None = None
    net_income_attributable: Decimal | None = None
    eps: Decimal | None = None

    # --- Balance sheet (point-in-time at quarter end) ---
    inventory: Decimal | None = None
    accounts_receivable: Decimal | None = None
    short_term_debt: Decimal | None = None
    long_term_debt: Decimal | None = None
    total_assets: Decimal | None = None
    total_liabilities: Decimal | None = None
    total_equity: Decimal | None = None
    equity_attributable: Decimal | None = None
    shares_outstanding: Decimal | None = None
    book_value_per_share: Decimal | None = None

    # --- Cash flow (single-quarter, period_type='Q') ---
    operating_cash_flow: Decimal | None = None
    capex: Decimal | None = None  # negative number per MOPS convention
    investing_cash_flow: Decimal | None = None
    financing_cash_flow: Decimal | None = None
    free_cash_flow: Decimal | None = None


class MonthlyRevenue(BaseModel):
    """Monthly revenue disclosure (公布日 = 次月 10 號)."""

    model_config = ConfigDict(frozen=True)

    ticker: str
    year_month: str = Field(pattern=r"^\d{4}-\d{2}$")  # "2024-09"
    publish_date: date
    source: MonthlySource = "db"
    revenue: Decimal
    yoy: Decimal | None = None  # year-over-year % (e.g. 12.5 means +12.5%)
    mom: Decimal | None = None  # month-over-month %


class DailyPrice(BaseModel):
    """Daily OHLCV. close is unadjusted (used for PE/PB/PS, market cap)."""

    model_config = ConfigDict(frozen=True)

    ticker: str
    trade_date: date
    source: PriceSource = "db"
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    close: Decimal | None = None
    volume: int | None = None


class StockMeta(BaseModel):
    model_config = ConfigDict(frozen=True)

    ticker: str
    name: str
    market: Literal["TWSE", "TPEx"]
    industry: str | None = None
    listed_date: date | None = None
