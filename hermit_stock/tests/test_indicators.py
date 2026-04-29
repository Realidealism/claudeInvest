"""Unit tests for indicators with hand-computed expected values."""

from __future__ import annotations

from decimal import Decimal

import pytest

from hermit_stock.indicators import (
    cashflow,
    efficiency,
    leverage,
    per_share,
    profitability,
    revenue,
)
from tests.fixtures.synthetic import linear_growth_quarters, make_monthly, make_quarter

# --- revenue ---


def test_ttm_revenue_sums_last_four_quarters() -> None:
    qs = [make_quarter(2023, q, revenue=Decimal(100)) for q in (1, 2, 3, 4)]
    qs += [make_quarter(2024, q, revenue=Decimal(200)) for q in (1, 2, 3, 4)]
    assert revenue.ttm_revenue(qs) == Decimal(800)
    assert revenue.ttm_revenue(qs[:-4]) == Decimal(400)


def test_ttm_revenue_returns_none_when_missing() -> None:
    qs = [make_quarter(2023, q, revenue=Decimal(100)) for q in (1, 2, 3)]
    qs.append(make_quarter(2023, 4, revenue=None))
    assert revenue.ttm_revenue(qs) is None


def test_revenue_yoy_ttm_doubles() -> None:
    qs = [make_quarter(2023, q, revenue=Decimal(100)) for q in (1, 2, 3, 4)]
    qs += [make_quarter(2024, q, revenue=Decimal(200)) for q in (1, 2, 3, 4)]
    yoy = revenue.revenue_yoy_ttm(qs)
    assert yoy == Decimal(1)


def test_quarterly_qoq_and_yoy() -> None:
    qs = [make_quarter(2023, q, revenue=Decimal(100)) for q in (1, 2, 3, 4)]
    qs.append(make_quarter(2024, 1, revenue=Decimal(120)))
    assert revenue.quarterly_qoq(qs) == Decimal("0.20")
    assert revenue.quarterly_yoy(qs) == Decimal("0.20")


def test_monthly_revenue_12m_high() -> None:
    monthly = [make_monthly(2024, m, Decimal(100 + m)) for m in range(1, 13)]
    assert revenue.is_monthly_revenue_12m_high(monthly) is True
    monthly[-1] = make_monthly(2024, 12, Decimal(50))
    assert revenue.is_monthly_revenue_12m_high(monthly) is False


def test_cumulative_yoy_ytd() -> None:
    monthly = [make_monthly(2023, m, Decimal(100)) for m in range(1, 13)]
    monthly += [make_monthly(2024, m, Decimal(150)) for m in range(1, 7)]
    yoy = revenue.cumulative_yoy_ytd(monthly)
    assert yoy == Decimal("0.5")


# --- profitability ---


def test_ttm_net_income() -> None:
    qs = [make_quarter(2024, q, net_income=Decimal(10)) for q in (1, 2, 3, 4)]
    assert profitability.ttm_net_income(qs) == Decimal(40)


def test_net_income_yoy_ttm() -> None:
    qs = [make_quarter(2023, q, net_income=Decimal(10)) for q in (1, 2, 3, 4)]
    qs += [make_quarter(2024, q, net_income=Decimal(15)) for q in (1, 2, 3, 4)]
    assert profitability.net_income_yoy_ttm(qs) == Decimal("0.5")


def test_annual_gross_margin() -> None:
    qs = [
        make_quarter(2024, q, revenue=Decimal(100), gross_profit=Decimal(40)) for q in (1, 2, 3, 4)
    ]
    assert profitability.annual_gross_margin(qs, 2024) == Decimal("0.4")


def test_gross_margin_consecutive_rises() -> None:
    qs = []
    for year, gm in [(2022, Decimal("0.3")), (2023, Decimal("0.35")), (2024, Decimal("0.4"))]:
        for q in (1, 2, 3, 4):
            rev = Decimal(100)
            qs.append(make_quarter(year, q, revenue=rev, gross_profit=rev * gm))
    rises = profitability.gross_margin_consecutive_rises(qs, lookback_years=3)
    assert rises == 2


# --- efficiency ---


def test_inventory_days() -> None:
    q = make_quarter(2024, 1, inventory=Decimal(91), cogs=Decimal(91))
    assert efficiency.inventory_days(q) == Decimal(91)


def test_consecutive_rises_at_end() -> None:
    assert efficiency.consecutive_rises_at_end([Decimal(1), Decimal(2), Decimal(3)]) == 2
    assert efficiency.consecutive_rises_at_end([Decimal(3), Decimal(2), Decimal(3)]) == 1
    assert efficiency.consecutive_rises_at_end([Decimal(3), Decimal(2)]) == 0


# --- leverage ---


def test_debt_ratio() -> None:
    q = make_quarter(2024, 4, total_assets=Decimal(100), total_liabilities=Decimal(40))
    assert leverage.debt_ratio(q) == Decimal("0.4")


def test_debt_ratio_consecutive_rises() -> None:
    qs = []
    for year, ratio in [(2022, Decimal("0.3")), (2023, Decimal("0.4")), (2024, Decimal("0.5"))]:
        qs.append(
            make_quarter(
                year,
                4,
                total_assets=Decimal(100),
                total_liabilities=Decimal(100) * ratio,
            )
        )
    assert leverage.debt_ratio_consecutive_rises_years(qs, lookback=3) == 2


# --- cashflow ---


def test_annual_fcf_uses_free_cash_flow_field() -> None:
    qs = [make_quarter(2024, q, free_cash_flow=Decimal(10)) for q in (1, 2, 3, 4)]
    assert cashflow.annual_fcf(qs, 2024) == Decimal(40)


def test_annual_fcf_falls_back_to_ocf_plus_capex() -> None:
    qs = [
        make_quarter(2024, q, operating_cash_flow=Decimal(20), capex=Decimal(-5))
        for q in (1, 2, 3, 4)
    ]
    assert cashflow.annual_fcf(qs, 2024) == Decimal(60)


def test_fcf_consecutive_negative_years() -> None:
    qs = []
    for year, fcf_q in [
        (2021, Decimal(10)),
        (2022, Decimal(-10)),
        (2023, Decimal(-5)),
        (2024, Decimal(-3)),
    ]:
        for q in (1, 2, 3, 4):
            qs.append(make_quarter(year, q, free_cash_flow=fcf_q))
    assert cashflow.fcf_consecutive_negative_years(qs) == 3


# --- per_share ---


def test_eps_ttm() -> None:
    qs = [make_quarter(2024, q, eps=Decimal("1.5")) for q in (1, 2, 3, 4)]
    assert per_share.eps_ttm(qs) == Decimal(6)


def test_bvps_uses_field_then_falls_back() -> None:
    q1 = make_quarter(2024, 1, equity_attributable=Decimal(1000), shares_outstanding=Decimal(100))
    assert per_share.bvps(q1) == Decimal(10)


def test_sps_ttm() -> None:
    qs = [
        make_quarter(2024, q, revenue=Decimal(1000), shares_outstanding=Decimal(100))
        for q in (1, 2, 3, 4)
    ]
    assert per_share.sps_ttm(qs) == Decimal(40)


# --- linear-growth fixture sanity ---


def test_linear_growth_fixture_produces_increasing_revenue() -> None:
    qs = linear_growth_quarters(n_quarters=8, qoq_growth=Decimal("0.05"))
    revs = [q.revenue for q in qs]
    assert all(
        revs[i] is not None and revs[i + 1] is not None and revs[i + 1] > revs[i]  # type: ignore[operator]
        for i in range(len(revs) - 1)
    )
    assert profitability.net_income_yoy_ttm(qs) is not None


def test_revenue_yoy_ttm_handles_zero_division() -> None:
    qs = [make_quarter(2023, q, revenue=Decimal(0)) for q in (1, 2, 3, 4)]
    qs += [make_quarter(2024, q, revenue=Decimal(100)) for q in (1, 2, 3, 4)]
    assert revenue.revenue_yoy_ttm(qs) is None


@pytest.mark.parametrize("n", [0, 1, 2, 3])
def test_ttm_returns_none_when_too_few_quarters(n: int) -> None:
    qs = [make_quarter(2024, q + 1, revenue=Decimal(100)) for q in range(n)]
    assert revenue.ttm_revenue(qs) is None
