"""Lookahead-bias invariant: every record consumed by indicators at as_of `t`
must have publish_date <= t.

Per design doc §5 / §9.3, this is a hard invariant. The test feeds a mixed
dataset (some records published before, some after `t`) through the as_of
filter and asserts:

    1. Every QuarterlyReport / MonthlyRevenue returned has publish_date <= t.
    2. Records with publish_date > t are NEVER present, regardless of period.
    3. Indicators computed on the filtered set don't reach forward — they are
       byte-identical to indicators computed on a dataset truncated before `t`.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from hermit_stock.data.as_of import filter_monthly, filter_quarterly
from hermit_stock.data.publish_date import (
    monthly_publish_date,
    quarter_period_end,
    quarter_publish_date,
)
from hermit_stock.indicators import profitability, revenue
from tests.fixtures.synthetic import make_monthly, make_quarter


def test_publish_date_never_after_as_of() -> None:
    qs = []
    for year in (2022, 2023, 2024):
        for q in (1, 2, 3, 4):
            qs.append(make_quarter(year, q, revenue=Decimal(100)))
    monthly = []
    for year in (2022, 2023, 2024):
        for m in range(1, 13):
            monthly.append(make_monthly(year, m, Decimal(10)))

    as_of = date(2024, 6, 30)
    filtered_q = filter_quarterly(qs, as_of)
    filtered_m = filter_monthly(monthly, as_of)

    assert all(r.publish_date <= as_of for r in filtered_q)
    assert all(r.publish_date <= as_of for r in filtered_m)


def test_q3_report_unavailable_until_nov_14() -> None:
    qs = [make_quarter(2024, q, revenue=Decimal(100)) for q in (1, 2, 3, 4)]

    # 2024Q3 publishes 2024-11-14. On 2024-11-13, it must be excluded.
    nov_13 = date(2024, 11, 13)
    nov_14 = date(2024, 11, 14)

    f13 = filter_quarterly(qs, nov_13)
    f14 = filter_quarterly(qs, nov_14)

    assert all(r.period != "2024Q3" for r in f13)
    assert any(r.period == "2024Q3" for r in f14)


def test_indicator_at_t_uses_only_published_data() -> None:
    """Compute revenue.ttm_revenue at t. The result must equal the same
    computation on the dataset truncated to publish_date <= t — even if the
    raw dataset contains future periods.
    """
    qs_full = [make_quarter(2023, q, revenue=Decimal(100)) for q in (1, 2, 3, 4)]
    qs_full += [make_quarter(2024, q, revenue=Decimal(200)) for q in (1, 2, 3, 4)]

    # Inject a "future" report that should be ignored.
    future_q = make_quarter(2025, 1, revenue=Decimal(999))
    qs_with_future = qs_full + [future_q]

    as_of = date(2025, 5, 14)  # 2025Q1 publishes 5/15, so still excluded
    filtered = filter_quarterly(qs_with_future, as_of)

    truncated = filter_quarterly(qs_full, as_of)
    assert revenue.ttm_revenue(filtered) == revenue.ttm_revenue(truncated)


def test_q4_2024_unavailable_until_2025_03_31() -> None:
    """Annual reports lag 90 days — 2024Q4 publishes 2025-03-31."""
    qs = [make_quarter(2024, q, net_income=Decimal(50)) for q in (1, 2, 3, 4)]

    # On 2025-03-30, 2024Q4 must NOT be in the filtered set.
    f = filter_quarterly(qs, date(2025, 3, 30))
    assert all(r.period != "2024Q4" for r in f)
    assert profitability.net_income_yoy_ttm(f) is None  # not enough data


def test_monthly_publish_date_is_next_month_10th() -> None:
    assert monthly_publish_date("2024-09") == date(2024, 10, 10)
    assert monthly_publish_date("2024-12") == date(2025, 1, 10)


def test_quarter_publish_date_helpers_self_consistent() -> None:
    """Sanity: quarter_publish_date is always strictly after period_end."""
    for year in (2020, 2024):
        for q in (1, 2, 3, 4):
            assert quarter_publish_date(year, q) > quarter_period_end(year, q)


def test_full_invariant_sweep() -> None:
    """For a representative grid of as_of dates, every returned record
    satisfies publish_date <= as_of.
    """
    qs = []
    for year in range(2020, 2026):
        for q in (1, 2, 3, 4):
            qs.append(make_quarter(year, q, revenue=Decimal(100)))

    sweep_dates = [
        date(y, m, d) for y in range(2020, 2026) for m in (3, 5, 8, 11) for d in (1, 14, 15, 28)
    ]
    for t in sweep_dates:
        for r in filter_quarterly(qs, t):
            assert r.publish_date <= t, f"violation at {t}: {r.period}"
