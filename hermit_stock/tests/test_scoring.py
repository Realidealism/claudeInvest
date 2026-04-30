"""Unit tests for scoring rules F1-F8 and grade aggregation."""

from __future__ import annotations

from decimal import Decimal

import pytest

from hermit_stock.scoring import rules, scorer
from hermit_stock.scoring.rules import Thresholds
from tests.fixtures.synthetic import linear_growth_quarters, make_monthly, make_quarter


def _flat_quarters(n: int = 12, *, year_start: int = 2020) -> list:
    """n quarters of identical values, large enough for all rules."""
    out = []
    for i in range(n):
        year = year_start + i // 4
        q = (i % 4) + 1
        out.append(
            make_quarter(
                year,
                q,
                revenue=Decimal(100),
                cogs=Decimal(60),
                gross_profit=Decimal(40),
                net_income=Decimal(20),
                eps=Decimal("1.0"),
                inventory=Decimal(15),
                total_assets=Decimal(400),
                total_liabilities=Decimal(160),
                total_equity=Decimal(240),
                equity_attributable=Decimal(240),
                shares_outstanding=Decimal(20),
                operating_cash_flow=Decimal(22),
                capex=Decimal(-5),
                free_cash_flow=Decimal(17),
            )
        )
    return out


# --- F1 / F2 ---


def test_f1_passes_when_ttm_yoy_above_threshold() -> None:
    qs = linear_growth_quarters(n_quarters=12, qoq_growth=Decimal("0.10"))
    t = Thresholds()
    r = rules.f1_earnings_growth(qs, t)
    assert r.passed is True


def test_f1_unknown_when_too_few_quarters() -> None:
    qs = linear_growth_quarters(n_quarters=4)
    r = rules.f1_earnings_growth(qs, Thresholds())
    assert r.passed is None


def test_f2_fails_when_growth_below_threshold() -> None:
    qs = linear_growth_quarters(n_quarters=12, qoq_growth=Decimal("0.001"))
    r = rules.f2_revenue_growth(qs, Thresholds())
    assert r.passed is False


# --- F3 ---


def test_f3_passes_when_gross_margin_rises_two_years() -> None:
    qs = []
    for year, gm in [(2022, Decimal("0.3")), (2023, Decimal("0.35")), (2024, Decimal("0.4"))]:
        for q in (1, 2, 3, 4):
            qs.append(make_quarter(year, q, revenue=Decimal(100), gross_profit=Decimal(100) * gm))
    r = rules.f3_gross_margin_rising(qs, Thresholds())
    assert r.passed is True
    assert r.value == 2


def test_f3_fails_when_margin_flat() -> None:
    qs = _flat_quarters(12)
    r = rules.f3_gross_margin_rising(qs, Thresholds())
    assert r.passed is False


# --- F4 ---


def test_f4_passes_when_inventory_days_stable() -> None:
    qs = _flat_quarters(8)
    r = rules.f4_inventory_days_not_rising(qs, Thresholds())
    assert r.passed is True


def test_f4_fails_when_inventory_days_rise_two_quarters() -> None:
    qs = []
    for i, inv in enumerate([10, 20, 30]):
        qs.append(make_quarter(2024, i + 1, cogs=Decimal(100), inventory=Decimal(inv)))
    r = rules.f4_inventory_days_not_rising(qs, Thresholds())
    assert r.passed is False


# --- F5 ---


def test_f5_fails_when_debt_ratio_rises_three_years() -> None:
    qs = []
    for year, ratio in [
        (2021, Decimal("0.2")),
        (2022, Decimal("0.3")),
        (2023, Decimal("0.4")),
        (2024, Decimal("0.5")),
    ]:
        qs.append(
            make_quarter(year, 4, total_assets=Decimal(100), total_liabilities=Decimal(100) * ratio)
        )
    r = rules.f5_debt_ratio_not_rising(qs, Thresholds())
    assert r.passed is False


# --- F6 ---


def test_f6_passes_when_fcf_positive() -> None:
    qs = _flat_quarters(12)
    r = rules.f6_fcf_healthy(qs, Thresholds())
    assert r.passed is True


def test_f6_fails_when_fcf_negative_three_years() -> None:
    qs = []
    for year, fcf_q in [
        (2022, Decimal(-5)),
        (2023, Decimal(-5)),
        (2024, Decimal(-5)),
    ]:
        for q in (1, 2, 3, 4):
            qs.append(make_quarter(year, q, free_cash_flow=fcf_q))
    r = rules.f6_fcf_healthy(qs, Thresholds())
    assert r.passed is False


# --- F7 ---


def test_f7_passes_when_latest_yoy_above_cumulative_and_12m_high() -> None:
    monthly = []
    for year in (2023, 2024):
        for m in range(1, 13):
            rev = Decimal(100) if year == 2023 else Decimal(100 + m * 10)
            monthly.append(
                make_monthly(year, m, rev, yoy=Decimal(220) if year == 2024 and m == 12 else None)
            )
    r = rules.f7_monthly_momentum(monthly)
    assert r.passed is True


def test_f7_unknown_when_too_few_months() -> None:
    monthly = [make_monthly(2024, m, Decimal(100), yoy=Decimal(10)) for m in range(1, 5)]
    r = rules.f7_monthly_momentum(monthly)
    assert r.passed is None


# --- F8 ---


def test_f8_passes_when_qoq_positive_and_yoy_accelerating() -> None:
    qs = []
    revs = [100, 100, 100, 100, 110, 130]  # year-1: 100x4; year-2 Q1=110, Q2=130
    for i, rev in enumerate(revs):
        year = 2023 + i // 4
        q = (i % 4) + 1
        qs.append(make_quarter(year, q, revenue=Decimal(rev)))
    r = rules.f8_quarterly_momentum(qs)
    assert r.passed is True


def test_f8_unknown_when_too_few_quarters() -> None:
    qs = [make_quarter(2024, q, revenue=Decimal(100)) for q in (1, 2, 3)]
    r = rules.f8_quarterly_momentum(qs)
    assert r.passed is None


# --- aggregator + grade ---


def test_evaluate_all_returns_eight_results() -> None:
    qs = linear_growth_quarters(n_quarters=12)
    monthly = [make_monthly(2024, m, Decimal(100)) for m in range(1, 13)]
    results = rules.evaluate_all(qs, monthly)
    assert len(results) == 8
    codes = [r.code for r in results]
    assert codes == [f"F{i}" for i in range(1, 9)]


@pytest.mark.parametrize(
    "passes,expected",
    [(8, "A"), (7, "A"), (6, "B"), (5, "B"), (4, "C"), (3, "C"), (2, "D"), (0, "D")],
)
def test_grade_cutoffs(passes: int, expected: str) -> None:
    fake = [rules.RuleResult(f"F{i}", "x", i < passes, None, None, "") for i in range(8)]
    sb = scorer.score(fake)
    assert sb.grade == expected
    assert sb.score == passes


def test_unknown_does_not_count_as_pass() -> None:
    fake = [
        rules.RuleResult("F1", "x", None, None, None, ""),
        rules.RuleResult("F2", "x", True, None, None, ""),
    ] + [rules.RuleResult(f"F{i}", "x", False, None, None, "") for i in range(3, 9)]
    sb = scorer.score(fake)
    assert sb.score == 1
    assert sb.unknown_count == 1
    assert sb.grade == "D"
