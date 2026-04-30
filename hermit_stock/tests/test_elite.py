"""Tests for elite-quality override."""

from __future__ import annotations

from decimal import Decimal

from hermit_stock.scoring.elite import evaluate_elite, has_elite_quality
from tests.fixtures.synthetic import make_quarter


def _quarters_with(
    *,
    revenue: Decimal = Decimal(1000),
    gross_profit: Decimal | None = None,
    operating_income: Decimal | None = None,
    net_income: Decimal | None = None,
    free_cash_flow: Decimal | None = None,
    equity: Decimal = Decimal(2000),
) -> list:
    """4 identical quarters for TTM tests."""
    return [
        make_quarter(
            2024,
            q,
            revenue=revenue,
            gross_profit=gross_profit,
            operating_income=operating_income,
            net_income=net_income,
            free_cash_flow=free_cash_flow,
            equity_attributable=equity,
            total_equity=equity,
        )
        for q in (1, 2, 3, 4)
    ]


def test_elite_gross_margin_threshold() -> None:
    # 60% gross margin → elite via gross
    qs = _quarters_with(revenue=Decimal(100), gross_profit=Decimal(60))
    flags = evaluate_elite(qs)
    assert flags.gross_margin_ok is True
    assert flags.any_ok is True


def test_elite_op_margin_threshold() -> None:
    qs = _quarters_with(
        revenue=Decimal(100),
        gross_profit=Decimal(35),  # below 40% → gross fails
        operating_income=Decimal(30),  # 30% → above 25% threshold
    )
    flags = evaluate_elite(qs)
    assert flags.gross_margin_ok is False
    assert flags.op_margin_ok is True
    assert flags.any_ok is True


def test_elite_roe_threshold() -> None:
    # ROE = (4 * 50) / 800 = 25% → above 20%
    qs = _quarters_with(
        revenue=Decimal(100),
        gross_profit=Decimal(20),  # 20% gross — fails
        operating_income=Decimal(10),  # 10% op — fails
        net_income=Decimal(50),
        equity=Decimal(800),
    )
    flags = evaluate_elite(qs)
    assert flags.gross_margin_ok is False
    assert flags.op_margin_ok is False
    assert flags.roe_ok is True
    assert flags.any_ok is True


def test_elite_fcf_margin_threshold() -> None:
    # FCF margin = 25/100 = 25% → above 20%
    qs = _quarters_with(
        revenue=Decimal(100),
        gross_profit=Decimal(20),
        operating_income=Decimal(5),
        net_income=Decimal(5),
        free_cash_flow=Decimal(25),
        equity=Decimal(2000),  # ROE = 20/2000 = 1% — fails
    )
    flags = evaluate_elite(qs)
    assert flags.fcf_margin_ok is True
    assert flags.any_ok is True


def test_not_elite_when_all_below_thresholds() -> None:
    qs = _quarters_with(
        revenue=Decimal(100),
        gross_profit=Decimal(20),
        operating_income=Decimal(5),
        net_income=Decimal(2),
        free_cash_flow=Decimal(5),  # 5% margin
        equity=Decimal(2000),
    )
    assert has_elite_quality(qs) is False


def test_elite_returns_false_when_too_few_quarters() -> None:
    qs = [make_quarter(2024, 1, revenue=Decimal(100), gross_profit=Decimal(60))]
    assert has_elite_quality(qs) is False


def test_elite_handles_missing_fields() -> None:
    """If a field is missing for any quarter, that flag is False but others
    may still trigger."""
    qs = [
        make_quarter(
            2024,
            q,
            revenue=Decimal(100),
            gross_profit=Decimal(60) if q < 4 else None,  # missing Q4
            net_income=Decimal(50),
            equity_attributable=Decimal(800),
            total_equity=Decimal(800),
        )
        for q in (1, 2, 3, 4)
    ]
    flags = evaluate_elite(qs)
    assert flags.gross_margin_ok is False  # missing field
    assert flags.roe_ok is True  # equity present, NI present


def test_elite_threshold_inclusive() -> None:
    # exactly 40% gross → should qualify (>= 40%)
    qs = _quarters_with(revenue=Decimal(100), gross_profit=Decimal(40))
    assert evaluate_elite(qs).gross_margin_ok is True


def test_2330_like_quality_qualifies() -> None:
    """TSMC-like: gross ~57%, op ~47%, ROE ~25%, FCF ~30% → all 4 elite flags."""
    qs = _quarters_with(
        revenue=Decimal(1000),
        gross_profit=Decimal(570),
        operating_income=Decimal(470),
        net_income=Decimal(400),
        free_cash_flow=Decimal(300),
        equity=Decimal(6000),  # ROE = 1600/6000 = 26.7%
    )
    flags = evaluate_elite(qs)
    assert flags.gross_margin_ok is True
    assert flags.op_margin_ok is True
    assert flags.roe_ok is True
    assert flags.fcf_margin_ok is True
