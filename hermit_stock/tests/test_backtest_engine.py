"""Backtest engine smoke + lookahead-invariant tests."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pandas as pd

from hermit_stock.backtest.engine import (
    BacktestConfig,
    build_adj_close_table,
    run_backtest,
    select_top_k,
)
from hermit_stock.data.models import DailyPrice, StockMeta
from tests.fixtures.synthetic import linear_growth_quarters, make_monthly, make_quarter


def _meta(t: str, name: str | None = None) -> StockMeta:
    return StockMeta(ticker=t, name=name or t, market="TWSE", industry="tech")


def _flat_qs(ticker: str) -> list:
    out = []
    for i in range(12):
        year = 2020 + i // 4
        q = (i % 4) + 1
        out.append(
            make_quarter(
                year,
                q,
                ticker=ticker,
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


def _strong_monthly(ticker: str) -> list:
    out = []
    for m in range(1, 13):
        out.append(make_monthly(2023, m, Decimal(100), ticker=ticker))
    for m in range(1, 13):
        out.append(
            make_monthly(
                2024, m, Decimal(100 + m * 10), ticker=ticker, yoy=Decimal(220) if m == 12 else None
            )
        )
    return out


def _make_prices(ticker: str, start: date, n: int, base: float = 100.0) -> list[DailyPrice]:
    return [
        DailyPrice(
            ticker=ticker,
            trade_date=start + timedelta(days=i),
            source="db",
            close=Decimal(str(base + i * 0.05)),
        )
        for i in range(n)
    ]


def test_select_top_k_orders_by_enabled_score_desc() -> None:
    metas = [_meta("STRONG"), _meta("WEAK")]
    quarterly = {
        "STRONG": linear_growth_quarters(n_quarters=12, qoq_growth=Decimal("0.10")),
        "WEAK": _flat_qs("WEAK"),
    }
    monthly = {"STRONG": _strong_monthly("STRONG"), "WEAK": []}

    picks = select_top_k(
        date(2025, 1, 31),
        metas,
        quarterly,
        monthly,
        enabled_rules=frozenset({"F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"}),
        top_k=10,
        min_score_floor=0,
    )
    tickers = [t for (t, _, _) in picks]
    assert tickers[0] == "STRONG"  # best score first


def test_select_top_k_respects_score_floor() -> None:
    metas = [_meta("WEAK")]
    quarterly = {"WEAK": _flat_qs("WEAK")}
    monthly = {"WEAK": []}
    picks = select_top_k(
        date(2025, 1, 31),
        metas,
        quarterly,
        monthly,
        enabled_rules=frozenset({"F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"}),
        top_k=10,
        min_score_floor=8,  # require all 8 → WEAK can't pass
    )
    assert picks == []


def test_build_adj_close_table_handles_empty() -> None:
    df = build_adj_close_table({}, {}, {}, {})
    assert df.empty


def test_run_backtest_smoke_with_synthetic_data() -> None:
    metas = [_meta("STRONG"), _meta("OTHER")]
    quarterly = {
        "STRONG": linear_growth_quarters(n_quarters=12, qoq_growth=Decimal("0.10")),
        "OTHER": linear_growth_quarters(n_quarters=12, qoq_growth=Decimal("0.05")),
    }
    monthly = {"STRONG": _strong_monthly("STRONG"), "OTHER": []}

    prices_start = date(2024, 1, 1)
    prices_end_n = 250  # ~1 year of "trading days" (we use calendar days here)
    prices = {
        "STRONG": _make_prices("STRONG", prices_start, prices_end_n, base=100.0),
        "OTHER": _make_prices("OTHER", prices_start, prices_end_n, base=50.0),
    }
    adj_close = build_adj_close_table(quarterly, prices, {}, {})
    cfg = BacktestConfig(
        start=date(2024, 6, 1),
        end=date(2024, 12, 31),
        top_k=2,
        min_score_floor=0,
        initial_cash=100_000.0,
    )
    result = run_backtest(
        cfg,
        metas=metas,
        quarterly_by_ticker=quarterly,
        monthly_by_ticker=monthly,
        adj_close=adj_close,
    )
    assert isinstance(result.nav, pd.Series)
    assert len(result.nav) > 0
    assert len(result.rebalance_dates_used) >= 1


def test_run_backtest_lookahead_safe() -> None:
    """Cannot 'see' future quarters: at 2024-06-01, only 2024Q1 should affect score."""
    metas = [_meta("STRONG")]
    qs = linear_growth_quarters(n_quarters=12, qoq_growth=Decimal("0.10"))
    quarterly = {"STRONG": qs}
    monthly = {"STRONG": _strong_monthly("STRONG")}

    picks_early = select_top_k(
        date(2022, 1, 1),  # 2020Q1 published 2020-05-15; 2021Q4 publishes 2022-03-31
        metas,
        quarterly,
        monthly,
        enabled_rules=frozenset({"F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"}),
        top_k=10,
        min_score_floor=0,
    )
    picks_late = select_top_k(
        date(2024, 6, 1),
        metas,
        quarterly,
        monthly,
        enabled_rules=frozenset({"F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"}),
        top_k=10,
        min_score_floor=0,
    )
    # Late should have access to more quarters → more rules can be evaluated
    early_unknowns = picks_early[0][2] if picks_early else []
    late_unknowns = picks_late[0][2] if picks_late else []
    early_unknown_count = sum(1 for r in early_unknowns if r.passed is None)
    late_unknown_count = sum(1 for r in late_unknowns if r.passed is None)
    assert early_unknown_count >= late_unknown_count
