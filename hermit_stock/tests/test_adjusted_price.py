"""Forward-adjusted close: hand-computed expected values."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pandas as pd

from hermit_stock.data.adjusted_price import adjusted_close_series, build_events
from hermit_stock.data.models import DailyPrice


def _make_prices(start: date, n: int, base: float = 100.0) -> list[DailyPrice]:
    return [
        DailyPrice(
            ticker="T",
            trade_date=start + timedelta(days=i),
            source="db",
            close=Decimal(str(base + i * 0.0)),
        )
        for i in range(n)
    ]


def test_no_events_returns_close_unchanged() -> None:
    prices = _make_prices(date(2024, 1, 1), 10, base=100.0)
    s = adjusted_close_series(prices, [], [])
    assert all(v == 100.0 for v in s.values)
    assert len(s) == 10


def test_single_cash_dividend_scales_pre_dates() -> None:
    prices = _make_prices(date(2024, 1, 1), 10, base=100.0)
    # Cash dividend 5 NTD on 2024-01-06 (the 5th index → ex-date in middle)
    ex = date(2024, 1, 6)
    s = adjusted_close_series(prices, [(ex, Decimal(5), Decimal(0))], [])
    # Pre-event close: 100 * (95/100) = 95.0
    assert abs(s.loc[date(2024, 1, 5)] - 95.0) < 1e-9
    # On the ex-date itself and after: unchanged
    assert s.loc[date(2024, 1, 6)] == 100.0
    assert s.loc[date(2024, 1, 9)] == 100.0


def test_stock_dividend_scales_pre_dates() -> None:
    prices = _make_prices(date(2024, 1, 1), 10, base=100.0)
    # 10% stock dividend (R = 0.10)
    ex = date(2024, 1, 6)
    s = adjusted_close_series(prices, [(ex, Decimal(0), Decimal("0.10"))], [])
    # factor = 1 / (1 + 0.10) ≈ 0.9091; pre 100 → 90.909..
    assert abs(s.loc[date(2024, 1, 5)] - 100.0 / 1.10) < 1e-9
    assert s.loc[date(2024, 1, 9)] == 100.0


def test_combined_cash_and_stock_on_same_ex_date() -> None:
    prices = _make_prices(date(2024, 1, 1), 10, base=100.0)
    ex = date(2024, 1, 6)
    # cash 5 + stock 0.10
    s = adjusted_close_series(prices, [(ex, Decimal(5), Decimal("0.10"))], [])
    expected_factor = (95.0 / 100.0) * (1.0 / 1.10)
    assert abs(s.loc[date(2024, 1, 5)] - 100.0 * expected_factor) < 1e-9


def test_multiple_dividends_accumulate() -> None:
    prices = _make_prices(date(2024, 1, 1), 12, base=100.0)
    # Two cash dividends: 5 on 2024-01-04, 3 on 2024-01-08
    s = adjusted_close_series(
        prices,
        [
            (date(2024, 1, 4), Decimal(5), Decimal(0)),
            (date(2024, 1, 8), Decimal(3), Decimal(0)),
        ],
        [],
    )
    # Pre-2024-01-04: factor = (95/100) * (97/100) = 0.9215
    # Between 2024-01-04 and 2024-01-08 (excl): factor = 97/100
    assert abs(s.loc[date(2024, 1, 3)] - 100.0 * 0.95 * 0.97) < 1e-9
    assert abs(s.loc[date(2024, 1, 7)] - 100.0 * 0.97) < 1e-9
    assert s.loc[date(2024, 1, 9)] == 100.0


def test_capital_reduction_uses_stored_ratio() -> None:
    prices = _make_prices(date(2024, 1, 1), 10, base=100.0)
    # 2603-style: pre=80.8, post=187 → ratio=0.432086
    s = adjusted_close_series(prices, [], [(date(2024, 1, 6), Decimal("0.432086"))])
    assert abs(s.loc[date(2024, 1, 5)] - 100.0 * 0.432086) < 1e-6
    assert s.loc[date(2024, 1, 9)] == 100.0


def test_event_before_first_price_is_skipped() -> None:
    prices = _make_prices(date(2024, 1, 5), 5, base=100.0)
    # Ex-date earlier than any price → no prev_close → skipped
    events = build_events(prices, [(date(2024, 1, 1), Decimal(5), Decimal(0))], [])
    assert events == []


def test_latest_close_unchanged_under_forward_adjustment() -> None:
    prices = _make_prices(date(2024, 1, 1), 30, base=100.0)
    s = adjusted_close_series(
        prices,
        [(date(2024, 1, 15), Decimal(5), Decimal(0))],
        [],
    )
    # Forward adjustment guarantees latest close == raw close
    assert s.loc[date(2024, 1, 30)] == 100.0


def test_returns_pandas_series_with_correct_index() -> None:
    prices = _make_prices(date(2024, 1, 1), 5, base=100.0)
    s = adjusted_close_series(prices, [], [])
    assert isinstance(s, pd.Series)
    assert s.name == "adj_close"
