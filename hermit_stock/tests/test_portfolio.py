"""Portfolio mark-to-market and rebalance tests."""

from __future__ import annotations

from datetime import date

from hermit_stock.backtest.portfolio import BUY_COST, SELL_COST, Portfolio


def test_initial_mark_to_market_returns_cash() -> None:
    p = Portfolio(cash=1_000_000.0)
    nav = p.mark_to_market(date(2024, 1, 1), {})
    assert nav == 1_000_000.0


def test_buy_then_mark_to_market() -> None:
    p = Portfolio(cash=1_000_000.0)
    p.rebalance_equal_weight(date(2024, 1, 1), ["A", "B"], {"A": 100.0, "B": 100.0})
    # Each slot ≈ 500_000 / (1+BUY_COST)
    nav = p.mark_to_market(date(2024, 1, 1), {"A": 100.0, "B": 100.0})
    # NAV slightly less than initial due to buy costs
    assert nav < 1_000_000.0
    assert nav > 1_000_000.0 * (1 - BUY_COST - 0.001)


def test_rebalance_sells_dropped_tickers() -> None:
    p = Portfolio(cash=1_000_000.0)
    p.rebalance_equal_weight(date(2024, 1, 1), ["A", "B"], {"A": 100.0, "B": 100.0})
    # On rebalance day 2, swap B for C
    p.rebalance_equal_weight(date(2024, 4, 1), ["A", "C"], {"A": 100.0, "B": 100.0, "C": 50.0})
    assert "B" not in p.holdings
    assert "C" in p.holdings


def test_new_flag_set_on_first_entry_then_cleared() -> None:
    p = Portfolio(cash=1_000_000.0)
    p.rebalance_equal_weight(date(2024, 1, 1), ["A"], {"A": 100.0})
    assert p.holdings["A"].is_new_this_period is True
    # Re-rebalance, A still held → flag cleared
    p.rebalance_equal_weight(date(2024, 4, 1), ["A"], {"A": 100.0})
    assert p.holdings["A"].is_new_this_period is False


def test_holdings_log_records_each_rebalance() -> None:
    p = Portfolio(cash=1_000_000.0)
    p.rebalance_equal_weight(date(2024, 1, 1), ["A"], {"A": 100.0})
    p.rebalance_equal_weight(date(2024, 4, 1), ["A", "B"], {"A": 100.0, "B": 50.0})
    assert len(p.holdings_log) == 2


def test_sell_proceeds_reflect_costs() -> None:
    p = Portfolio(cash=1_000_000.0)
    p.rebalance_equal_weight(date(2024, 1, 1), ["A"], {"A": 100.0})
    cash_before = p.cash
    shares_a = p.holdings["A"].shares
    p.rebalance_equal_weight(date(2024, 4, 1), [], {"A": 100.0})
    expected_proceeds = shares_a * 100.0 * (1.0 - SELL_COST)
    assert abs(p.cash - (cash_before + expected_proceeds)) < 1.0
