"""Tests for the quarter-aware resume in scrapers/financials.py.

The resume used to ask only whether each year in range had any row, so once a
stock's Q1 was in, the whole year counted as done and 2026Q2 was never fetched.

No database and no network: only the pure functions are exercised.
"""
from datetime import date

import pytest

from scrapers.financials import _complete_periods, _covered, expected_period


@pytest.mark.parametrize("today, expected", [
    (date(2026, 1, 15), (2025, 3)),   # Q3 deadline 11-14 long past, annual not due
    (date(2026, 4, 6), (2025, 3)),    # annual due 03-31 + 7 = 04-07
    (date(2026, 4, 7), (2025, 4)),
    (date(2026, 5, 21), (2025, 4)),   # Q1 due 05-15 + 7 = 05-22
    (date(2026, 5, 22), (2026, 1)),
    (date(2026, 8, 20), (2026, 1)),   # Q2 due 08-14 + 7 = 08-21
    (date(2026, 8, 21), (2026, 2)),
    (date(2026, 10, 12), (2026, 2)),
    (date(2026, 11, 20), (2026, 2)),  # Q3 due 11-14 + 7 = 11-21
    (date(2026, 11, 21), (2026, 3)),
])
def test_expected_period(today, expected):
    assert expected_period(today) == expected


def test_year_with_rows_but_missing_due_quarter_is_not_covered():
    # The exact 2026Q2 situation: 2026 has Q1, Q2 is due.
    periods = {(2026, 1)}
    assert not _covered({2026}, periods, 2026, 2026, (2026, 2))


def test_due_quarter_present_is_covered():
    periods = {(2026, 1), (2026, 2)}
    assert _covered({2026}, periods, 2026, 2026, (2026, 2))


def test_missing_year_is_not_covered():
    assert not _covered({2024}, {(2024, 4)}, 2024, 2025, (2023, 4))


def test_quarter_missing_from_one_table_is_not_complete():
    # Income landed, balance sheet failed: the stock must be fetched again.
    by_table = {
        "tw.income_statements": {(2026, 1), (2026, 2)},
        "tw.balance_sheets": {(2026, 1)},
        "tw.cash_flows": {(2026, 1), (2026, 2)},
    }
    periods = _complete_periods(by_table)
    assert periods == {(2026, 1)}
    assert not _covered({2026}, periods, 2026, 2026, (2026, 2))


def test_quarter_in_every_table_is_complete():
    by_table = {t: {(2026, 1), (2026, 2)} for t in
                ("tw.income_statements", "tw.balance_sheets", "tw.cash_flows")}
    assert _complete_periods(by_table) == {(2026, 1), (2026, 2)}


def test_due_quarter_outside_range_does_not_matter():
    # A multi-year historical backfill keeps its old year-level meaning.
    periods = {(2020, 4), (2021, 4)}
    assert _covered({2020, 2021}, periods, 2020, 2021, (2026, 2))
