"""Tests for the quarterly-statement coverage check in the daily health check.

2026Q2 statements were missing for two months: nothing scheduled the scraper
and nothing watched the tables. The check asks whether the latest quarter whose
filing deadline has passed is actually present, against the quarter before.

No database and no Telegram: the query helper is replaced.
"""
from datetime import date

import pytest

import scrapers.financials as fin
import telegram_bot.health_check as hc


def test_deadlines_match_the_scraper():
    # Copied rather than imported; this keeps the two copies equal.
    assert hc._FILING_DEADLINES == fin.FILING_DEADLINES
    assert hc._FILING_GRACE_DAYS == fin.FILING_GRACE_DAYS


@pytest.mark.parametrize("today", [
    date(2026, 1, 15), date(2026, 4, 7), date(2026, 5, 22),
    date(2026, 8, 21), date(2026, 10, 12), date(2026, 11, 21),
])
def test_due_quarter_matches_the_scraper(today):
    assert hc._due_quarter(today) == fin.expected_period(today)


def test_previous_quarter_wraps_the_year():
    assert hc._previous_quarter(2026, 1) == (2025, 4)
    assert hc._previous_quarter(2026, 3) == (2026, 2)


def _counts(monkeypatch, table_counts):
    """table_counts: {(table, (year, quarter)): n}"""
    def fake_scalar(sql, params=()):
        table = sql.split(" FROM ")[1].split()[0]
        return table_counts.get((table, tuple(params)))
    monkeypatch.setattr(hc, "_scalar", fake_scalar)


def test_missing_due_quarter_is_reported(monkeypatch):
    _counts(monkeypatch, {(t, (2026, 1)): 1942 for _, t in hc._FINANCIAL_TABLES})
    lines = hc._financial_coverage(date(2026, 10, 12))
    assert len(lines) == 3
    assert lines[0] == "季報 損益表 2026Q2 僅 0 家（上一季 1,942 家；期限 08-14）"


def test_full_coverage_is_quiet(monkeypatch):
    counts = {}
    for _, t in hc._FINANCIAL_TABLES:
        counts[(t, (2026, 1))] = 1942
        counts[(t, (2026, 2))] = 1900
    _counts(monkeypatch, counts)
    assert hc._financial_coverage(date(2026, 10, 12)) == []


def test_just_under_the_ratio_is_reported(monkeypatch):
    counts = {}
    for _, t in hc._FINANCIAL_TABLES:
        counts[(t, (2026, 1))] = 1000
        counts[(t, (2026, 2))] = 899
    _counts(monkeypatch, counts)
    assert len(hc._financial_coverage(date(2026, 10, 12))) == 3


def test_no_previous_quarter_does_not_divide_or_report(monkeypatch):
    _counts(monkeypatch, {})
    assert hc._financial_coverage(date(2026, 10, 12)) == []
