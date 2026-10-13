"""Tests for the holiday-schedule parser in scrapers/market_holidays.py.

The source list mixes closures with markers for trading days. Reading a marker
as a closure would tell the trading-day gate that a real trading day is a
holiday, which is the silent skip this table exists to prevent.

No database and no network: only the pure functions are exercised.
"""
from datetime import date

import pytest

import scrapers.market_holidays as mh
from holiday_fixture_2026 import RAW_2026
from scrapers.market_holidays import _roc_to_date, parse


def test_roc_date():
    assert _roc_to_date("1150101") == date(2026, 1, 1)
    assert _roc_to_date("1151225") == date(2026, 12, 25)
    assert _roc_to_date("") is None
    assert _roc_to_date("1151345") is None


def test_every_row_of_the_real_list_parses():
    by_date, errors = parse(RAW_2026)
    assert errors == 0
    assert len(by_date) == 27


def test_trading_day_markers_are_not_closures():
    by_date, _ = parse(RAW_2026)
    open_days = sorted(d for d, r in by_date.items() if not r["is_closed"])
    assert open_days == [date(2026, 1, 2), date(2026, 2, 11), date(2026, 2, 23)]


def test_settlement_only_days_are_closures():
    by_date, _ = parse(RAW_2026)
    assert by_date[date(2026, 2, 12)]["is_closed"]
    assert by_date[date(2026, 2, 13)]["is_closed"]


def test_weekday_closures_of_2026():
    by_date, _ = parse(RAW_2026)
    closed_weekdays = sorted(d for d, r in by_date.items()
                             if r["is_closed"] and d.weekday() < 5)
    assert closed_weekdays == [
        date(2026, 1, 1),
        date(2026, 2, 12), date(2026, 2, 13),
        date(2026, 2, 16), date(2026, 2, 17), date(2026, 2, 18),
        date(2026, 2, 19), date(2026, 2, 20),
        date(2026, 2, 27),
        date(2026, 4, 3), date(2026, 4, 6),
        date(2026, 5, 1), date(2026, 6, 19),
        date(2026, 9, 25), date(2026, 9, 28),
        date(2026, 10, 9), date(2026, 10, 26),
        date(2026, 12, 25),
    ]


def test_a_closure_wins_when_a_date_is_listed_twice():
    rows = [
        {"Name": "某節開始交易日", "Date": "1150701"},
        {"Name": "某節", "Date": "1150701"},
    ]
    by_date, _ = parse(rows)
    assert by_date[date(2026, 7, 1)]["is_closed"]
    by_date, _ = parse(list(reversed(rows)))
    assert by_date[date(2026, 7, 1)]["is_closed"]


def test_unreadable_rows_are_counted_not_stored():
    by_date, errors = parse([
        {"Name": "端午節", "Date": "1150619"},
        {"Name": "端午節", "Date": "not a date"},
        {"Date": "1150620"},
        {"Name": "", "Date": "1150621"},
    ])
    assert errors == 3
    assert list(by_date) == [date(2026, 6, 19)]


def test_description_markup_is_stripped():
    by_date, _ = parse([{"Name": "農曆春節前最後交易日", "Date": "1150211",
                         "Description": "農曆春節前最後交易。<br>"}])
    assert by_date[date(2026, 2, 11)]["description"] == "農曆春節前最後交易。"
    by_date, _ = parse([{"Name": "x", "Date": "1150212", "Description": ""}])
    assert by_date[date(2026, 2, 12)]["description"] is None


def test_dates_far_from_the_expected_year_are_parse_errors():
    # A Gregorian '20260101' reads as ROC year 2026, i.e. the year 3937.
    rows = [{"Name": "開國紀念日", "Date": "20260101"},
            {"Name": "端午節", "Date": "1150619"}]
    by_date, errors = parse(rows, near_year=2026)
    assert errors == 1
    assert list(by_date) == [date(2026, 6, 19)]


def test_neighbouring_years_are_accepted():
    rows = [{"Name": "a", "Date": "1141231"}, {"Name": "b", "Date": "1160101"}]
    by_date, errors = parse(rows, near_year=2026)
    assert errors == 0 and len(by_date) == 2


# ── scrape_date ────────────────────────────────────────────────────────────

class _Today(date):
    """date.today() pinned, so the fixture year stays 'this year'."""
    pinned = date(2026, 10, 13)

    @classmethod
    def today(cls):
        return cls.pinned


def _scrape(monkeypatch, response, today=date(2026, 10, 13)):
    saved = []
    _Today.pinned = today
    monkeypatch.setattr(mh, "date", _Today)
    monkeypatch.setattr(mh, "fetch_json_retry", lambda url, validate=None: response)
    monkeypatch.setattr(mh, "_save", lambda by_date: saved.append(by_date) or len(by_date))
    return mh.scrape_date(today), saved


def test_scrape_saves_the_whole_list(monkeypatch):
    result, saved = _scrape(monkeypatch, RAW_2026)
    assert (result.records, result.api_rows, result.parse_errors) == (27, 27, 0)
    assert result.failed_sources == ()
    assert len(saved[0]) == 27


@pytest.mark.parametrize("response", [None, [], {"stat": "error"}])
def test_scrape_with_no_usable_response_saves_nothing(monkeypatch, response):
    result, saved = _scrape(monkeypatch, response)
    assert (result.records, result.api_rows) == (0, 0)
    assert saved == [{}]


def test_source_still_on_last_year_is_named_as_a_failure(monkeypatch):
    # Early January: a full, parseable 2026 list that leaves 2027 uncovered.
    result, _ = _scrape(monkeypatch, RAW_2026, today=date(2027, 1, 4))
    assert result.records == 27
    assert result.failed_sources == ("2027 年休市日曆（來源仍是舊年度）",)


def test_source_already_on_next_year_is_not_a_failure(monkeypatch):
    # If the source switches early, the table still holds the current year.
    result, _ = _scrape(monkeypatch, RAW_2026, today=date(2025, 12, 20))
    assert result.failed_sources == ()
