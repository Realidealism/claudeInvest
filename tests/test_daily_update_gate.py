"""Tests for daily_update's trading-day gate.

An absent TAIEX row used to mean "holiday". A fetch that fails without raising
leaves no row either, so an outage on 2026-09-18 skipped the pipeline, exited 0
and went unnoticed for two days. The gate now asks the exchange calendar, and
only a date the calendar closes is a holiday.

Everything here calls _trading_day_gate or the pure functions under it, never
the pipeline behind the gate. No database, no network, no Telegram.
"""
from datetime import date, timedelta
from pathlib import Path

import pytest

import daily_update as du
import telegram_bot.health_check as hc
from daily_update_guards import no_side_effects  # noqa: F401
import utils.http_client as http_client
from holiday_fixture_2026 import RAW_2026
from scrapers.market_holidays import parse

CALENDAR_2026 = {d: r["is_closed"] for d, r in parse(RAW_2026)[0].items()}

OPEN_DAY = date(2026, 9, 18)      # the day that was lost
HOLIDAY = date(2026, 10, 9)       # 國慶日補假


# ── _is_scheduled_closure ──────────────────────────────────────────────────

def test_listed_closure_is_closed():
    assert du._is_scheduled_closure(CALENDAR_2026, date(2026, 2, 12)) is True


def test_trading_day_marker_is_open():
    # Listed in the source, but as the first trading day of the year.
    assert du._is_scheduled_closure(CALENDAR_2026, date(2026, 1, 2)) is False


def test_unlisted_day_is_open():
    assert du._is_scheduled_closure(CALENDAR_2026, OPEN_DAY) is False


def test_year_without_a_calendar_is_unknown():
    assert du._is_scheduled_closure({}, date(2027, 1, 1)) is None


# ── _trading_day_gate ──────────────────────────────────────────────────────

class _Gate:
    """Stand-ins for everything the gate touches, with a record of calls."""

    def __init__(self, monkeypatch, *, has_taiex=False, rows=CALENDAR_2026,
                 gate_ok=True, send_ok=True, month=None):
        self.sent = []
        self.scraped = []
        self.rows = rows
        index_result = (True, None) if gate_ok else (False, "INDEX TRACE")

        def run_scraper(label, module_path, func_name, trade_date, expect_rows=False,
                        recent=None):
            self.scraped.append(label)
            return index_result if label == du.INDEX_SCRAPER[0] else (True, None)

        def holiday_rows(year):
            rows = self.rows
            if isinstance(rows, Exception):
                raise rows
            return rows.pop(0) if isinstance(rows, list) else rows

        def exchange_month(trade_date):
            if isinstance(month, Exception):
                raise month
            return month

        def send_sync(text):
            self.sent.append(text)
            return send_ok

        monkeypatch.setattr(du, "run_scraper", run_scraper)
        monkeypatch.setattr(du, "_has_taiex", lambda d: has_taiex)
        monkeypatch.setattr(du, "_holiday_rows", holiday_rows)
        monkeypatch.setattr(du, "_exchange_month", exchange_month)
        monkeypatch.setattr(du, "send_sync", send_sync)
        monkeypatch.setattr(du, "_holiday_refresh_tried", set())


def _decide(trade_date):
    return du._trading_day_gate(trade_date)[0]


def test_taiex_present_runs_without_asking_the_calendar(monkeypatch):
    g = _Gate(monkeypatch, has_taiex=True, rows=RuntimeError("calendar asked"))
    assert _decide(OPEN_DAY) == "run"
    assert g.sent == []


@pytest.mark.parametrize("gate_ok, result", [(True, (True, None)), (False, (False, "INDEX TRACE"))])
def test_index_scraper_result_is_handed_back(monkeypatch, gate_ok, result):
    # The pipeline's summary reports it; a TAIEX row from an earlier run does
    # not make tonight's index scraper a success.
    _Gate(monkeypatch, has_taiex=True, gate_ok=gate_ok)
    assert du._trading_day_gate(OPEN_DAY) == ("run", *result)


def test_scheduled_trading_day_with_an_empty_fetch_aborts_and_alerts(monkeypatch):
    # The 2026-09-18 shape: the scraper returned without raising (gate_ok),
    # there is no TAIEX row, and the calendar does not close the day.
    g = _Gate(monkeypatch, gate_ok=True)
    assert _decide(OPEN_DAY) == "abort"
    assert len(g.sent) == 1
    msg = g.sent[0]
    assert "2026-09-18 交易日判定失敗" in msg
    assert "休市日曆排定這一天開市" in msg
    assert "證交所目前沒有回應" in msg
    assert "daily_update.exe 2026-09-18 2026-09-18" in msg
    assert "抓取程式本身回報失敗" not in msg


def test_scraper_failure_is_named_in_the_alert(monkeypatch):
    g = _Gate(monkeypatch, gate_ok=False)
    assert _decide(OPEN_DAY) == "abort"
    assert "抓取程式本身回報失敗" in g.sent[0]


def test_calendar_holiday_is_skipped_quietly(monkeypatch):
    g = _Gate(monkeypatch)
    assert _decide(HOLIDAY) == "holiday"
    assert g.sent == []


def test_calendar_holiday_is_quiet_even_when_the_scraper_failed(monkeypatch):
    g = _Gate(monkeypatch, gate_ok=False)
    assert _decide(HOLIDAY) == "holiday"
    assert g.sent == []


def test_missing_year_is_fetched_once_then_aborts(monkeypatch):
    g = _Gate(monkeypatch, rows={})
    assert _decide(OPEN_DAY) == "abort"
    assert g.scraped == [du.INDEX_SCRAPER[0], du.HOLIDAY_SCRAPER[0]]
    assert "休市日曆沒有 2026 年的資料" in g.sent[0]

    # Same process, same year: no second request.
    assert _decide(OPEN_DAY) == "abort"
    assert g.scraped.count(du.HOLIDAY_SCRAPER[0]) == 1
    assert len(g.sent) == 2


def test_missing_year_that_the_fetch_fills_is_then_decided(monkeypatch):
    g = _Gate(monkeypatch, rows=[{}, CALENDAR_2026])
    assert _decide(HOLIDAY) == "holiday"
    assert g.scraped == [du.INDEX_SCRAPER[0], du.HOLIDAY_SCRAPER[0]]
    assert g.sent == []


def test_calendar_query_error_aborts_and_alerts(monkeypatch):
    g = _Gate(monkeypatch, rows=RuntimeError("relation does not exist"))
    assert _decide(HOLIDAY) == "abort"
    assert "或查詢失敗" in g.sent[0]


def test_undelivered_alert_still_aborts(monkeypatch):
    # send_sync reports failure by returning False; it does not raise.
    g = _Gate(monkeypatch, send_ok=False)
    assert _decide(OPEN_DAY) == "abort"
    assert len(g.sent) == 1


def test_probe_error_does_not_block_the_alert(monkeypatch):
    g = _Gate(monkeypatch, month=RuntimeError("boom"))
    assert _decide(OPEN_DAY) == "abort"
    assert "證交所目前沒有回應" in g.sent[0]


def test_future_date_is_skipped_without_alert_or_calendar(monkeypatch):
    g = _Gate(monkeypatch, rows=RuntimeError("calendar asked"))
    assert _decide(date.today() + timedelta(days=1)) == "holiday"
    assert g.sent == []


def test_running_for_today_mentions_unpublished_data(monkeypatch):
    # A calendar that closes nothing near today, whatever day the test runs.
    g = _Gate(monkeypatch, rows={date(2000, 1, 1): True})
    assert _decide(date.today()) == "abort"
    assert "收盤資料公布前" in g.sent[0]

    assert _decide(OPEN_DAY) == "abort"
    assert "收盤資料公布前" not in g.sent[1]


# ── update_date wiring ─────────────────────────────────────────────────────

@pytest.fixture
def no_run_record(monkeypatch):
    monkeypatch.setattr(du, "_run_start", lambda d: None)
    monkeypatch.setattr(du, "_run_finish", lambda *a: None)


@pytest.mark.parametrize("gate, expected", [("abort", "aborted"), ("holiday", "holiday")])
def test_update_date_stops_at_the_gate(monkeypatch, no_run_record, gate, expected):
    def pipeline_reached(*a, **k):
        raise AssertionError("update_date went past the gate")

    monkeypatch.setattr(du, "_trading_day_gate", lambda d: (gate, True, None))
    monkeypatch.setattr(du, "run_scraper", pipeline_reached)
    monkeypatch.setattr(du, "SCRAPERS", [("tripwire", "scrapers.none", "f", True)])
    assert du.update_date(OPEN_DAY) == expected


class _FirstScraperReached(Exception):
    pass


def test_update_date_gets_from_the_gate_to_the_first_scraper(monkeypatch, no_run_record):
    # The lines between the gate and the scraper loop read the index scraper's
    # result. Stop at the first scraper: nothing behind it may run in a test.
    def first_scraper(*a, **k):
        raise _FirstScraperReached

    def beyond_the_loop(*a, **k):
        raise AssertionError("update_date touched the database or Telegram")

    monkeypatch.setattr(du, "_trading_day_gate", lambda d: ("run", False, "INDEX TRACE"))
    monkeypatch.setattr(du, "run_scraper", first_scraper)
    monkeypatch.setattr(du, "get_cursor", beyond_the_loop)
    monkeypatch.setattr(du, "send_sync", beyond_the_loop)
    monkeypatch.setattr(du, "SCRAPERS", [("tripwire", "scrapers.none", "f", True)])
    with pytest.raises(_FirstScraperReached):
        du.update_date(OPEN_DAY)


# ── a year the calendar does not cover ─────────────────────────────────────
# The source only serves the current year. For an earlier year the exchange's
# settled list of trading days for that month stands in for the calendar.

PAST = date(date.today().year - 1, 1, 28)
PAST_MONTH = {"stat": "OK", "days": {PAST.replace(day=20), PAST.replace(day=21)}}


def test_past_year_date_absent_from_the_exchange_month_is_a_holiday(monkeypatch):
    g = _Gate(monkeypatch, rows={}, month=PAST_MONTH)
    assert _decide(PAST) == "holiday"
    assert g.sent == []


def test_past_year_without_an_answer_aborts(monkeypatch):
    g = _Gate(monkeypatch, rows={}, month=None)
    assert _decide(PAST) == "abort"
    assert "沒有回應" in g.sent[0]


def test_past_year_date_the_exchange_lists_aborts(monkeypatch):
    g = _Gate(monkeypatch, rows={}, month={"stat": "OK", "days": {PAST}})
    assert _decide(PAST) == "abort"
    assert "已有這一天的資料" in g.sent[0]


def test_past_year_answer_without_data_aborts(monkeypatch):
    g = _Gate(monkeypatch, rows={}, month={"stat": "查詢日期小於99年1月4日，請重新查詢!", "days": set()})
    assert _decide(PAST) == "abort"
    assert "沒有給資料" in g.sent[0]


def test_current_year_never_falls_back_to_the_exchange_month(monkeypatch):
    # A date missing from this year's list may just not be published yet.
    day = date(date.today().year, 1, 1)
    month = {"stat": "OK", "days": {day.replace(day=2)}}
    g = _Gate(monkeypatch, rows={}, month=month)
    assert _decide(day) == "abort"
    assert len(g.sent) == 1


def test_calendar_that_covers_the_year_is_not_overruled(monkeypatch):
    # The typhoon rule: the calendar schedules the day, the exchange has no
    # such day, and that still alerts — also for a past year, once the table
    # holds one.
    g = _Gate(monkeypatch, rows={PAST.replace(day=1): True}, month=PAST_MONTH)
    assert _decide(PAST) == "abort"
    assert "休市日曆排定這一天開市" in g.sent[0]
    assert "可能是颱風等臨時休市" in g.sent[0]


# ── _exchange_month / _probe_line ──────────────────────────────────────────
# First column as FMTQIK returned it on 2026-10-13: ROC year, slashes, padded.

JULY = {"stat": "OK", "data": [["115/07/08", "x"], ["115/07/09", "x"], ["115/07/13", "x"]]}
JULY_DAYS = {date(2026, 7, 8), date(2026, 7, 9), date(2026, 7, 13)}


def _month(monkeypatch, response):
    monkeypatch.setattr(http_client, "fetch_json", lambda url, params=None, **k: response)
    return du._exchange_month(date(2026, 7, 10))


def test_month_no_response(monkeypatch):
    assert _month(monkeypatch, None) is None


def test_month_days_are_parsed(monkeypatch):
    assert _month(monkeypatch, JULY) == {"stat": "OK", "days": JULY_DAYS}


def test_month_answer_without_data(monkeypatch):
    stat = "很抱歉，沒有符合條件的資料!"
    assert _month(monkeypatch, {"stat": stat}) == {"stat": stat, "days": set()}


def test_month_unreadable_rows_do_not_raise(monkeypatch):
    month = _month(monkeypatch, {"stat": "OK", "data": [7, None]})
    assert month["days"] == set() and month["stat"] != "OK"


def test_absent_from_month():
    month = {"stat": "OK", "days": JULY_DAYS}
    assert du._absent_from_month(month, date(2026, 7, 10)) is True
    assert du._absent_from_month(month, date(2026, 7, 9)) is False
    assert du._absent_from_month(None, date(2026, 7, 10)) is False
    assert du._absent_from_month({"stat": "OK", "days": set()}, date(2026, 7, 10)) is False
    assert du._absent_from_month({"stat": "no", "days": JULY_DAYS}, date(2026, 7, 10)) is False


def test_probe_line_wording():
    month = {"stat": "OK", "days": JULY_DAYS}
    assert "沒有回應" in du._probe_line(None, date(2026, 7, 10))
    line = du._probe_line(month, date(2026, 7, 10))
    assert "本月 3 個交易日" in line and "可忽略" in line
    line = du._probe_line(month, date(2026, 7, 9))
    assert "已有這一天的資料" in line and "可忽略" not in line
    line = du._probe_line({"stat": "no", "days": set()}, date(2026, 7, 10))
    assert "沒有給資料（stat=no）" in line and "可忽略" not in line


def test_probe_line_for_today_does_not_suggest_a_closure():
    # Before the close, a month without today is the normal state.
    today = date.today()
    month = {"stat": "OK", "days": {today - timedelta(days=1)}}
    line = du._probe_line(month, today)
    assert "還沒有今天" in line and "可忽略" not in line


# ── packaging ──────────────────────────────────────────────────────────────

def test_every_scraper_is_bundled_into_the_exe():
    # Scrapers are reached through importlib, outside run_scraper's try. One
    # missing from hiddenimports stops the whole nightly run at that scraper.
    spec = Path(du.__file__).with_name("daily_update.spec")
    bundled, _ = hc._spec_contents(spec)
    wanted = {entry[1] for entry in du.SCRAPERS}
    wanted |= {du.INDEX_SCRAPER[1], du.HOLIDAY_SCRAPER[1]}
    assert wanted - set(bundled) == set()
