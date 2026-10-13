"""Tests for the trading-calendar check in the daily health check.

Every other check measures against MAX(TAIEX). When the index itself was not
fetched, all tables stop on the same day and the probe reported a healthy
system (2026-09-18). The exchange calendar is the outside reference: it says
which days should have an index row.

No database and no Telegram: the three query helpers are replaced.
"""
from datetime import date, timedelta

import pytest

import daily_update as du
import telegram_bot.health_check as hc
from holiday_fixture_2026 import RAW_2026
from scrapers.market_holidays import parse

CALENDAR_2026 = {d: r["is_closed"] for d, r in parse(RAW_2026)[0].items()}


def _calendar(monkeypatch, by_year=None, missing_taiex=(), traded_closures=()):
    """Replace the queries. Every weekday has a TAIEX row except missing_taiex."""
    by_year = {2026: CALENDAR_2026} if by_year is None else by_year
    asked = []

    def taiex_days(first, last):
        asked.append((first, last))
        days = set()
        d = first
        while d <= last:
            if d not in missing_taiex:
                days.add(d)
            d += timedelta(days=1)
        return days

    monkeypatch.setattr(hc, "_holiday_rows", lambda year: by_year.get(year, {}))
    monkeypatch.setattr(hc, "_taiex_days", taiex_days)
    monkeypatch.setattr(hc, "_closed_days_that_traded", lambda today: list(traded_closures))
    return asked


def test_closure_rule_matches_the_gate():
    # Copied rather than imported; this keeps the two copies equal.
    d = date(2026, 1, 1)
    while d.year == 2026:
        assert hc._is_scheduled_closure(CALENDAR_2026, d) == du._is_scheduled_closure(CALENDAR_2026, d)
        d += timedelta(days=1)
    assert hc._is_scheduled_closure({}, d) is du._is_scheduled_closure({}, d) is None


@pytest.mark.parametrize("today, expected", [
    # Tuesday: the 7 days before it, minus the weekend and the 10-09 closure.
    (date(2026, 10, 13), [date(2026, 10, 6), date(2026, 10, 7), date(2026, 10, 8), date(2026, 10, 12)]),
    # Saturday: Monday to Friday of that week.
    (date(2026, 10, 17), [date(2026, 10, 12), date(2026, 10, 13), date(2026, 10, 14),
                          date(2026, 10, 15), date(2026, 10, 16)]),
    # Last day of the lunar new year break: the whole window is closed.
    (date(2026, 2, 21), []),
    # 02-11 and 01-02 are listed in the source, as trading days.
    (date(2026, 2, 13), [date(2026, 2, 6), date(2026, 2, 9), date(2026, 2, 10), date(2026, 2, 11)]),
    (date(2026, 1, 5), [date(2026, 1, 2)]),
])
def test_open_weekdays(monkeypatch, today, expected):
    _calendar(monkeypatch, {2025: {date(2025, 12, 29): True, date(2025, 12, 30): True,
                                   date(2025, 12, 31): True},
                            2026: CALENDAR_2026})
    assert hc._open_weekdays(today) == (expected, [])


def test_year_without_a_calendar_counts_as_open_and_is_named(monkeypatch):
    _calendar(monkeypatch)                       # 2026 only
    days, missing = hc._open_weekdays(date(2027, 1, 5))
    assert missing == [2027]
    # 2026-12-29..31 from the calendar; 2027-01-01 and 01-04 only by weekday.
    assert days == [date(2026, 12, 29), date(2026, 12, 30), date(2026, 12, 31),
                    date(2027, 1, 1), date(2027, 1, 4)]


def test_lost_day_is_reported(monkeypatch):
    # 09-18 was a trading day and TAIEX stopped at 09-17.
    _calendar(monkeypatch, missing_taiex={date(2026, 9, 18)})
    lines, want = hc._calendar_gap(date(2026, 9, 19), date(2026, 9, 17))
    assert lines == ["大盤指數缺 09-18，休市日曆排定開市（颱風等臨時休市可忽略）"]
    assert want == date(2026, 9, 18)


def test_lost_day_is_still_reported_after_later_nights_succeeded(monkeypatch):
    # Friday lost, the probe did not run on Saturday or Monday, Monday night
    # succeeded. Tuesday morning must still name Friday.
    _calendar(monkeypatch, missing_taiex={date(2026, 9, 18)})
    lines, want = hc._calendar_gap(date(2026, 9, 22), date(2026, 9, 21))
    assert lines == ["大盤指數缺 09-18，休市日曆排定開市（颱風等臨時休市可忽略）"]
    assert want == date(2026, 9, 21)             # the newest day did arrive


def test_lost_day_leaves_the_report_after_the_window(monkeypatch):
    _calendar(monkeypatch, missing_taiex={date(2026, 9, 18)})
    assert hc._calendar_gap(date(2026, 9, 25), date(2026, 9, 24))[0] != []
    # 09-25 and 09-28 are closures; 09-29 is the next trading day.
    assert hc._calendar_gap(date(2026, 9, 30), date(2026, 9, 29)) == ([], date(2026, 9, 29))


def test_several_lost_days_are_each_named(monkeypatch):
    _calendar(monkeypatch, missing_taiex={date(2026, 10, 7), date(2026, 10, 8), date(2026, 10, 12)})
    lines, want = hc._calendar_gap(date(2026, 10, 13), date(2026, 10, 6))
    assert [line[:11] for line in lines] ==["大盤指數缺 10-07", "大盤指數缺 10-08", "大盤指數缺 10-12"]
    assert want == date(2026, 10, 12)


def test_up_to_date_is_quiet(monkeypatch):
    _calendar(monkeypatch)
    assert hc._calendar_gap(date(2026, 10, 13), date(2026, 10, 12)) == ([], date(2026, 10, 12))


def test_run_in_the_evening_after_todays_pipeline_is_quiet(monkeypatch):
    _calendar(monkeypatch)
    assert hc._calendar_gap(date(2026, 10, 13), date(2026, 10, 13)) == ([], date(2026, 10, 13))


def test_morning_after_a_holiday_is_quiet(monkeypatch):
    # Friday 10-09 has no index row and the calendar closes it.
    _calendar(monkeypatch, missing_taiex={date(2026, 10, 9)})
    assert hc._calendar_gap(date(2026, 10, 12), date(2026, 10, 8)) == ([], date(2026, 10, 8))


def test_window_that_is_all_closures_asks_nothing(monkeypatch):
    asked = _calendar(monkeypatch)
    assert hc._calendar_gap(date(2026, 2, 21), date(2026, 2, 11)) == ([], date(2026, 2, 11))
    assert asked == []


def test_missing_year_is_reported_and_the_comparison_still_runs(monkeypatch):
    # No 2027 calendar yet. 01-01 is really a holiday, so it has no index row:
    # reporting it is the accepted cost of not going quiet.
    _calendar(monkeypatch, missing_taiex={date(2027, 1, 1), date(2027, 1, 4)})
    lines, want = hc._calendar_gap(date(2027, 1, 5), date(2026, 12, 31))
    assert lines == [
        "休市日曆沒有 2027 年資料（該年度只能排除週末）",
        "大盤指數缺 01-01，休市日曆排定開市（颱風等臨時休市可忽略）",
        "大盤指數缺 01-04，休市日曆排定開市（颱風等臨時休市可忽略）",
    ]
    assert want == date(2027, 1, 4)


def test_closure_that_traded_is_reported(monkeypatch):
    _calendar(monkeypatch, traded_closures=[date(2026, 10, 9)])
    lines, _ = hc._calendar_gap(date(2026, 10, 13), date(2026, 10, 12))
    assert lines == ["休市日曆把 10-09 標為休市，但當天有大盤指數（日曆解析規則可能失效）"]


def test_a_failing_check_reports_itself(monkeypatch):
    def boom(year):
        raise RuntimeError("relation tw.market_holidays does not exist")
    monkeypatch.setattr(hc, "_holiday_rows", boom)
    lines, want = hc._calendar_gap(date(2026, 10, 13), date(2026, 10, 12))
    assert len(lines) == 1 and lines[0].startswith("交易日曆檢查本身失敗：RuntimeError")
    assert want == date(2026, 10, 12)


def test_header_names_the_day_the_data_should_have_reached(monkeypatch):
    sent = []
    monkeypatch.setattr(hc, "_latest_trading_day", lambda: date(2026, 9, 17))
    monkeypatch.setattr(hc, "_scalar", lambda sql, params=(): date(2026, 9, 17)
                        if "MAX(" in sql and "updated_at" not in sql else None)
    monkeypatch.setattr(hc, "_DAILY_TABLES", [])
    monkeypatch.setattr(hc, "_SPARSE_DAILY_TABLES", [])
    monkeypatch.setattr(hc, "_PERIODIC_TABLES", [])
    monkeypatch.setattr(hc, "_dead_column_groups", lambda latest: [])
    monkeypatch.setattr(hc, "_financial_coverage", lambda today: [])
    monkeypatch.setattr(hc, "_stale_exes", lambda: [])
    monkeypatch.setattr(hc, "_corporate_actions", lambda latest: [])
    monkeypatch.setattr(hc, "_backup_section", lambda now: None)
    monkeypatch.setattr(hc, "_calendar_gap", lambda today, latest: (["大盤指數缺 09-18"], date(2026, 9, 18)))
    monkeypatch.setattr(hc, "_nightly_run_problems", lambda today: [])
    monkeypatch.setattr(hc, "send_sync", lambda text, **kw: sent.append(text) or True)
    assert hc.run_daily() == 1
    assert sent == ["[健康檢查] 盤後 pipeline 異常（應更新至 09-18）：\n- 大盤指數缺 09-18"]
