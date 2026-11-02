"""The conference index in the daily health check is aged in trading sessions.

It is refreshed only by the trading-night run. Aged in calendar days (3 until
2026-11-02) it cried wolf on 2026-10-27: 10-23 closed for a typhoon, the
weekend, and 10-26 a make-up holiday left four calendar days and no trading
night after its last refresh on 10-22.

No database and no Telegram: the query helper answers from a session list.
"""
from datetime import date

import pytest

import telegram_bot.health_check as hc

# TAIEX sessions around the 2026-10-23 typhoon and the 10-26 make-up holiday
SESSIONS = [date(2026, 10, 20), date(2026, 10, 21), date(2026, 10, 22),
            date(2026, 10, 27), date(2026, 10, 28)]


@pytest.fixture
def daily(monkeypatch):
    sent, state, sql_seen = [], {}, []

    def scalar(sql, params=()):
        sql_seen.append(sql)
        if "tw.investor_conferences" in sql:
            return state["conf"]
        if "count(*)" in sql:                       # _trading_days_since
            d, latest = params
            return sum(d < s <= latest for s in SESSIONS)
        return state["latest"] if "MAX(" in sql else None

    monkeypatch.setattr(hc, "_scalar", scalar)
    monkeypatch.setattr(hc, "_latest_trading_day", lambda: state["latest"])
    monkeypatch.setattr(hc, "_DAILY_TABLES", [])
    monkeypatch.setattr(hc, "_PERIODIC_TABLES", [])
    monkeypatch.setattr(hc, "_SPARSE_DAILY_TABLES",
                        [r for r in hc._SPARSE_DAILY_TABLES if r[0] == "法說會索引"])
    monkeypatch.setattr(hc, "_dead_column_groups", lambda latest: [])
    monkeypatch.setattr(hc, "_financial_coverage", lambda today: [])
    monkeypatch.setattr(hc, "_stale_exes", lambda: [])
    monkeypatch.setattr(hc, "_calendar_gap", lambda today, latest: ([], latest))
    monkeypatch.setattr(hc, "_nightly_run_problems", lambda today: [])
    monkeypatch.setattr(hc, "_corporate_actions", lambda latest: [])
    monkeypatch.setattr(hc, "_backup_section", lambda now: None)
    monkeypatch.setattr(hc, "send_sync", lambda text, **kw: sent.append(text) or True)
    return sent, state, sql_seen


def test_holidays_are_not_a_gap(daily):
    # 10-27 08:30: the last session is still 10-22, refreshed that night
    sent, state, _ = daily
    state.update(latest=date(2026, 10, 22), conf=date(2026, 10, 22))
    assert hc.run_daily() == 0
    assert sent == ["[健康檢查] ✓ 系統正常（資料更新至 10-22）"]


def test_one_trading_night_missed_is_reported(daily):
    # 10-28 08:30: 10-27 was a session and its night did not refresh the index
    sent, state, _ = daily
    state.update(latest=date(2026, 10, 27), conf=date(2026, 10, 22))
    assert hc.run_daily() == 1
    assert sent == ["[健康檢查] 盤後 pipeline 異常（應更新至 10-27）：\n"
                    "- 法說會索引 停在 10-22（已 1 個交易日未更新，容許 0）"]


def test_empty_index_is_reported(daily):
    sent, state, _ = daily
    state.update(latest=date(2026, 10, 27), conf=None)
    assert hc.run_daily() == 1
    assert sent[0].endswith("- 法說會索引 無任何資料")


def test_index_is_read_as_a_taipei_date(daily):
    # written ~22:00 Taipei, i.e. 14:00 UTC: the date is the trading night's
    _, state, sql_seen = daily
    state.update(latest=date(2026, 10, 22), conf=date(2026, 10, 22))
    hc.run_daily()
    q = [s for s in sql_seen if "tw.investor_conferences" in s]
    assert q == ["SELECT MAX((updated_at AT TIME ZONE 'Asia/Taipei')::date) AS d "
                 "FROM tw.investor_conferences"]


def test_index_is_no_longer_aged_in_calendar_days():
    assert "法說會索引" not in {r[0] for r in hc._PERIODIC_TABLES}
