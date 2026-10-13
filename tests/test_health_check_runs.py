"""Tests for the nightly-run check in the daily health check.

daily_update records each weekday run in tw.daily_update_runs. This check is
what turns that record into a line the morning after: the run never started,
it died, a core step failed, or a partial night's summary never arrived.

No database and no Telegram: the two query helpers are replaced.
"""
from datetime import date, datetime, timedelta, timezone

import pytest

import daily_update as du
import telegram_bot.health_check as hc
from holiday_fixture_2026 import RAW_2026
from scrapers.market_holidays import parse

CALENDAR_2026 = {d: r["is_closed"] for d, r in parse(RAW_2026)[0].items()}
UTC = timezone.utc

# Tuesday morning. The window's open weekdays: 10-13 (Tue) .. 10-19 (Mon).
TODAY = date(2026, 10, 20)
NOW = datetime(2026, 10, 20, 0, 30, tzinfo=UTC)          # 08:30 Taipei
WINDOW = [date(2026, 10, 14), date(2026, 10, 15), date(2026, 10, 16), date(2026, 10, 19)]


def run(trade_date, outcome="success", *, hour=21, minute=30, day_offset=0,
        summary_sent=True, steps=()):
    """A record as the database returns it: started_at is aware and in UTC.
    hour/minute are Taipei time on trade_date + day_offset."""
    local = datetime(trade_date.year, trade_date.month, trade_date.day, hour, minute,
                     tzinfo=hc.TPE_TZ) + timedelta(days=day_offset)
    return {"trade_date": trade_date, "started_at": local.astimezone(UTC),
            "outcome": outcome, "summary_sent": summary_sent,
            "steps": [{"label": l, "status": s, "core": l in du.CORE_STEP_LABELS, "trace": None}
                      for l, s in steps]}


def check(monkeypatch, rows, today=TODAY, now=NOW):
    asked = []

    def run_rows(first):
        asked.append(first)
        return sorted((r for r in rows if r["trade_date"] >= first),
                      key=lambda r: r["started_at"])

    monkeypatch.setattr(hc, "_holiday_rows", lambda year: CALENDAR_2026 if year == 2026 else {})
    monkeypatch.setattr(hc, "_run_rows", run_rows)
    # Pinned here so that moving the real constant (it follows the day the
    # exe was rebuilt) does not disturb these fixtures.
    monkeypatch.setattr(hc, "_RUN_RECORDS_SINCE", WINDOW[0])
    return hc._nightly_run_problems(today, now), asked


def clean_week():
    return [run(d) for d in WINDOW]


def test_constants_match_daily_update():
    assert hc._EXPORT_STEP in du.CORE_STEP_LABELS
    assert hc.TPE_TZ.utcoffset(None) == timedelta(hours=8)


def test_window_starts_at_the_first_recorded_night(monkeypatch):
    # 10-13 is in the 7-day window but predates the record.
    lines, asked = check(monkeypatch, clean_week())
    assert lines == []
    assert asked == [date(2026, 10, 14)]


def test_clean_week_is_quiet(monkeypatch):
    assert check(monkeypatch, clean_week())[0] == []


def test_before_the_first_recorded_night_nothing_is_asked(monkeypatch):
    lines, asked = check(monkeypatch, [], today=date(2026, 10, 14))
    assert lines == [] and asked == []


def test_night_without_a_record_is_reported(monkeypatch):
    rows = [r for r in clean_week() if r["trade_date"] != date(2026, 10, 15)]
    assert check(monkeypatch, rows)[0] == ["10-15 盤後流程沒有執行紀錄"]


def test_empty_table_reports_every_night(monkeypatch):
    # The record never being written must not look like a healthy week.
    lines, _ = check(monkeypatch, [])
    assert lines == [f"{d:%m-%d} 盤後流程沒有執行紀錄" for d in WINDOW]


def test_run_that_died_is_reported_with_its_local_start_time(monkeypatch):
    rows = clean_week()[:-1] + [run(date(2026, 10, 19), "running", hour=21, minute=31)]
    assert check(monkeypatch, rows)[0] == ["10-19 盤後流程沒有跑完（21:31 開始）"]


def test_run_still_inside_its_time_limit_is_quiet(monkeypatch):
    rows = clean_week()[:-1] + [run(date(2026, 10, 19), "running", hour=21, minute=30)]
    now = datetime(2026, 10, 19, 13, 40, tzinfo=UTC)      # 21:40 Taipei, same evening
    assert check(monkeypatch, rows, now=now)[0] == []
    later = datetime(2026, 10, 19, 15, 31, tzinfo=UTC)    # 23:31 Taipei
    assert check(monkeypatch, rows, now=later)[0] == ["10-19 盤後流程沒有跑完（21:30 開始）"]


def test_failed_night_names_the_failed_and_blocked_steps(monkeypatch):
    rows = clean_week()[:-1] + [run(date(2026, 10, 19), "failed", steps=[
        ("TWSE daily prices", "failed"), ("市場廣度", "blocked"), ("月營收", "ok")])]
    assert check(monkeypatch, rows)[0] == ["10-19 盤後流程失敗：TWSE daily prices、市場廣度"]


def test_failed_night_is_reported_even_when_the_summary_arrived(monkeypatch):
    rows = clean_week()[:-1] + [run(date(2026, 10, 19), "failed", summary_sent=True,
                                    steps=[("市場廣度", "failed")])]
    assert len(check(monkeypatch, rows)[0]) == 1


def test_failed_night_stays_in_the_report_until_it_leaves_the_window(monkeypatch):
    rows = clean_week()
    rows[0] = run(date(2026, 10, 14), "failed", steps=[("市場廣度", "failed")])
    assert check(monkeypatch, rows)[0] == ["10-14 盤後流程失敗：市場廣度"]


def test_partial_night_is_quiet_when_its_summary_arrived(monkeypatch):
    rows = clean_week()[:-1] + [run(date(2026, 10, 19), "partial", summary_sent=True,
                                    steps=[("月營收", "failed")])]
    assert check(monkeypatch, rows)[0] == []


@pytest.mark.parametrize("summary_sent", [False, None])
def test_partial_night_is_reported_when_its_summary_did_not_arrive(monkeypatch, summary_sent):
    rows = clean_week()[:-1] + [run(date(2026, 10, 19), "partial", summary_sent=summary_sent,
                                    steps=[("月營收", "failed")])]
    assert check(monkeypatch, rows)[0] == ["10-19 盤後流程有步驟失敗、摘要未送達：月營收"]


@pytest.mark.parametrize("outcome", ["aborted", "holiday", "success"])
def test_quiet_outcomes(monkeypatch, outcome):
    # aborted: the missing index row behind it is _calendar_gap's line.
    rows = clean_week()[:-1] + [run(date(2026, 10, 19), outcome, summary_sent=None)]
    assert check(monkeypatch, rows)[0] == []


def test_the_newest_run_of_a_date_decides(monkeypatch):
    failed = run(date(2026, 10, 16), "failed", steps=[("市場廣度", "failed")])
    rerun = run(date(2026, 10, 16), "success", day_offset=1, hour=10)
    rows = [r for r in clean_week() if r["trade_date"] != date(2026, 10, 16)]
    assert check(monkeypatch, rows + [failed, rerun])[0] == []
    # ... in both directions: a later backfill that fails is the state now.
    good = run(date(2026, 10, 16))
    broke = run(date(2026, 10, 16), "failed", day_offset=2, hour=9,
                steps=[("TPEx daily prices", "failed")])
    assert check(monkeypatch, rows + [good, broke])[0] == ["10-16 盤後流程失敗：TPEx daily prices"]


def test_run_made_before_its_date_does_not_count(monkeypatch):
    # A manual run for tomorrow ends at the gate as 'holiday'. If the real
    # night then never runs, that row must not stand in for it.
    early = run(date(2026, 10, 19), "holiday", day_offset=-1, hour=15, summary_sent=None)
    rows = clean_week()[:-1] + [early]
    assert check(monkeypatch, rows)[0] == ["10-19 盤後流程沒有執行紀錄"]
    assert check(monkeypatch, rows + [run(date(2026, 10, 19))])[0] == []


def test_export_failure_is_dropped_once_a_later_export_went_through(monkeypatch):
    rows = clean_week()
    rows[1] = run(date(2026, 10, 15), "failed", steps=[("前端匯出部署", "failed")])
    # The nights after it are clean but carry no steps here: still reported.
    assert check(monkeypatch, rows)[0] == ["10-15 盤後流程失敗：前端匯出部署"]
    rows[2] = run(date(2026, 10, 16), steps=[("前端匯出部署", "ok")])
    assert check(monkeypatch, rows)[0] == []


def test_export_failure_is_kept_while_no_later_export_went_through(monkeypatch):
    rows = clean_week()
    rows[0] = run(date(2026, 10, 14), steps=[("前端匯出部署", "ok")])      # earlier, not later
    rows[1] = run(date(2026, 10, 15), "failed", steps=[("前端匯出部署", "failed")])
    rows[2] = run(date(2026, 10, 16), "failed", steps=[("前端匯出部署", "blocked")])
    assert check(monkeypatch, rows)[0] == [
        "10-15 盤後流程失敗：前端匯出部署", "10-16 盤後流程失敗：前端匯出部署"]


def test_later_export_does_not_excuse_other_core_failures(monkeypatch):
    rows = clean_week()
    rows[1] = run(date(2026, 10, 15), "failed", steps=[
        ("多空評比 + 操作訊號 + 策略持倉快照", "failed"), ("前端匯出部署", "blocked")])
    rows[2] = run(date(2026, 10, 16), steps=[("前端匯出部署", "ok")])
    assert check(monkeypatch, rows)[0] == [
        "10-15 盤後流程失敗：多空評比 + 操作訊號 + 策略持倉快照、前端匯出部署"]


@pytest.mark.parametrize("summary_sent, expected", [
    (True, []),
    (False, ["10-15 盤後流程有步驟失敗、摘要未送達：CNN Fear & Greed"]),
])
def test_made_up_export_failure_leaves_the_nights_other_failures(monkeypatch, summary_sent, expected):
    # Export and a non-core scraper both failed; the next night's export went
    # through. What remains is a partial night, reported like any other.
    assert "CNN Fear & Greed" in {entry[0] for entry in du.SCRAPERS}
    rows = clean_week()
    rows[1] = run(date(2026, 10, 15), "failed", summary_sent=summary_sent, steps=[
        ("CNN Fear & Greed", "failed"), ("前端匯出部署", "failed")])
    rows[2] = run(date(2026, 10, 16), steps=[("前端匯出部署", "ok")])
    assert check(monkeypatch, rows)[0] == expected


def test_failed_night_without_steps_still_reads_as_a_sentence(monkeypatch):
    rows = clean_week()[:-1] + [run(date(2026, 10, 19), "failed")]
    assert check(monkeypatch, rows)[0] == ["10-19 盤後流程失敗：紀錄裡沒有步驟"]


def test_closure_in_the_window_expects_no_run(monkeypatch):
    # 10-26 (Mon) is a closure: Tuesday 10-27 must not ask for its run.
    today = date(2026, 10, 27)
    days = [date(2026, 10, 20), date(2026, 10, 21), date(2026, 10, 22), date(2026, 10, 23)]
    lines, _ = check(monkeypatch, [run(d) for d in days], today=today,
                     now=datetime(2026, 10, 27, 0, 30, tzinfo=UTC))
    assert lines == []


def test_a_failing_check_reports_itself(monkeypatch):
    def boom(first):
        raise RuntimeError("relation tw.daily_update_runs does not exist")

    monkeypatch.setattr(hc, "_holiday_rows", lambda year: CALENDAR_2026)
    monkeypatch.setattr(hc, "_run_rows", boom)
    monkeypatch.setattr(hc, "_RUN_RECORDS_SINCE", WINDOW[0])
    lines = hc._nightly_run_problems(TODAY, NOW)
    assert len(lines) == 1 and lines[0].startswith("盤後執行紀錄檢查本身失敗：RuntimeError")


def test_run_daily_includes_the_lines(monkeypatch):
    sent = []
    monkeypatch.setattr(hc, "_latest_trading_day", lambda: date(2026, 10, 19))
    monkeypatch.setattr(hc, "_scalar", lambda sql, params=(): None)
    monkeypatch.setattr(hc, "_DAILY_TABLES", [])
    monkeypatch.setattr(hc, "_SPARSE_DAILY_TABLES", [])
    monkeypatch.setattr(hc, "_PERIODIC_TABLES", [])
    monkeypatch.setattr(hc, "_dead_column_groups", lambda latest: [])
    monkeypatch.setattr(hc, "_financial_coverage", lambda today: [])
    monkeypatch.setattr(hc, "_stale_exes", lambda: [])
    monkeypatch.setattr(hc, "_corporate_actions", lambda latest: [])
    monkeypatch.setattr(hc, "_backup_section", lambda now: None)
    monkeypatch.setattr(hc, "_calendar_gap", lambda today, latest: ([], latest))
    monkeypatch.setattr(hc, "_nightly_run_problems", lambda today: ["10-19 盤後流程沒有執行紀錄"])
    monkeypatch.setattr(hc, "send_sync", lambda text, **kw: sent.append(text) or True)
    assert hc.run_daily() == 1
    assert sent == ["[健康檢查] 盤後 pipeline 異常（應更新至 10-19）：\n- 10-19 盤後流程沒有執行紀錄"]
