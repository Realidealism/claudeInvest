"""Tests for the known-pause list in the daily health check.

A periodic check whose source was switched off on purpose reported the same
line every day: true, expected, and enough to keep the daily message from ever
being quiet. A pause skips that check without a word while it is in force, and
it cannot be forgotten into permanence: the day after it expires the check
runs again and says so.

No database and no Telegram: every query helper is replaced.
"""
from datetime import date, datetime, timedelta

import pytest

import telegram_bot.health_check as hc

TODAY = date.today()
REVENUE = "年報產品別營收"
REVENUE_TABLE = "tw.product_revenue_status"


def _entry(check=REVENUE, started=TODAY - timedelta(days=20), expires=TODAY + timedelta(days=10)):
    return {"check": check, "reason": "r", "started": started, "expires": expires,
            "resume_condition": "c"}


# ── one check on one day ───────────────────────────────

def test_no_entry_means_no_pause(monkeypatch):
    monkeypatch.setattr(hc, "_KNOWN_PAUSES", [])
    assert hc._pause(REVENUE, TODAY) == (False, "")


@pytest.mark.parametrize("today, expected", [
    (date(2026, 9, 13), (False, "")),                               # before it started
    (date(2026, 9, 14), (True, "")),                                # first day
    (date(2026, 10, 20), (True, "")),
    (date(2026, 11, 6), (True, "")),                                # last day, still quiet
    (date(2026, 11, 7), (False, "，暫停已於 11-06 到期、需重新裁定")),   # the day after
    (date(2027, 3, 1), (False, "，暫停已於 11-06 到期、需重新裁定")),    # and it never goes quiet again
])
def test_pause_is_in_force_from_start_through_expiry(monkeypatch, today, expected):
    monkeypatch.setattr(hc, "_KNOWN_PAUSES",
                        [_entry(started=date(2026, 9, 14), expires=date(2026, 11, 6))])
    assert hc._pause(REVENUE, today) == expected


def test_pause_applies_only_to_the_check_it_names(monkeypatch):
    monkeypatch.setattr(hc, "_KNOWN_PAUSES", [_entry()])
    assert hc._pause("法說會索引", TODAY) == (False, "")


# ── the daily message ────────────────────────────────

@pytest.fixture
def daily(monkeypatch):
    """run_daily with every helper replaced. `ages` maps a periodic table to the
    days since its last update (absent: fresh; None: the table is empty)."""
    sent, ages = [], {}

    def scalar(sql, params=()):
        if "updated_at" in sql:
            table = sql.rsplit("FROM ", 1)[1].strip()
            if table in ages and ages[table] is None:
                return None
            return datetime.now(hc.timezone.utc) - timedelta(days=ages.get(table, 0), hours=1)
        return date(2026, 10, 5) if "MAX(" in sql else 0

    monkeypatch.setattr(hc, "_latest_trading_day", lambda: date(2026, 10, 5))
    monkeypatch.setattr(hc, "_scalar", scalar)
    monkeypatch.setattr(hc, "_dead_column_groups", lambda latest: [])
    monkeypatch.setattr(hc, "_stale_exes", lambda: [])
    monkeypatch.setattr(hc, "_calendar_gap", lambda today, latest: ([], latest))
    monkeypatch.setattr(hc, "_nightly_run_problems", lambda today: [])
    monkeypatch.setattr(hc, "_corporate_actions", lambda latest: [])
    monkeypatch.setattr(hc, "_backup_section", lambda now: None)
    monkeypatch.setattr(hc, "send_sync", lambda text, **kw: sent.append((text, kw)) or True)
    return sent, ages


def test_without_a_pause_the_stale_source_is_reported_as_before(daily, monkeypatch):
    sent, ages = daily
    ages[REVENUE_TABLE] = 22
    monkeypatch.setattr(hc, "_KNOWN_PAUSES", [])
    assert hc.run_daily() == 1
    assert sent == [("[健康檢查] 盤後 pipeline 異常（應更新至 10-05）：\n- 年報產品別營收 已 22 天未更新（容許 9 天）", {})]


def test_a_pause_in_force_leaves_the_day_a_silent_ok(daily, monkeypatch):
    sent, ages = daily
    ages[REVENUE_TABLE] = 22
    monkeypatch.setattr(hc, "_KNOWN_PAUSES", [_entry(expires=TODAY)])   # last day counts
    assert hc.run_daily() == 0
    assert sent == [("[健康檢查] ✓ 系統正常（資料更新至 10-05）", {"silent": True})]


def test_the_day_after_expiry_the_check_is_back_and_says_why(daily, monkeypatch):
    sent, ages = daily
    ages[REVENUE_TABLE] = 22
    gone = TODAY - timedelta(days=1)
    monkeypatch.setattr(hc, "_KNOWN_PAUSES", [_entry(expires=gone)])
    assert hc.run_daily() == 1
    assert sent == [("[健康檢查] 盤後 pipeline 異常（應更新至 10-05）：\n"
                     f"- 年報產品別營收 已 22 天未更新（容許 9 天），暫停已於 {gone:%m-%d} 到期、需重新裁定", {})]


def test_an_expired_pause_adds_nothing_when_the_source_is_healthy_again(daily, monkeypatch):
    sent, ages = daily
    monkeypatch.setattr(hc, "_KNOWN_PAUSES", [_entry(expires=TODAY - timedelta(days=1))])
    assert hc.run_daily() == 0
    assert sent == [("[健康檢查] ✓ 系統正常（資料更新至 10-05）", {"silent": True})]


def test_a_pause_does_not_hide_another_stale_source(daily, monkeypatch):
    sent, ages = daily
    ages[REVENUE_TABLE] = 22
    ages["tw.company_affiliations"] = 201
    monkeypatch.setattr(hc, "_KNOWN_PAUSES", [_entry()])
    assert hc.run_daily() == 1
    assert sent == [("[健康檢查] 盤後 pipeline 異常（應更新至 10-05）：\n- 轉投資關係 已 201 天未更新（容許 200 天）", {})]


def test_an_empty_table_is_covered_by_the_pause_and_reported_after_it(daily, monkeypatch):
    sent, ages = daily
    ages[REVENUE_TABLE] = None
    monkeypatch.setattr(hc, "_KNOWN_PAUSES", [_entry()])
    assert hc.run_daily() == 0
    gone = TODAY - timedelta(days=1)
    monkeypatch.setattr(hc, "_KNOWN_PAUSES", [_entry(expires=gone)])
    assert hc.run_daily() == 1
    assert sent[-1][0].endswith(f"- 年報產品別營收 無任何資料，暫停已於 {gone:%m-%d} 到期、需重新裁定")


# ── the list that is actually deployed ─────────────────────

def test_every_deployed_pause_is_complete_and_bounded():
    for p in hc._KNOWN_PAUSES:
        assert set(p) == {"check", "reason", "started", "expires", "resume_condition"}, p
        assert type(p["started"]) is date and type(p["expires"]) is date, p
        assert p["started"] < p["expires"], p
        assert p["reason"].strip() and p["resume_condition"].strip(), p


def test_every_deployed_pause_names_a_check_that_honours_it():
    """A misspelt or renamed label would be a pause that silently never applies."""
    labels = {row[0] for row in hc._PERIODIC_TABLES}
    for p in hc._KNOWN_PAUSES:
        assert p["check"] in labels, p["check"]


def test_one_entry_per_check():
    names = [p["check"] for p in hc._KNOWN_PAUSES]
    assert len(names) == len(set(names))
