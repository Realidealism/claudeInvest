"""Tests for the database-backup freshness probe in the daily health check.

The monthly backup stopped for three months in 2026 because its script was
lost while the scheduled task stayed in place, and nothing looked at the
archives themselves. This probe does. It judges freshness from the monthly
cycle, not from a fixed age, and it must never fail silently: a broken check
has to show up in the message like any other problem.

Everything runs against a temporary folder. No database, no Docker, no Telegram.
"""
import subprocess
from datetime import date, datetime

import pytest

import backup_monthly as bm
import telegram_bot.health_check as hc


def _folder(tmp_path, *names):
    for name in names:
        (tmp_path / name).write_bytes(b"x")
    return tmp_path


# ── which 1st is due ─────────────────────────────────

@pytest.mark.parametrize("now, due", [
    (datetime(2026, 10, 31, 23, 59), date(2026, 10, 1)),   # end of month: this month's dump
    (datetime(2026, 11, 1, 2, 59), date(2026, 10, 1)),     # before the task is even scheduled
    (datetime(2026, 11, 1, 4, 59), date(2026, 10, 1)),     # scheduled, still inside the grace period
    (datetime(2026, 11, 1, 5, 0), date(2026, 11, 1)),      # grace period over
    (datetime(2026, 11, 1, 8, 30), date(2026, 11, 1)),     # when the probe actually runs
    (datetime(2027, 1, 1, 1, 0), date(2026, 12, 1)),       # year boundary
])
def test_due_date_follows_the_monthly_cycle(now, due):
    assert hc._backup_due_date(now) == due


# ── the five situations the folder can be in ───────────────

def test_fresh_dump_is_silent(tmp_path):
    _folder(tmp_path, "invest_20261001.dump")
    assert hc._backup_problems(datetime(2026, 10, 20, 8, 30), tmp_path) == []


def test_dump_taken_by_hand_mid_month_counts_for_that_month_only(tmp_path):
    _folder(tmp_path, "invest_20261006.dump")
    assert hc._backup_problems(datetime(2026, 10, 31, 8, 30), tmp_path) == []
    late = hc._backup_problems(datetime(2026, 11, 1, 8, 30), tmp_path)
    assert late == ["最新一份是 invest_20261006.dump（26 天前），11-01 的月備沒有產出"]


def test_month_turn_does_not_alarm_before_the_grace_period_ends(tmp_path):
    _folder(tmp_path, "invest_20261001.dump")
    assert hc._backup_problems(datetime(2026, 11, 1, 4, 59), tmp_path) == []
    assert hc._backup_problems(datetime(2026, 11, 1, 5, 0), tmp_path) != []


def test_no_dump_at_all(tmp_path):
    _folder(tmp_path, "themes_backup_20260901.json")
    (tmp_path / "lost_modules_20260702").mkdir()
    assert hc._backup_problems(datetime(2026, 10, 20, 8, 30), tmp_path) == [f"{tmp_path} 裡沒有任何完成的備份"]


def test_only_a_partial_is_not_a_backup(tmp_path):
    _folder(tmp_path, "invest_20261101.dump.partial")
    problems = hc._backup_problems(datetime(2026, 11, 1, 8, 30), tmp_path)
    assert problems == [f"{tmp_path} 裡沒有任何完成的備份（只有未完成的 invest_20261101.dump.partial）"]


def test_newest_of_several_old_dumps_is_named(tmp_path):
    _folder(tmp_path, "invest_20260630.dump", "invest_20260701.dump", "invest_20260601.dump")
    problems = hc._backup_problems(datetime(2026, 10, 6, 8, 30), tmp_path)
    assert problems == ["最新一份是 invest_20260701.dump（97 天前），10-01 的月備沒有產出"]


def test_a_partial_next_to_an_old_dump_does_not_rescue_it(tmp_path):
    _folder(tmp_path, "invest_20261001.dump", "invest_20261101.dump.partial")
    assert hc._backup_problems(datetime(2026, 11, 1, 8, 30), tmp_path) != []


# ── names that must not count ──────────────────────────

@pytest.mark.parametrize("name", [
    "invest_20261301.dump",        # month 13: the date does not parse
    "invest_2026101.dump",         # seven digits
    "invest_20261001.dump.bak",
    "Invest_20261001.DUMP.txt",
    "themes_backup_20261001.json",
])
def test_lookalike_names_are_ignored(tmp_path, name):
    _folder(tmp_path, name)
    assert hc._backup_problems(datetime(2026, 10, 20, 8, 30), tmp_path) == [f"{tmp_path} 裡沒有任何完成的備份"]


def test_missing_folder_is_reported_not_raised(tmp_path):
    gone = tmp_path / "not_there"
    assert hc._backup_problems(datetime(2026, 10, 20, 8, 30), gone) == [f"備份目錄讀不到：{gone}（FileNotFoundError）"]


# ── the paragraph in the daily message ───────────────────

def test_section_is_none_when_fresh(tmp_path, monkeypatch):
    monkeypatch.setattr(hc, "_BACKUP_DIR", _folder(tmp_path, "invest_20261001.dump"))
    assert hc._backup_section(datetime(2026, 10, 20, 8, 30)) is None


def test_section_has_its_own_heading(tmp_path, monkeypatch):
    monkeypatch.setattr(hc, "_BACKUP_DIR", _folder(tmp_path, "invest_20260701.dump"))
    section = hc._backup_section(datetime(2026, 10, 6, 8, 30))
    assert section.startswith("資料庫備份異常：\n- 最新一份是 invest_20260701.dump")
    assert "盤後 pipeline" not in section


def test_a_crashing_check_is_reported_as_a_problem(monkeypatch):
    def boom(now, backup_dir=None):
        raise RuntimeError("disk on fire")
    monkeypatch.setattr(hc, "_backup_problems", boom)
    assert hc._backup_section(datetime(2026, 10, 20, 8, 30)) == "資料庫備份異常：\n- 備份檢查本身失敗：RuntimeError: disk on fire"


# ── run_daily: one more paragraph, nothing else disturbed ───────────

@pytest.fixture
def daily(monkeypatch):
    """run_daily with every database-backed helper replaced; returns the sent messages."""
    sent = []
    monkeypatch.setattr(hc, "_latest_trading_day", lambda: date(2026, 10, 5))
    monkeypatch.setattr(hc, "_scalar", lambda sql, params=(): date(2026, 10, 5) if "MAX(" in sql and "updated_at" not in sql
                        else datetime.now(hc.timezone.utc) if "updated_at" in sql else 0)
    monkeypatch.setattr(hc, "_dead_column_groups", lambda latest: [])
    monkeypatch.setattr(hc, "_stale_exes", lambda: [])
    monkeypatch.setattr(hc, "_corporate_actions", lambda latest: [])
    monkeypatch.setattr(hc, "send_sync", lambda text, **kw: sent.append((text, kw)) or True)
    return sent


def test_healthy_day_with_fresh_backup_stays_a_silent_ok(daily, monkeypatch):
    monkeypatch.setattr(hc, "_backup_section", lambda now: None)
    assert hc.run_daily() == 0
    assert len(daily) == 1 and "系統正常" in daily[0][0] and daily[0][1] == {"silent": True}


def test_old_backup_alone_makes_the_day_unhealthy(daily, monkeypatch):
    monkeypatch.setattr(hc, "_backup_section", lambda now: "資料庫備份異常：\n- 最新一份是 X")
    assert hc.run_daily() == 1
    assert daily == [("[健康檢查] 資料庫備份異常：\n- 最新一份是 X", {})]


def test_backup_paragraph_is_added_after_the_existing_ones(daily, monkeypatch):
    monkeypatch.setattr(hc, "_stale_exes", lambda: ["a.exe 落後"])
    monkeypatch.setattr(hc, "_corporate_actions", lambda latest: ["1234 復牌"])
    monkeypatch.setattr(hc, "_backup_section", lambda now: "資料庫備份異常：\n- 最新一份是 X")
    assert hc.run_daily() == 1
    text = daily[0][0]
    assert text.index("盤後 pipeline 異常") < text.index("公司行為") < text.index("資料庫備份異常")
    assert "a.exe 落後" in text and "1234 復牌" in text


def test_a_crashing_backup_check_does_not_cost_the_other_paragraphs(daily, monkeypatch):
    def boom(now, backup_dir=None):
        raise OSError("device not ready")
    monkeypatch.setattr(hc, "_stale_exes", lambda: ["a.exe 落後"])
    monkeypatch.setattr(hc, "_backup_problems", boom)
    assert hc.run_daily() == 1
    text = daily[0][0]
    assert "a.exe 落後" in text and "備份檢查本身失敗：OSError: device not ready" in text


# ── the two definitions must agree ──────────────────────

def test_probe_watches_the_folder_the_backup_writes_to():
    assert hc._BACKUP_DIR == bm.OUT_DIR


def test_probe_accepts_exactly_what_the_backup_script_writes(tmp_path, monkeypatch):
    """Run the real backup code with Docker faked out, then ask the probe about its output."""
    def fake_run(cmd, **kw):
        if "pg_dump" in cmd:
            kw["stdout"].write(b"archive")
            return subprocess.CompletedProcess(cmd, 0, stderr=b"")
        kw["stdin"].read()
        return subprocess.CompletedProcess(cmd, 0, stdout=b"1; 0 1 TABLE DATA tw daily_prices postgres\n", stderr=b"")
    monkeypatch.setattr(bm.subprocess, "run", fake_run)
    written = bm.run("invest-db", tmp_path, keep=6)
    assert hc._BACKUP_NAME.match(written.name)
    assert hc._backup_problems(datetime.now(), tmp_path) == []
