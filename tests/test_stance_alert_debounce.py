"""stance_alert: 防守邊界要連續 CONFIRM_CHECKS 次才推播; 攻防解除附出口說明."""
from datetime import datetime, timedelta, timezone

import pytest

from daily_update_guards import no_side_effects  # noqa: F401
import telegram_bot.watchers.stance_alert as sa

NOW = datetime(2026, 10, 22, 10, 0, tzinfo=timezone(timedelta(hours=8)))


@pytest.fixture
def run(monkeypatch, tmp_path):
    sent = []
    monkeypatch.setattr(sa, "_STATE_FILE", tmp_path / "state")
    monkeypatch.setattr(sa, "_state_in_process", None)
    monkeypatch.setattr(sa, "_now_tpe", lambda: NOW)
    monkeypatch.setattr(sa, "_todays_longs", lambda: ([], {}))
    monkeypatch.setattr(sa, "send_sync", lambda msg: sent.append(msg) or True)

    def step(stance, reason=""):
        monkeypatch.setattr(sa, "_live_stance", lambda now: {
            "stance3": stance, "stance3_reason": reason, "exposure": "持有"})
        sa._run_check(respect_gates=False)
        return list(sent)
    return step


def test_enter_needs_two_consecutive_checks(run):
    assert run("攻擊") == []
    assert run("防守") == []                      # 1/2: pending
    out = run("防守")                             # 2/2: push
    assert len(out) == 1 and out[0].startswith("[攻防轉防守]")


def test_one_check_blip_does_not_push(run):
    run("防守"); run("防守")                       # confirmed 防守 (one push)
    assert len(run("攻擊")) == 1                  # pending exit
    assert len(run("防守")) == 1                  # back: no exit push, pending dropped
    assert len(run("攻擊")) == 1                  # counts from 1 again
    assert len(run("攻擊")) == 2


def test_exit_push_names_the_exit(run):
    run("防守"); run("防守")
    run("攻擊", "攻擊（排列：強空）：盤中波段防守守滿 5 天解除；排列仍空，明日可能轉趨勢防守")
    out = run("攻擊", "攻擊（排列：強空）：盤中波段防守守滿 5 天解除；排列仍空，明日可能轉趨勢防守")
    assert out[-1].startswith("[攻防解除]")
    assert "守滿 5 天解除" in out[-1] and "明日" not in out[-1]
