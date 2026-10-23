"""Tests for the intraday snapshot daemon's per-step watchdog.

A hung step must end the whole process with a non-zero exit code and leave
every thread's stack behind, because NSSM can only restart a process that
exits and the scene is gone once it does. Each unit of work carries its own
deadline (pass 1200s, other steps 450s) and sleeps carry none.

The deadline behaviour runs in a child process, since the watchdog kills the
process it lives in. The loop wiring and the alert path use stand-in modules:
no database, no git, no Telegram.
"""
import subprocess
import sys
import textwrap
import threading
import time
import types
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pytest

import intraday_snapshot as m

REPO = Path(__file__).resolve().parents[1]

CHILD = textwrap.dedent(
    """
    import sys, threading, time
    sys.path.insert(0, {repo!r})
    import intraday_snapshot as m

    def _stuck_here():
        time.sleep(30)

    def _bystander():
        time.sleep(60)

    if __name__ == "__main__":
        threading.Thread(target=_bystander, daemon=True).start()
        mode = sys.argv[1]
        if mode == "stuck":
            with m._deadline(1):
                _stuck_here()
        elif mode == "cancel":
            with m._deadline(2):
                pass
            time.sleep(4)
        elif mode == "rearm":
            with m._deadline(30):
                pass
            with m._deadline(1):
                _stuck_here()
        print("finished")
    """
)


def _child(tmp_path, mode):
    script = tmp_path / "child.py"
    script.write_text(CHILD.format(repo=str(REPO)), encoding="utf-8")
    t0 = time.time()
    r = subprocess.run([sys.executable, str(script), mode],
                       capture_output=True, text=True, timeout=60)
    return r, time.time() - t0


# -- the deadline itself --------------------------------------------------

def test_expired_deadline_kills_process_and_dumps_all_threads(tmp_path):
    r, elapsed = _child(tmp_path, "stuck")
    assert r.returncode == 1
    assert elapsed < 10
    assert "Timeout" in r.stderr
    assert "_stuck_here" in r.stderr
    assert "_bystander" in r.stderr
    assert "finished" not in r.stdout


def test_finished_block_cancels_its_deadline(tmp_path):
    r, _ = _child(tmp_path, "cancel")
    assert r.returncode == 0
    assert "Timeout" not in r.stderr
    assert "finished" in r.stdout


def test_later_shorter_deadline_replaces_earlier_one(tmp_path):
    r, elapsed = _child(tmp_path, "rearm")
    assert r.returncode == 1
    assert elapsed < 10
    assert "_stuck_here" in r.stderr


def test_default_thresholds():
    assert m.PASS_WATCHDOG_SECONDS == 1200
    assert m.STEP_WATCHDOG_SECONDS == 450


# -- which work runs under which deadline ---------------------------------

class _Recorder:
    """Stands in for _deadline and remembers which deadline each call ran under."""

    def __init__(self):
        self.active = []
        self.calls = []

    @contextmanager
    def deadline(self, seconds):
        self.active.append(seconds)
        try:
            yield
        finally:
            self.active.pop()

    def note(self, name):
        self.calls.append((name, list(self.active)))


class _Stop(threading.Event):
    def __init__(self, rec):
        super().__init__()
        self.rec = rec

    def wait(self, timeout=None):
        self.rec.note("sleep")
        return self.is_set()


def _fake_intraday(monkeypatch, rec, *, in_session, bucket_count=3):
    session = types.ModuleType("intraday.session")
    session.in_snapshot_session = lambda now: in_session
    session.in_post_close_window = lambda now: not in_session
    session.now_tpe = lambda: datetime(2026, 10, 8, 13, 40)
    session.seconds_until_next_open = lambda now: 60.0
    store = types.ModuleType("intraday.store")

    def bucket_count_fn(today):
        rec.note("bucket_poll")
        return bucket_count

    store.post_close_bucket_count = bucket_count_fn
    store.is_market_closed = lambda today: False
    estimate = types.ModuleType("intraday.estimate")
    estimate.HCurveNotReadyError = type("HCurveNotReadyError", (Exception,), {})
    monkeypatch.setitem(sys.modules, "intraday.session", session)
    monkeypatch.setitem(sys.modules, "intraday.store", store)
    monkeypatch.setitem(sys.modules, "intraday.estimate", estimate)


def test_pass_runs_entirely_under_pass_deadline(monkeypatch):
    rec = _Recorder()
    monkeypatch.setattr(m, "_deadline", rec.deadline)
    monkeypatch.setattr(m, "_maybe_scrape_vix", lambda: rec.note("vix"))
    snap = types.ModuleType("analysis.intraday_snapshot")

    def run(final_pass=False):
        rec.note("snapshot")
        return {"stocks_evaluated": 7}

    snap.run = run
    gen = types.ModuleType("export.generate")
    gen.export_intraday = lambda: rec.note("export")
    monkeypatch.setitem(sys.modules, "analysis.intraday_snapshot", snap)
    monkeypatch.setitem(sys.modules, "export.generate", gen)

    assert m._run_pass() == 7
    assert rec.calls == [("vix", [1200]), ("snapshot", [1200]), ("export", [1200])]


def test_in_session_push_runs_under_step_deadline(monkeypatch):
    rec = _Recorder()
    stop = _Stop(rec)
    _fake_intraday(monkeypatch, rec, in_session=True)
    monkeypatch.setattr(m, "_deadline", rec.deadline)
    monkeypatch.setattr(m, "_run_pass", lambda final_pass=False: rec.note("pass") or 1)

    def push_signals():
        rec.note("push_signals")
        stop.set()

    monkeypatch.setattr(m, "_push_signals", push_signals)
    m._run_loop(stop)
    assert rec.calls == [("pass", []), ("push_signals", [450]), ("sleep", [])]


def test_post_close_poll_and_pushes_run_under_step_deadline(monkeypatch):
    rec = _Recorder()
    stop = _Stop(rec)
    _fake_intraday(monkeypatch, rec, in_session=False)
    monkeypatch.setattr(m, "_deadline", rec.deadline)
    seen = {}

    def run_pass(final_pass=False):
        seen["final_pass"] = final_pass
        rec.note("pass")
        return 1

    def push_final():
        rec.note("push_final")
        stop.set()

    monkeypatch.setattr(m, "_run_pass", run_pass)
    monkeypatch.setattr(m, "_push_signals", lambda: rec.note("push_signals"))
    monkeypatch.setattr(m, "_push_final", push_final)
    m._run_loop(stop)
    assert seen["final_pass"] is True
    assert rec.calls == [
        ("bucket_poll", [450]),
        ("pass", []),
        ("push_signals", [450]),
        ("push_final", [450]),
    ]


def test_post_close_wait_sleeps_outside_any_deadline(monkeypatch):
    rec = _Recorder()
    stop = _Stop(rec)
    _fake_intraday(monkeypatch, rec, in_session=False, bucket_count=0)
    monkeypatch.setattr(m, "_deadline", rec.deadline)

    def wait(timeout=None):
        rec.note("sleep")
        stop.set()
        return True

    monkeypatch.setattr(stop, "wait", wait)
    m._run_loop(stop)
    assert rec.calls == [("bucket_poll", [450]), ("sleep", [])]


# -- a failed git-push alert leaves a trace -------------------------------

def _fake_push(monkeypatch, alert):
    push = types.ModuleType("intraday_git_push")
    push.push = lambda: 1
    cron = types.ModuleType("telegram_bot.cron_alert")
    cron.main = alert
    monkeypatch.setitem(sys.modules, "intraday_git_push", push)
    monkeypatch.setitem(sys.modules, "telegram_bot.cron_alert", cron)


def test_alert_nonzero_return_is_logged(monkeypatch, capsys):
    _fake_push(monkeypatch, lambda argv: 1)
    m._push_final()
    out = capsys.readouterr().out
    assert "final-pass push failed" in out
    assert "git-push alert returned 1" in out


def test_alert_exception_is_logged_with_traceback(monkeypatch, capsys):
    def boom(argv):
        raise RuntimeError("alert boom")

    _fake_push(monkeypatch, boom)
    m._push_final()
    captured = capsys.readouterr()
    assert "git-push alert failed:" in captured.out
    assert "RuntimeError: alert boom" in captured.err


def test_alert_success_adds_no_warning(monkeypatch, capsys):
    _fake_push(monkeypatch, lambda argv: 0)
    m._push_final()
    assert "git-push alert" not in capsys.readouterr().out


# -- a pass thrown away for per-stock errors is alerted once a day --------

from analysis.snapshot_errors import PerStockErrors  # noqa: E402
import telegram_bot.cron_alert as cron_alert  # noqa: E402

ERRS = PerStockErrors([("signal buy", "2330", "ValueError: x")])


def _fake_alert(monkeypatch, rec, rc=0):
    sent = []
    cron = types.ModuleType("telegram_bot.cron_alert")

    def main(argv):
        rec.note("alert")
        sent.append(argv)
        return rc
    cron.main = main
    monkeypatch.setitem(sys.modules, "telegram_bot.cron_alert", cron)
    monkeypatch.setattr(m, "_per_stock_alerted_on", None)
    return sent


def test_per_stock_errors_alert_once_a_day_under_the_step_deadline(monkeypatch):
    rec = _Recorder()
    monkeypatch.setattr(m, "_deadline", rec.deadline)
    sent = _fake_alert(monkeypatch, rec)
    m._alert_per_stock_errors(ERRS)
    m._alert_per_stock_errors(ERRS)
    assert rec.calls == [("alert", [450])]
    assert sent == [["snapshot_exceptions", str(ERRS)]]
    # The next day it goes out again.
    monkeypatch.setattr(m, "_per_stock_alerted_on", datetime(2000, 1, 1).date())
    m._alert_per_stock_errors(ERRS)
    assert len(sent) == 2


def test_undelivered_per_stock_alert_is_tried_again(monkeypatch, capsys):
    rec = _Recorder()
    monkeypatch.setattr(m, "_deadline", rec.deadline)
    sent = _fake_alert(monkeypatch, rec, rc=1)
    m._alert_per_stock_errors(ERRS)
    m._alert_per_stock_errors(ERRS)
    assert len(sent) == 2 and "alert returned 1" in capsys.readouterr().out


@pytest.mark.parametrize("in_session", [True, False])
def test_pass_with_per_stock_errors_alerts_outside_the_pass_deadline(monkeypatch, in_session):
    rec = _Recorder()
    stop = _Stop(rec)
    _fake_intraday(monkeypatch, rec, in_session=in_session)
    monkeypatch.setattr(m, "_deadline", rec.deadline)
    _fake_alert(monkeypatch, rec)

    def run_pass(final_pass=False):
        rec.note("pass")
        raise ERRS

    def wait(timeout=None):
        rec.note("sleep")
        stop.set()
        return True

    monkeypatch.setattr(m, "_run_pass", run_pass)
    monkeypatch.setattr(stop, "wait", wait)
    m._run_loop(stop)
    calls = [c for c in rec.calls if c[0] != "bucket_poll"]
    assert calls == [("pass", []), ("alert", [450]), ("sleep", [])]


def test_pass_deadline_is_cancelled_before_the_alert(monkeypatch):
    # The real _run_pass: the error leaves its pass deadline before the
    # alert arms the step deadline (there is only one faulthandler timer).
    rec = _Recorder()
    stop = _Stop(rec)
    _fake_intraday(monkeypatch, rec, in_session=True)
    monkeypatch.setattr(m, "_deadline", rec.deadline)
    monkeypatch.setattr(m, "_maybe_scrape_vix", lambda: rec.note("vix"))
    _fake_alert(monkeypatch, rec)
    snap = types.ModuleType("analysis.intraday_snapshot")

    def run(final_pass=False):
        rec.note("snapshot")
        raise ERRS
    snap.run = run
    gen = types.ModuleType("export.generate")
    gen.export_intraday = lambda: rec.note("export")
    monkeypatch.setitem(sys.modules, "analysis.intraday_snapshot", snap)
    monkeypatch.setitem(sys.modules, "export.generate", gen)

    def wait(timeout=None):
        rec.note("sleep")
        stop.set()
        return True
    monkeypatch.setattr(stop, "wait", wait)
    m._run_loop(stop)
    assert rec.calls == [("vix", [1200]), ("snapshot", [1200]), ("alert", [450]), ("sleep", [])]


def test_alert_that_raises_is_logged_and_tried_again(monkeypatch, capsys):
    rec = _Recorder()
    monkeypatch.setattr(m, "_deadline", rec.deadline)
    _fake_alert(monkeypatch, rec)

    def boom(argv):
        raise RuntimeError("telegram down")
    sys.modules["telegram_bot.cron_alert"].main = boom
    m._alert_per_stock_errors(ERRS)
    assert m._per_stock_alerted_on is None
    assert "per-stock-errors alert failed" in capsys.readouterr().out


def test_other_pass_failures_do_not_send_the_per_stock_alert(monkeypatch):
    rec = _Recorder()
    stop = _Stop(rec)
    _fake_intraday(monkeypatch, rec, in_session=True)
    monkeypatch.setattr(m, "_deadline", rec.deadline)
    sent = _fake_alert(monkeypatch, rec)

    def run_pass(final_pass=False):
        raise RuntimeError("db down")

    def wait(timeout=None):
        stop.set()
        return True

    monkeypatch.setattr(m, "_run_pass", run_pass)
    monkeypatch.setattr(stop, "wait", wait)
    m._run_loop(stop)
    assert sent == []


def test_cron_alert_composes_the_per_stock_message():
    assert cron_alert._EVENTS["snapshot_exceptions"]("逐檔例外 1 處") == \
        "盤中快照逐檔例外，這一輪沒有寫入：逐檔例外 1 處"
