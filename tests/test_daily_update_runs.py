"""Tests for how a nightly run is classified, recorded and turned into an
exit code.

A step failure used to end in exit 0 with nothing but a Telegram summary to
show for it. Each weekday run now leaves a row in tw.daily_update_runs and a
log file, and the exit code follows the outcome.

No database: the cursor is replaced. The SQL itself was run against a
throwaway PostgreSQL when it was written (tmp/sysreview-2026-10/inject/
container_run_record.py).
"""
import io
import json
import sys
from contextlib import contextmanager
from datetime import date

import pytest

import daily_update as du
import db.connection
from daily_update_guards import no_side_effects, real_open_run_log  # noqa: F401

WEEKDAY = date(2026, 10, 14)
SATURDAY = date(2026, 10, 17)
SNAPSHOT = "多空評比 + 操作訊號 + 策略持倉快照"


# ── classification ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("results, outcome", [
    ([("TWSE daily prices", "ok"), ("月營收", "skip")], "success"),
    ([], "success"),
    ([("TWSE daily prices", "ok"), ("法說會預告推播", "failed")], "partial"),
    ([("TWSE daily prices", "failed"), ("法說會預告推播", "ok")], "failed"),
    ([("市場廣度", "failed")], "failed"),
    ([("Market indices", "failed")], "failed"),
    ([(SNAPSHOT, "blocked")], "failed"),
    ([("前端匯出部署", "blocked"), ("日線訊號推播", "blocked")], "failed"),
    ([("日線訊號推播", "blocked")], "partial"),
])
def test_run_outcome(results, outcome):
    assert du._run_outcome(results) == outcome


@pytest.mark.parametrize("outcome, code", [
    ("success", 0), ("holiday", 0), ("weekend", 0),
    ("partial", 2), ("failed", 1), ("aborted", 1),
    ("running", 1), (None, 1), (True, 1),
])
def test_exit_code(outcome, code):
    assert du._exit_code(outcome) == code


def test_range_reports_the_worst_day(monkeypatch):
    by_day = {date(2026, 10, 12): "success", date(2026, 10, 13): "partial",
              date(2026, 10, 14): "failed", date(2026, 10, 15): "partial",
              date(2026, 10, 16): "holiday", date(2026, 10, 17): "weekend"}
    monkeypatch.setattr(du, "update_date", by_day.__getitem__)
    assert du.update_range(date(2026, 10, 12), date(2026, 10, 13)) == "partial"
    assert du.update_range(date(2026, 10, 12), date(2026, 10, 17)) == "failed"
    assert du.update_range(date(2026, 10, 15), date(2026, 10, 17)) == "partial"
    assert du.update_range(date(2026, 10, 16), date(2026, 10, 17)) == "success"


# ── the summary text ───────────────────────────────────────────────────────

def test_summary_without_blocked_steps_reads_as_before():
    results = [("A", "ok"), ("B", "failed"), ("C", "skip")]
    assert du._build_daily_update_message(WEEKDAY, results, {"B": "boom"}) == (
        "[每日更新] 2026-10-14\n"
        "失敗：B\n"
        "成功 1 / 失敗 1 / 跳過 1\n"
        "── B ──\n"
        "boom\n"
        "重跑指令：daily_update.exe 2026-10-14"
    )
    assert du._build_daily_update_message(WEEKDAY, [("A", "ok"), ("C", "skip")]) == (
        "[每日更新] 2026-10-14 全部成功（成功 1 / 失敗 0 / 跳過 1）")


def test_summary_names_the_blocked_steps():
    results = [("A", "ok"), (SNAPSHOT, "failed"), ("前端匯出部署", "blocked"), ("C", "skip")]
    msg = du._build_daily_update_message(WEEKDAY, results, {})
    assert msg.splitlines()[:4] == [
        "[每日更新] 2026-10-14",
        f"失敗：{SNAPSHOT}",
        "因上游失敗未執行：前端匯出部署",
        "成功 1 / 失敗 1 / 跳過 1 / 未執行 1",
    ]


def test_blocked_alone_is_never_all_clear():
    msg = du._build_daily_update_message(WEEKDAY, [("A", "ok"), ("B", "blocked")], {})
    assert "全部成功" not in msg and "因上游失敗未執行：B" in msg


# ── update_date around the pipeline body ───────────────────────────────────

class _Record:
    """Stand-ins for the body and the two record writers."""

    def __init__(self, monkeypatch, body, run_id=7):
        self.started = []
        self.finished = []
        self.chip_only = []

        def run_start(trade_date):
            self.started.append(trade_date)
            return run_id

        def run_finish(*args):
            self.finished.append(args)

        monkeypatch.setattr(du, "_update_trading_day", body)
        monkeypatch.setattr(du, "_run_start", run_start)
        monkeypatch.setattr(du, "_run_finish", run_finish)
        monkeypatch.setattr(du, "update_chip_only", self.chip_only.append)


def test_finished_run_is_recorded(monkeypatch):
    def body(trade_date, results, traces):
        results.append(("A", "failed"))
        traces["A"] = "boom"
        return "partial", True

    rec = _Record(monkeypatch, body)
    assert du.update_date(WEEKDAY) == "partial"
    assert rec.started == [WEEKDAY]
    assert rec.finished == [(7, WEEKDAY, "partial", [("A", "failed")], {"A": "boom"}, True)]


@pytest.mark.parametrize("outcome", ["holiday", "aborted"])
def test_gate_outcome_is_recorded(monkeypatch, outcome):
    rec = _Record(monkeypatch, lambda d, results, traces: (outcome, None))
    assert du.update_date(WEEKDAY) == outcome
    assert rec.finished == [(7, WEEKDAY, outcome, [], {}, None)]


def test_weekend_keeps_no_record(monkeypatch):
    def body(*args):
        raise AssertionError("the weekday pipeline ran on a Saturday")

    rec = _Record(monkeypatch, body)
    assert du.update_date(SATURDAY) == "weekend"
    assert rec.chip_only == [SATURDAY]
    assert rec.started == [] and rec.finished == []


def test_unhandled_exception_is_recorded_as_failed_and_raised(monkeypatch):
    def body(trade_date, results, traces):
        results.append(("A", "ok"))
        raise ModuleNotFoundError("No module named 'scrapers.gone'")

    rec = _Record(monkeypatch, body)
    with pytest.raises(ModuleNotFoundError):
        du.update_date(WEEKDAY)
    (run_id, trade_date, outcome, results, traces, summary_sent), = rec.finished
    assert (run_id, trade_date, outcome, summary_sent) == (7, WEEKDAY, "failed", None)
    # What finished before the exception is kept.
    assert results == [("A", "ok"), (du.UNHANDLED_STEP, "failed")]
    assert traces[du.UNHANDLED_STEP].endswith(
        "ModuleNotFoundError: No module named 'scrapers.gone'")


def test_interrupted_run_is_recorded_too(monkeypatch):
    # Ctrl+C on a manual re-run must not leave 'running' as the newest record.
    def body(trade_date, results, traces):
        raise KeyboardInterrupt

    rec = _Record(monkeypatch, body)
    with pytest.raises(KeyboardInterrupt):
        du.update_date(WEEKDAY)
    assert [args[2] for args in rec.finished] == ["failed"]


def test_record_that_cannot_be_opened_does_not_stop_the_run(monkeypatch):
    rec = _Record(monkeypatch, lambda d, results, traces: ("success", True), run_id=None)
    assert du.update_date(WEEKDAY) == "success"
    assert rec.finished == [(None, WEEKDAY, "success", [], {}, True)]


# ── the record writers ─────────────────────────────────────────────────────

class _Db:
    def __init__(self, monkeypatch, error=None):
        self.executed = []
        self.committed = 0
        db = self

        class Cursor:
            def execute(self, sql, params=None):
                if error:
                    raise error
                db.executed.append((sql, params))

            def fetchone(self):
                return {"id": 42}

        @contextmanager
        def get_cursor(commit=True):
            yield Cursor()
            db.committed += commit

        monkeypatch.setattr(du, "get_cursor", get_cursor)


def test_run_start_returns_the_new_id(monkeypatch):
    db = _Db(monkeypatch)
    assert du._run_start(WEEKDAY) == 42
    assert db.executed == [(du._RUN_START_SQL, (WEEKDAY,))]
    assert db.committed == 1


def test_run_start_survives_a_dead_database(monkeypatch, capsys):
    _Db(monkeypatch, error=RuntimeError("connection refused"))
    assert du._run_start(WEEKDAY) is None
    assert "run record not opened" in capsys.readouterr().out


def test_run_finish_updates_the_opening_row(monkeypatch):
    db = _Db(monkeypatch)
    du._run_finish(42, WEEKDAY, "partial", [("A", "failed")], {"A": "boom"}, False)
    (sql, params), = db.executed
    assert sql == du._RUN_FINISH_SQL
    assert params[:2] == ("partial", False) and params[3] == 42
    assert json.loads(params[2]) == [
        {"label": "A", "status": "failed", "core": False, "trace": "boom"}]
    assert db.committed == 1


def test_run_finish_inserts_when_there_is_no_opening_row(monkeypatch):
    db = _Db(monkeypatch)
    du._run_finish(None, WEEKDAY, "success", [], {}, True)
    (sql, params), = db.executed
    assert sql == du._RUN_INSERT_SQL
    assert params == (WEEKDAY, "success", True, "[]")


def test_run_finish_survives_a_dead_database(monkeypatch, capsys):
    _Db(monkeypatch, error=RuntimeError("connection refused"))
    du._run_finish(42, WEEKDAY, "success", [], {}, True)      # must not raise
    assert "run record not written" in capsys.readouterr().out


def test_steps_json():
    results = [("TWSE daily prices", "failed"), ("月營收", "skip"), (du.UNHANDLED_STEP, "failed")]
    traces = {"TWSE daily prices": "ValueError: 中文\nNUL \x00 here", du.UNHANDLED_STEP: "boom"}
    text = du._steps_json(results, traces)
    assert "\x00" not in text and "\\u0000" not in text
    assert "中文" in text                                     # not \\u-escaped
    assert json.loads(text) == [
        {"label": "TWSE daily prices", "status": "failed", "core": True,
         "trace": "ValueError: 中文\nNUL  here"},
        {"label": "月營收", "status": "skip", "core": False, "trace": None},
        {"label": du.UNHANDLED_STEP, "status": "failed", "core": True, "trace": "boom"},
    ]


# ── the run log ────────────────────────────────────────────────────────────

@pytest.fixture
def log_root(monkeypatch, tmp_path):
    # Unfrozen, the log goes next to daily_update.py; point that at tmp_path.
    monkeypatch.setattr(du, "__file__", str(tmp_path / "daily_update.py"))
    monkeypatch.setattr(du, "_open_run_log", real_open_run_log)
    return tmp_path / "logs" / "daily_update"


def _only_log(folder):
    (path,) = list(folder.glob("*.log"))
    return path.read_text(encoding="utf-8")


def test_log_mirrors_stdout_and_stderr(monkeypatch, log_root):
    out, err = io.StringIO(), io.StringIO()
    monkeypatch.setattr(sys, "stdout", out)
    monkeypatch.setattr(sys, "stderr", err)
    restore = du._open_run_log()
    print("to stdout 中文")
    print("to stderr", file=sys.stderr)
    restore()
    assert sys.stdout is out and sys.stderr is err
    print("after restore")
    assert out.getvalue() == "to stdout 中文\nafter restore\n"
    assert err.getvalue() == "to stderr\n"
    assert _only_log(log_root) == "to stdout 中文\nto stderr\n"


def test_log_error_does_not_reach_the_pipeline(monkeypatch, log_root):
    out = io.StringIO()
    monkeypatch.setattr(sys, "stdout", out)
    restore = du._open_run_log()
    print("before")

    class Full:
        def write(self, text):
            raise OSError(28, "No space left on device")

        def close(self):
            pass

    sys.stdout._log.file.close()
    sys.stdout._log.file = Full()
    print("during")                                           # must not raise
    print("after")
    restore()
    assert out.getvalue() == "before\nduring\nafter\n"


def test_log_works_without_a_console(monkeypatch, log_root):
    monkeypatch.setattr(sys, "stdout", None)
    restore = du._open_run_log()
    sys.stdout.write("no console\n")
    sys.stdout.flush()
    restore()
    assert sys.stdout is None
    assert _only_log(log_root) == "no console\n"


def test_log_keeps_the_line_the_console_could_not_print(monkeypatch, log_root):
    class Cp950Console(io.StringIO):
        def write(self, text):
            text.encode("cp950")                              # strict, like a legacy console
            return super().write(text)

    monkeypatch.setattr(sys, "stdout", Cp950Console())
    restore = du._open_run_log()
    print("before")
    with pytest.raises(UnicodeEncodeError):
        print("score \u2265 80 \u2713")
    print("after")
    restore()
    assert _only_log(log_root) == "before\nscore \u2265 80 \u2713after\n"


def test_restore_leaves_a_stream_someone_else_replaced(monkeypatch, log_root):
    monkeypatch.setattr(sys, "stdout", io.StringIO())
    restore = du._open_run_log()
    theirs = io.StringIO()
    sys.stdout = theirs
    restore()
    assert sys.stdout is theirs


def test_log_that_cannot_be_opened_leaves_stdout_alone(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(du, "__file__", str(tmp_path / "daily_update.py"))
    monkeypatch.setattr(du, "_open_run_log", real_open_run_log)
    (tmp_path / "logs").write_text("a file where the folder should be")
    before = sys.stdout
    restore = du._open_run_log()
    assert sys.stdout is before
    restore()
    assert "run log not opened" in capsys.readouterr().out


def test_tee_reports_the_original_streams_properties(monkeypatch, log_root):
    out = io.StringIO()
    monkeypatch.setattr(sys, "stdout", out)
    restore = du._open_run_log()
    assert sys.stdout.isatty() is False
    restore()


# ── _main ──────────────────────────────────────────────────────────────────

@pytest.fixture
def main(monkeypatch, log_root):
    monkeypatch.setattr(db.connection, "init_db", lambda: None)
    return log_root


@pytest.mark.parametrize("outcome, code", [("success", 0), ("partial", 2), ("failed", 1)])
def test_main_turns_the_outcome_into_the_exit_code(monkeypatch, main, outcome, code):
    monkeypatch.setattr(du, "update_date", lambda d: outcome)
    assert du._main(["2026-10-14"]) == code
    # The log ends by saying what the run amounted to.
    assert _only_log(main).endswith(f"Outcome: {outcome} (exit code {code})\n")


def test_main_restores_the_streams_before_returning(monkeypatch, main):
    # pause_before_exit() runs next; see _open_run_log for why this matters.
    out = io.StringIO()
    monkeypatch.setattr(sys, "stdout", out)
    seen = []
    monkeypatch.setattr(du, "update_date", lambda d: seen.append(sys.stdout) or "success")
    du._main(["2026-10-14"])
    assert seen[0] is not out                                 # mirrored during the run
    assert sys.stdout is out


def test_main_logs_and_survives_an_exception(monkeypatch, main):
    out = io.StringIO()
    monkeypatch.setattr(sys, "stdout", out)
    monkeypatch.setattr(sys, "stderr", io.StringIO())

    def boom(d):
        raise RuntimeError("pipeline blew up")

    monkeypatch.setattr(du, "update_date", boom)
    assert du._main(["2026-10-14"]) == 1
    assert sys.stdout is out
    log = _only_log(main)
    assert "[ERROR] pipeline blew up" in log and "RuntimeError: pipeline blew up" in log


def test_main_with_a_dead_database_exits_1_and_logs(monkeypatch, main):
    def refused():
        raise RuntimeError("connection refused")

    monkeypatch.setattr(db.connection, "init_db", refused)
    monkeypatch.setattr(sys, "stderr", io.StringIO())
    assert du._main([]) == 1
    assert "connection refused" in _only_log(main)


def test_main_dispatches_on_the_arguments(monkeypatch, main):
    calls = []
    monkeypatch.setattr(du, "update_date", lambda d: calls.append(("date", d)) or "success")
    monkeypatch.setattr(du, "update_range", lambda a, b: calls.append(("range", a, b)) or "partial")
    assert du._main(["2026-10-14"]) == 0
    assert du._main(["2026-10-12", "2026-10-14"]) == 2
    assert du._main(["a", "b", "c"]) == 1
    assert calls == [("date", date(2026, 10, 14)),
                     ("range", date(2026, 10, 12), date(2026, 10, 14))]
