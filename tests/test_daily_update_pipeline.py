"""Drives the whole nightly pipeline body with every collaborator replaced.

What a step's failure does to the steps behind it used to be untested: a
snapshot that failed still let the frontend export run, and a step held back
by an upstream failure was recorded as "skip", the same as one that was not
due. The body imports its collaborators inside the function, so each one is
replaced in sys.modules; a guard test keeps that list equal to what the body
really imports and calls.

No database, no network, no git, no Telegram, no subprocess.
"""
import ast
import inspect
import sys
import textwrap
import types
from contextlib import contextmanager
from datetime import date

import pytest

import daily_update as du
from daily_update_guards import no_side_effects  # noqa: F401

# A Wednesday inside every monthly and quarterly window, so no step is skipped.
DAY = date(2026, 10, 14)

SNAPSHOT = "多空評比 + 操作訊號 + 策略持倉快照"

# module -> the names the body imports from it
STAND_INS = {
    "scrapers.revenue": ["scrape_month"],
    "scrapers.sitca": ["scrape_monthly", "scrape_quarterly", "PeriodNotAvailable"],
    "scan_signals": ["scan_period"],
    "strategies.registry": ["scan_daily", "save_signals"],
    "signals.etf_multi_exit": ["scan"],
    "signals.etf_consecutive_reduction": ["scan"],
    "signals.etf_abnormal_exit": ["scan"],
    "analysis.market_breadth": ["calculate_market_breadth", "save_market_breadth"],
    "analysis.daily_liquidity": ["compute_daily_liquidity"],
    "analysis.build_market_vol": ["main"],
    "analysis.daily_snapshot": ["run"],
    "scrapers.stock_alerts": ["scrape_date"],
    "telegram_bot.handlers.score": ["_next_trading_day"],
    "export.generate": ["export_all"],
    "analysis.disposal_audit": ["run_audit"],
    "telegram_bot.push_daily_signals": ["push_daily_signals"],
    "telegram_bot.push_conference_calendar": ["push_conference_calendar"],
}
# Standard-library modules the body imports; subprocess.run is replaced below.
STDLIB_IMPORTS = {"os", "subprocess"}
# Module-level functions of daily_update the body calls and this file replaces ...
REPLACED_CALLS = {"_trading_day_gate", "run_scraper", "get_cursor", "detect_delisted",
                  "_git_push_frontend", "send_sync"}
# ... and the ones that are safe to run as they are.
PURE_CALLS = {"_capture_trace", "_build_daily_update_message", "_run_outcome", "Path"}

RETURNS = {
    "strategies.registry.scan_daily": [],
    "signals.etf_multi_exit.scan": [],
    "signals.etf_consecutive_reduction.scan": [],
    "signals.etf_abnormal_exit.scan": [],
    "analysis.market_breadth.calculate_market_breadth": [],
    "analysis.market_breadth.save_market_breadth": 0,
    "analysis.daily_liquidity.compute_daily_liquidity": 0,
}


class _Cursor:
    def execute(self, sql, params=None):
        # The body only reads through its own cursors; a write added there
        # must not pass unnoticed.
        assert sql.lstrip().upper().startswith("SELECT"), sql

    def fetchone(self):
        return {"max": None}

    def fetchall(self):
        return []


class _Night:
    """One run of the body. fail: stand-in names ("analysis.daily_snapshot.run")
    or scraper labels that fail."""

    def __init__(self, monkeypatch, fail=(), send=True, through_update_date=False):
        self.calls = []
        self.sent = []
        self.subprocesses = []
        fail = set(fail)

        def stand_in(name):
            def _call(*args, **kwargs):
                self.calls.append(name)
                if name in fail:
                    raise RuntimeError(f"{name} failed")
                return RETURNS.get(name)
            return _call

        for module, names in STAND_INS.items():
            attrs = {n: stand_in(f"{module}.{n}") for n in names}
            if "PeriodNotAvailable" in attrs:
                attrs["PeriodNotAvailable"] = type("PeriodNotAvailable", (Exception,), {})
            monkeypatch.setitem(sys.modules, module, types.SimpleNamespace(**attrs))

        def run_scraper(label, module_path, func_name, trade_date, expect_rows=False):
            self.calls.append(f"scraper:{label}")
            return (False, f"{label} failed") if label in fail else (True, None)

        def send_sync(text):
            self.sent.append(text)
            if isinstance(send, Exception):
                raise send
            return send

        @contextmanager
        def get_cursor(commit=True):
            yield _Cursor()

        monkeypatch.setattr(du, "_trading_day_gate", lambda d: ("run", True, None))
        monkeypatch.setattr(du, "run_scraper", run_scraper)
        monkeypatch.setattr(du, "get_cursor", get_cursor)
        monkeypatch.setattr(du, "detect_delisted", stand_in("detect_delisted"))
        monkeypatch.setattr(du, "_git_push_frontend", stand_in("_git_push_frontend"))
        monkeypatch.setattr(du, "send_sync", send_sync)
        def subprocess_run(argv, **kwargs):
            self.subprocesses.append(argv)
            return types.SimpleNamespace(returncode=0, stdout="", stderr="")

        import subprocess
        monkeypatch.setattr(subprocess, "run", subprocess_run)

        if through_update_date:
            finished = []
            monkeypatch.setattr(du, "_run_start", lambda d: 1)
            monkeypatch.setattr(du, "_run_finish", lambda *args: finished.append(args))
            self.outcome = du.update_date(DAY)
            (_, _, self.recorded_outcome, self.results, self.traces, self.summary_sent), = finished
        else:
            self.results = []
            self.traces = {}
            self.outcome, self.summary_sent = du._update_trading_day(DAY, self.results, self.traces)
        self.status = dict(self.results)
        # The only subprocess of the night is the hermit snapshot.
        assert [argv[:2] for argv in self.subprocesses] == [["uv", "run"]]

    def with_status(self, status):
        return {label for label, s in self.results if s == status}


# ── the stand-in list matches the body ─────────────────────────────────────

def _body():
    return ast.parse(textwrap.dedent(inspect.getsource(du._update_trading_day)))


def test_every_import_in_the_body_has_a_stand_in():
    # A new import without a stand-in would run for real inside these tests.
    imported = set()
    for node in ast.walk(_body()):
        if isinstance(node, ast.ImportFrom):
            imported.add(node.module)
        elif isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
    assert imported == set(STAND_INS) | STDLIB_IMPORTS


def test_every_imported_name_has_a_stand_in():
    for node in ast.walk(_body()):
        if isinstance(node, ast.ImportFrom):
            assert {alias.name for alias in node.names} <= set(STAND_INS[node.module])


def test_every_module_level_call_in_the_body_is_accounted_for():
    called = {node.func.id for node in ast.walk(_body())
              if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
              and node.func.id in vars(du)}
    assert called == REPLACED_CALLS | PURE_CALLS


def test_every_method_call_on_a_process_or_file_handle_is_accounted_for():
    # Attribute calls are invisible to the test above: `_sp.run(["git", ...])`
    # or `Path(...).unlink()` would slip through it.
    risky = {"_sp", "_os", "subprocess", "os", "sys", "Path", "shutil"}
    called = set()
    for node in ast.walk(_body()):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            base = node.func.value
            while isinstance(base, (ast.Attribute, ast.Call)):
                base = base.func if isinstance(base, ast.Call) else base.value
            if isinstance(base, ast.Name) and base.id in risky:
                called.add(f"{base.id}.{node.func.attr}")
    assert called == {"_sp.run"}


# ── a clean night ──────────────────────────────────────────────────────────

def test_clean_night(monkeypatch):
    night = _Night(monkeypatch)
    assert (night.outcome, night.summary_sent) == ("success", True)
    assert night.with_status("failed") == set() and night.with_status("blocked") == set()
    assert night.with_status("skip") == set()
    assert "export.generate.export_all" in night.calls and "_git_push_frontend" in night.calls
    assert "telegram_bot.push_daily_signals.push_daily_signals" in night.calls
    assert len(night.sent) == 1 and "全部成功" in night.sent[0]


def test_every_core_label_is_a_real_step(monkeypatch):
    # A core label no step produces would demote that step's failure to partial.
    night = _Night(monkeypatch)
    assert du.CORE_STEP_LABELS <= set(night.status)


def test_every_step_is_recorded_once(monkeypatch):
    night = _Night(monkeypatch)
    labels = [label for label, _ in night.results]
    assert len(labels) == len(set(labels))
    assert {entry[0] for entry in du.SCRAPERS} <= set(labels)


def test_update_date_carries_the_night_into_the_record(monkeypatch):
    # The real wrapper around the real body, end to end.
    night = _Night(monkeypatch, fail={"scrapers.revenue.scrape_month"}, send=False,
                   through_update_date=True)
    assert night.outcome == night.recorded_outcome == "partial"
    assert night.summary_sent is False
    assert night.status["月營收"] == "failed" and "月營收" in night.traces
    assert du.CORE_STEP_LABELS <= set(night.status)


# ── what a failure does downstream ─────────────────────────────────────────

def test_failed_snapshot_holds_back_the_export_and_the_signal_push(monkeypatch):
    night = _Night(monkeypatch, fail={"analysis.daily_snapshot.run"})
    assert night.status[SNAPSHOT] == "failed"
    assert night.with_status("blocked") == {"前端匯出部署", "日線訊號推播"}
    assert "export.generate.export_all" not in night.calls
    assert "_git_push_frontend" not in night.calls
    assert "telegram_bot.push_daily_signals.push_daily_signals" not in night.calls
    assert night.outcome == "failed"
    # Steps that do not depend on the snapshot still run.
    assert night.status["處置預測 audit"] == "ok" and night.status["法說會預告推播"] == "ok"
    assert "因上游失敗未執行：前端匯出部署, 日線訊號推播" in night.sent[0]
    assert "全部成功" not in night.sent[0]


def test_failed_price_scraper_blocks_the_whole_tail(monkeypatch):
    night = _Night(monkeypatch, fail={"TWSE daily prices"})
    assert night.with_status("failed") == {"TWSE daily prices"}
    assert night.with_status("blocked") == {
        "市場廣度", "每日流動性", "市場波動基準", SNAPSHOT, "前端匯出部署", "日線訊號推播"}
    assert "analysis.daily_snapshot.run" not in night.calls
    assert "export.generate.export_all" not in night.calls
    assert night.outcome == "failed"
    assert night.with_status("skip") == set()


@pytest.mark.parametrize("broken, label", [
    ("analysis.market_breadth.calculate_market_breadth", "市場廣度"),
    ("analysis.build_market_vol.main", "市場波動基準"),
])
def test_failed_snapshot_input_blocks_snapshot_and_export(monkeypatch, broken, label):
    night = _Night(monkeypatch, fail={broken})
    assert night.with_status("failed") == {label}
    assert night.with_status("blocked") == {SNAPSHOT, "前端匯出部署", "日線訊號推播"}
    assert "analysis.daily_snapshot.run" not in night.calls
    assert night.outcome == "failed"


def test_failed_export_is_a_failed_night(monkeypatch):
    night = _Night(monkeypatch, fail={"_git_push_frontend"})
    assert night.with_status("failed") == {"前端匯出部署"}
    assert night.outcome == "failed"


@pytest.mark.parametrize("broken, label", [
    ("telegram_bot.push_conference_calendar.push_conference_calendar", "法說會預告推播"),
    ("scrapers.revenue.scrape_month", "月營收"),
    ("Margin trading", "Margin trading"),
])
def test_non_core_failure_is_a_partial_night(monkeypatch, broken, label):
    assert label in {entry[0] for entry in du.SCRAPERS} | {"法說會預告推播", "月營收"}
    night = _Night(monkeypatch, fail={broken})
    assert night.with_status("failed") == {label}
    assert night.with_status("blocked") == set()
    assert night.outcome == "partial"
    assert "export.generate.export_all" in night.calls


# ── the summary's delivery ─────────────────────────────────────────────────

def test_undelivered_summary_is_recorded(monkeypatch):
    assert _Night(monkeypatch, send=False).summary_sent is False


def test_summary_that_raises_counts_as_undelivered(monkeypatch):
    night = _Night(monkeypatch, send=RuntimeError("telegram down"))
    assert night.summary_sent is False
    assert night.outcome == "success"


# ── the gate ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("gate, outcome", [("holiday", "holiday"), ("abort", "aborted")])
def test_gate_that_stops_the_run(monkeypatch, gate, outcome):
    def pipeline_reached(*a, **k):
        raise AssertionError("went past the gate")

    monkeypatch.setattr(du, "_trading_day_gate", lambda d: (gate, True, None))
    monkeypatch.setattr(du, "run_scraper", pipeline_reached)
    results, traces = [], {}
    assert du._update_trading_day(DAY, results, traces) == (outcome, None)
    assert results == []
