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
from utils.format_shift import ScrapeResult

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
                  "_git_push_frontend", "send_sync", "_revenue_counts", "_stance_gaps"}
# ... and the ones that are safe to run as they are.
PURE_CALLS = {"_capture_trace", "_build_daily_update_message", "_run_outcome", "Path",
              "_revenue_months", "_revenue_verdict", "_missing_funds_text",
              "_result_problem"}

RETURNS = {
    "strategies.registry.scan_daily": [],
    "signals.etf_multi_exit.scan": [],
    "signals.etf_consecutive_reduction.scan": [],
    "signals.etf_abnormal_exit.scan": [],
    "analysis.market_breadth.calculate_market_breadth": [],
    "analysis.market_breadth.save_market_breadth": 0,
    "analysis.daily_liquidity.compute_daily_liquidity": 2200,
    # The next-day disposal pull: all four legs answered.
    "scrapers.stock_alerts.scrape_date": ScrapeResult(24, 24, 0),
    # A normal night: both markets published, every fund got, messages sent.
    "scrapers.revenue.scrape_month": {"sii": ("ok", 987), "otc": ("ok", 864)},
    "scrapers.sitca.scrape_monthly": [],
    "scrapers.sitca.scrape_quarterly": [],
    "telegram_bot.push_daily_signals.push_daily_signals": 0,
    "telegram_bot.push_conference_calendar.push_conference_calendar": 0,
    # Companies on record per market, for both months the revenue step reads.
    "_revenue_counts": {"sii": 980, "otc": 850},
    # Every stance input there for the last ten trading days.
    "_stance_gaps": [],
    # Stocks audited, false positives, false negatives.
    "analysis.disposal_audit.run_audit": (500, 0, 0),
    # What the body reads as the latest tw.fund_holdings_monthly period.
    "fund_latest_period": "202609",
    # Whether any ETF holdings diff is stored in tw.etf_holdings_diff for the night.
    "etf_diff_stored": True,
    # The snapshot carried every position held the night before.
    "analysis.daily_snapshot.run": {"position_counts": {"revisions": 0, "exited_unseen": 0}},
}


class _Cursor:
    """The body's reads: the latest fund-holdings period, and whether any ETF
    holdings diff is stored for the night."""

    def __init__(self, latest_period, etf_stored, day):
        self.latest_period = latest_period
        self.etf_stored = etf_stored
        self.day = day
        self.sql = ""

    def execute(self, sql, params=None):
        # The body only reads through its own cursors; a write added there
        # must not pass unnoticed.
        assert sql.lstrip().upper().startswith("SELECT"), sql
        assert "fund_holdings_monthly" in sql or "tw.etf_holdings_diff " in sql, sql
        if "tw.etf_holdings_diff " in sql:
            # The night's own date, not the latest stored one.
            assert "trade_date = %s" in sql and params == (self.day,), (sql, params)
        self.sql = sql

    def fetchone(self):
        if "tw.etf_holdings_diff " in self.sql:
            return {"?column?": 1} if self.etf_stored else None
        return {"max": self.latest_period}

    def fetchall(self):
        return []


class _Night:
    """One run of the body. fail: stand-in names ("analysis.daily_snapshot.run")
    or scraper labels that fail."""

    def __init__(self, monkeypatch, fail=(), send=True, through_update_date=False,
                 returns=None, day=DAY, today=None):
        self.calls = []
        self.sent = []
        self.subprocesses = []
        fail = set(fail)
        values = {**RETURNS, **(returns or {})}

        def stand_in(name):
            def _call(*args, **kwargs):
                self.calls.append(name)
                if name in fail:
                    raise RuntimeError(f"{name} failed")
                value = values.get(name)
                return value(*args) if callable(value) else value
            return _call

        for module, names in STAND_INS.items():
            attrs = {n: stand_in(f"{module}.{n}") for n in names}
            if "PeriodNotAvailable" in attrs:
                attrs["PeriodNotAvailable"] = type("PeriodNotAvailable", (Exception,), {})
            monkeypatch.setitem(sys.modules, module, types.SimpleNamespace(**attrs))

        self.scraper_flags = {}

        def run_scraper(label, module_path, func_name, trade_date, expect_rows=False,
                        recent=None):
            self.calls.append(f"scraper:{label}")
            self.scraper_flags[label] = (expect_rows, recent)
            return (False, f"{label} failed") if label in fail else (True, None)

        def send_sync(text):
            self.sent.append(text)
            if isinstance(send, Exception):
                raise send
            return send

        @contextmanager
        def get_cursor(commit=True):
            yield _Cursor(values["fund_latest_period"], values["etf_diff_stored"], day)

        # The night runs on its own trade date, as the 21:30 task does, so the
        # checks reserved for recent dates apply however old DAY gets; a later
        # today= makes it a backfill.
        class _Date(date):
            @classmethod
            def today(cls):
                return today or day

        monkeypatch.setattr(du, "date", _Date)
        monkeypatch.setattr(du, "_trading_day_gate", lambda d: ("run", True, None))
        monkeypatch.setattr(du, "run_scraper", run_scraper)
        monkeypatch.setattr(du, "get_cursor", get_cursor)
        monkeypatch.setattr(du, "detect_delisted", stand_in("detect_delisted"))
        monkeypatch.setattr(du, "_git_push_frontend", stand_in("_git_push_frontend"))
        monkeypatch.setattr(du, "_revenue_counts", stand_in("_revenue_counts"))
        monkeypatch.setattr(du, "_stance_gaps", stand_in("_stance_gaps"))
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
            self.outcome = du.update_date(day)
            (_, _, self.recorded_outcome, self.results, self.traces, self.summary_sent), = finished
        else:
            self.results = []
            self.traces = {}
            self.outcome, self.summary_sent = du._update_trading_day(day, self.results, self.traces)
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


def test_revised_positions_are_reported_in_the_summary(monkeypatch):
    night = _Night(monkeypatch, returns={
        "analysis.daily_snapshot.run": {"position_counts": {"revisions": 3, "exited_unseen": 0}}})
    assert night.sent[0].splitlines()[1:] == ["持倉重算改寫 3 筆（見 tw.position_revisions）"]
    assert "全部成功" in night.sent[0]


def test_exits_a_missing_night_hid_are_reported_apart(monkeypatch):
    night = _Night(monkeypatch, returns={
        "analysis.daily_snapshot.run": {"position_counts": {"revisions": 0, "exited_unseen": 2}}})
    assert night.sent[0].splitlines()[1:] == ["快照缺日內出場 2 筆（見 tw.position_revisions）"]


def test_no_revision_line_when_nothing_was_revised(monkeypatch):
    night = _Night(monkeypatch)
    assert len(night.sent[0].splitlines()) == 1 and "持倉重算改寫" not in night.sent[0]


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
        "市場廣度", "每日流動性", "市場波動基準", "攻防資料", SNAPSHOT, "前端匯出部署",
        "日線訊號推播"}
    assert "_stance_gaps" not in night.calls
    assert "analysis.daily_snapshot.run" not in night.calls
    assert "export.generate.export_all" not in night.calls
    assert night.outcome == "failed"
    assert night.with_status("skip") == set()


@pytest.mark.parametrize("broken, label, also_blocked", [
    # A night without breadth has no stance row for that reason alone; the
    # stance check stands down rather than report it a second time.
    ("analysis.market_breadth.calculate_market_breadth", "市場廣度", {"攻防資料"}),
    ("analysis.build_market_vol.main", "市場波動基準", set()),
])
def test_failed_snapshot_input_blocks_snapshot_and_export(monkeypatch, broken, label,
                                                          also_blocked):
    night = _Night(monkeypatch, fail={broken})
    assert night.with_status("failed") == {label}
    assert night.with_status("blocked") == {SNAPSHOT, "前端匯出部署", "日線訊號推播"} | also_blocked
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


# ── steps judged on what they returned ─────────────────────────────────────

def test_revenue_market_without_an_answer_fails_the_step(monkeypatch):
    # DAY is the 14th: past the filing deadline, the companies on record decide.
    night = _Night(monkeypatch, returns={
        "scrapers.revenue.scrape_month": {"sii": ("ok", 987), "otc": ("no_response", 0)},
        "_revenue_counts": lambda ym: {"sii": 980, "otc": 0 if ym == "2026-09" else 850},
    })
    assert night.status["月營收"] == "failed"
    assert night.traces["月營收"] == (
        "上櫃 2026-09 只有 0 家（2026-08 有 850 家；這次：沒有回應或 HTTP 錯誤）")
    assert night.outcome == "partial"


def test_revenue_market_without_an_answer_is_fine_once_on_record(monkeypatch):
    night = _Night(monkeypatch, returns={
        "scrapers.revenue.scrape_month": {"sii": ("ok", 987), "otc": ("no_response", 0)},
    })
    assert night.status["月營收"] == "ok" and night.outcome == "success"


def test_revenue_reads_the_fetched_month_and_the_one_before(monkeypatch):
    asked = []
    _Night(monkeypatch, returns={
        "_revenue_counts": lambda ym: asked.append(ym) or {"sii": 980, "otc": 850}})
    assert asked == ["2026-09", "2026-08"]


def test_unreadable_revenue_page_fails_the_step(monkeypatch):
    night = _Night(monkeypatch, returns={
        "scrapers.revenue.scrape_month": {"sii": ("unreadable", 0), "otc": ("ok", 864)}})
    assert night.status["月營收"] == "failed"
    assert night.traces["月營收"] == "上市頁面讀不出資料"


@pytest.mark.parametrize("name, label", [
    ("scrapers.sitca.scrape_monthly", "SITCA 月持股"),
    ("scrapers.sitca.scrape_quarterly", "SITCA 季持股"),
])
def test_sitca_with_missing_funds_fails_the_step(monkeypatch, name, label):
    missing = ["nomura-quality：A0032 查詢失敗：ReadTimeout",
               "nomura-hightech：A0032 查詢失敗：ReadTimeout"]
    night = _Night(monkeypatch, returns={name: missing})
    assert night.status[label] == "failed"
    assert night.traces[label] == "\n".join(missing)
    assert night.outcome == "partial"


def test_long_sitca_reason_is_kept_to_three_lines(monkeypatch):
    # The summary is not split; past 4096 characters Telegram drops it whole.
    missing = [f"fund{i}：A00{i} 查詢失敗：ReadTimeout: read timed out" for i in range(13)]
    night = _Night(monkeypatch, returns={"scrapers.sitca.scrape_monthly": missing})
    assert night.traces["SITCA 月持股"].splitlines() == [
        "13 檔缺：" + ", ".join(f"fund{i}" for i in range(13)),
        missing[0],
        "（完整原因見 log）",
    ]


def test_revenue_before_the_deadline_does_not_query_the_counts(monkeypatch):
    asked = []
    night = _Night(monkeypatch, day=date(2026, 10, 5), returns={
        "_revenue_counts": lambda ym: asked.append(ym) or {"sii": 980, "otc": 850}})
    assert asked == [] and night.status["月營收"] == "ok"


@pytest.mark.parametrize("name, label", [
    ("telegram_bot.push_daily_signals.push_daily_signals", "日線訊號推播"),
    ("telegram_bot.push_conference_calendar.push_conference_calendar", "法說會預告推播"),
])
def test_push_that_was_not_delivered_fails_the_step(monkeypatch, name, label):
    night = _Night(monkeypatch, returns={name: 2})
    assert night.status[label] == "failed"
    assert night.traces[label] == "Telegram 傳送失敗"
    assert night.outcome == "partial"


def test_audit_of_no_stock_fails_the_step(monkeypatch):
    night = _Night(monkeypatch, returns={"analysis.disposal_audit.run_audit": (0, 0, 0)})
    assert night.status["處置預測 audit"] == "failed"
    assert night.traces["處置預測 audit"].startswith("稽核 0 檔")
    assert night.outcome == "partial"


def test_audit_of_no_stock_is_not_judged_on_a_backfill_date(monkeypatch):
    night = _Night(monkeypatch, returns={"analysis.disposal_audit.run_audit": (0, 0, 0)},
                   today=DAY.replace(year=DAY.year + 1))
    assert night.status["處置預測 audit"] == "ok"


def test_fund_scan_without_any_period_fails_the_step(monkeypatch):
    # Used to skip the scan and record "ok".
    night = _Night(monkeypatch, returns={"fund_latest_period": None})
    assert night.status["基金信號掃描"] == "failed"
    assert night.traces["基金信號掃描"] == "基金持股表沒有任何期別"
    assert "scan_signals.scan_period" not in night.calls
    assert night.outcome == "partial"


def test_fund_scan_runs_on_the_latest_period(monkeypatch):
    assert _Night(monkeypatch).status["基金信號掃描"] == "ok"


def test_fund_scan_after_the_20th_is_a_skip(monkeypatch):
    night = _Night(monkeypatch, returns={"fund_latest_period": None},
                   day=date(2026, 10, 21))
    assert night.status["基金信號掃描"] == "skip"


ETF_SCANS = ("strategies.registry.scan_daily", "signals.etf_multi_exit.scan",
             "signals.etf_consecutive_reduction.scan", "signals.etf_abnormal_exit.scan")


def test_etf_scan_without_a_diff_for_the_day_is_blocked(monkeypatch):
    # Used to rescan the last diff days and record "ok".
    night = _Night(monkeypatch, returns={"etf_diff_stored": False})
    assert night.status["ETF 信號掃描"] == "blocked"
    assert not set(ETF_SCANS) & set(night.calls)
    assert night.outcome == "partial"


def test_etf_scan_runs_when_any_fund_has_a_diff(monkeypatch):
    night = _Night(monkeypatch)
    assert night.status["ETF 信號掃描"] == "ok"
    assert set(ETF_SCANS) <= set(night.calls)


def test_etf_scan_runs_when_only_some_funds_failed(monkeypatch):
    # Ruling: only a day with nothing stored is blocked; the holdings step
    # reports the funds that failed.
    night = _Night(monkeypatch, fail={"ETF holdings"})
    assert night.status["ETF holdings"] == "failed"
    assert night.status["ETF 信號掃描"] == "ok"
    assert set(ETF_SCANS) <= set(night.calls)


GAP = "10-13 缺：期交所外資期貨；10-14 的 buy／sell 攻防閘門沒有依據。補跑：daily_update.exe 2026-10-13"


def test_stance_gap_fails_the_night_and_holds_nothing_back(monkeypatch):
    night = _Night(monkeypatch, returns={"_stance_gaps": [GAP]})
    assert night.status["攻防資料"] == "failed"
    assert night.traces["攻防資料"] == GAP
    assert "攻防資料" in du.CORE_STEP_LABELS and night.outcome == "failed"
    # Tonight's signals read T-1's stance; the rest of the night runs.
    assert night.status[SNAPSHOT] == "ok" and night.status["前端匯出部署"] == "ok"
    assert night.status["日線訊號推播"] == "ok"


def test_stance_check_that_raises_fails_the_step(monkeypatch):
    night = _Night(monkeypatch, fail={"_stance_gaps"})
    assert night.status["攻防資料"] == "failed" and "攻防資料" in night.traces


def test_stance_check_is_left_to_recent_nights(monkeypatch):
    # A backfilled date's ten-day window is history, not tonight's problem.
    night = _Night(monkeypatch, returns={"_stance_gaps": [GAP]},
                   today=DAY.replace(year=DAY.year + 1))
    assert night.status["攻防資料"] == "skip" and "_stance_gaps" not in night.calls


def test_every_scraper_is_judged_on_its_failed_legs_on_a_recent_night(monkeypatch):
    # Event sources expect no rows but are still judged on legs that did not
    # answer; that only happens if the loop passes recent through.
    night = _Night(monkeypatch)
    assert night.scraper_flags == {label: (expect, True)
                                   for label, _, _, expect in du.SCRAPERS}


def test_liquidity_without_rows_fails_the_step(monkeypatch):
    night = _Night(monkeypatch, returns={"analysis.daily_liquidity.compute_daily_liquidity": 0})
    assert night.status["每日流動性"] == "failed"
    assert night.traces["每日流動性"] == "流動性 0 筆"
    # A core step since step 4a: the snapshot reads it.
    assert "每日流動性" in du.CORE_STEP_LABELS and night.outcome == "failed"


def test_next_day_disposal_leg_that_did_not_answer_fails_the_step(monkeypatch):
    # Used to be recorded "ok" whatever came back: a dead leg quietly reverts
    # positions.json's disposal_status to the prediction.
    night = _Night(monkeypatch, returns={"scrapers.stock_alerts.scrape_date":
                                         ScrapeResult(9, 9, 0, ("上櫃處置股（沒有回應）",))})
    assert night.status["明日處置名單抓取"] == "failed"
    assert "上櫃處置股（沒有回應）" in night.traces["明日處置名單抓取"]
    assert night.outcome == "partial"


def test_next_day_disposal_that_raises_fails_the_step(monkeypatch):
    night = _Night(monkeypatch, fail={"scrapers.stock_alerts.scrape_date"})
    assert night.status["明日處置名單抓取"] == "failed"
    assert "明日處置名單抓取" in night.traces


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
