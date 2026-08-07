"""Tests for daily_update's silent-failure guards.

Every scraper reports through run_scraper, and for a long time it only noticed
failures that raised. Scrapers that answered HTTP 200 with no rows, or that
swallowed a request error into an empty list, were recorded as successes and
the telegram summary said "全部成功" while the frontend served stale data.

These cover the judgement (_result_problem), the control flow around it
(run_scraper), the registry that drives it, and the two remaining places in
update_date that used to fail quietly. No database, no network: the whole
point is that this layer is decidable without either.
"""
import importlib
from datetime import date
from types import SimpleNamespace

import pytest

import daily_update as du
from utils.format_shift import (
    SHIFT_ERROR_RATE, SHIFT_MIN_ROWS, ScrapeResult,
)

TRADE_DATE = date(2026, 8, 6)


# ── _result_problem ────────────────────────────────────────────────────────

def test_healthy_result_is_not_a_problem():
    assert du._result_problem(ScrapeResult(900, 900, 0), expect_rows=True) is None


def test_zero_rows_is_a_problem_when_rows_are_expected():
    problem = du._result_problem(ScrapeResult(0, 0, 0), expect_rows=True)
    assert problem is not None
    assert "0 筆" in problem


def test_zero_rows_is_fine_when_rows_are_not_expected():
    """The exemption that keeps nine scrapers from alerting every single day.

    Shareholder distribution, the insider feeds and treasury stock all carry a
    run-once-per-period guard, so 0 rows is their normal answer on most days.
    """
    assert du._result_problem(ScrapeResult(0, 0, 0), expect_rows=False) is None


def test_one_dead_leg_is_caught_even_though_records_is_large():
    """The failure mode a total-row-count check cannot see.

    margin, institutional, SBL, day-trading, price-limits and odd-lot sum
    every leg into records, so TPEx dying while TWSE succeeds still leaves
    records in the thousands.
    """
    problem = du._result_problem(
        ScrapeResult(1000, 1000, 0, ("TPEx",)), expect_rows=True
    )
    assert problem is not None
    assert "TPEx" in problem


def test_all_legs_alive_is_not_a_problem():
    assert du._result_problem(
        ScrapeResult(1800, 1800, 0, ()), expect_rows=True
    ) is None


def test_dead_leg_ignored_while_backfilling():
    """expect_rows is switched off for old dates, and legs go with it."""
    assert du._result_problem(
        ScrapeResult(1000, 1000, 0, ("TPEx",)), expect_rows=False
    ) is None


def test_format_shift_is_caught_regardless_of_row_count():
    problem = du._result_problem(
        ScrapeResult(59, 100, 41), expect_rows=True
    )
    assert problem is not None
    assert "格式改版" in problem


def test_error_rate_just_below_threshold_is_accepted():
    below = int(SHIFT_ERROR_RATE * 100) - 1
    assert du._result_problem(
        ScrapeResult(100 - below, 100, below), expect_rows=True
    ) is None


def test_small_samples_do_not_trip_the_shift_check():
    """A handful of unparseable rows is not evidence the API changed shape."""
    rows = SHIFT_MIN_ROWS - 1
    assert du._result_problem(
        ScrapeResult(0, rows, rows), expect_rows=False
    ) is None


def test_result_without_a_contract_is_exempt():
    """ETF holdings returns None; there is no row count to judge."""
    assert du._result_problem(None, expect_rows=True) is None


# ── SCRAPERS registry ──────────────────────────────────────────────────────

def test_every_registry_entry_declares_expect_rows():
    """The flag lives in the tuple so adding a scraper forces the decision.

    A separate set of labels would silently stop matching after a rename, and
    a new scraper would default to unmonitored.
    """
    for entry in du.SCRAPERS:
        assert len(entry) == 4, entry
        label, module_path, func_name, expect_rows = entry
        assert isinstance(expect_rows, bool), f"{label}: {expect_rows!r}"
        assert module_path.startswith("scrapers."), label


def test_critical_labels_all_exist_in_the_registry():
    """A typo here would silently disable the downstream gate."""
    labels = {e[0] for e in du.SCRAPERS}
    assert du.CRITICAL_SCRAPER_LABELS <= labels


@pytest.mark.parametrize("label", [
    "TWSE daily prices", "TPEx daily prices", "Margin trading",
    "Institutional investors", "Private placements",
])
def test_core_sources_are_monitored(label):
    """These carry a value every trading day; 0 rows means a broken scrape."""
    entry = next(e for e in du.SCRAPERS if e[0] == label)
    assert entry[3] is True


@pytest.mark.parametrize("label", [
    "Shareholder dist.", "Insider holdings", "Insider pledge events",
    "Treasury stock", "ETF holdings",
])
def test_periodic_and_contractless_sources_are_exempt(label):
    """Alerting on these would fire almost every day."""
    entry = next(e for e in du.SCRAPERS if e[0] == label)
    assert entry[3] is False


# ── run_scraper ────────────────────────────────────────────────────────────

def _stub_module(monkeypatch, behaviour):
    """Point importlib at a fake scraper and count how often it is called."""
    calls = []

    def scrape_date(trade_date):
        calls.append(trade_date)
        return behaviour()

    # run_scraper imports importlib inside the function body, so it picks up
    # whatever the real module exposes at call time -- patch that, not a
    # module attribute on daily_update.
    monkeypatch.setattr(
        importlib, "import_module",
        lambda path: SimpleNamespace(scrape_date=scrape_date),
    )
    monkeypatch.setattr(du, "SCRAPER_RETRY_WAIT", 0)
    return calls


def test_healthy_scraper_succeeds_on_the_first_call(monkeypatch):
    calls = _stub_module(monkeypatch, lambda: ScrapeResult(900, 900, 0))
    ok, trace = du.run_scraper("x", "scrapers.x", "scrape_date",
                               TRADE_DATE, expect_rows=True)
    assert (ok, trace, len(calls)) == (True, None, 1)


def test_soft_failure_is_not_retried(monkeypatch):
    """Retrying here would multiply into nine requests per leg.

    utils.http_client.fetch_json_retry already retries the same
    HTTP-200-but-invalid condition three times one level down, and the
    sources without that inner retry are the ones whose own comments say
    they fear being blocked.
    """
    calls = _stub_module(monkeypatch, lambda: ScrapeResult(0, 0, 0))
    ok, trace = du.run_scraper("x", "scrapers.x", "scrape_date",
                               TRADE_DATE, expect_rows=True)
    assert ok is False
    assert trace is not None
    assert len(calls) == 1


def test_format_shift_is_not_retried(monkeypatch):
    """A changed response shape parses identically however often it is asked."""
    calls = _stub_module(monkeypatch, lambda: ScrapeResult(59, 100, 41))
    ok, _ = du.run_scraper("x", "scrapers.x", "scrape_date",
                           TRADE_DATE, expect_rows=False)
    assert ok is False
    assert len(calls) == 1


def test_exception_is_retried(monkeypatch):
    """Crashes are transient in a way empty responses are not."""
    def boom():
        raise RuntimeError("simulated crash")

    calls = _stub_module(monkeypatch, boom)
    ok, trace = du.run_scraper("x", "scrapers.x", "scrape_date",
                               TRADE_DATE, expect_rows=True)
    assert ok is False
    assert len(calls) == du.SCRAPER_MAX_RETRIES
    assert "simulated crash" in trace


def test_exception_that_clears_is_reported_as_success(monkeypatch):
    outcomes = [RuntimeError("flaky"), ScrapeResult(900, 900, 0)]

    def flaky():
        item = outcomes.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    calls = _stub_module(monkeypatch, flaky)
    ok, trace = du.run_scraper("x", "scrapers.x", "scrape_date",
                               TRADE_DATE, expect_rows=True)
    assert (ok, trace, len(calls)) == (True, None, 2)


# ── telegram summary ───────────────────────────────────────────────────────

def test_summary_reports_success_when_nothing_failed():
    msg = du._build_daily_update_message(TRADE_DATE, [("A", "ok"), ("B", "ok")])
    assert "全部成功" in msg


def test_summary_names_failures_and_carries_the_reason():
    msg = du._build_daily_update_message(
        TRADE_DATE,
        [("A", "ok"), ("Margin trading", "failed")],
        {"Margin trading": "抓到 0 筆資料"},
    )
    assert "全部成功" not in msg
    assert "Margin trading" in msg
    assert "抓到 0 筆資料" in msg


def test_summary_flags_a_critical_failure_as_blocking_the_frontend():
    critical = sorted(du.CRITICAL_SCRAPER_LABELS)[0]
    msg = du._build_daily_update_message(TRADE_DATE, [(critical, "failed")])
    assert "關鍵抓檔失敗" in msg
    assert "前端未更新" in msg


def test_summary_does_not_flag_a_non_critical_failure_as_blocking():
    msg = du._build_daily_update_message(TRADE_DATE, [("Hog prices (毛豬)", "failed")])
    assert "關鍵抓檔失敗" not in msg


# ── the two paths that used to fail quietly ────────────────────────────────

class _EmptyCursor:
    """Cursor whose first SELECT — today's stock ids — comes back empty."""
    def execute(self, *args, **kwargs):
        pass

    def fetchall(self):
        return []


class _EmptyCursorCtx:
    def __enter__(self):
        return _EmptyCursor()

    def __exit__(self, *exc):
        return False


def test_delist_detection_raises_when_no_prices_exist(monkeypatch):
    """Used to print [SKIP] and return, which update_date recorded as "ok".

    Every price scraper coming back empty for a date we believe is a trading
    day is an anomaly, not a benign skip.
    """
    monkeypatch.setattr(du, "get_cursor", lambda *a, **k: _EmptyCursorCtx())
    with pytest.raises(RuntimeError, match="No price data"):
        du.detect_delisted(date.today())


def test_delist_detection_stays_quiet_for_an_old_backfill_date(monkeypatch):
    """The benign guard must survive the change above."""
    monkeypatch.setattr(du, "get_cursor", lambda *a, **k: _EmptyCursorCtx())
    old = date.today().replace(year=date.today().year - 3)
    du.detect_delisted(old)   # returns, does not raise


def test_next_day_disposal_failure_reaches_the_summary():
    """Used to print [WARN] and never touch results or failure_traces.

    A silent failure here reverts positions.json's disposal_status to the
    prediction, which looks exactly like a day with no disposals.
    """
    import inspect
    src = inspect.getsource(du.update_date)
    block = src[src.index("明日處置名單抓取"):][:800]
    assert 'results.append(("明日處置名單抓取", "ok")' in block
    assert '_capture_trace(failure_traces, "明日處置名單抓取")' in block
    assert 'results.append(("明日處置名單抓取", "failed")' in block
