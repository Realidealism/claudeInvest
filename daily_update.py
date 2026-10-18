"""
Daily market data update script.

Usage:
  python daily_update.py              # update today
  python daily_update.py 2026-04-02   # update specific date
  python daily_update.py 2026-04-01 2026-04-07  # update date range

Each scraper runs independently; failures are logged but do not stop the rest.
"""

import io
import json
import sys
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path

from db.connection import get_cursor
from telegram_bot.notify import send_sync
from utils.console import pause_before_exit
from utils.format_shift import ScrapeResult, SHIFT_ERROR_RATE, SHIFT_MIN_ROWS

# ---------------------------------------------------------------------------
# Scraper registry — order matters (prices first, derived data last)
# ---------------------------------------------------------------------------
# Trading-day gate: TAIEX is scraped first. When it is absent, the exchange
# calendar (tw.market_holidays) says whether that is a holiday or a failure.
INDEX_SCRAPER = ("Market indices", "scrapers.index_prices", "scrape_date")
HOLIDAY_SCRAPER = ("Market holidays", "scrapers.market_holidays", "scrape_date")

# Labels in SCRAPERS whose failure invalidates everything downstream
# (daily_liquidity, market_breadth, daily_snapshot, export). When any of
# these fails after retries, the downstream steps skip rather than write
# a partial snapshot that gets shipped to Vercel.
CRITICAL_SCRAPER_LABELS = {
    "TWSE daily prices",
    "TPEx daily prices",
}

# The chain every page of the frontend depends on. A night in which one of
# these failed, or was blocked by something upstream, is a failed night; a
# failure anywhere else makes it a partial one (see _run_outcome). A label that
# no step produces would silently demote a core failure to partial, so
# tests/test_daily_update_pipeline.py pins every entry to a real step.
CORE_STEP_LABELS = CRITICAL_SCRAPER_LABELS | {
    INDEX_SCRAPER[0],
    "市場廣度",
    "每日流動性",
    "市場波動基準",
    # Not read by the frontend, but buy and sell gate on it; a gap left
    # unfilled silently changes their signals (see _stance_gaps).
    "攻防資料",
    "多空評比 + 操作訊號 + 策略持倉快照",
    "前端匯出部署",
}
# Stands in for the steps when the pipeline itself raised.
UNHANDLED_STEP = "未處理的例外"

# Fourth field is expect_rows: True when the source must yield rows on every
# trading day, so records==0 means the scraper failed silently (HTTP 200 with
# an empty body, or a swallowed request error) rather than "nothing happened
# today". False sources legitimately return 0 rows and are exempt from that
# check — each one carries the reason inline — but any of them that reports
# failed_sources (a leg that did not answer or could not be read) is still
# judged on those for recent dates. Keep the flag in the tuple rather than a separate
# label set: a set silently stops matching when a label is renamed, and a new
# scraper would default to unmonitored.
SCRAPERS = [
    # Core daily prices
    ("TWSE daily prices",       "scrapers.twse",             "scrape_date", True),
    ("TPEx daily prices",       "scrapers.tpex",             "scrape_date", True),
    ("TWSE after-hours",        "scrapers.twse_after_hours", "scrape_date", True),
    ("TPEx after-hours",        "scrapers.tpex_after_hours", "scrape_date", True),
    ("ESB emerging prices",     "scrapers.tpex_emerging",    "scrape_date", True),
    # Supplemental data
    ("Odd-lot (all sessions)",  "scrapers.odd_lot",          "scrape_date", True),
    ("Margin trading",          "scrapers.margin",           "scrape_date", True),
    ("CNN Fear & Greed",        "scrapers.cnn_feargreed",    "scrape_date", True),
    ("Market quotes (大宗行情)", "scrapers.market_quote",     "scrape_date", True),
    ("Shipping/memory quotes",  "scrapers.market_html",      "scrape_date", True),
    ("Hog prices (毛豬)",        "scrapers.market_hog",       "scrape_date", True),
    ("Price limits",            "scrapers.price_limits",     "scrape_date", True),
    ("Institutional investors", "scrapers.institutional",    "scrape_date", True),
    # ETF holdings — scrape_date() returns None (no ScrapeResult); it raises
    # instead when any fund failed, parsed no holdings, or served a file for
    # another day.
    ("ETF holdings",            "scrapers.etf_holdings",     "scrape_date", False),
    # Securities lending
    ("SBL (借券賣出)",           "scrapers.securities_lending", "scrape_date", True),
    # Day trading
    ("Day trading (當沖)",       "scrapers.day_trading",        "scrape_date", True),
    # TAIFEX futures/options (期貨/選擇權) — non-critical; the after-hours
    # session for the same day may be incomplete and self-corrects on the next
    # run. Futures and institutional are monitored all the same: the day
    # session is always there by 21:30 (77-104 futures rows; institutional
    # 69 futures + 30 options rows on every night logged and every trading day
    # in the table), and both feed the thermometer's stance. Run by hand
    # before TAIFEX publishes, they will report a failure.
    ("TAIFEX futures",          "scrapers.taifex_futures",       "scrape_date", True),
    ("TAIFEX institutional",    "scrapers.taifex_institutional", "scrape_date", True),
    # P/C ratio: one row on every trading day in the table (120 of 120), so
    # it is monitored too; a run by hand before TAIFEX publishes will report.
    ("TAIFEX P/C ratio",        "scrapers.taifex_pc_ratio",      "scrape_date", True),
    # Alerts — event-driven; exempt from the 0-row check (a quiet disposal
    # list is normal). The scraper itself reports a leg that did not answer,
    # and an empty attention list for a day that has traded (never seen in
    # 120 days) -- so a run by hand before TWSE/TPEx publish will report it.
    ("Stock alerts (注意/處置)", "scrapers.stock_alerts",       "scrape_date", False),
    # Forward-looking board (除權除息預告) — feeds the morning-brief ex-dividend alert
    # Event-driven; exempt (no upcoming ex-dividend => 0 rows)
    ("Dividend calendar (除權息)", "scrapers.dividend_calendar", "scrape_date", False),
    # Forward-looking board (停券/融券最後回補日) — covering-squeeze calendar
    # Event-driven; exempt
    ("Short-cover calendar (停券)", "scrapers.short_cover_calendar", "scrape_date", False),
    # 減資參考價 — feeds post_break_blackout, which needs the effective date to
    # blank out the bars whose indicator windows span the unadjusted seam.
    # Event-driven; exempt (a reduction lands only a few times a month)
    ("Capital reductions (減資)", "scrapers.capital_reductions", "scrape_date", False),
    # Current-year AGM board (股東會開會日期, OpenAPI) — AGMs cluster in Q2;
    # exempt (0 rows outside the season is normal)
    ("Shareholder meetings (股東會)", "scrapers.shareholder_meetings", "scrape_date", False),
    # Investor conferences (法說會): announcements arrive any day and carry a
    # future date, so the index is refreshed daily and decks are archived once
    # their conference has passed; exempt (quiet weeks are normal)
    ("Investor conferences (法說會)", "scrapers.investor_conferences", "scrape_date", False),
    # Weekly/monthly — exempt from the 0-row check: shareholder dist. saves
    # once per week, and the _run_completed guards make a range run fetch only
    # once. Each still reports a source that did not answer (failed_sources).
    ("Shareholder dist.",       "scrapers.shareholder_distribution", "scrape_date", False),
    ("Insider holdings",        "scrapers.insider_holdings",  "scrape_date", False),
    ("Insider pledge events",   "scrapers.insider_pledge_events", "scrape_date", False),
    # Monthly filing window; exempt (no transfer declared this month => 0 rows)
    ("Insider share transfers", "scrapers.insider_share_transfers", "scrape_date", False),
    # Monitored: fetch() pulls the full cumulative table and upserts every row,
    # so records is always large. A 0 means the request failed and was
    # swallowed into an empty list.
    ("Private placements",      "scrapers.private_placements", "scrape_date", True),
    ("Treasury stock",          "scrapers.treasury_stock",    "scrape_date", False),
    # Monitored: the source returns the whole current-year list on every call.
    (*HOLIDAY_SCRAPER, True),
]


def _has_taiex(trade_date: date) -> bool:
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT 1 FROM tw.index_prices WHERE index_id='TAIEX' AND trade_date=%s",
            (trade_date,),
        )
        return cur.fetchone() is not None


SCRAPER_MAX_RETRIES = 3
SCRAPER_RETRY_WAIT  = 10  # seconds

# expect_rows only applies to recent dates. Backfilling an old range via
# update_range() hits sources that did not exist back then (odd-lot, day
# trading, SBL ...), where 0 rows is the correct answer — judging those would
# both spam the summary and burn SCRAPER_RETRY_WAIT on every one of them.
EXPECT_ROWS_MAX_AGE_DAYS = 7


def _capture_trace(failure_traces: dict[str, str], label: str) -> None:
    """Print current exception traceback to stdout AND save last 3 non-blank
    lines into failure_traces[label] for the telegram summary. Three lines
    is enough to carry exception type, message, and the last call-site
    without blowing past Telegram's 4096-char limit."""
    buf = io.StringIO()
    traceback.print_exc(file=buf)
    full = buf.getvalue()
    print(full, end="")
    lines = [l for l in full.splitlines() if l.strip()]
    if lines:
        failure_traces[label] = "\n".join(lines[-3:])


_FAILED_SOURCES_SHOWN = 8  # a per-stock source can fail dozens at once


def _result_problem(result, expect_rows: bool, recent: bool | None = None) -> str | None:
    """Judge a scraper's ScrapeResult, returning a reason string when it
    represents a silent failure, or None when it looks healthy.

    Catches the failure modes that never raise: an empty response (HTTP 200
    with no data, or a request error swallowed into an empty list), a failed
    leg or item (failed_sources), and a format shift (the API still answers
    but most rows no longer parse).

    expect_rows: the source has rows every trading day, so 0 is a failure.
    recent: the date is recent enough to judge failed_sources at all (an old
    backfill hits legs that did not exist yet). Event-driven sources put in
    failed_sources only what they know failed (a leg that did not answer or
    could not be read, an item the source never returned), so they are judged
    even though 0 rows is fine for them. None means "same as expect_rows".
    """
    if not isinstance(result, ScrapeResult):
        # No return contract to judge (e.g. ETF holdings returns None).
        return None
    if recent is None:
        recent = expect_rows

    if expect_rows and result.records == 0:
        return f"抓到 0 筆資料（此來源每個交易日應有資料）：{result}"

    # A multi-leg scraper sums every leg into records, so one dead leg still
    # leaves records far above zero. Judge the legs individually.
    if recent and result.failed_sources:
        shown = result.failed_sources[:_FAILED_SOURCES_SHOWN]
        more = len(result.failed_sources) - len(shown)
        return (f"以下資料來源失敗：{'、'.join(shown)}"
                f"{f' 等 {len(result.failed_sources)} 項' if more else ''}"
                f"（其餘共 {result.records} 筆）")

    # Same criteria as historical_update._check_shift, shared thresholds.
    if result.api_rows >= SHIFT_MIN_ROWS and result.error_rate > SHIFT_ERROR_RATE:
        return (
            f"疑似 API 格式改版：{result.parse_errors}/{result.api_rows} 列解析失敗 "
            f"（{result.error_rate:.0%}，門檻 {SHIFT_ERROR_RATE:.0%}）"
        )

    return None


def run_scraper(
    label: str, module_path: str, func_name: str, trade_date: date,
    expect_rows: bool = False, recent: bool | None = None,
) -> tuple[bool, str | None]:
    """Import and run a single scraper. Retries up to SCRAPER_MAX_RETRIES
    times on an exception. Returns (success, last_trace) where last_trace is
    the last failure's traceback summary (last 3 non-blank lines) on failure.

    A scraper that returns without raising is still failed when its
    ScrapeResult shows a silent failure (see _result_problem); the reason
    takes last_trace's place in the telegram summary.

    Soft failures are NOT retried here. Transient empty responses are already
    covered one level down by utils.http_client.fetch_json_retry (3 attempts,
    10s apart, on the same HTTP-200-but-invalid condition), so retrying the
    whole scraper would multiply into 9 requests per leg -- aimed at sources
    that may be rate-limiting us in the first place (MOPS, the HTML sites).
    A format shift is deterministic: re-requesting parses identically."""
    import time
    import importlib
    mod = importlib.import_module(module_path)
    fn  = getattr(mod, func_name)

    last_trace: str | None = None
    for attempt in range(1, SCRAPER_MAX_RETRIES + 1):
        try:
            problem = _result_problem(fn(trade_date), expect_rows, recent)
            if problem is None:
                return True, None
            print(f"\n  [SOFT-FAIL] {label}: {problem}")
            return False, problem
        except Exception:
            print(f"\n  [ERROR] {label} (attempt {attempt}/{SCRAPER_MAX_RETRIES}):")
            buf = io.StringIO()
            traceback.print_exc(file=buf)
            full = buf.getvalue()
            print(full, end="")
            lines = [l for l in full.splitlines() if l.strip()]
            if lines:
                last_trace = "\n".join(lines[-3:])
            if attempt < SCRAPER_MAX_RETRIES:
                print(f"  Retrying in {SCRAPER_RETRY_WAIT}s ...")
                time.sleep(SCRAPER_RETRY_WAIT)

    return False, last_trace


def _holiday_rows(year: int) -> dict[date, bool]:
    """{holiday_date: is_closed} for one year of tw.market_holidays."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT holiday_date, is_closed FROM tw.market_holidays "
            "WHERE holiday_date BETWEEN %s AND %s",
            (date(year, 1, 1), date(year, 12, 31)),
        )
        return {r["holiday_date"]: r["is_closed"] for r in cur.fetchall()}


def _is_scheduled_closure(rows: dict[date, bool], trade_date: date) -> bool | None:
    """True when the calendar closes the date, False when it covers the year
    and does not, None when it has nothing for that year (rows is empty).

    A date listed with is_closed=FALSE is a trading day: the source marks the
    first and last trading days around a long holiday that way."""
    if not rows:
        return None
    return rows.get(trade_date, False)


# Years the gate already tried to load in this process. A range backfill over
# a year the source no longer serves would otherwise re-request it every day.
_holiday_refresh_tried: set[int] = set()


def _scheduled_closure(trade_date: date) -> bool | None:
    """_is_scheduled_closure against the stored calendar. A year with no rows
    is fetched once: the scheduled refresh only runs on trading days, so the
    first run of a new year can arrive before the calendar does."""
    closure = _is_scheduled_closure(_holiday_rows(trade_date.year), trade_date)
    if closure is None and trade_date.year not in _holiday_refresh_tried:
        _holiday_refresh_tried.add(trade_date.year)
        run_scraper(*HOLIDAY_SCRAPER, trade_date, expect_rows=True)
        closure = _is_scheduled_closure(_holiday_rows(trade_date.year), trade_date)
    return closure


def _exchange_month(trade_date: date) -> dict | None:
    """The exchange's own list of trading days for trade_date's month, asked
    right now: {"stat", "days"}, or None when it does not answer. A single
    request with no soft retries — the gate alert waits on this."""
    from scrapers.index_prices import TWSE_STAT_URL, _roc_to_date
    from utils.http_client import fetch_json
    data = fetch_json(TWSE_STAT_URL,
                      params={"date": f"{trade_date:%Y%m}01", "response": "json"})
    if not isinstance(data, dict):
        return None
    try:
        days = {_roc_to_date(str(r[0])) for r in data.get("data") or []} - {None}
    except (TypeError, IndexError, KeyError):
        return {"stat": "回應格式無法解析", "days": set()}
    return {"stat": data.get("stat"), "days": days}


def _absent_from_month(month: dict | None, trade_date: date) -> bool:
    """The exchange answered with that month's trading days and the date is
    not one of them."""
    return (month is not None and month["stat"] == "OK"
            and bool(month["days"]) and trade_date not in month["days"])


def _probe_line(month: dict | None, trade_date: date) -> str:
    """One line for the gate alert: what the exchange says right now."""
    if month is None:
        return "證交所目前沒有回應，可能是斷線。"
    if trade_date in month["days"]:
        return "證交所已有這一天的資料，但沒有寫進資料庫。"
    if not _absent_from_month(month, trade_date):
        return f"證交所有回應，但沒有給資料（stat={month['stat']}）。"
    n = len(month["days"])
    if trade_date == date.today():
        # Before the close this is the normal state, not a sign of a closure.
        return f"證交所目前有回應（本月 {n} 個交易日，還沒有今天）。"
    return (f"證交所目前有回應（本月 {n} 個交易日，沒有這一天），"
            f"可能是颱風等臨時休市，可忽略。")


def _gate_alert(trade_date: date, closure: bool | None, gate_ok: bool,
                month: dict | None) -> str:
    lines = [f"[每日更新] {trade_date} 交易日判定失敗"]
    if closure is None:
        lines.append(f"休市日曆沒有 {trade_date.year} 年的資料（或查詢失敗），"
                     f"無法判定這一天是否休市，整批 scraper 已中止。")
    else:
        lines.append("休市日曆排定這一天開市，但沒有當日大盤指數，整批 scraper 已中止。")
    if not gate_ok:
        lines.append("大盤指數的抓取程式本身回報失敗。")
    lines.append(_probe_line(month, trade_date))
    if trade_date == date.today():
        lines.append("若在收盤資料公布前執行，也會出現這則訊息。")
    lines.append("確認是交易日後請回補：")
    lines.append(f"C:\\Claude\\Invest\\dist\\daily_update.exe {trade_date} {trade_date}")
    return "\n".join(lines)


def _trading_day_gate(trade_date: date) -> tuple[str, bool, str | None]:
    """(decision, index scraper ok, index scraper trace). The decision is what
    update_date does with a weekday: "run" the pipeline, skip a "holiday", or
    "abort" because the date cannot be shown to be either. The other two are
    run_scraper's result for the index scraper, which the summary reports.

    An absent TAIEX row used to mean holiday. A fetch that fails without
    raising leaves no row either, so a network outage across the 21:30 slot on
    2026-09-18 skipped the whole pipeline, exited 0, and cost a trading day
    that nothing noticed for two days. The exchange calendar now decides: only
    a date it closes is a holiday. A scheduled trading day with no TAIEX row
    aborts and alerts, which also happens on an unscheduled closure (typhoon):
    the calendar cannot know about those and one alert is the accepted cost.

    The calendar source only serves the current year, so a backfill into an
    earlier year finds no calendar. There the exchange's list of trading days
    for that month stands in: it is long settled, and a date missing from it
    was a closure. This never applies to the current year, where a missing
    date may simply not have been published yet.
    """
    print(f"\n--- {INDEX_SCRAPER[0]} (trading-day gate) ---")
    # expect_rows=False: a holiday legitimately has no TAIEX row.
    gate_ok, gate_trace = run_scraper(*INDEX_SCRAPER, trade_date, expect_rows=False)
    if _has_taiex(trade_date):
        return "run", gate_ok, gate_trace

    if trade_date > date.today():
        print(f"\n[NOT YET] {trade_date} is in the future — nothing to fetch.")
        return "holiday", gate_ok, gate_trace

    try:
        closure = _scheduled_closure(trade_date)
    except Exception:
        # No calendar to ask is not a holiday.
        traceback.print_exc()
        closure = None
    if closure:
        print(f"\n[HOLIDAY] {trade_date} is closed on the exchange calendar "
              f"— skipping remaining scrapers.")
        return "holiday", gate_ok, gate_trace

    try:
        month = _exchange_month(trade_date)
    except Exception:
        traceback.print_exc()
        month = None
    if (closure is None and trade_date.year < date.today().year
            and _absent_from_month(month, trade_date)):
        print(f"\n[HOLIDAY] {trade_date} is not among the exchange's trading "
              f"days for that month — skipping remaining scrapers.")
        return "holiday", gate_ok, gate_trace

    reason = ("no exchange calendar for that year" if closure is None
              else "a scheduled trading day")
    print(f"\n[GATE FAILED] {trade_date}: no TAIEX row on {reason} "
          f"- aborting the rest of the pipeline.")
    try:
        send_sync(_gate_alert(trade_date, closure, gate_ok, month))
    except Exception:
        traceback.print_exc()
    return "abort", gate_ok, gate_trace


_TRACE_MAX_CHARS = 500     # one step's reason in the summary
_SUMMARY_MAX_CHARS = 3800  # the whole summary; Telegram's hard limit is 4096


def _build_daily_update_message(
    trade_date: date,
    results: list[tuple[str, str]],
    failure_traces: dict[str, str] | None = None,
) -> str:
    """Compose the Telegram summary for one update_date() run.

    Mirrors the Chinese summary printed at the end of update_date so users
    receive the same view they would see in the terminal. When
    failure_traces is supplied, each failed label gets its last-3-line
    traceback appended so the user can see the underlying exception from
    the telegram message without re-opening the terminal.
    """
    failure_traces = failure_traces or {}
    ok_n = sum(1 for _, s in results if s == "ok")
    failed = [label for label, s in results if s == "failed"]
    blocked = [label for label, s in results if s == "blocked"]
    skip_n = sum(1 for _, s in results if s == "skip")
    counts = f"成功 {ok_n} / 失敗 {len(failed)} / 跳過 {skip_n}"
    if blocked:
        counts += f" / 未執行 {len(blocked)}"
    if failed or blocked:
        critical_failed = [l for l in failed if l in CRITICAL_SCRAPER_LABELS]
        lines = [f"[每日更新] {trade_date}"]
        if failed:
            lines.append(f"失敗：{', '.join(failed)}")
        if blocked:
            lines.append(f"因上游失敗未執行：{', '.join(blocked)}")
        lines.append(counts)
        if critical_failed:
            lines.append(
                f"【關鍵抓檔失敗】{', '.join(critical_failed)}，"
                f"下游已跳過，前端未更新"
            )
        reasons = []
        for label in failed:
            tr = failure_traces.get(label)
            if tr:
                if len(tr) > _TRACE_MAX_CHARS:
                    tr = tr[:_TRACE_MAX_CHARS] + "…（完整見 log）"
                reasons.append(f"── {label} ──\n{tr}")
        rerun = f"重跑指令：daily_update.exe {trade_date}"
        # Telegram refuses a message over 4096 characters outright, so a night
        # with many failing sources would lose the whole summary. Drop reasons
        # from the end until it fits; the log has all of them.
        msg = "\n".join(lines + reasons + [rerun])
        while len(msg) > _SUMMARY_MAX_CHARS and reasons:
            reasons.pop()
            msg = "\n".join(lines + reasons + ["（其餘失敗原因見 log）", rerun])
        return msg
    return f"[每日更新] {trade_date} 全部成功（{counts}）"


# ---------------------------------------------------------------------------
# What a night amounted to, and where that is kept
# ---------------------------------------------------------------------------
# A step ends as one of:
#   ok       it ran and did its job
#   failed   it ran and did not
#   skip     it was not due today (monthly / quarterly windows)
#   blocked  it was due, but something upstream failed, so it did not run
#
# The night as a whole is one of the outcomes below. The exit code is all Task
# Scheduler keeps; the row in tw.daily_update_runs is what the morning health
# check reads, and it is the only trace left when the Telegram summary could
# not be sent.
_OUTCOME_EXIT_CODES = {
    "success": 0,   # nothing failed, nothing was blocked
    "holiday": 0,   # the gate found the market closed
    "weekend": 0,   # chip-only update; no run record
    "partial": 2,   # only non-core steps failed
    "failed":  1,   # a core step failed or was blocked, or the pipeline raised
    "aborted": 1,   # the trading-day gate could not establish the date
}


def _exit_code(outcome) -> int:
    return _OUTCOME_EXIT_CODES.get(outcome, 1)


def _severity(outcome) -> int:
    return {0: 0, 2: 1, 1: 2}[_exit_code(outcome)]


def _run_outcome(results: list[tuple[str, str]]) -> str:
    bad = [label for label, status in results if status in ("failed", "blocked")]
    if any(label in CORE_STEP_LABELS for label in bad):
        return "failed"
    return "partial" if bad else "success"


def _steps_json(results: list[tuple[str, str]], failure_traces: dict[str, str]) -> str:
    steps = []
    for label, status in results:
        trace = failure_traces.get(label)
        steps.append({
            "label": label,
            "status": status,
            "core": label in CORE_STEP_LABELS or label == UNHANDLED_STEP,
            # jsonb rejects NUL inside a string.
            "trace": trace.replace("\x00", "") if trace else None,
        })
    return json.dumps(steps, ensure_ascii=False)


_RUN_START_SQL = "INSERT INTO tw.daily_update_runs (trade_date) VALUES (%s) RETURNING id"
_RUN_FINISH_SQL = """
    UPDATE tw.daily_update_runs
    SET finished_at = NOW(), outcome = %s, summary_sent = %s, steps = %s::jsonb
    WHERE id = %s
"""
# Used when the opening row could not be written but the database is back.
_RUN_INSERT_SQL = """
    INSERT INTO tw.daily_update_runs (trade_date, finished_at, outcome, summary_sent, steps)
    VALUES (%s, NOW(), %s, %s, %s::jsonb)
"""


def _run_start(trade_date: date) -> int | None:
    """Record that a run began. A run that dies leaves this row as 'running'.
    The record never stops the pipeline: None when it could not be written."""
    try:
        with get_cursor() as cur:
            cur.execute(_RUN_START_SQL, (trade_date,))
            run_id = cur.fetchone()["id"]
        return run_id
    except Exception:
        print("  [WARN] run record not opened:")
        traceback.print_exc()
        return None


def _run_finish(run_id: int | None, trade_date: date, outcome: str,
                results: list[tuple[str, str]], failure_traces: dict[str, str],
                summary_sent: bool | None) -> None:
    try:
        steps = _steps_json(results, failure_traces)
        with get_cursor() as cur:
            if run_id is None:
                cur.execute(_RUN_INSERT_SQL, (trade_date, outcome, summary_sent, steps))
            else:
                cur.execute(_RUN_FINISH_SQL, (outcome, summary_sent, steps, run_id))
    except Exception:
        print("  [WARN] run record not written:")
        traceback.print_exc()


class _RunLog:
    """The log file shared by the stdout and stderr mirrors. Best-effort: the
    first error on it switches it off for both."""

    def __init__(self, file):
        self.file = file

    def write(self, text: str) -> None:
        if self.file is None:
            return
        try:
            self.file.write(text)
        except Exception:
            self.close()

    def close(self) -> None:
        file, self.file = self.file, None
        if file is not None:
            try:
                file.close()
            except Exception:
                pass


class _Tee:
    """Passes writes to the original stream and copies them into the run log."""

    def __init__(self, original, log: _RunLog):
        self._original = original
        self._log = log

    def write(self, text: str) -> int:
        # Log first: it never raises, and when the original does (a console
        # that cannot encode the text) that line is the one worth keeping.
        self._log.write(text)
        if self._original is not None:
            self._original.write(text)
        return len(text)

    def flush(self) -> None:
        if self._original is not None:
            self._original.flush()

    def __getattr__(self, name):
        # encoding, isatty, fileno ...: whatever the original says.
        return getattr(self._original, name)


def _open_run_log():
    """Mirror stdout and stderr into logs/daily_update/<start time>.log and
    return the function that undoes it.

    Call the returned function before pause_before_exit(): that waits on
    input() in a daemon thread, and with sys.stdout replaced the interpreter
    can die in a fatal error at shutdown, taking the exit code with it.

    Only what this process prints through Python lands in the file. Pool
    workers and git subprocesses write to the console directly.
    """
    # Frozen: __file__ points into the temporary unpack directory.
    if getattr(sys, "frozen", False):
        root = Path(sys.executable).parent.parent
    else:
        root = Path(__file__).parent
    try:
        folder = root / "logs" / "daily_update"
        folder.mkdir(parents=True, exist_ok=True)
        log = _RunLog(open(folder / f"{datetime.now():%Y%m%d_%H%M%S}.log", "w",
                           encoding="utf-8", errors="replace", buffering=1))
    except Exception as exc:
        print(f"[WARN] run log not opened: {exc}")
        return lambda: None

    out, err = sys.stdout, sys.stderr
    tee_out, tee_err = _Tee(out, log), _Tee(err, log)
    sys.stdout, sys.stderr = tee_out, tee_err

    def restore():
        # Only undo our own replacement: if something else swapped a stream
        # in the meantime, putting the old one back would drop theirs.
        if sys.stdout is tee_out:
            sys.stdout = out
        if sys.stderr is tee_err:
            sys.stderr = err
        log.close()

    return restore


DELIST_THRESHOLD_DAYS = 20  # consecutive trading days absent before marking delisted
DELIST_RECENT_DAYS    = 7   # only run delist detection when trade_date is within this many days of today


def detect_delisted(trade_date: date):
    """
    Compare today's API stock list against DB active stocks.
    Mark stocks as delisted only if they have been absent for
    DELIST_THRESHOLD_DAYS consecutive trading days, to avoid false
    positives from temporary halts (重訊停牌, 減資換發, 處置等).

    TWSE/TPEx daily price APIs return ALL listed stocks (even with no trades),
    so any active stock missing from the list is not currently trading.

    Only runs when trade_date is recent (within DELIST_RECENT_DAYS of today),
    because the cutoff is derived from the latest TAIEX dates in DB and
    historical backfills would otherwise mark currently-active stocks as
    delisted (their last_seen would be far in the past relative to "now").
    """
    days_old = (date.today() - trade_date).days
    if days_old > DELIST_RECENT_DAYS:
        print(f"  [SKIP] Delist detection: {trade_date} is {days_old} days old "
              f"(threshold {DELIST_RECENT_DAYS}); historical backfills cannot "
              f"reliably detect delistings.")
        return

    with get_cursor() as cur:
        # Get stock_ids that were scraped today (appeared in API)
        cur.execute("""
            SELECT DISTINCT stock_id FROM tw.daily_prices
            WHERE trade_date = %s AND close_price IS NOT NULL
        """, (trade_date,))
        today_ids = {r["stock_id"] for r in cur.fetchall()}

        if not today_ids:
            # Not a benign skip like the two guards above: every price scraper
            # came back with nothing for a date we believe is a trading day.
            # Raise so update_date's wrapper reports it instead of logging "ok".
            raise RuntimeError(
                f"No price data in DB for {trade_date} — cannot detect "
                f"delistings. Check whether the TWSE/TPEx price scrapers ran."
            )

        # Get currently active stocks in DB
        cur.execute("""
            SELECT stock_id, name, market FROM tw.stocks
            WHERE is_active = TRUE AND market IN ('TWSE', 'TPEx')
        """)
        active = cur.fetchall()

        # Stocks active in DB but missing from today's API
        missing = [s for s in active if s["stock_id"] not in today_ids]

        if not missing:
            print(f"  All {len(active)} active stocks found in today's data.")
            return

        # Check how many recent trading days each missing stock has been absent
        # Use the last N trading days from index_prices as calendar reference
        cur.execute("""
            SELECT DISTINCT trade_date FROM tw.index_prices
            WHERE index_id = 'TAIEX'
            ORDER BY trade_date DESC
            LIMIT %s
        """, (DELIST_THRESHOLD_DAYS,))
        recent_days = [r["trade_date"] for r in cur.fetchall()]

        if len(recent_days) < DELIST_THRESHOLD_DAYS:
            print(f"  [SKIP] Only {len(recent_days)} trading days in DB, need {DELIST_THRESHOLD_DAYS} for delist detection.")
            return

        cutoff_date = recent_days[-1]  # oldest of the recent N days
        delisted, suspended = [], []

        for s in missing:
            cur.execute("""
                SELECT MAX(trade_date) AS last_seen FROM tw.daily_prices
                WHERE stock_id = %s AND close_price IS NOT NULL
            """, (s["stock_id"],))
            row = cur.fetchone()
            last_seen = row["last_seen"] if row else None

            if last_seen is None or last_seen < cutoff_date:
                # Absent for >= threshold days -> mark delisted
                cur.execute("""
                    UPDATE tw.stocks
                    SET is_active = FALSE, delisted_date = %s, updated_at = NOW()
                    WHERE stock_id = %s
                """, (last_seen or trade_date, s["stock_id"]))
                delisted.append(s)
                print(f"  [DELISTED] {s['stock_id']} {s['name']} ({s['market']}) last seen: {last_seen}")
            else:
                suspended.append(s)

        if suspended:
            print(f"  {len(suspended)} stock(s) temporarily absent (< {DELIST_THRESHOLD_DAYS} days, likely suspended).")
        if delisted:
            print(f"  Marked {len(delisted)} stock(s) as delisted.")


def _git_push_frontend(target_branch: str = "main"):
    """Publish the exported JSON to `target_branch` (main) and push for Vercel.

    Data is always committed onto main regardless of which branch this checkout
    is on. 00631L / other feature work leaves the repo on a non-main branch, and
    a plain `git commit` there strands the daily data off main — which is what
    Vercel deploys. We publish through a throwaway worktree pinned to
    origin/<target>, so the current branch and working tree are never touched.
    The retry re-bases onto a freshly fetched origin/<target>, covering the race
    with intraday_publish (which also pushes data to main). The whole critical
    section additionally holds publish_lock so concurrent publishers can't
    collide on git's internal ref locks, and exhausting the retries raises so
    the failure lands in the run summary instead of a silent log line."""
    import subprocess
    import sys
    import shutil

    from publish_lock import publish_lock

    # PyInstaller exe: __file__ points to temp dir; exe lives in dist/
    if getattr(sys, 'frozen', False):
        repo = Path(sys.executable).parent.parent
    else:
        repo = Path(__file__).parent
    data_dir = repo / "frontend" / "public" / "data"
    if not data_dir.exists():
        print("  [SKIP] No frontend/public/data/ directory.")
        return

    rel = "frontend/public/data"
    wt = repo.parent / "_invest_publish_main"

    def git(*args, cwd, check=True):
        return subprocess.run(["git", *args], cwd=cwd, check=check)

    with publish_lock():
        for attempt in range(2):
            if wt.exists():  # clean a leftover from a previously crashed run
                subprocess.run(["git", "worktree", "remove", "--force", str(wt)],
                               cwd=repo, check=False)
                subprocess.run(["git", "worktree", "prune"], cwd=repo, check=False)
            git("fetch", "origin", target_branch, cwd=repo)
            git("worktree", "add", "--force", "--detach", str(wt),
                f"origin/{target_branch}", cwd=repo)
            try:
                shutil.copytree(data_dir, wt / "frontend" / "public" / "data",
                                dirs_exist_ok=True)
                git("add", rel, cwd=wt)
                if subprocess.run(["git", "diff", "--cached", "--quiet", "--", rel],
                                  cwd=wt).returncode == 0:
                    print("  No JSON changes to deploy.")
                    return
                git("commit", "-m",
                    f"Update frontend data ({date.today().isoformat()})", cwd=wt)
                if subprocess.run(["git", "push", "origin", f"HEAD:{target_branch}"],
                                  cwd=wt).returncode == 0:
                    print(f"  Pushed to GitHub {target_branch} — Vercel will auto-deploy.")
                    return
                print(f"  push to {target_branch} rejected (attempt {attempt + 1}); "
                      f"refetching and retrying ...")
            finally:
                subprocess.run(["git", "worktree", "remove", "--force", str(wt)],
                               cwd=repo, check=False)
    raise RuntimeError(
        f"frontend data push to {target_branch} failed after retries"
    )


def update_chip_only(trade_date: date):
    """Weekend-only path: refresh TDCC weekly shareholder distribution
    (集保戶股權分散表), regenerate chip_picks.json, and push for Vercel.

    The daily price/chip scrapers have no new data on a non-trading day, but
    TDCC's weekly distribution — the sole input to chip_picks.json (集保大戶
    選股) — publishes on Saturdays. scrape_date ignores trade_date and always
    fetches the latest week, so the weekend date is just the trigger."""
    try:
        from scrapers.shareholder_distribution import scrape_date as scrape_shareholder
        from export.generate import export_chip_picks

        print("\n--- Shareholder distribution (集保) ---")
        scrape_shareholder(trade_date)

        # PyInstaller exe: __file__ is a temp dir; the repo is exe's parent.parent.
        if getattr(sys, 'frozen', False):
            repo = Path(sys.executable).parent.parent
        else:
            repo = Path(__file__).parent
        out = repo / "frontend" / "public" / "data"
        out.mkdir(parents=True, exist_ok=True)

        print("\n--- Export chip_picks ---")
        with get_cursor(commit=False) as cur:
            export_chip_picks(cur, out)

        print("\n--- Frontend push ---")
        _git_push_frontend()
        from telegram_bot.chip_report import build_chip_report
        report = build_chip_report(out / "chip_picks.json")
        msg = report or f"[週六集保更新] {trade_date} chip_picks 已更新"
    except Exception:
        traceback.print_exc()
        msg = f"[週六集保更新] {trade_date} 失敗，見 log"

    try:
        send_sync(msg)
    except Exception:
        traceback.print_exc()


# ---------------------------------------------------------------------------
# Monthly revenue: was the publication window's fetch good enough?
# ---------------------------------------------------------------------------
# Filing deadline is the 10th. Until then a market MOPS has not published, or
# one with few companies in it, is normal; from the 11th each market has to
# hold at least this share of the month before's companies.
_REVENUE_DEADLINE_DAY = 10
_REVENUE_MIN_RATIO = 0.9
_REVENUE_MARKETS = {"sii": ("上市", "TWSE"), "otc": ("上櫃", "TPEx")}
_REVENUE_STATE_TEXT = {
    "no_response": "沒有回應或 HTTP 錯誤",
    "unpublished": "尚未公布",
    "unreadable": "頁面讀不出資料",
    "ok": "有資料",
}


def _month_before(year: int, month: int) -> tuple[int, int]:
    return (year, month - 1) if month > 1 else (year - 1, 12)


def _revenue_months(trade_date: date) -> tuple[str, str]:
    """(month fetched, month compared against) as 'YYYY-MM'."""
    fetched = _month_before(trade_date.year, trade_date.month)
    base = _month_before(*fetched)
    return f"{fetched[0]}-{fetched[1]:02d}", f"{base[0]}-{base[1]:02d}"


def _revenue_counts(year_month: str) -> dict[str, int]:
    """Companies with revenue for year_month in the database, per market slug."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT s.market, COUNT(*) AS n
            FROM tw.monthly_revenue r JOIN tw.stocks s USING (stock_id)
            WHERE r.year_month = %s AND s.market IN ('TWSE', 'TPEx')
            GROUP BY s.market
            """,
            (year_month,),
        )
        by_label = {r["market"]: r["n"] for r in cur.fetchall()}
    return {slug: by_label.get(label, 0) for slug, (_, label) in _REVENUE_MARKETS.items()}


def _revenue_verdict(by_market: dict[str, tuple[str, int]], day: int,
                     have: dict[str, int], base: dict[str, int],
                     fetched: str = "", base_month: str = "") -> tuple[str, str | None]:
    """(status, reason) for the monthly revenue step.

    by_market: what each market's page was this run (scrapers.revenue states).
    have / base: companies on record for the fetched month (after this run's
    save) and for the month before it, counted the same way.
    """
    problems = []
    for slug, (name, _) in _REVENUE_MARKETS.items():
        state, _n = by_market.get(slug, ("no_response", 0))
        if state == "unreadable":
            problems.append(f"{name}{_REVENUE_STATE_TEXT[state]}")
    if day <= _REVENUE_DEADLINE_DAY:
        for slug, (name, _) in _REVENUE_MARKETS.items():
            if by_market.get(slug, ("no_response", 0))[0] == "no_response":
                problems.append(f"{name}{_REVENUE_STATE_TEXT['no_response']}")
        if problems:
            return "failed", "；".join(problems)
        if all(by_market.get(slug, ("", 0))[0] == "unpublished" for slug in _REVENUE_MARKETS):
            return "skip", "尚未公布"
        return "ok", None

    # From the 11th the companies on record decide. A market that did not
    # answer tonight is fine if an earlier night already got it.
    for slug, (name, _) in _REVENUE_MARKETS.items():
        need = base.get(slug, 0)
        got = have.get(slug, 0)
        short = got < _REVENUE_MIN_RATIO * need if need else got == 0
        if short:
            state = by_market.get(slug, ("no_response", 0))[0]
            problems.append(f"{name} {fetched} 只有 {got} 家（{base_month} 有 {need} 家；"
                            f"這次：{_REVENUE_STATE_TEXT.get(state, state)}）")
    if problems:
        return "failed", "；".join(problems)
    return "ok", None


def _missing_funds_text(missing: list[str]) -> str:
    """The SITCA reason for the summary, kept to three lines like any trace:
    the summary has no splitting, and past 4096 characters Telegram drops it
    whole. The full list is in the log."""
    if len(missing) <= 3:
        return "\n".join(missing)
    codes = ", ".join(line.split("：", 1)[0] for line in missing)
    return f"{len(missing)} 檔缺：{codes}\n{missing[0]}\n（完整原因見 log）"


def update_date(trade_date: date) -> str:
    """Run the pipeline for one date and return what it amounted to (one of
    _OUTCOME_EXIT_CODES). A weekday run is recorded in tw.daily_update_runs."""
    if trade_date.weekday() >= 5:
        # Market closed. Skip the daily pipeline but still refresh the one
        # dataset that updates on a non-trading day: TDCC weekly 集保 →
        # chip_picks.json. Saturday is when TDCC publishes; Sunday re-runs are
        # a no-op (already in DB, no JSON diff, nothing pushed).
        print(f"[{trade_date}] weekend — chip-only update (集保 → chip_picks.json).")
        update_chip_only(trade_date)
        return "weekend"

    # Created here so that what finished before an unhandled exception is
    # still on record.
    results: list[tuple[str, str]] = []
    failure_traces: dict[str, str] = {}
    run_id = _run_start(trade_date)
    try:
        outcome, summary_sent = _update_trading_day(trade_date, results, failure_traces)
    except BaseException:
        # BaseException: a Ctrl+C during a manual re-run would otherwise leave
        # 'running' as the newest record of a date that had already succeeded.
        results.append((UNHANDLED_STEP, "failed"))
        lines = [l for l in traceback.format_exc().splitlines() if l.strip()]
        failure_traces[UNHANDLED_STEP] = "\n".join(lines[-3:])
        _run_finish(run_id, trade_date, "failed", results, failure_traces, None)
        raise
    _run_finish(run_id, trade_date, outcome, results, failure_traces, summary_sent)
    return outcome


# ---------------------------------------------------------------------------
# 攻防 inputs
# ---------------------------------------------------------------------------
_STANCE_LOOKBACK = 10   # trading days re-checked each night, up to tonight
# Frame columns each stance input fills; NaN there means the row exists but
# the latch voted on a hole (inner merge keeps rows with NULL values).
_STANCE_VALUE_COLUMNS = ("mgn", "noi", "tx", "st", "c", "v")
_OPTIONS_SINCE = date(2023, 6, 26)   # first day of tw.taifex_inst_options
# Where each input comes from, to name what a gap day is missing. The filters
# copy analysis.market_thermometer._load_merged_frame; they only name the
# source -- whether a day is a gap is read off the stance frame itself.
_STANCE_SOURCES = (
    ("融資餘額", "SELECT 1 FROM tw.margin_summary WHERE trade_date = %s "
                "AND margin_balance_value IS NOT NULL"),
    ("期交所外資期貨", "SELECT 1 FROM tw.taifex_inst_futures WHERE trade_date = %s "
                    "AND product = '臺股期貨' AND investor = '外資及陸資' AND net_oi IS NOT NULL"),
    ("加權指數", "SELECT 1 FROM tw.index_prices WHERE trade_date = %s "
                "AND index_id = 'TAIEX' AND close_price IS NOT NULL"),
    ("市場廣度", "SELECT 1 FROM tw.market_breadth WHERE trade_date = %s "
                "AND short_trend_total IS NOT NULL"),
    ("期交所台指期", "SELECT 1 FROM tw.taifex_futures_daily WHERE trade_date = %s "
                  "AND contract = 'TX' AND session = '一般' AND contract_month NOT LIKE '%%/%%' "
                  "AND volume IS NOT NULL AND close_price IS NOT NULL"),
)
_OPTIONS_SOURCE = ("期交所外資選擇權",
                   "SELECT DISTINCT call_put FROM tw.taifex_inst_options WHERE trade_date = %s "
                   "AND product LIKE '臺指%%' AND investor LIKE '外資%%' AND net_oi IS NOT NULL")


def _stance_gaps(trade_date: date) -> list[str]:
    """One line per recent trading day the 攻防 stance is short of an input.

    A day is short when the stance frame has no row for it (one of the five
    inner-merged tables missed it), when the row holds a NaN input, or, from
    the day the options table starts, when the options vote has nothing to
    read. Each line names the missing sources and the trading day whose
    buy/sell gate had (or will have) nothing to go on. Read-only.
    """
    import math
    from analysis.market_thermometer import daily_stance_frame
    with get_cursor(commit=False) as cur:
        m = daily_stance_frame(cur)
        cur.execute("SELECT trade_date FROM tw.index_prices WHERE index_id = 'TAIEX' "
                    "AND trade_date <= %s ORDER BY trade_date DESC LIMIT %s",
                    (trade_date, _STANCE_LOOKBACK))
        days = sorted(r["trade_date"] for r in cur.fetchall())
        rows = {d.date(): i for i, d in enumerate(m["d"])}
        spine_start = min(rows) if rows else None

        def short(day):
            i = rows.get(day)
            if i is None:
                return True
            if any(math.isnan(float(m[col].iloc[i])) for col in _STANCE_VALUE_COLUMNS):
                return True
            return day >= _OPTIONS_SINCE and math.isnan(float(m["opt_bull"].iloc[i]))

        def missing_sources(day):
            missing = []
            for label, sql in _STANCE_SOURCES:
                cur.execute(sql, (day,))
                if cur.fetchone() is None:
                    missing.append(label)
            if day >= _OPTIONS_SINCE:
                label, sql = _OPTIONS_SOURCE
                cur.execute(sql, (day,))
                if not {"C", "P"} <= {r["call_put"] for r in cur.fetchall()}:
                    missing.append(label)
            return missing

        # The trading day after trade_date, when a backfill makes it known.
        cur.execute("SELECT MIN(trade_date) AS d FROM tw.index_prices "
                    "WHERE index_id = 'TAIEX' AND trade_date > %s", (trade_date,))
        after = cur.fetchone()["d"]

        lines = []
        for k, day in enumerate(days):
            if spine_start is None or day < spine_start or not short(day):
                continue
            sources = "、".join(missing_sources(day)) or \
                "無法判定（見 analysis.market_thermometer._load_merged_frame）"
            affected = days[k + 1] if k + 1 < len(days) else after
            effect = (f"{affected:%m-%d} 的 buy／sell 攻防閘門沒有依據" if affected
                      else "明晚 buy／sell 的攻防閘門會沒有依據")
            lines.append(f"{day:%m-%d} 缺：{sources}；{effect}。補跑：daily_update.exe {day}")
    # Newest first: the summary cuts a long reason from the end, and tonight's
    # gap is the one still to be acted on.
    return lines[::-1]


def _update_trading_day(trade_date: date, results: list[tuple[str, str]],
                        failure_traces: dict[str, str]) -> tuple[str, bool | None]:
    """Run all scrapers and derived steps for a weekday, appending each step's
    (label, status) to results. Returns (outcome, whether the Telegram summary
    was delivered); the latter is None when the gate stopped the run."""
    print(f"\n{'='*60}")
    print(f"  Daily update: {trade_date}")
    print(f"{'='*60}")

    gate, gate_ok, gate_trace = _trading_day_gate(trade_date)
    if gate != "run":
        return ("holiday" if gate == "holiday" else "aborted"), None

    results.append((INDEX_SCRAPER[0], "ok" if gate_ok else "failed"))
    if not gate_ok and gate_trace:
        failure_traces[INDEX_SCRAPER[0]] = gate_trace

    judge_rows = (date.today() - trade_date).days <= EXPECT_ROWS_MAX_AGE_DAYS
    if not judge_rows:
        print(f"\n[BACKFILL] {trade_date} is older than "
              f"{EXPECT_ROWS_MAX_AGE_DAYS} days — empty-result checks disabled.")

    for label, module_path, func_name, expect_rows in SCRAPERS:
        print(f"\n--- {label} ---")
        success, trace = run_scraper(
            label, module_path, func_name, trade_date,
            expect_rows=expect_rows and judge_rows, recent=judge_rows,
        )
        results.append((label, "ok" if success else "failed"))
        if not success and trace:
            failure_traces[label] = trace

    # Monthly revenue: fetch during the publication window (1st–15th); from
    # the 11th the companies on record decide (see _revenue_verdict).
    if trade_date.day <= 15:
        print(f"\n--- Monthly revenue ---")
        try:
            from scrapers.revenue import scrape_month
            fetched, base_month = _revenue_months(trade_date)
            y, m = (int(part) for part in fetched.split("-"))
            by_market = scrape_month(y, m)
            have = base = {}
            if trade_date.day > _REVENUE_DEADLINE_DAY:
                have, base = _revenue_counts(fetched), _revenue_counts(base_month)
            print(f"  pages: {by_market}; on record {fetched}: {have}, "
                  f"{base_month}: {base}")
            status, reason = _revenue_verdict(by_market, trade_date.day, have, base,
                                              fetched, base_month)
            if reason:
                print(f"  [{status.upper()}] 月營收: {reason}")
            elif status == "ok" and any(s != "ok" for s, _ in by_market.values()):
                print(f"  [NOTE] 月營收: this run's pages {by_market}, "
                      f"but {fetched} is already on record: {have}")
            if status == "failed":
                failure_traces["月營收"] = reason
            results.append(("月營收", status))
        except Exception:
            print("  [ERROR] Monthly revenue scraper failed:")
            _capture_trace(failure_traces, "月營收")
            results.append(("月營收", "failed"))
    else:
        results.append(("月營收", "skip"))

    # SITCA fund holdings: monthly top-10 (available ~10th business day).
    # Import + run are split so a failed import (missing bs4 etc.) marks the
    # step as failed instead of triggering UnboundLocalError on
    # `except PeriodNotAvailable` and killing the whole daily_update run.
    if trade_date.day <= 20:
        print(f"\n--- SITCA monthly fund holdings ---")
        try:
            from scrapers.sitca import scrape_monthly, PeriodNotAvailable
        except ImportError:
            print("  [ERROR] SITCA module import failed:")
            _capture_trace(failure_traces, "SITCA 月持股")
            results.append(("SITCA 月持股", "failed"))
        else:
            try:
                m = trade_date.month - 1
                y = trade_date.year
                if m == 0:
                    m = 12
                    y -= 1
                missing = scrape_monthly(f"{y}{m:02d}")
                if missing:
                    failure_traces["SITCA 月持股"] = _missing_funds_text(missing)
                results.append(("SITCA 月持股", "failed" if missing else "ok"))
            except PeriodNotAvailable as e:
                print(f"  [SKIP] {e}")
                results.append(("SITCA 月持股", "skip"))
            except Exception:
                print("  [ERROR] SITCA monthly scraper failed:")
                _capture_trace(failure_traces, "SITCA 月持股")
                results.append(("SITCA 月持股", "failed"))
    else:
        results.append(("SITCA 月持股", "skip"))

    # SITCA quarterly holdings: available ~15th of quarter-end+1 month
    quarter_end_months = {1: 12, 2: 12, 4: 3, 5: 3, 7: 6, 8: 6, 10: 9, 11: 9}
    if trade_date.month in quarter_end_months and trade_date.day <= 20:
        print(f"\n--- SITCA quarterly fund holdings ---")
        try:
            from scrapers.sitca import scrape_quarterly, PeriodNotAvailable
        except ImportError:
            print("  [ERROR] SITCA module import failed:")
            _capture_trace(failure_traces, "SITCA 季持股")
            results.append(("SITCA 季持股", "failed"))
        else:
            try:
                qm = quarter_end_months[trade_date.month]
                qy = trade_date.year if qm < trade_date.month else trade_date.year - 1
                missing = scrape_quarterly(f"{qy}{qm:02d}")
                if missing:
                    failure_traces["SITCA 季持股"] = _missing_funds_text(missing)
                results.append(("SITCA 季持股", "failed" if missing else "ok"))
            except PeriodNotAvailable as e:
                print(f"  [SKIP] {e}")
                results.append(("SITCA 季持股", "skip"))
            except Exception:
                print("  [ERROR] SITCA quarterly scraper failed:")
                _capture_trace(failure_traces, "SITCA 季持股")
                results.append(("SITCA 季持股", "failed"))
    else:
        results.append(("SITCA 季持股", "skip"))

    # Signal scanning: run after SITCA monthly scraper on publication days
    if trade_date.day <= 20:
        print(f"\n--- Signal scanning ---")
        try:
            from scan_signals import scan_period
            with get_cursor(commit=False) as cur:
                cur.execute("SELECT MAX(period) FROM tw.fund_holdings_monthly")
                latest = list(cur.fetchone().values())[0]
            if latest:
                scan_period(latest, trade_date)
                results.append(("基金信號掃描", "ok"))
            else:
                # Nothing was scanned; used to be recorded "ok" all the same.
                print("  [ERROR] tw.fund_holdings_monthly has no period; nothing to scan.")
                failure_traces["基金信號掃描"] = "基金持股表沒有任何期別"
                results.append(("基金信號掃描", "failed"))
        except Exception:
            print("  [ERROR] Signal scanning failed:")
            _capture_trace(failure_traces, "基金信號掃描")
            results.append(("基金信號掃描", "failed"))
    else:
        results.append(("基金信號掃描", "skip"))

    # Daily ETF signal scan (runs every trading day, after ETF holdings scraper)
    etf_signal_count = 0
    print(f"\n--- ETF signal scan (daily) ---")
    try:
        from strategies.registry import scan_daily, save_signals
        from signals.etf_multi_exit import scan as scan_etf_multi_exit
        from signals.etf_consecutive_reduction import scan as scan_etf_consecutive_reduction
        from signals.etf_abnormal_exit import scan as scan_etf_abnormal_exit
        with get_cursor() as cur:
            # The scans read only the diffs; a day with none would have the
            # windowed scans re-issue the last days' signals under this date.
            # The holdings failure behind it is reported by its own step.
            cur.execute("SELECT 1 FROM tw.etf_holdings_diff WHERE trade_date = %s LIMIT 1",
                        (trade_date,))
            stored = cur.fetchone() is not None
            if not stored:
                print(f"  [SKIP] no ETF holdings diff stored for {trade_date}.")
            else:
                signals = scan_daily(trade_date, cur)
                # Append standalone short ETF signals
                signals.extend(scan_etf_multi_exit(trade_date, cur))
                signals.extend(scan_etf_consecutive_reduction(trade_date, cur))
                signals.extend(scan_etf_abnormal_exit(trade_date, cur))
                if signals:
                    n = save_signals(signals, cur)
                    etf_signal_count = len(signals)
                    by_type = {}
                    for s in signals:
                        by_type.setdefault(s["signal_type"], []).append(s)
                    for stype, items in sorted(by_type.items()):
                        tickers = ", ".join(s["ticker"] for s in items[:5])
                        suffix = f" +{len(items)-5}" if len(items) > 5 else ""
                        print(f"  {stype}: {len(items)} ({tickers}{suffix})")
                else:
                    print("  No ETF signals.")
        results.append(("ETF 信號掃描", "ok" if stored else "blocked"))
    except Exception:
        print("  [ERROR] ETF signal scan failed:")
        _capture_trace(failure_traces, "ETF 信號掃描")
        results.append(("ETF 信號掃描", "failed"))

    # Hermit-stock fundamental screener snapshot.
    # Runs every weekday: ~20s, writes top-50 to tw.hermit_screen_snapshot,
    # diffs vs previous snapshot to flag NEW entrants / EXITs / big movers.
    # Invoked as a subprocess through hermit_stock's own uv-managed venv to
    # avoid dependency conflicts (hermit_stock pulls pydantic/typer/etc.
    # which are not in this parent project's venv).
    print(f"\n--- Hermit-stock fundamental snapshot ---")
    try:
        import os as _os
        import subprocess as _sp
        # PyInstaller exe: __file__ points to temp _MEI dir which has no
        # hermit_stock subfolder. Use sys.executable's parent.parent to reach
        # the real repo root (mirrors _git_push_frontend).
        if getattr(sys, 'frozen', False):
            _repo = Path(sys.executable).parent.parent
        else:
            _repo = Path(__file__).parent
        hs_dir = _repo / "hermit_stock"
        # Force UTF-8 stdout/stderr in the child Python; otherwise it inherits
        # Windows cp950 and chokes on Unicode chars like ≥ in print(...).
        _env = {**_os.environ, "PYTHONIOENCODING": "utf-8"}
        proc = _sp.run(
            ["uv", "run", "python", "-m", "hermit_stock.daily_check",
             trade_date.isoformat()],
            cwd=str(hs_dir),
            capture_output=True, text=True,
            encoding="utf-8", errors="replace",  # tolerate cp950 in uv warnings
            timeout=300, check=False,
            env=_env,
        )
        if proc.stdout:
            print(proc.stdout, end="")
        if proc.returncode != 0:
            stderr = proc.stderr or "<no stderr>"
            print(stderr, end="")
            stderr_lines = [l for l in stderr.splitlines() if l.strip()]
            if stderr_lines:
                failure_traces["贏勢股快照"] = "\n".join(stderr_lines[-3:])
            results.append(("贏勢股快照", "failed"))
        else:
            results.append(("贏勢股快照", "ok"))
    except Exception:
        print("  [ERROR] Hermit-stock daily check failed:")
        _capture_trace(failure_traces, "贏勢股快照")
        results.append(("贏勢股快照", "failed"))

    # Detect delisted stocks after all price scrapers have run
    print(f"\n--- Delist detection ---")
    try:
        detect_delisted(trade_date)
        results.append(("下市偵測", "ok"))
    except Exception:
        print("  [ERROR] Delist detection failed:")
        _capture_trace(failure_traces, "下市偵測")
        results.append(("下市偵測", "failed"))

    # Downstream gate: if any CRITICAL scraper failed (e.g. TWSE prices),
    # market_breadth / daily_liquidity / daily_snapshot would compute on
    # half the universe and export would publish a partial positions.json
    # to Vercel. Skip the whole tail rather than ship half-empty data.
    critical_failed = [l for l, st in results
                       if st == "failed" and l in CRITICAL_SCRAPER_LABELS]

    # Market breadth aggregate (depends on close/money/volume per stock).
    breadth_days = 0
    breadth_ok = False
    market_vol_ok = False
    snapshot_ok = False
    print(f"\n--- Market breadth ---")
    if critical_failed:
        print(f"  [SKIP] critical scraper(s) failed: {', '.join(critical_failed)}; "
              f"breadth would be partial.")
        results.append(("市場廣度", "blocked"))
    else:
        try:
            from analysis.market_breadth import calculate_market_breadth, save_market_breadth
            mb_results = calculate_market_breadth(last_n_days=3)
            # Coverage guard: scrapers can report "ok" yet return only part of
            # the universe (silent partial). The critical_failed gate above is
            # exception-based and misses this. Persisting a half-count row would
            # leave a permanent spike, since the 3-day rolling window never
            # revisits it. Reject the latest day if its active-stock count is
            # far below the recent norm.
            latest_mb = max(mb_results, key=lambda d: d.trade_date) if mb_results else None
            if latest_mb is not None:
                with get_cursor(commit=False) as cur:
                    cur.execute(
                        """
                        SELECT active_stocks FROM tw.market_breadth
                        WHERE active_stocks > 0 AND trade_date < %s
                        ORDER BY trade_date DESC LIMIT 20
                        """,
                        (latest_mb.trade_date,),
                    )
                    recent = sorted(r["active_stocks"] for r in cur.fetchall())
                if recent:
                    median = recent[len(recent) // 2]
                    if latest_mb.active_stocks < 0.7 * median:
                        raise RuntimeError(
                            f"breadth coverage too low: {latest_mb.active_stocks} "
                            f"active stocks vs recent median {median} — likely a "
                            f"partial scrape; refusing to persist."
                        )
            breadth_days = save_market_breadth(mb_results)
            print(f"  Updated {breadth_days} day(s) of market_breadth.")
            results.append(("市場廣度", "ok"))
            breadth_ok = True
        except Exception:
            print("  [ERROR] Market breadth computation failed:")
            _capture_trace(failure_traces, "市場廣度")
            results.append(("市場廣度", "failed"))

    # Daily stock liquidity: money_level / dead_fish / halted / on_alert.
    # Consumed by the intraday ORB pipeline to exclude untradable names.
    print(f"\n--- Daily liquidity ---")
    if critical_failed:
        print(f"  [SKIP] critical scraper(s) failed: {', '.join(critical_failed)}; "
              f"liquidity would be partial.")
        results.append(("每日流動性", "blocked"))
    else:
        try:
            from analysis.daily_liquidity import compute_daily_liquidity
            n = compute_daily_liquidity(trade_date)
            print(f"  Stored liquidity rows: {n}")
            # About 2,200 rows on every trading day; none means the prices it
            # reads were not there, and the snapshot's universe would be empty.
            if n == 0 and judge_rows:
                failure_traces["每日流動性"] = "流動性 0 筆"
                results.append(("每日流動性", "failed"))
            else:
                results.append(("每日流動性", "ok"))
        except Exception:
            print("  [ERROR] Daily liquidity computation failed:")
            _capture_trace(failure_traces, "每日流動性")
            results.append(("每日流動性", "failed"))

    # Market volatility baseline: the daily cross-sectional median Parkinson-233 vol.
    # The ScoreBoard 波動 cell divides each stock's volatility by this to get its relative
    # volatility, so a stale row scores the whole universe against the wrong market level.
    print(f"\n--- Market volatility baseline ---")
    if critical_failed:
        print(f"  [SKIP] critical scraper(s) failed: {', '.join(critical_failed)}; "
              f"the median would be taken over half the universe.")
        results.append(("市場波動基準", "blocked"))
    else:
        try:
            from analysis.build_market_vol import main as build_market_vol
            build_market_vol()
            results.append(("市場波動基準", "ok"))
            market_vol_ok = True
        except Exception:
            print("  [ERROR] Market volatility computation failed:")
            _capture_trace(failure_traces, "市場波動基準")
            results.append(("市場波動基準", "failed"))

    # 攻防 inputs. buy and sell gate on the previous TAIEX day's stance; a day
    # one of its inputs missed has no stance row, or votes on a hole, and
    # nothing else says so. Re-checked over the last few trading days, so a
    # gap nobody filled is reported every night. Holds nothing back: tonight's
    # signals read T-1, and no later step reads the stance inputs.
    print(f"\n--- 攻防資料 ---")
    if critical_failed or not breadth_ok:
        print("  [SKIP] prices or market breadth failed; the stance is short for "
              "that reason, already reported.")
        results.append(("攻防資料", "blocked"))
    elif not judge_rows:
        print(f"  [SKIP] {trade_date} is a backfill date; gaps are judged on recent nights.")
        results.append(("攻防資料", "skip"))
    else:
        try:
            gaps = _stance_gaps(trade_date)
            for gap in gaps:
                print(f"  [GAP] {gap}")
            if gaps:
                failure_traces["攻防資料"] = "\n".join(gaps)
                results.append(("攻防資料", "failed"))
            else:
                results.append(("攻防資料", "ok"))
        except Exception:
            print("  [ERROR] stance input check failed:")
            _capture_trace(failure_traces, "攻防資料")
            results.append(("攻防資料", "failed"))

    # Combined daily snapshot — score top-300 long/short + 6 signal-factory
    # fires + unified-strategy open positions, all in one per-stock pass.
    # Skip if market_breadth failed: load_stock_data pulls market_state from
    # tw.market_breadth, and stale rows would silently degrade every output.
    # Same for market_vol, which the 波動 cell divides by.
    print(f"\n--- Daily snapshot (score + signal + positions) ---")
    if critical_failed:
        print(f"  [SKIP] critical scraper(s) failed: {', '.join(critical_failed)}.")
        results.append(("多空評比 + 操作訊號 + 策略持倉快照", "blocked"))
    elif not breadth_ok:
        print("  [SKIP] market_breadth failed; daily_snapshot needs fresh market_state.")
        results.append(("多空評比 + 操作訊號 + 策略持倉快照", "blocked"))
    elif not market_vol_ok:
        print("  [SKIP] market_vol failed; the 波動 cell needs a fresh market baseline.")
        results.append(("多空評比 + 操作訊號 + 策略持倉快照", "blocked"))
    else:
        try:
            from analysis.daily_snapshot import run as run_daily_snapshot
            run_daily_snapshot(trade_date)
            results.append(("多空評比 + 操作訊號 + 策略持倉快照", "ok"))
            snapshot_ok = True
        except Exception:
            print("  [ERROR] Daily snapshot failed:")
            _capture_trace(failure_traces, "多空評比 + 操作訊號 + 策略持倉快照")
            results.append(("多空評比 + 操作訊號 + 策略持倉快照", "failed"))

    # Pull the next-trading-day disposal batch (announced this evening,
    # period_start = today+1) BEFORE the export so positions.json's
    # disposal_status reflects the DEFINITIVE announcement rather than a
    # prediction. Keyed on date.today() to match _get_disposal_status's own
    # "next trading day" lookup. Non-blocking, but still reported: a silent
    # failure here quietly reverts positions.json's disposal_status to the
    # prediction, which looks identical to a day with no disposals.
    print(f"\n--- 明日處置名單抓取 ---")
    try:
        from scrapers.stock_alerts import scrape_date as _scrape_alerts
        from telegram_bot.handlers.score import _next_trading_day
        problem = _result_problem(_scrape_alerts(_next_trading_day(date.today())),
                                  expect_rows=False, recent=True)
        if problem:
            print(f"  [SOFT-FAIL] 明日處置名單抓取: {problem}")
            failure_traces["明日處置名單抓取"] = problem
            results.append(("明日處置名單抓取", "failed"))
        else:
            results.append(("明日處置名單抓取", "ok"))
    except Exception:
        print("  [ERROR] next-day disposal scrape failed:")
        _capture_trace(failure_traces, "明日處置名單抓取")
        results.append(("明日處置名單抓取", "failed"))

    # Export JSON + git push for Vercel auto-deploy
    print(f"\n--- Frontend export + deploy ---")
    if critical_failed:
        print(f"  [SKIP] critical scraper(s) failed: {', '.join(critical_failed)}; "
              f"would publish partial positions.json to Vercel.")
        results.append(("前端匯出部署", "blocked"))
    elif not snapshot_ok:
        # Scores and positions would stay on the previous day while every
        # other page moved to today, and nothing on the site would show it.
        print("  [SKIP] daily snapshot did not complete; the site stays on the "
              "previous day as a whole.")
        results.append(("前端匯出部署", "blocked"))
    else:
        try:
            from export.generate import export_all
            export_all()
            _git_push_frontend()
            results.append(("前端匯出部署", "ok"))
        except Exception:
            print("  [ERROR] Export/deploy failed:")
            _capture_trace(failure_traces, "前端匯出部署")
            results.append(("前端匯出部署", "failed"))

    # Disposal prediction audit. Logs discrepancies to
    # tw.disposal_prediction_audit + pushes them to Telegram. Non-critical —
    # failures don't abort daily_update.
    print(f"\n--- 處置預測 audit ---")
    try:
        from analysis.disposal_audit import run_audit
        # Audit trade_date itself, not the next trading day. The scrape for
        # date X only lands disposals with period_start <= X (latest = X);
        # period_start = X+1 is announced X's session but isn't captured
        # until X+1's run. Auditing the future date compared predictions
        # against actuals that weren't in the DB yet, producing spurious
        # false positives that never self-corrected. The live forewarning
        # (predict_disposal_trigger / intraday) is unaffected — this only
        # changes when the backward-looking accuracy check is scored.
        audit_target = trade_date
        audited, _, _ = run_audit(audit_target, push=True)
        # Every night since it went live (2026-05-22) audited 427-760 stocks:
        # those with an attention notice in the last 60 days plus that day's
        # disposals. Zero means there was no previous trading day or no alert
        # data at all. It cannot see tonight's alerts missing (the window is
        # 60 days); the Stock alerts step reports that.
        if audited == 0 and judge_rows:
            failure_traces["處置預測 audit"] = (
                "稽核 0 檔：沒有前一交易日，或近 60 天沒有注意股、當天處置股資料")
            results.append(("處置預測 audit", "failed"))
        else:
            results.append(("處置預測 audit", "ok"))
    except Exception:
        print("  [ERROR] Audit failed:")
        _capture_trace(failure_traces, "處置預測 audit")
        results.append(("處置預測 audit", "failed"))

    # Full-market daily signal digest → Telegram. Self-gating: stays silent
    # on backfill runs (trade_date is not the newest snapshot), on no-fire
    # days, and on re-runs of an already-pushed date. Non-critical.
    print(f"\n--- 日線訊號推播 ---")
    if not snapshot_ok:
        # The snapshot can fail after its tables are written. Pushing those
        # signals while the export is held back would have Telegram and the
        # site disagree.
        print("  [SKIP] daily snapshot did not complete.")
        results.append(("日線訊號推播", "blocked"))
    else:
        try:
            from telegram_bot.push_daily_signals import push_daily_signals
            if push_daily_signals(trade_date) == 2:
                failure_traces["日線訊號推播"] = "Telegram 傳送失敗"
                results.append(("日線訊號推播", "failed"))
            else:
                results.append(("日線訊號推播", "ok"))
        except Exception:
            print("  [ERROR] Daily signal push failed:")
            _capture_trace(failure_traces, "日線訊號推播")
            results.append(("日線訊號推播", "failed"))

    # Two-week investor conference calendar → Telegram. Anchored on the wall
    # clock rather than trade_date, and self-gating on its own sentinel, so a
    # backfill range pushes once instead of once per day. Silent when no
    # conference falls inside the window. Non-critical.
    print(f"\n--- 法說會預告推播 ---")
    try:
        from telegram_bot.push_conference_calendar import push_conference_calendar
        if push_conference_calendar() == 2:
            failure_traces["法說會預告推播"] = "Telegram 傳送失敗"
            results.append(("法說會預告推播", "failed"))
        else:
            results.append(("法說會預告推播", "ok"))
    except Exception:
        print("  [ERROR] Conference calendar push failed:")
        _capture_trace(failure_traces, "法說會預告推播")
        results.append(("法說會預告推播", "failed"))

    # -----------------------------------------------------------------------
    # Final summary (Chinese)
    # -----------------------------------------------------------------------
    ok_list     = [r for r in results if r[1] == "ok"]
    failed_list = [r for r in results if r[1] == "failed"]
    skip_list   = [r for r in results if r[1] == "skip"]
    blocked_list = [r for r in results if r[1] == "blocked"]

    print(f"\n{'='*60}")
    print(f"  每日更新總結：{trade_date}")
    print(f"{'='*60}")
    print(f"  成功：{len(ok_list)}　失敗：{len(failed_list)}　跳過：{len(skip_list)}"
          f"　未執行：{len(blocked_list)}")
    if failed_list:
        print(f"  [X] 失敗項目：{', '.join(r[0] for r in failed_list)}")
    if blocked_list:
        print(f"  [X] 因上游失敗未執行：{', '.join(r[0] for r in blocked_list)}")
    if etf_signal_count:
        print(f"  - ETF 信號：{etf_signal_count} 筆")
    if breadth_days:
        print(f"  - 市場廣度：更新 {breadth_days} 天")
    if skip_list:
        print(f"  - 跳過（非執行區間）：{', '.join(r[0] for r in skip_list)}")
    status = "全部成功" if not (failed_list or blocked_list) else "有失敗項目"
    print(f"\n  最終狀態：{status}")
    print(f"{'='*60}")

    # Telegram summary: silent if TELEGRAM_BOT_TOKEN is not configured.
    try:
        summary_sent = bool(send_sync(
            _build_daily_update_message(trade_date, results, failure_traces)))
    except Exception:
        traceback.print_exc()
        summary_sent = False

    print()
    return _run_outcome(results), summary_sent


def update_range(start: date, end: date) -> str:
    """The worst outcome among the days of the range."""
    current = start
    worst = "success"
    while current <= end:
        outcome = update_date(current)
        if _severity(outcome) > _severity(worst):
            worst = outcome
        current += timedelta(days=1)
    return worst


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def _main(args: list[str]) -> int:
    """Run for the dates in args and return the process exit code.

    The exit code matters: it is all Task Scheduler's LastTaskResult keeps of
    a run, and a silent 0 is what let 2026-09-18 go unnoticed for two days.
    """
    close_run_log = _open_run_log()
    try:
        from db.connection import init_db
        print("Initializing database schema ...")
        init_db()
        print()

        if len(args) == 0:
            outcome = update_date(date.today())
        elif len(args) == 1:
            outcome = update_date(date.fromisoformat(args[0]))
        elif len(args) == 2:
            outcome = update_range(date.fromisoformat(args[0]), date.fromisoformat(args[1]))
        else:
            print("Usage: python daily_update.py [start_date] [end_date]")
            return 1
        print(f"Outcome: {outcome} (exit code {_exit_code(outcome)})")
        return _exit_code(outcome)
    except Exception as e:
        print(f"\n[ERROR] {e}")
        traceback.print_exc()
        return 1
    finally:
        # Before the caller's pause_before_exit(); see _open_run_log.
        close_run_log()


if __name__ == "__main__":
    # Required when packaged as a Windows exe — multiprocessing workers
    # spawn by re-running the executable, and without freeze_support they'd
    # re-execute __main__ recursively. No-op in unfrozen Python. It also has
    # to come before _main(): a worker must not get as far as opening a run
    # log of its own.
    from multiprocessing import freeze_support
    freeze_support()

    exit_code = _main(sys.argv[1:])
    pause_before_exit()
    sys.exit(exit_code)
