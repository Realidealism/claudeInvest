"""
Daily market data update script.

Usage:
  python daily_update.py              # update today
  python daily_update.py 2026-04-02   # update specific date
  python daily_update.py 2026-04-01 2026-04-07  # update date range

Each scraper runs independently; failures are logged but do not stop the rest.
"""

import io
import sys
import traceback
from datetime import date, timedelta
from pathlib import Path

from db.connection import get_cursor
from telegram_bot.notify import send_sync
from utils.console import pause_before_exit
from utils.format_shift import ScrapeResult, SHIFT_ERROR_RATE, SHIFT_MIN_ROWS

# ---------------------------------------------------------------------------
# Scraper registry — order matters (prices first, derived data last)
# ---------------------------------------------------------------------------
# Trading-day gate: TAIEX is scraped first; absent TAIEX => non-trading day.
INDEX_SCRAPER = ("Market indices", "scrapers.index_prices", "scrape_date")

# Labels in SCRAPERS whose failure invalidates everything downstream
# (daily_liquidity, market_breadth, daily_snapshot, export). When any of
# these fails after retries, the downstream steps skip rather than write
# a partial snapshot that gets shipped to Vercel.
CRITICAL_SCRAPER_LABELS = {
    "TWSE daily prices",
    "TPEx daily prices",
}

# Fourth field is expect_rows: True when the source must yield rows on every
# trading day, so records==0 means the scraper failed silently (HTTP 200 with
# an empty body, or a swallowed request error) rather than "nothing happened
# today". False sources legitimately return 0 rows and are exempt — each one
# carries the reason inline. Keep the flag in the tuple rather than a separate
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
    # ETF holdings — exempt: scrape_date() returns None (no ScrapeResult), so
    # there is no row count to judge. Needs a return contract before it can be
    # monitored.
    ("ETF holdings",            "scrapers.etf_holdings",     "scrape_date", False),
    # Securities lending
    ("SBL (借券賣出)",           "scrapers.securities_lending", "scrape_date", True),
    # Day trading
    ("Day trading (當沖)",       "scrapers.day_trading",        "scrape_date", True),
    # TAIFEX futures/options (期貨/選擇權) — non-critical; after-hours session
    # for the same day may be incomplete and self-corrects on the next run.
    # Exempt for that same reason: a same-day 0 is a timing artifact.
    ("TAIFEX futures",          "scrapers.taifex_futures",       "scrape_date", False),
    ("TAIFEX institutional",    "scrapers.taifex_institutional", "scrape_date", False),
    ("TAIFEX P/C ratio",        "scrapers.taifex_pc_ratio",      "scrape_date", False),
    # Alerts — event-driven; exempt (no alert issued today => 0 rows)
    ("Stock alerts (注意/處置)", "scrapers.stock_alerts",       "scrape_date", False),
    # Forward-looking board (除權除息預告) — feeds the morning-brief ex-dividend alert
    # Event-driven; exempt (no upcoming ex-dividend => 0 rows)
    ("Dividend calendar (除權息)", "scrapers.dividend_calendar", "scrape_date", False),
    # Forward-looking board (停券/融券最後回補日) — covering-squeeze calendar
    # Event-driven; exempt
    ("Short-cover calendar (停券)", "scrapers.short_cover_calendar", "scrape_date", False),
    # Current-year AGM board (股東會開會日期, OpenAPI) — AGMs cluster in Q2;
    # exempt (0 rows outside the season is normal)
    ("Shareholder meetings (股東會)", "scrapers.shareholder_meetings", "scrape_date", False),
    # Weekly/monthly (idempotent, run once per period) — all exempt: the
    # _run_completed guard makes 0 rows the norm on every day except the first
    # run of each period.
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


def _result_problem(result, expect_rows: bool) -> str | None:
    """Judge a scraper's ScrapeResult, returning a reason string when it
    represents a silent failure, or None when it looks healthy.

    Catches the two failure modes that never raise: an empty response (HTTP
    200 with no data, or a request error swallowed into an empty list) and a
    format shift (the API still answers but most rows no longer parse).
    """
    if not isinstance(result, ScrapeResult):
        # No return contract to judge (e.g. ETF holdings returns None).
        return None

    if expect_rows and result.records == 0:
        return f"抓到 0 筆資料（此來源每個交易日應有資料）：{result}"

    # A multi-leg scraper sums every leg into records, so one dead leg still
    # leaves records far above zero. Judge the legs individually.
    if expect_rows and result.failed_sources:
        return (f"以下資料來源沒有回傳資料："
                f"{'、'.join(result.failed_sources)}（其餘來源共 {result.records} 筆）")

    # Same criteria as historical_update._check_shift, shared thresholds.
    if result.api_rows >= SHIFT_MIN_ROWS and result.error_rate > SHIFT_ERROR_RATE:
        return (
            f"疑似 API 格式改版：{result.parse_errors}/{result.api_rows} 列解析失敗 "
            f"（{result.error_rate:.0%}，門檻 {SHIFT_ERROR_RATE:.0%}）"
        )

    return None


def run_scraper(
    label: str, module_path: str, func_name: str, trade_date: date,
    expect_rows: bool = False,
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
            problem = _result_problem(fn(trade_date), expect_rows)
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
    skip_n = sum(1 for _, s in results if s == "skip")
    counts = f"成功 {ok_n} / 失敗 {len(failed)} / 跳過 {skip_n}"
    if failed:
        critical_failed = [l for l in failed if l in CRITICAL_SCRAPER_LABELS]
        lines = [
            f"[每日更新] {trade_date}",
            f"失敗：{', '.join(failed)}",
            counts,
        ]
        if critical_failed:
            lines.append(
                f"【關鍵抓檔失敗】{', '.join(critical_failed)}，"
                f"下游已跳過，前端未更新"
            )
        for label in failed:
            tr = failure_traces.get(label)
            if tr:
                lines.append(f"── {label} ──")
                lines.append(tr)
        lines.append(f"重跑指令：daily_update.exe {trade_date}")
        return "\n".join(lines)
    return f"[每日更新] {trade_date} 全部成功（{counts}）"


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


def update_date(trade_date: date):
    """Run all scrapers for a single trading date."""
    if trade_date.weekday() >= 5:
        # Market closed. Skip the daily pipeline but still refresh the one
        # dataset that updates on a non-trading day: TDCC weekly 集保 →
        # chip_picks.json. Saturday is when TDCC publishes; Sunday re-runs are
        # a no-op (already in DB, no JSON diff, nothing pushed).
        print(f"[{trade_date}] weekend — chip-only update (集保 → chip_picks.json).")
        update_chip_only(trade_date)
        return

    print(f"\n{'='*60}")
    print(f"  Daily update: {trade_date}")
    print(f"{'='*60}")

    # Trading-day gate: TAIEX index must exist for the date to be valid.
    print(f"\n--- {INDEX_SCRAPER[0]} (trading-day gate) ---")
    # expect_rows=False: a holiday legitimately has no TAIEX row; the gate is
    # enforced by _has_taiex() below instead.
    gate_ok, gate_trace = run_scraper(*INDEX_SCRAPER, trade_date, expect_rows=False)
    if not _has_taiex(trade_date):
        print(f"\n[HOLIDAY] {trade_date} has no TAIEX data — skipping remaining scrapers.")
        return

    # Track results: list of (label, status) where status is "ok", "failed", "skip"
    results = []
    failure_traces: dict[str, str] = {}
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
            expect_rows=expect_rows and judge_rows,
        )
        results.append((label, "ok" if success else "failed"))
        if not success and trace:
            failure_traces[label] = trace

    # Monthly revenue: fetch during the publication window (1st–12th)
    if trade_date.day <= 15:
        print(f"\n--- Monthly revenue ---")
        try:
            from scrapers.revenue import scrape_month
            m = trade_date.month - 1
            y = trade_date.year
            if m == 0:
                m = 12
                y -= 1
            scrape_month(y, m)
            results.append(("月營收", "ok"))
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
                scrape_monthly(f"{y}{m:02d}")
                results.append(("SITCA 月持股", "ok"))
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
                scrape_quarterly(f"{qy}{qm:02d}")
                results.append(("SITCA 季持股", "ok"))
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
        results.append(("ETF 信號掃描", "ok"))
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
    print(f"\n--- Market breadth ---")
    if critical_failed:
        print(f"  [SKIP] critical scraper(s) failed: {', '.join(critical_failed)}; "
              f"breadth would be partial.")
        results.append(("市場廣度", "skip"))
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
        results.append(("每日流動性", "skip"))
    else:
        try:
            from analysis.daily_liquidity import compute_daily_liquidity
            n = compute_daily_liquidity(trade_date)
            print(f"  Stored liquidity rows: {n}")
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
        results.append(("市場波動基準", "skip"))
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

    # Combined daily snapshot — score top-300 long/short + 6 signal-factory
    # fires + unified-strategy open positions, all in one per-stock pass.
    # Skip if market_breadth failed: load_stock_data pulls market_state from
    # tw.market_breadth, and stale rows would silently degrade every output.
    # Same for market_vol, which the 波動 cell divides by.
    print(f"\n--- Daily snapshot (score + signal + positions) ---")
    if critical_failed:
        print(f"  [SKIP] critical scraper(s) failed: {', '.join(critical_failed)}.")
        results.append(("多空評比 + 操作訊號 + 策略持倉快照", "skip"))
    elif not breadth_ok:
        print("  [SKIP] market_breadth failed; daily_snapshot needs fresh market_state.")
        results.append(("多空評比 + 操作訊號 + 策略持倉快照", "skip"))
    elif not market_vol_ok:
        print("  [SKIP] market_vol failed; the 波動 cell needs a fresh market baseline.")
        results.append(("多空評比 + 操作訊號 + 策略持倉快照", "skip"))
    else:
        try:
            from analysis.daily_snapshot import run as run_daily_snapshot
            run_daily_snapshot(trade_date)
            results.append(("多空評比 + 操作訊號 + 策略持倉快照", "ok"))
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
        _scrape_alerts(_next_trading_day(date.today()))
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
        results.append(("前端匯出部署", "skip"))
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
        run_audit(audit_target, push=True)
        results.append(("處置預測 audit", "ok"))
    except Exception:
        print("  [ERROR] Audit failed:")
        _capture_trace(failure_traces, "處置預測 audit")
        results.append(("處置預測 audit", "failed"))

    # Full-market daily signal digest → Telegram. Self-gating: stays silent
    # on backfill runs (trade_date is not the newest snapshot), on no-fire
    # days, and on re-runs of an already-pushed date. Non-critical.
    print(f"\n--- 日線訊號推播 ---")
    try:
        from telegram_bot.push_daily_signals import push_daily_signals
        push_daily_signals(trade_date)
        results.append(("日線訊號推播", "ok"))
    except Exception:
        print("  [ERROR] Daily signal push failed:")
        _capture_trace(failure_traces, "日線訊號推播")
        results.append(("日線訊號推播", "failed"))

    # -----------------------------------------------------------------------
    # Final summary (Chinese)
    # -----------------------------------------------------------------------
    ok_list     = [r for r in results if r[1] == "ok"]
    failed_list = [r for r in results if r[1] == "failed"]
    skip_list   = [r for r in results if r[1] == "skip"]

    print(f"\n{'='*60}")
    print(f"  每日更新總結：{trade_date}")
    print(f"{'='*60}")
    print(f"  成功：{len(ok_list)}　失敗：{len(failed_list)}　跳過：{len(skip_list)}")
    if failed_list:
        print(f"  [X] 失敗項目：{', '.join(r[0] for r in failed_list)}")
    if etf_signal_count:
        print(f"  - ETF 信號：{etf_signal_count} 筆")
    if breadth_days:
        print(f"  - 市場廣度：更新 {breadth_days} 天")
    if skip_list:
        print(f"  - 跳過（非執行區間）：{', '.join(r[0] for r in skip_list)}")
    status = "全部成功" if not failed_list else "有失敗項目"
    print(f"\n  最終狀態：{status}")
    print(f"{'='*60}")

    # Telegram summary: silent if TELEGRAM_BOT_TOKEN is not configured.
    try:
        send_sync(_build_daily_update_message(trade_date, results, failure_traces))
    except Exception:
        traceback.print_exc()

    print()


def update_range(start: date, end: date):
    current = start
    while current <= end:
        update_date(current)
        current += timedelta(days=1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Required when packaged as a Windows exe — multiprocessing workers
    # spawn by re-running the executable, and without freeze_support they'd
    # re-execute __main__ recursively. No-op in unfrozen Python.
    from multiprocessing import freeze_support
    freeze_support()

    from db.connection import init_db
    print("Initializing database schema ...")
    init_db()
    print()

    args = sys.argv[1:]

    try:
        if len(args) == 0:
            update_date(date.today())
        elif len(args) == 1:
            update_date(date.fromisoformat(args[0]))
        elif len(args) == 2:
            update_range(date.fromisoformat(args[0]), date.fromisoformat(args[1]))
        else:
            print("Usage: python daily_update.py [start_date] [end_date]")
            sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] {e}")
        traceback.print_exc()

    pause_before_exit()
