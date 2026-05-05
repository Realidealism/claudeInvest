"""
Daily market data update script.

Usage:
  python daily_update.py              # update today
  python daily_update.py 2026-04-02   # update specific date
  python daily_update.py 2026-04-01 2026-04-07  # update date range

Each scraper runs independently; failures are logged but do not stop the rest.
"""

import sys
import traceback
from datetime import date, timedelta
from pathlib import Path

from db.connection import get_cursor

# ---------------------------------------------------------------------------
# Scraper registry — order matters (prices first, derived data last)
# ---------------------------------------------------------------------------
# Trading-day gate: TAIEX is scraped first; absent TAIEX => non-trading day.
INDEX_SCRAPER = ("Market indices", "scrapers.index_prices", "scrape_date")

SCRAPERS = [
    # Core daily prices
    ("TWSE daily prices",       "scrapers.twse",             "scrape_date"),
    ("TPEx daily prices",       "scrapers.tpex",             "scrape_date"),
    ("TWSE after-hours",        "scrapers.twse_after_hours", "scrape_date"),
    ("TPEx after-hours",        "scrapers.tpex_after_hours", "scrape_date"),
    ("ESB emerging prices",     "scrapers.tpex_emerging",    "scrape_date"),
    # Supplemental data
    ("Odd-lot (all sessions)",  "scrapers.odd_lot",          "scrape_date"),
    ("Margin trading",          "scrapers.margin",           "scrape_date"),
    ("Price limits",            "scrapers.price_limits",     "scrape_date"),
    ("Institutional investors", "scrapers.institutional",    "scrape_date"),
    # ETF holdings
    ("ETF holdings",            "scrapers.etf_holdings",     "scrape_date"),
    # Securities lending
    ("SBL (借券賣出)",           "scrapers.securities_lending", "scrape_date"),
    # Day trading
    ("Day trading (當沖)",       "scrapers.day_trading",        "scrape_date"),
    # Alerts
    ("Stock alerts (注意/處置)", "scrapers.stock_alerts",       "scrape_date"),
    # Weekly/monthly (idempotent, run once per period)
    ("Shareholder dist.",       "scrapers.shareholder_distribution", "scrape_date"),
    ("Insider holdings",        "scrapers.insider_holdings",  "scrape_date"),
    ("Treasury stock",          "scrapers.treasury_stock",    "scrape_date"),
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


def run_scraper(label: str, module_path: str, func_name: str, trade_date: date) -> bool:
    """Import and run a single scraper. Retries up to SCRAPER_MAX_RETRIES times."""
    import time
    import importlib
    mod = importlib.import_module(module_path)
    fn  = getattr(mod, func_name)

    for attempt in range(1, SCRAPER_MAX_RETRIES + 1):
        try:
            fn(trade_date)
            return True
        except Exception:
            print(f"\n  [ERROR] {label} (attempt {attempt}/{SCRAPER_MAX_RETRIES}):")
            traceback.print_exc()
            if attempt < SCRAPER_MAX_RETRIES:
                print(f"  Retrying in {SCRAPER_RETRY_WAIT}s ...")
                time.sleep(SCRAPER_RETRY_WAIT)

    return False


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
            print("  [SKIP] No price data for today, cannot detect delistings.")
            return

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


def _git_push_frontend():
    """Commit updated JSON data and push to trigger Vercel deploy."""
    import subprocess
    import sys
    # PyInstaller exe: __file__ points to temp dir; exe lives in dist/
    if getattr(sys, 'frozen', False):
        repo = Path(sys.executable).parent.parent
    else:
        repo = Path(__file__).parent
    data_dir = repo / "frontend" / "public" / "data"
    if not data_dir.exists():
        print("  [SKIP] No frontend/public/data/ directory.")
        return

    result = subprocess.run(
        ["git", "status", "--porcelain", str(data_dir)],
        capture_output=True, text=True, cwd=repo,
    )
    if not result.stdout.strip():
        print("  No JSON changes to deploy.")
        return

    subprocess.run(["git", "add", str(data_dir)], cwd=repo, check=True)
    subprocess.run(
        ["git", "commit", "-m", f"Update frontend data ({date.today().isoformat()})"],
        cwd=repo, check=True,
    )
    subprocess.run(["git", "push"], cwd=repo, check=True)
    print("  Pushed to GitHub — Vercel will auto-deploy.")


def update_date(trade_date: date):
    """Run all scrapers for a single trading date."""
    if trade_date.weekday() >= 5:
        print(f"[SKIP] {trade_date} is a weekend, no market data.")
        return

    print(f"\n{'='*60}")
    print(f"  Daily update: {trade_date}")
    print(f"{'='*60}")

    # Trading-day gate: TAIEX index must exist for the date to be valid.
    print(f"\n--- {INDEX_SCRAPER[0]} (trading-day gate) ---")
    gate_ok = run_scraper(*INDEX_SCRAPER, trade_date)
    if not _has_taiex(trade_date):
        print(f"\n[HOLIDAY] {trade_date} has no TAIEX data — skipping remaining scrapers.")
        return

    # Track results: list of (label, status) where status is "ok", "failed", "skip"
    results = []
    results.append((INDEX_SCRAPER[0], "ok" if gate_ok else "failed"))

    for label, module_path, func_name in SCRAPERS:
        print(f"\n--- {label} ---")
        success = run_scraper(label, module_path, func_name, trade_date)
        results.append((label, "ok" if success else "failed"))

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
            traceback.print_exc()
            results.append(("月營收", "failed"))
    else:
        results.append(("月營收", "skip"))

    # SITCA fund holdings: monthly top-10 (available ~10th business day)
    if trade_date.day <= 20:
        print(f"\n--- SITCA monthly fund holdings ---")
        try:
            from scrapers.sitca import scrape_monthly
            m = trade_date.month - 1
            y = trade_date.year
            if m == 0:
                m = 12
                y -= 1
            scrape_monthly(f"{y}{m:02d}")
            results.append(("SITCA 月持股", "ok"))
        except Exception:
            print("  [ERROR] SITCA monthly scraper failed:")
            traceback.print_exc()
            results.append(("SITCA 月持股", "failed"))
    else:
        results.append(("SITCA 月持股", "skip"))

    # SITCA quarterly holdings: available ~15th of quarter-end+1 month
    quarter_end_months = {1: 12, 2: 12, 4: 3, 5: 3, 7: 6, 8: 6, 10: 9, 11: 9}
    if trade_date.month in quarter_end_months and trade_date.day <= 20:
        print(f"\n--- SITCA quarterly fund holdings ---")
        try:
            from scrapers.sitca import scrape_quarterly
            qm = quarter_end_months[trade_date.month]
            qy = trade_date.year if qm < trade_date.month else trade_date.year - 1
            scrape_quarterly(f"{qy}{qm:02d}")
            results.append(("SITCA 季持股", "ok"))
        except Exception:
            print("  [ERROR] SITCA quarterly scraper failed:")
            traceback.print_exc()
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
            traceback.print_exc()
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
        traceback.print_exc()
        results.append(("ETF 信號掃描", "failed"))

    # Hermit-stock fundamental screener snapshot.
    # Runs every weekday: ~20s, writes top-50 to tw.hermit_screen_snapshot,
    # diffs vs previous snapshot to flag NEW entrants / EXITs / big movers.
    # Invoked as a subprocess through hermit_stock's own uv-managed venv to
    # avoid dependency conflicts (hermit_stock pulls pydantic/typer/etc.
    # which are not in this parent project's venv).
    print(f"\n--- Hermit-stock fundamental snapshot ---")
    try:
        import subprocess as _sp
        hs_dir = Path(__file__).parent / "hermit_stock"
        proc = _sp.run(
            ["uv", "run", "python", "-m", "hermit_stock.daily_check",
             trade_date.isoformat()],
            cwd=str(hs_dir),
            capture_output=True, text=True,
            encoding="utf-8", errors="replace",  # tolerate cp950 in uv warnings
            timeout=300, check=False,
        )
        if proc.stdout:
            print(proc.stdout, end="")
        if proc.returncode != 0:
            print(proc.stderr or "<no stderr>", end="")
            results.append(("贏勢股快照", "failed"))
        else:
            results.append(("贏勢股快照", "ok"))
    except Exception:
        print("  [ERROR] Hermit-stock daily check failed:")
        traceback.print_exc()
        results.append(("贏勢股快照", "failed"))

    # Detect delisted stocks after all price scrapers have run
    print(f"\n--- Delist detection ---")
    try:
        detect_delisted(trade_date)
        results.append(("下市偵測", "ok"))
    except Exception:
        print("  [ERROR] Delist detection failed:")
        traceback.print_exc()
        results.append(("下市偵測", "failed"))

    # Market breadth aggregate (depends on close/money/volume per stock).
    breadth_days = 0
    breadth_ok = False
    print(f"\n--- Market breadth ---")
    try:
        from analysis.market_breadth import calculate_market_breadth, save_market_breadth
        mb_results = calculate_market_breadth(last_n_days=3)
        breadth_days = save_market_breadth(mb_results)
        print(f"  Updated {breadth_days} day(s) of market_breadth.")
        results.append(("市場廣度", "ok"))
        breadth_ok = True
    except Exception:
        print("  [ERROR] Market breadth computation failed:")
        traceback.print_exc()
        results.append(("市場廣度", "failed"))

    # Daily stock liquidity: money_level / dead_fish / halted / on_alert.
    # Consumed by the intraday ORB pipeline to exclude untradable names.
    print(f"\n--- Daily liquidity ---")
    try:
        from analysis.daily_liquidity import compute_daily_liquidity
        n = compute_daily_liquidity(trade_date)
        print(f"  Stored liquidity rows: {n}")
        results.append(("每日流動性", "ok"))
    except Exception:
        print("  [ERROR] Daily liquidity computation failed:")
        traceback.print_exc()
        results.append(("每日流動性", "failed"))

    # Combined daily snapshot — score top-100 long/short + 6 signal-factory
    # fires + unified-strategy open positions, all in one per-stock pass.
    # Skip if market_breadth failed: load_stock_data pulls market_state from
    # tw.market_breadth, and stale rows would silently degrade every output.
    print(f"\n--- Daily snapshot (score + signal + positions) ---")
    if not breadth_ok:
        print("  [SKIP] market_breadth failed; daily_snapshot needs fresh market_state.")
        results.append(("多空評比 + 操作訊號 + 策略持倉快照", "skip"))
    else:
        try:
            from analysis.daily_snapshot import run as run_daily_snapshot
            run_daily_snapshot(trade_date)
            results.append(("多空評比 + 操作訊號 + 策略持倉快照", "ok"))
        except Exception:
            print("  [ERROR] Daily snapshot failed:")
            traceback.print_exc()
            results.append(("多空評比 + 操作訊號 + 策略持倉快照", "failed"))

    # Export JSON + git push for Vercel auto-deploy
    print(f"\n--- Frontend export + deploy ---")
    try:
        from export.generate import export_all
        export_all()
        _git_push_frontend()
        results.append(("前端匯出部署", "ok"))
    except Exception:
        print("  [ERROR] Export/deploy failed:")
        traceback.print_exc()
        results.append(("前端匯出部署", "failed"))

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
        print(f"  ✘ 失敗項目：{', '.join(r[0] for r in failed_list)}")
    if etf_signal_count:
        print(f"  ▸ ETF 信號：{etf_signal_count} 筆")
    if breadth_days:
        print(f"  ▸ 市場廣度：更新 {breadth_days} 天")
    if skip_list:
        print(f"  - 跳過（非執行區間）：{', '.join(r[0] for r in skip_list)}")
    status = "全部成功 ✔" if not failed_list else "有失敗項目 ✘"
    print(f"\n  最終狀態：{status}")
    print(f"{'='*60}")

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

    input("\nPress Enter to exit...")
