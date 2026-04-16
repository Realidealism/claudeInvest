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

    ok = 1 if gate_ok else 0
    failed = [] if gate_ok else [INDEX_SCRAPER[0]]

    for label, module_path, func_name in SCRAPERS:
        print(f"\n--- {label} ---")
        success = run_scraper(label, module_path, func_name, trade_date)
        if success:
            ok += 1
        else:
            failed.append(label)

    # Monthly revenue: fetch during the publication window (1st–12th)
    if trade_date.day <= 15:
        print(f"\n--- Monthly revenue ---")
        try:
            from scrapers.revenue import scrape_month
            # Fetch previous month's revenue
            m = trade_date.month - 1
            y = trade_date.year
            if m == 0:
                m = 12
                y -= 1
            scrape_month(y, m)
        except Exception:
            print("  [ERROR] Monthly revenue scraper failed:")
            traceback.print_exc()

    # Detect delisted stocks after all price scrapers have run
    print(f"\n--- Delist detection ---")
    try:
        detect_delisted(trade_date)
    except Exception:
        print("  [ERROR] Delist detection failed:")
        traceback.print_exc()

    # Market breadth aggregate (depends on close/money/volume per stock).
    print(f"\n--- Market breadth ---")
    try:
        from analysis.market_breadth import calculate_market_breadth, save_market_breadth
        results = calculate_market_breadth(last_n_days=3)
        n = save_market_breadth(results)
        print(f"  Updated {n} day(s) of market_breadth.")
    except Exception:
        print("  [ERROR] Market breadth computation failed:")
        traceback.print_exc()

    total_count = 1 + len(SCRAPERS)  # include the gate
    print(f"\n{'='*60}")
    print(f"  Done: {ok}/{total_count} scrapers succeeded.")
    if failed:
        print(f"  Failed: {', '.join(failed)}")
    print(f"{'='*60}\n")


def update_range(start: date, end: date):
    current = start
    while current <= end:
        update_date(current)
        current += timedelta(days=1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
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
