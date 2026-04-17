"""
Historical data backfill script.

Processes dates newest-to-oldest by default (stops at first format shift so
you can adjust the scraper and resume — already-saved dates are skipped).

Usage:
  python historical_update.py                        # last 10 years, newest-first
  python historical_update.py 2025-04-01             # backfill from date to earliest TAIEX in DB
  python historical_update.py 2016-01-01 2020-12-31  # specific range
  python historical_update.py --scrapers twse,tpex   # only these scrapers
  python historical_update.py --force                # skip DB presence checks
  python historical_update.py --oldest-first         # process old->new instead
  python historical_update.py --skip-index           # use DB trading calendar, skip index API

Format shift detection:
  If a scraper returns parse errors on more than 40% of the rows the API
  provided (and the API gave at least 10 rows), the run stops immediately
  and prints the offending scraper and date. Fix the scraper then re-run;
  completed dates will be skipped automatically.

Available scraper aliases (--scrapers):
  twse, tpex, twse_ah, tpex_ah, esb, odd, margin, limits, inst, index, shareholder, sbl, insider, treasury
"""

import importlib
import sys
import traceback
from datetime import date, timedelta

from db.connection import get_cursor
from utils.format_shift import FormatShiftError, ScrapeResult

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------
SHIFT_ERROR_RATE = 0.40   # stop if >40% of API rows fail to parse
SHIFT_MIN_ROWS   = 10     # only check when API returned >= this many rows

# ---------------------------------------------------------------------------
# Scraper registry
# (label, module, func, skip_key, [aliases for --scrapers filter])
# ---------------------------------------------------------------------------
SCRAPERS = [
    ("TWSE daily prices",   "scrapers.twse",             "scrape_date", "twse_prices",  ["twse"]),
    ("TPEx daily prices",   "scrapers.tpex",             "scrape_date", "tpex_prices",  ["tpex"]),
    ("TWSE after-hours",    "scrapers.twse_after_hours", "scrape_date", "twse_ah",      ["twse_ah"]),
    ("TPEx after-hours",    "scrapers.tpex_after_hours", "scrape_date", "tpex_ah",      ["tpex_ah"]),
    ("ESB emerging prices", "scrapers.tpex_emerging",    "scrape_date", "emerging",     ["esb"]),
    ("Odd-lot (all)",       "scrapers.odd_lot",          "scrape_date", "odd_lot",      ["odd"]),
    ("Margin trading",      "scrapers.margin",           "scrape_date", "margin",       ["margin"]),
    ("Price limits",        "scrapers.price_limits",     "scrape_date", "price_limits", ["limits"]),
    ("Institutional",       "scrapers.institutional",    "scrape_date", "institutional",["inst"]),
    ("Market indices",      "scrapers.index_prices",     "scrape_date", "index",        ["index"]),
    ("Shareholder dist.",   "scrapers.shareholder_distribution", "scrape_date", "shareholder", ["shareholder"]),
    ("SBL (借券賣出)",      "scrapers.securities_lending", "scrape_date", "sbl",          ["sbl"]),
    ("Insider holdings",    "scrapers.insider_holdings",   "scrape_date", "insider",      ["insider"]),
    ("Treasury stock",      "scrapers.treasury_stock",     "scrape_date", "treasury",     ["treasury"]),
    ("Day trading (當沖)",  "scrapers.day_trading",        "scrape_date", "daytrade",     ["daytrade"]),
    ("Stock alerts",        "scrapers.stock_alerts",       "scrape_date", "alerts",       ["alerts"]),
]

# ---------------------------------------------------------------------------
# DB skip-check queries (True = already have data, skip this scraper+date)
# ---------------------------------------------------------------------------
_SKIP_QUERIES = {
    "twse_prices": """
        SELECT 1 FROM tw.daily_prices dp
        JOIN tw.stocks s ON s.stock_id = dp.stock_id
        WHERE dp.trade_date = %s AND s.market = 'TWSE' AND dp.close_price IS NOT NULL
        LIMIT 1
    """,
    "tpex_prices": """
        SELECT 1 FROM tw.daily_prices dp
        JOIN tw.stocks s ON s.stock_id = dp.stock_id
        WHERE dp.trade_date = %s AND s.market = 'TPEx' AND dp.close_price IS NOT NULL
        LIMIT 1
    """,
    "twse_ah": """
        SELECT 1 FROM tw.daily_prices dp
        JOIN tw.stocks s ON s.stock_id = dp.stock_id
        WHERE dp.trade_date = %s AND s.market = 'TWSE' AND dp.ah_volume IS NOT NULL
        LIMIT 1
    """,
    "tpex_ah": """
        SELECT 1 FROM tw.daily_prices dp
        JOIN tw.stocks s ON s.stock_id = dp.stock_id
        WHERE dp.trade_date = %s AND s.market = 'TPEx' AND dp.ah_volume IS NOT NULL
        LIMIT 1
    """,
    "emerging": """
        SELECT 1 FROM tw.daily_prices dp
        JOIN tw.stocks s ON s.stock_id = dp.stock_id
        WHERE dp.trade_date = %s AND s.market = 'ESB' AND dp.close_price IS NOT NULL
        LIMIT 1
    """,
    "odd_lot":      "SELECT 1 FROM tw.daily_prices WHERE trade_date = %s AND ol_volume IS NOT NULL LIMIT 1",
    "margin":       "SELECT 1 FROM tw.daily_prices WHERE trade_date = %s AND margin_balance IS NOT NULL LIMIT 1",
    "price_limits": "SELECT 1 FROM tw.daily_prices WHERE trade_date = %s AND limit_up IS NOT NULL LIMIT 1",
    "institutional":"SELECT 1 FROM tw.daily_prices WHERE trade_date = %s AND foreign_buy IS NOT NULL LIMIT 1",
    "index":        "SELECT 1 FROM tw.index_prices WHERE trade_date = %s AND advance IS NOT NULL LIMIT 1",
    "sbl":          "SELECT 1 FROM tw.daily_prices WHERE trade_date = %s AND sbl_balance IS NOT NULL LIMIT 1",
    "daytrade":     "SELECT 1 FROM tw.daily_prices WHERE trade_date = %s AND dt_volume IS NOT NULL LIMIT 1",
    "alerts":       "SELECT 1 FROM tw.stock_alerts WHERE alert_date = %s LIMIT 1",
}


def _is_done(skip_key: str, trade_date: date, cur) -> bool:
    """Return True if this scraper already has data for trade_date."""
    query = _SKIP_QUERIES.get(skip_key)
    if not query:
        return False
    cur.execute(query, (trade_date,))
    return cur.fetchone() is not None


class ScraperCrashError(Exception):
    """Raised when a scraper throws an unhandled exception."""

    def __init__(self, scraper: str, trade_date: date, original: Exception):
        self.scraper = scraper
        self.trade_date = trade_date
        self.original = original
        super().__init__(f"[{scraper}] Crash on {trade_date}: {original}")


SCRAPER_MAX_RETRIES = 3
SCRAPER_RETRY_WAIT  = 10  # seconds between retries


def _run_scraper(label: str, module: str, func: str, trade_date: date) -> ScrapeResult:
    """Import and call scrape_date(). Retries up to SCRAPER_MAX_RETRIES times."""
    import time
    mod = importlib.import_module(module)
    fn  = getattr(mod, func)

    for attempt in range(1, SCRAPER_MAX_RETRIES + 1):
        try:
            result = fn(trade_date)
            if isinstance(result, ScrapeResult):
                return result
            return ScrapeResult(records=int(result or 0), api_rows=0, parse_errors=0)
        except Exception as e:
            print(f"  [ERROR] {label} (attempt {attempt}/{SCRAPER_MAX_RETRIES}):")
            traceback.print_exc()
            if attempt < SCRAPER_MAX_RETRIES:
                print(f"  Retrying in {SCRAPER_RETRY_WAIT}s ...")
                time.sleep(SCRAPER_RETRY_WAIT)
            else:
                raise ScraperCrashError(label, trade_date, e)


def _check_shift(label: str, trade_date: date, result: ScrapeResult):
    """Raise FormatShiftError if parse error rate exceeds threshold."""
    if result.api_rows >= SHIFT_MIN_ROWS and result.error_rate > SHIFT_ERROR_RATE:
        raise FormatShiftError(
            scraper=label,
            trade_date=trade_date,
            details=(
                f"{result.parse_errors}/{result.api_rows} rows failed to parse "
                f"({result.error_rate:.0%} error rate, threshold {SHIFT_ERROR_RATE:.0%})"
            ),
        )


def process_date(trade_date: date, scrapers: list, force: bool):
    """
    Run all scrapers for one trading date.
    Raises FormatShiftError on detection; other scraper errors are logged and skipped.
    """
    with get_cursor() as cur:
        for label, module, func, skip_key, _ in scrapers:
            if not force and _is_done(skip_key, trade_date, cur):
                print(f"  [SKIP] {label} — already in DB")
                continue

            print(f"\n--- {label} ---")
            result = _run_scraper(label, module, func, trade_date)
            _check_shift(label, trade_date, result)


def _get_trading_days_from_db(start: date, end: date) -> list:
    """Get trading days from existing index_prices data (no API calls)."""
    with get_cursor() as cur:
        cur.execute("""
            SELECT DISTINCT trade_date FROM tw.index_prices
            WHERE index_id = 'TAIEX' AND trade_date >= %s AND trade_date <= %s
            ORDER BY trade_date
        """, (start, end))
        return [r["trade_date"] for r in cur.fetchall()]


def run(
    start: date,
    end: date,
    scraper_filter: list | None = None,
    force: bool = False,
    oldest_first: bool = False,
    skip_index: bool = False,
):
    # Filter scrapers
    active = SCRAPERS
    if scraper_filter:
        filter_set = set(scraper_filter)
        active = [s for s in SCRAPERS if any(a in filter_set for a in s[4])]
        if not active:
            print(f"No scrapers matched filter: {scraper_filter}")
            print(f"Available aliases: {', '.join(a for s in SCRAPERS for a in s[4])}")
            return

    # When only index is requested, skip the redundant Step 1 bulk fetch
    # (Step 2 will call scrape_date per day, which is what we want for advance/decline)
    if scraper_filter and scraper_filter == ["index"] and not skip_index:
        skip_index = True
        print("  (auto --skip-index: Step 2 will run per-day index scraper)")

    if skip_index:
        # Use existing trading calendar from DB
        print(f"\n{'='*60}")
        print(f"  Step 1: Loading trading calendar from DB (--skip-index) ...")
        print(f"{'='*60}\n")
        all_days = _get_trading_days_from_db(start, end)
    else:
        # Step 1: Fetch trading calendar + save index data (by month, no redundant calls)
        print(f"\n{'='*60}")
        print(f"  Step 1: Fetching trading calendar & index data ...")
        print(f"{'='*60}\n")

        from scrapers.index_prices import get_trading_days_and_save_index
        all_days = get_trading_days_and_save_index(start, end)

    if not all_days:
        print("No trading days found in this range.")
        input("\nPress Enter to exit...")
        return

    days = list(reversed(all_days)) if not oldest_first else all_days
    direction = "oldest→newest" if oldest_first else "newest→oldest"

    # Index OHLCV is already saved in Step 1 (bulk monthly).
    # But advance/decline requires per-day API calls via scrape_date().
    # Keep index in Step 2 only when user explicitly requested it.
    if scraper_filter and "index" in scraper_filter:
        other_scrapers = active
    else:
        other_scrapers = [s for s in active if "index" not in s[4]]

    # Step 2: Run remaining scrapers on confirmed trading days
    print(f"\n{'='*60}")
    print(f"  Step 2: Historical backfill")
    print(f"  Range     : {start} to {end}")
    print(f"  Direction : {direction}")
    print(f"  Days      : {len(days)} actual trading days")
    print(f"  Scrapers  : {', '.join(s[0] for s in other_scrapers)}")
    print(f"  Force     : {force}")
    print(f"{'='*60}\n")

    for i, trade_date in enumerate(days, 1):
        print(f"\n{'='*60}")
        print(f"  [{i}/{len(days)}]  {trade_date}")
        print(f"{'='*60}")

        try:
            process_date(trade_date, other_scrapers, force)

        except FormatShiftError as e:
            print(f"\n{'!'*60}")
            print(f"  FORMAT SHIFT DETECTED — stopping")
            print(f"  Scraper : {e.scraper}")
            print(f"  Date    : {e.trade_date}")
            print(f"  Details : {e.details}")
            print(f"{'!'*60}")
            print()
            print("Next steps:")
            print(f"  1. Inspect the API response for {e.scraper} around {e.trade_date}")
            print(f"  2. Update the relevant parse function in the scraper")
            print(f"  3. Re-run — completed dates will be skipped automatically")
            input("\nPress Enter to exit...")
            sys.exit(1)

        except ScraperCrashError as e:
            print(f"\n{'!'*60}")
            print(f"  SCRAPER ERROR — stopping")
            print(f"  Scraper : {e.scraper}")
            print(f"  Date    : {e.trade_date}")
            print(f"  Error   : {e.original}")
            print(f"{'!'*60}")
            input("\nPress Enter to exit...")
            sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  Historical update complete: {len(days)} dates processed.")
    print(f"{'='*60}")
    input("\nPress Enter to exit...")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _get_earliest_taiex_date() -> date | None:
    """Query the earliest TAIEX date in the DB, or return None."""
    try:
        with get_cursor() as cur:
            cur.execute("""
                SELECT MIN(trade_date) FROM tw.index_prices
                WHERE index_id = 'TAIEX'
            """)
            row = cur.fetchone()
            return row["min"] if row and row["min"] else None
    except Exception:
        return None


def _parse_args():
    """Parse command-line arguments."""
    today = date.today()
    start = today.replace(year=today.year - 10)
    end   = today - timedelta(days=1)
    force        = False
    oldest_first = False
    skip_index   = False
    scraper_filter = None

    positional = []
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == "--force":
            force = True
        elif arg == "--oldest-first":
            oldest_first = True
        elif arg == "--skip-index":
            skip_index = True
        elif arg == "--scrapers" and i + 1 < len(sys.argv):
            scraper_filter = sys.argv[i + 1].split(",")
            i += 1
        elif not arg.startswith("--"):
            positional.append(arg)
        else:
            print(f"Unknown argument: {arg}")
            sys.exit(1)
        i += 1

    if len(positional) == 1:
        # Single date = end point; start = earliest TAIEX date in DB or 10 years ago
        end = date.fromisoformat(positional[0])
        start = _get_earliest_taiex_date() or today.replace(year=today.year - 10)
    elif len(positional) >= 2:
        start = date.fromisoformat(positional[0])
        end = date.fromisoformat(positional[1])

    if start > end:
        print(f"Error: start ({start}) must be before end ({end})")
        sys.exit(1)

    return start, end, force, oldest_first, skip_index, scraper_filter


if __name__ == "__main__":
    # Ensure all tables exist before scraping
    from db.connection import init_db
    print("Initializing database schema ...")
    init_db()
    print()

    start, end, force, oldest_first, skip_index, scraper_filter = _parse_args()
    run(start, end, scraper_filter=scraper_filter, force=force, oldest_first=oldest_first, skip_index=skip_index)
