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

# ---------------------------------------------------------------------------
# Scraper registry — order matters (prices first, derived data last)
# ---------------------------------------------------------------------------
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
    # Index
    ("Market indices",          "scrapers.index_prices",     "scrape_date"),
]


def run_scraper(label: str, module_path: str, func_name: str, trade_date: date) -> bool:
    """Import and run a single scraper. Returns True on success."""
    try:
        import importlib
        mod = importlib.import_module(module_path)
        fn  = getattr(mod, func_name)
        fn(trade_date)
        return True
    except Exception:
        print(f"\n  [ERROR] {label} failed:")
        traceback.print_exc()
        return False


def update_date(trade_date: date):
    """Run all scrapers for a single trading date."""
    if trade_date.weekday() >= 5:
        print(f"[SKIP] {trade_date} is a weekend, no market data.")
        return

    print(f"\n{'='*60}")
    print(f"  Daily update: {trade_date}")
    print(f"{'='*60}")

    ok, failed = 0, []
    for label, module_path, func_name in SCRAPERS:
        print(f"\n--- {label} ---")
        success = run_scraper(label, module_path, func_name, trade_date)
        if success:
            ok += 1
        else:
            failed.append(label)

    print(f"\n{'='*60}")
    print(f"  Done: {ok}/{len(SCRAPERS)} scrapers succeeded.")
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
    args = sys.argv[1:]

    if len(args) == 0:
        update_date(date.today())
    elif len(args) == 1:
        update_date(date.fromisoformat(args[0]))
    elif len(args) == 2:
        update_range(date.fromisoformat(args[0]), date.fromisoformat(args[1]))
    else:
        print("Usage: python daily_update.py [start_date] [end_date]")
        sys.exit(1)
