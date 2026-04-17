"""
Backfill TDCC shareholder distribution for the ~50 historical weeks the
portal retains. Iterates (data_date → stock_id) and skips any (stock, date)
already in DB so interrupted runs can be safely re-started.
"""

import sys
import time
from datetime import date

from db.connection import get_cursor
from scrapers.shareholder_distribution import (
    fetch_portal, get_available_dates, save_records,
)


def _load_stock_universe() -> list[str]:
    """All TWSE / TPEx / ESB stock IDs from tw.stocks."""
    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT stock_id FROM tw.stocks
            WHERE market IN ('TWSE', 'TPEx', 'ESB')
            ORDER BY stock_id
        """)
        return [r["stock_id"] for r in cur.fetchall()]


def _already_done(stock_id: str, data_date: date, cur) -> bool:
    cur.execute(
        "SELECT 1 FROM tw.shareholder_distribution "
        "WHERE stock_id = %s AND data_date = %s LIMIT 1",
        (stock_id, data_date),
    )
    return cur.fetchone() is not None


def backfill(dates: list[date], stocks: list[str]):
    total = len(dates) * len(stocks)
    done = 0
    saved = 0
    missing = 0

    for d in dates:
        print(f"\n=== {d} ({len(stocks)} stocks) ===")
        for sid in stocks:
            done += 1
            with get_cursor(commit=False) as cur:
                if _already_done(sid, d, cur):
                    continue

            tiers = fetch_portal(sid, d)
            if not tiers:
                missing += 1
                continue

            save_records({sid: tiers}, d)
            saved += 1

            if saved % 50 == 0:
                pct = done / total * 100
                print(f"  progress: {done}/{total} ({pct:.1f}%) "
                      f"saved={saved} missing={missing}")

    print(f"\nBackfill done. saved={saved}, missing={missing}, checked={done}")


if __name__ == "__main__":
    available = get_available_dates()
    print(f"Portal offers {len(available)} weeks "
          f"({available[-1]} to {available[0]})")

    if len(sys.argv) >= 3:
        start = date.fromisoformat(sys.argv[1])
        end   = date.fromisoformat(sys.argv[2])
        target = [d for d in available if start <= d <= end]
    else:
        target = available

    print(f"Target: {len(target)} weeks")
    stocks = _load_stock_universe()
    print(f"Universe: {len(stocks)} stocks")
    print(f"Estimated worst-case time: {len(target)*len(stocks)*2/3600:.1f} hours\n")

    try:
        backfill(target, stocks)
    except KeyboardInterrupt:
        print("\nInterrupted. Re-run the script to resume — already-saved rows will be skipped.")
