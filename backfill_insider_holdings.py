"""
Backfill monthly insider holdings (董監經理人持股餘額) via MOPS per-stock POST.
Iterates (year_month → stock_id). Skips already-saved (stock_id, year_month).
Default range: 2025-01 to the previous completed month.

Usage:
  python backfill_insider_holdings.py                         # 2025-01 to last month
  python backfill_insider_holdings.py 2023-01                 # from 2023-01
  python backfill_insider_holdings.py 2023-01 2024-12         # specific range
"""

import sys
from datetime import date

from db.connection import get_cursor
from scrapers.insider_holdings import fetch_mops_one, save_one


def _load_stock_universe() -> list[str]:
    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT stock_id FROM tw.stocks
            WHERE market IN ('TWSE', 'TPEx', 'ESB')
            ORDER BY stock_id
        """)
        return [r["stock_id"] for r in cur.fetchall()]


def _already_done(stock_id: str, year_month: str, cur) -> bool:
    cur.execute(
        "SELECT 1 FROM tw.insider_holdings "
        "WHERE stock_id = %s AND year_month = %s LIMIT 1",
        (stock_id, year_month),
    )
    return cur.fetchone() is not None


def _months_between(start: date, end: date) -> list[tuple[int, int]]:
    """Inclusive month range → list of (ad_year, month)."""
    months = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def _roc_ym(ad_year: int, month: int) -> str:
    """Convert (2024, 3) → '11303' (ROC yymm)."""
    return f"{ad_year - 1911:03d}{month:02d}"


def backfill(months: list[tuple[int, int]], stocks: list[str]):
    total = len(months) * len(stocks)
    done = 0
    saved = 0
    missing = 0
    skipped = 0

    for ad_year, month in months:
        ym = _roc_ym(ad_year, month)
        print(f"\n=== {ad_year}-{month:02d} (ROC {ym}, {len(stocks)} stocks) ===")
        for sid in stocks:
            done += 1
            with get_cursor(commit=False) as cur:
                if _already_done(sid, ym, cur):
                    skipped += 1
                    continue

            agg = fetch_mops_one(sid, ad_year, month)
            if not agg:
                missing += 1
                continue

            save_one(sid, ym, agg)
            saved += 1

            if saved % 50 == 0:
                pct = done / total * 100
                print(f"  progress: {done}/{total} ({pct:.1f}%) "
                      f"saved={saved} skipped={skipped} missing={missing}")

    print(f"\nBackfill done. saved={saved}, skipped={skipped}, missing={missing}, checked={done}")


if __name__ == "__main__":
    today = date.today()
    default_end_y = today.year if today.month > 1 else today.year - 1
    default_end_m = today.month - 1 if today.month > 1 else 12
    default_end = date(default_end_y, default_end_m, 1)

    if len(sys.argv) >= 3:
        start = date.fromisoformat(sys.argv[1] + "-01")
        end   = date.fromisoformat(sys.argv[2] + "-01")
    elif len(sys.argv) == 2:
        start = date.fromisoformat(sys.argv[1] + "-01")
        end   = default_end
    else:
        start = date(2025, 1, 1)
        end   = default_end

    months = _months_between(start, end)
    stocks = _load_stock_universe()
    print(f"Target: {len(months)} months ({months[0][0]}-{months[0][1]:02d} to {months[-1][0]}-{months[-1][1]:02d})")
    print(f"Universe: {len(stocks)} stocks")
    print(f"Estimated worst-case time: {len(months)*len(stocks)*3/3600:.1f} hours\n")

    try:
        backfill(months, stocks)
    except KeyboardInterrupt:
        print("\nInterrupted. Re-run the script to resume — already-saved rows will be skipped.")
