"""Backfill SBL (借券賣出) data for actual TAIEX trading days."""
import sys
from datetime import date
from db.connection import get_cursor
from scrapers.securities_lending import scrape_date


def _already_done(trade_date: date, cur) -> bool:
    cur.execute(
        "SELECT 1 FROM tw.daily_prices WHERE trade_date = %s AND sbl_balance IS NOT NULL LIMIT 1",
        (trade_date,),
    )
    return cur.fetchone() is not None


if __name__ == "__main__":
    start = date.fromisoformat(sys.argv[1]) if len(sys.argv) >= 2 else date(2016, 1, 4)
    end   = date.fromisoformat(sys.argv[2]) if len(sys.argv) >= 3 else date.today()

    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT trade_date FROM tw.index_prices
            WHERE index_id='TAIEX' AND trade_date BETWEEN %s AND %s
            ORDER BY trade_date
        """, (start, end))
        dates = [r["trade_date"] for r in cur.fetchall()]

    print(f"Backfilling {len(dates)} trading days from {dates[0]} to {dates[-1]}")
    skipped = 0
    for i, d in enumerate(dates, 1):
        with get_cursor(commit=False) as cur:
            if _already_done(d, cur):
                skipped += 1
                continue
        print(f"\n=== [{i}/{len(dates)}] {d} ===")
        try:
            scrape_date(d)
        except Exception as e:
            print(f"  ERROR: {e}")
    print(f"\nDone. processed={len(dates)-skipped}, skipped={skipped}")
