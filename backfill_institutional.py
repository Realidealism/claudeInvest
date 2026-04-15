"""Backfill institutional data for actual TAIEX trading days."""
from datetime import date
from db.connection import get_cursor
from scrapers.institutional import scrape_date

START = date(2016, 1, 4)
END   = date(2026, 4, 14)

with get_cursor(commit=False) as cur:
    cur.execute("""
        SELECT trade_date FROM tw.index_prices
        WHERE index_id='TAIEX' AND trade_date BETWEEN %s AND %s
        ORDER BY trade_date
    """, (START, END))
    dates = [r["trade_date"] for r in cur.fetchall()]

print(f"Backfilling {len(dates)} trading days from {dates[0]} to {dates[-1]}")
for i, d in enumerate(dates, 1):
    print(f"\n=== [{i}/{len(dates)}] {d} ===")
    try:
        scrape_date(d)
    except Exception as e:
        print(f"  ERROR: {e}")
print(f"\nDone. {len(dates)} days processed.")
