"""Backfill treasury stock buyback programs from MOPS t35sc09 (2016-present)."""
import sys
from datetime import date

from scrapers.treasury_stock import fetch_year, save_programs, MARKETS


if __name__ == "__main__":
    start_year = int(sys.argv[1]) if len(sys.argv) >= 2 else 2016
    end_year   = int(sys.argv[2]) if len(sys.argv) >= 3 else date.today().year

    total = 0
    for year in range(start_year, end_year + 1):
        for market in MARKETS:
            programs = fetch_year(year, market)
            saved = save_programs(programs)
            if saved:
                print(f"  {year} {market}: {saved} programs")
            total += saved

    print(f"\nDone. Total programs saved: {total}")
