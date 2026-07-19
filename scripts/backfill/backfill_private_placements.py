"""One-shot backfill for tw.private_placements.

The MOPS t116sb01 一覽表 is a full cumulative table (all history, ~ROC 97 onward),
so a single sii+otc fetch already backfills everything — this just invokes the daily
scrape once. Idempotent.

Usage:  python -m scripts.backfill.backfill_private_placements
"""
from datetime import date

from scrapers.private_placements import scrape_date

if __name__ == "__main__":
    result = scrape_date(date.today())
    print(f"DONE. {result}")
