"""One-shot backfill for tw.insider_share_transfers.

Iterates ROC years x months x (sii, otc), single-month market-wide queries (safely
under the aggregate cap), upserting every pre-declared transfer filing. Idempotent.

Usage:
  python -m scripts.backfill.backfill_insider_share_transfers            # ROC 113-115
  python -m scripts.backfill.backfill_insider_share_transfers 111 115    # ROC 111-115
"""
import sys
from datetime import date

from scrapers.insider_share_transfers import fetch, save_transfers


def main(roc_start: int, roc_end: int) -> None:
    today = date.today()
    cur_roc, cur_month = today.year - 1911, today.month
    total = 0
    for roc_year in range(roc_start, roc_end + 1):
        for month in range(1, 13):
            if roc_year == cur_roc and month > cur_month:
                break  # future months have no data
            for market in ("sii", "otc"):
                n = save_transfers(fetch(market, roc_year, month))
                total += n
            print(f"  ROC {roc_year}/{month:02d}: cumulative {total}")
    print(f"DONE. {total} transfer filings upserted (ROC {roc_start}-{roc_end}).")


if __name__ == "__main__":
    start = int(sys.argv[1]) if len(sys.argv) > 1 else 113
    end = int(sys.argv[2]) if len(sys.argv) > 2 else 115
    main(start, end)
