"""
Pre-market refresh of reference prices & daily limits.

Run once per trading day before 09:00 (cron or manual). Logs in to SinoPac
Shioaji, walks every Stock contract under TSE + OTC, and writes
`ref_price / limit_up / limit_down` into `tw.intraday_quotes` — the same
table the intraday pipeline already uses. The write path is in
`intraday/store.py :: upsert_reference`.

Usage:
    python pre_market_update.py              # use today's date
    python pre_market_update.py 2026-04-14   # override trade_date

Idempotent: running it twice on the same day just rewrites the same rows.
"""

import sys
import traceback
from datetime import date

from db.connection import init_db
# Import order matters: sinopac_reference pulls `from utils.classifier import ...`
# at module load, which must happen BEFORE sinopac_loader imports shioaji —
# shioaji transitively injects a C-extension module under the top-level name
# `utils`, shadowing our local utils/ package for any later imports.
from intraday.sinopac_reference import fetch_reference_rows
from intraday.sinopac_loader import load_api, logout_api
from intraday.store import upsert_reference


def main(trade_date: date | None = None) -> int:
    if trade_date is None:
        trade_date = date.today()

    print(f"[PRE] pre-market refresh for {trade_date}")

    init_db()

    api = None
    try:
        api = load_api()

        print("[PRE] fetching contracts ...")
        rows = fetch_reference_rows(api)
        print(f"[PRE] got {len(rows)} contracts with valid ref/limit")
        if not rows:
            print("[PRE] [ERROR] empty contract list — nothing to write")
            return 1

        written = upsert_reference(rows, trade_date=trade_date)
        print(f"[PRE] wrote {written} rows to tw.intraday_quotes")
        return 0
    except Exception:
        print("[PRE] [ERROR] pre-market refresh failed:")
        traceback.print_exc()
        return 1
    finally:
        if api is not None:
            logout_api(api)


if __name__ == "__main__":
    arg_date: date | None = None
    if len(sys.argv) > 1:
        arg_date = date.fromisoformat(sys.argv[1])
    sys.exit(main(arg_date))
