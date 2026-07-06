"""One-off backfill for tw.stock_liquidity_daily.

Populates liquidity classification for the last N trading days so the
intraday ORB pipeline has historical data immediately. Daily runs after
this point are handled by daily_update.py.

Usage (run from repo root):
  python -m scripts.backfill.backfill_liquidity          # last 80 trading days
  python -m scripts.backfill.backfill_liquidity 120      # last 120 trading days
  python -m scripts.backfill.backfill_liquidity 2026-02-01 2026-04-20  # explicit range

Trading days are taken from tw.index_prices (TAIEX) so weekends/holidays
are skipped automatically.
"""

import sys
from datetime import date

from db.connection import get_cursor, init_db
from analysis.daily_liquidity import compute_liquidity


DEFAULT_LOOKBACK = 80


def _recent_trading_days(n: int) -> list[date]:
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT trade_date
            FROM tw.index_prices
            WHERE index_id = 'TAIEX'
            ORDER BY trade_date DESC
            LIMIT %s
            """,
            (n,),
        )
        return sorted(r["trade_date"] for r in cur.fetchall())


def _trading_days_in_range(start: date, end: date) -> list[date]:
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT trade_date
            FROM tw.index_prices
            WHERE index_id = 'TAIEX' AND trade_date BETWEEN %s AND %s
            ORDER BY trade_date
            """,
            (start, end),
        )
        return [r["trade_date"] for r in cur.fetchall()]


def main(argv: list[str]) -> int:
    init_db()

    if len(argv) == 0:
        dates = _recent_trading_days(DEFAULT_LOOKBACK)
    elif len(argv) == 1:
        try:
            n = int(argv[0])
            dates = _recent_trading_days(n)
        except ValueError:
            print(f"[ERROR] single argument must be an integer lookback, got: {argv[0]}")
            return 1
    elif len(argv) == 2:
        start = date.fromisoformat(argv[0])
        end = date.fromisoformat(argv[1])
        dates = _trading_days_in_range(start, end)
    else:
        print("Usage: python -m scripts.backfill.backfill_liquidity [lookback_days | start_date end_date]")
        return 1

    if not dates:
        print("[ERROR] no trading days matched the requested window")
        return 1

    print(f"Backfilling liquidity for {len(dates)} day(s): {dates[0]} .. {dates[-1]}")
    total = compute_liquidity(dates)
    print(f"Done. Total rows upserted: {total}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
