"""
Backfill TAIFEX futures/options history into tw.taifex_* tables.

Usage (run from repo root):
    python -m scripts.backfill.backfill_taifex [start_date] [end_date] [--only fut,inst,pcr]

Defaults: start=2015-01-01, end=today, all three datasets.

  fut  : 期貨每日行情 (台指系列)  per trading day  (commodity_id=all)
  inst : 三大法人                per calendar year batch
  pcr  : Put/Call ratio          per calendar month batch

Futures OHLC days already present in the DB are skipped (resume-friendly).
inst/pcr use idempotent upserts and are re-pulled wholesale (cheap: few
requests). Trading days come from tw.index_prices (TAIEX), excluding holidays.
"""

import sys
from datetime import date, timedelta

from db.connection import get_cursor, init_db
from scrapers import taifex_futures
from scrapers import taifex_institutional as taifex_inst
from scrapers import taifex_pc_ratio


def _trading_days(start: date, end: date) -> list[date]:
    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT trade_date FROM tw.index_prices
            WHERE index_id='TAIEX' AND trade_date BETWEEN %s AND %s
            ORDER BY trade_date
        """, (start, end))
        return [r["trade_date"] for r in cur.fetchall()]


def _has_rows(table: str, trade_date: date) -> bool:
    with get_cursor(commit=False) as cur:
        cur.execute(
            f"SELECT 1 FROM tw.{table} WHERE trade_date = %s LIMIT 1",
            (trade_date,),
        )
        return cur.fetchone() is not None


def _backfill_daily(label: str, table: str, scraper, days: list[date]):
    print(f"\n##### {label}: {len(days)} trading days #####", flush=True)
    saved = skipped = 0
    for i, d in enumerate(days, 1):
        if _has_rows(table, d):
            skipped += 1
            continue
        print(f"[{i}/{len(days)}] {d}", flush=True)
        try:
            res = scraper.scrape_date(d)
            saved += res.records
        except Exception as e:
            print(f"  ERROR {d}: {e}", flush=True)
    print(f"{label} done: saved ~{saved} rows, skipped {skipped} existing days")


def _month_batches(start: date, end: date):
    cur = date(start.year, start.month, 1)
    while cur <= end:
        if cur.month == 12:
            nxt = date(cur.year + 1, 1, 1)
        else:
            nxt = date(cur.year, cur.month + 1, 1)
        yield max(start, cur), min(end, nxt - timedelta(days=1))
        cur = nxt


def _parse_args(argv: list[str]) -> tuple[list[str], set[str]]:
    """Return (positional date args, dataset set). Accepts --only=a,b,c."""
    positional, datasets = [], set()
    for a in argv:
        if a.startswith("--only="):
            datasets = set(a.split("=", 1)[1].split(","))
        elif not a.startswith("--"):
            positional.append(a)
    return positional, datasets or {"fut", "inst", "pcr"}


def main():
    args, datasets = _parse_args(sys.argv[1:])
    start = date.fromisoformat(args[0]) if len(args) >= 1 else date(2015, 1, 1)
    end = date.fromisoformat(args[1]) if len(args) >= 2 else date.today()

    init_db()
    print(f"TAIFEX backfill {start} ~ {end}  datasets={sorted(datasets)}")

    if "fut" in datasets:
        days = _trading_days(start, end)
        if not days:
            print("No trading days found in tw.index_prices for this range.")
        else:
            print(f"Trading days: {len(days)} ({days[0]} ~ {days[-1]})")
            _backfill_daily("FUTURES OHLC", "taifex_futures_daily",
                            taifex_futures, days)

    if "inst" in datasets:
        # Per-month batches: the TAIFEX institutional download is a rolling
        # ~3-year window, so a batch whose start predates the window is
        # rejected wholesale. Monthly batches capture everything still
        # available (older months simply return nothing).
        print(f"\n##### INSTITUTIONAL: per-month batches #####", flush=True)
        for s, e in _month_batches(start, end):
            taifex_inst.scrape_range(s, e)

    if "pcr" in datasets:
        print(f"\n##### PUT/CALL RATIO: per-month batches #####", flush=True)
        for s, e in _month_batches(start, end):
            taifex_pc_ratio.scrape_range(s, e)

    print("\nTAIFEX backfill complete.")


if __name__ == "__main__":
    main()
