"""
Backfill insider pledge / release EVENTS (內部人設質解質公告) via MOPS STAMAK03_1.

Iterates the full stock universe, one wide-range query per stock
(b_date = ROC 112/01/01, e_date = today). If a stock returns a suspiciously
large number of rows (>= CAP_HINT), re-queries it year-by-year to avoid any
potential row truncation on the MOPS side.

Resumable: upserts are idempotent (ON CONFLICT). Prints per-stock progress.

Usage (run from repo root):
  python -m scripts.backfill.backfill_insider_pledge_events            # full universe
  python -m scripts.backfill.backfill_insider_pledge_events --limit 8  # first 8 (smoke test)

NOTE: this script does NOT auto-run the full market on import — invoke it
explicitly (the operator launches the full run in the background separately).
"""

import argparse
from datetime import date

from db.connection import get_cursor
from scrapers.insider_pledge_events import fetch_events, save_events

CAP_HINT = 180  # rows-per-stock threshold that triggers per-year re-query

B_ROC_YEAR = 112  # earliest ROC year to backfill from (112/01/01)


def _load_stock_universe() -> list[str]:
    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT stock_id FROM tw.stocks
            WHERE market IN ('TWSE', 'TPEx', 'ESB')
            ORDER BY stock_id
        """)
        return [r["stock_id"] for r in cur.fetchall()]


def _roc_today() -> str:
    t = date.today()
    return f"{t.year - 1911}{t.month:02d}{t.day:02d}"


def _roc_year_range(roc_y: int) -> tuple[str, str]:
    return f"{roc_y}0101", f"{roc_y}1231"


def backfill(stocks: list[str]):
    e_date = _roc_today()
    b_date = f"{B_ROC_YEAR}0101"
    total = len(stocks)
    max_roc = int(_roc_today()[: len(str(date.today().year - 1911))])

    total_events = 0
    stocks_with_data = 0

    for i, sid in enumerate(stocks, 1):
        events = fetch_events(sid, b_date, e_date)

        # Safety net: a near-cap count may mean the response was truncated;
        # re-query year-by-year and merge (upsert dedupes).
        if len(events) >= CAP_HINT:
            merged: dict[tuple, dict] = {}
            for ev in events:
                merged[_ev_key(ev)] = ev
            for roc_y in range(B_ROC_YEAR, max_roc + 1):
                yb, ye = _roc_year_range(roc_y)
                for ev in fetch_events(sid, yb, ye):
                    merged[_ev_key(ev)] = ev
            events = list(merged.values())
            print(f"  [{sid}] cap-hint hit → per-year re-query, merged {len(events)} events")

        saved = save_events(events)
        total_events += saved
        if saved:
            stocks_with_data += 1

        if i % 25 == 0 or saved:
            print(f"  {i}/{total} {sid}: {saved} events "
                  f"(cum {total_events} events, {stocks_with_data} stocks w/ data)")

    print(f"\nBackfill done. {total_events} events across {stocks_with_data} "
          f"stocks (checked {total}).")


def _ev_key(ev: dict) -> tuple:
    return (
        ev["stock_id"], ev["change_date"], ev["insider_name"],
        ev["pledged_shares"], ev["released_shares"],
        ev["cumulative_pledged"], ev["report_date"],
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None,
                    help="only process the first N stocks (smoke test)")
    args = ap.parse_args()

    stocks = _load_stock_universe()
    if args.limit is not None:
        stocks = stocks[: args.limit]

    print(f"Universe: {len(stocks)} stocks "
          f"(b_date ROC {B_ROC_YEAR}0101 → {_roc_today()})")
    try:
        backfill(stocks)
    except KeyboardInterrupt:
        print("\nInterrupted. Re-run to resume — upserts are idempotent.")
