"""
Cboe volatility index scraper (VIX + VXSMH).

Source:
  https://cdn.cboe.com/api/global/us_indices/daily_prices/{SYMBOL}_History.csv

  VIX   — Cboe Volatility Index, SPX options, 30-day. Full history from
          1990-01-02 (~9.2k rows, ~470KB per fetch).
  VXSMH — Cboe Semiconductor ETF Volatility Index, SMH options, 30-day.
          Cboe only publishes a rolling window for the ETF series, so this
          file starts ~2025-09 (~220 rows). Older history accumulates in
          our DB from here on, same arrangement as scrapers.vix_tw.

Yahoo's ^VXSMH quote is not a substitute: it carries a current value but
range=2y returns a single bar, so history has to come from Cboe.

CSV format (comma separated, ASCII):
    DATE,OPEN,HIGH,LOW,CLOSE
    07/29/2026,62.380000,63.810000,60.700000,63.540000

Only CLOSE is persisted — that is what the /vix page charts.

Fetch throttling: vix_update.py is driven by intraday_publish.bat every
10 minutes during the session (~37 runs/day), while Cboe refreshes these
files once per day after the US close. So each symbol is skipped without
any HTTP when the DB already holds the latest expected trading day, or
when the last fetch was under an hour ago (which is what keeps US market
holidays from re-downloading all session long).
"""

from __future__ import annotations

import csv
import io
from datetime import date, timedelta
from typing import Iterable, Optional

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from utils.http_client import fetch

CSV_URL = "https://cdn.cboe.com/api/global/us_indices/daily_prices/{symbol}_History.csv"

SYMBOLS = ("VIX", "VXSMH")

# Skip a re-fetch if the newest stored row is this fresh (minutes).
REFETCH_AFTER_MIN = 60


def _prev_weekday(today: date) -> date:
    """Latest US trading day we can expect to be published, approximated
    as the previous weekday. US market holidays land us one day short —
    the fetched_at guard below stops that from re-downloading all day."""
    d = today - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _need_fetch(symbol: str, today: date) -> bool:
    """False when the DB is already current (or was refreshed minutes ago)."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            f"""
            SELECT max(trade_date) AS last_date,
                   bool_or(fetched_at > NOW() - INTERVAL '{REFETCH_AFTER_MIN} minutes') AS fresh
            FROM tw.vix_cboe
            WHERE symbol = %s
            """,
            (symbol,),
        )
        row = cur.fetchone()

    last_date = row["last_date"] if row else None
    if last_date is None:
        return True  # empty table — backfill
    if last_date >= _prev_weekday(today):
        return False
    return not row["fresh"]


def _fetch_symbol(symbol: str) -> Optional[str]:
    """Download one symbol's history CSV. Returns decoded text or None."""
    resp = fetch(CSV_URL.format(symbol=symbol), timeout=30)
    if resp is None:
        print(f"  Cboe {symbol}: fetch failed")
        return None
    return resp.text


def _parse_csv(symbol: str, text: str) -> Iterable[dict]:
    """Yield {symbol, trade_date, close} dicts from one history CSV."""
    reader = csv.reader(io.StringIO(text))
    header = next(reader, None)
    if not header or header[0].strip().upper() != "DATE":
        print(f"  Cboe {symbol}: unexpected header {header}")
        return

    try:
        close_idx = [h.strip().upper() for h in header].index("CLOSE")
    except ValueError:
        print(f"  Cboe {symbol}: no CLOSE column in header {header}")
        return

    for row in reader:
        if not row or len(row) <= close_idx:
            continue
        try:
            m, d, y = row[0].strip().split("/")
            trade_date = date(int(y), int(m), int(d))
            close = float(row[close_idx])
        except (ValueError, IndexError):
            continue
        yield {"symbol": symbol, "trade_date": trade_date, "close": close}


def save_rows(rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO tw.vix_cboe (symbol, trade_date, close)
        VALUES (%(symbol)s, %(trade_date)s, %(close)s)
        ON CONFLICT (symbol, trade_date) DO UPDATE SET
            close      = EXCLUDED.close,
            fetched_at = NOW()
    """
    with get_cursor() as cur:
        from psycopg2.extras import execute_batch
        execute_batch(cur, sql, rows, page_size=500)
    return len(rows)


def scrape_date(trade_date: date) -> ScrapeResult:
    """trade_date is only used as 'today' for the freshness check — each
    CSV carries its own full history, which we upsert wholesale."""
    saved = 0
    api_rows = 0
    skipped: list[str] = []

    for symbol in SYMBOLS:
        if not _need_fetch(symbol, trade_date):
            skipped.append(symbol)
            continue
        text = _fetch_symbol(symbol)
        if text is None:
            continue
        rows = list(_parse_csv(symbol, text))
        api_rows += len(rows)
        saved += save_rows(rows)
        if rows:
            print(f"  Cboe {symbol}: {len(rows)} rows, latest {rows[-1]['trade_date']} = {rows[-1]['close']}")

    if skipped:
        print(f"  Cboe: up to date, skipped {', '.join(skipped)}")
    print(f"  Cboe indices: saved {saved} rows (api={api_rows})")
    return ScrapeResult(records=saved, api_rows=api_rows, parse_errors=0)


if __name__ == "__main__":
    scrape_date(date.today())
