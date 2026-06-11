"""
TAIFEX TAIEX Options Volatility Index (TWVIX) scraper.

Source:
  https://www.taifex.com.tw/cht/7/vixDaily3MNew  (HTML index page)
  https://www.taifex.com.tw/file/taifex/Dailydownload/vix/log2data/{YYYYMM}new.txt
    (per-month tab-separated text dump in Big5 encoding)

The HTML page only links to the trailing ~3 calendar months of monthly
files. We fetch the current month plus the three prior months on each
run and upsert into tw.vix_tw; running daily lets older history
accumulate naturally even though TAIFEX only publishes a short window.

File format (Big5, tab + leading-space separated):
    交易日期\t時間(時/分/秒/毫秒)\t臺指選擇權波動率指數\t收盤前1分鐘平均指數
    --------\t------------------\t------------------\t------------------
    20260601\t13450000\t\t\t36.54\t\t36.52
    ...

We keep the closing value (`臺指選擇權波動率指數`, column index 2 after
split-on-whitespace).
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from utils.http_client import fetch

MONTH_URL = "https://www.taifex.com.tw/file/taifex/Dailydownload/vix/log2data/{ym}new.txt"


def _month_keys(today: date, lookback_months: int = 4) -> list[str]:
    """Return YYYYMM strings for `today` and the prior `lookback_months - 1`
    months, newest first. Default of 4 covers TAIFEX's published window."""
    keys: list[str] = []
    y, m = today.year, today.month
    for _ in range(lookback_months):
        keys.append(f"{y:04d}{m:02d}")
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return keys


def _fetch_month(ym: str) -> str | None:
    """Download one month's TAIFEX VIX text file. Returns decoded text or
    None on 404 / fetch failure."""
    url = MONTH_URL.format(ym=ym)
    resp = fetch(url, timeout=30)
    if resp is None:
        return None
    # TAIFEX serves Big5; their 404 fallback is also Big5 but the marker is
    # an HTML doctype.
    text = resp.content.decode("big5", errors="replace")
    if text.lstrip().lower().startswith("<html") or "<title>404</title>" in text:
        return None
    return text


def _parse_month_text(text: str) -> Iterable[dict]:
    """Yield {trade_date, close} dicts from one monthly file."""
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        # Skip header / divider rows; the data rows start with 8 digits.
        if not line[:8].isdigit():
            continue
        parts = line.split()
        # Expected: [YYYYMMDD, HHMMSSff, close_index, prev_minute_avg]
        if len(parts) < 3:
            continue
        date_str, _, close_str = parts[0], parts[1], parts[2]
        try:
            d = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
            close = float(close_str)
        except ValueError:
            continue
        yield {"trade_date": d, "close": close}


def save_rows(rows: list[dict]) -> int:
    if not rows:
        return 0
    # intraday_time is forced to NULL here: the daily file is the
    # official settled close, so any row this scraper writes overrides
    # whatever intraday snapshot the MIS poller left behind.
    sql = """
        INSERT INTO tw.vix_tw (trade_date, close, intraday_time)
        VALUES (%(trade_date)s, %(close)s, NULL)
        ON CONFLICT (trade_date) DO UPDATE SET
            close         = EXCLUDED.close,
            intraday_time = NULL,
            fetched_at    = NOW()
    """
    with get_cursor() as cur:
        from psycopg2.extras import execute_batch
        execute_batch(cur, sql, rows, page_size=100)
    return len(rows)


def scrape_date(trade_date: date) -> ScrapeResult:
    """trade_date is ignored — every run pulls the published 4-month
    window from TAIFEX. Older history is retained in DB."""
    rows: list[dict] = []
    api_rows = 0
    months_hit = 0
    for ym in _month_keys(trade_date):
        text = _fetch_month(ym)
        if text is None:
            continue
        months_hit += 1
        month_rows = list(_parse_month_text(text))
        api_rows += len(month_rows)
        rows.extend(month_rows)

    # De-dup by trade_date (later months don't overlap, but be safe).
    deduped: dict[date, dict] = {r["trade_date"]: r for r in rows}
    final = list(deduped.values())

    saved = save_rows(final)
    print(f"  TAIFEX TWVIX: saved {saved} rows (api={api_rows}, months={months_hit})")
    parse_errors = 0 if (months_hit > 0 or trade_date.weekday() >= 5) else 1
    return ScrapeResult(records=saved, api_rows=api_rows, parse_errors=parse_errors)


if __name__ == "__main__":
    scrape_date(date.today())
