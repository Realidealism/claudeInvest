"""
Global market quotes scraper — commodities, FX and foreign indices.

Source: Yahoo Finance v8 chart endpoint (unofficial but currently open —
no API key, no crumb, no cookie; verified 2026-07-11 across every ticker
below).

    https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1mo&interval=1d

It only answers to a browser User-Agent — a bare requests call gets an
empty body. utils.http_client.DEFAULT_HEADERS already sends one, which is
why every call here goes through fetch_json().

SYMBOLS is the single source of truth for the whole feature: the scraper
reads `ticker` from it, export/generate.py reads the display metadata off
the same dict. Our own `symbol` key is what lands in the DB, deliberately
decoupled from the upstream ticker so a source swap doesn't rewrite history.

Usage:
  python -m scrapers.market_quote            # last month (daily run)
  python -m scrapers.market_quote backfill   # 10 years (one-off seeding)
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from typing import Any

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json

CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"

# symbol -> upstream ticker + presentation metadata.
#
# category groups the cards on the frontend page; unit drives number
# formatting there. `dp` is the decimal precision to display. Everything from
# this source is a daily bar — `freq` exists because market_html.py carries
# weekly series (FBX) and the frontend labels moves off it.
SYMBOLS: dict[str, dict[str, Any]] = {
    # 能源
    "wti":     {"ticker": "CL=F",      "name": "西德州原油", "category": "energy",   "unit": "USD/桶",  "dp": 2, "freq": "daily"},
    "brent":   {"ticker": "BZ=F",      "name": "布蘭特原油", "category": "energy",   "unit": "USD/桶",  "dp": 2, "freq": "daily"},
    "natgas":  {"ticker": "NG=F",      "name": "天然氣",     "category": "energy",   "unit": "USD/MMBtu", "dp": 3, "freq": "daily"},
    # 金屬
    "copper":  {"ticker": "HG=F",      "name": "銅",         "category": "metal",    "unit": "USD/磅",  "dp": 4, "freq": "daily"},
    "gold":    {"ticker": "GC=F",      "name": "黃金",       "category": "metal",    "unit": "USD/盎司", "dp": 2, "freq": "daily"},
    "silver":  {"ticker": "SI=F",      "name": "白銀",       "category": "metal",    "unit": "USD/盎司", "dp": 3, "freq": "daily"},
    # 匯率 / 資金
    "dxy":     {"ticker": "DX-Y.NYB",  "name": "美元指數",   "category": "fx",       "unit": "點",      "dp": 3, "freq": "daily"},
    "usdtwd":  {"ticker": "TWD=X",     "name": "美元/台幣",  "category": "fx",       "unit": "TWD",     "dp": 3, "freq": "daily"},
    # 航運 (BDI itself has no free history; this ETF is the tradeable proxy
    # and carries the long series the BDI card charts against)
    "bdry":    {"ticker": "BDRY",      "name": "乾散貨 ETF", "category": "shipping", "unit": "USD",     "dp": 2, "freq": "daily"},
}


def _fetch_series(ticker: str, rng: str) -> list[tuple[date, float]] | None:
    """Pull one ticker's daily closes. Returns [(trade_date, close), ...]
    or None if the fetch/parse failed."""
    data = fetch_json(
        CHART_URL.format(ticker=ticker),
        params={"range": rng, "interval": "1d"},
        timeout=30,
    )
    try:
        result = data["chart"]["result"][0]
        stamps = result["timestamp"]
        closes = result["indicators"]["quote"][0]["close"]
        # Bars are stamped at the market's local session start; shifting by
        # the exchange's UTC offset before taking .date() is what keeps a
        # Taipei close on its own day rather than bleeding into the previous
        # UTC one.
        offset = result["meta"].get("gmtoffset", 0) or 0
    except (TypeError, KeyError, IndexError):
        print(f"  {ticker}: unexpected payload shape")
        return None

    out: list[tuple[date, float]] = []
    for ts, close in zip(stamps, closes):
        if close is None:
            continue
        d = datetime.fromtimestamp(ts + offset, tz=timezone.utc).date()
        out.append((d, float(close)))
    return out


def save_rows(rows: list[tuple[str, date, float]]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO tw.market_quote (symbol, trade_date, close)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol, trade_date) DO UPDATE SET
            close      = EXCLUDED.close,
            fetched_at = NOW()
    """
    with get_cursor() as cur:
        from psycopg2.extras import execute_batch
        execute_batch(cur, sql, rows, page_size=500)
    return len(rows)


def scrape(rng: str = "1mo") -> ScrapeResult:
    """Pull every symbol in SYMBOLS over `rng` and upsert. A month-long
    window on the daily run backfills holiday gaps and picks up any
    upstream revision for free; all upserts are idempotent."""
    rows: list[tuple[str, date, float]] = []
    api_rows = 0
    failures = 0

    for symbol, meta in SYMBOLS.items():
        series = _fetch_series(meta["ticker"], rng)
        if not series:
            print(f"  market_quote {symbol} ({meta['ticker']}): no data")
            failures += 1
            continue
        api_rows += len(series)
        rows.extend((symbol, d, c) for d, c in series)

    # Yahoo v8 is an unofficial endpoint — the day it starts refusing us
    # (401/429/removal), every symbol fails at once. Raising lets
    # daily_update's retry and failure-notification path see it; returning a
    # clean ScrapeResult would freeze the page with nobody the wiser.
    if failures == len(SYMBOLS):
        raise RuntimeError(
            f"market_quote: all {len(SYMBOLS)} symbols failed — "
            f"Yahoo v8 endpoint is likely blocking us"
        )

    saved = save_rows(rows)
    print(f"  market_quote: saved {saved} rows over {len(SYMBOLS) - failures}"
          f"/{len(SYMBOLS)} symbols (api={api_rows}, failed={failures})")
    return ScrapeResult(records=saved, api_rows=api_rows, parse_errors=failures)


def scrape_date(trade_date: date) -> ScrapeResult:
    """daily_update.py entry point. These are global series with their own
    sessions, so trade_date only marks the run — the window is always the
    trailing month."""
    return scrape("1mo")


if __name__ == "__main__":
    rng = "10y" if "backfill" in sys.argv else "1mo"
    scrape(rng)
