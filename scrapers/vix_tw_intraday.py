"""
Intraday TAIWAN VIX poller — fetches the MIS getQuoteListVIX endpoint
during the trading session and upserts the latest snapshot into
tw.vix_tw.

API:
  POST https://mis.taifex.com.tw/futures/api/getQuoteListVIX
  Body: {} (empty JSON; the endpoint always returns the single VIX
        quote, no symbol filter needed)
  Response shape (RtCode "0" means success):
    {
      "RtCode": "0",
      "RtData": {
        "QuoteList": [
          {
            "SymbolID":    "TAIWANVIX",
            "CLastPrice":  "43.58",   # last traded VIX (intraday or close)
            "CDate":       "20260611",
            "CTime":       "134500",  # HHMMSS of last update
            ...
          }
        ]
      }
    }

Behaviour:
  - During session (09:00–13:45 TPE) CTime advances and CLastPrice tracks
    the live VIX. We upsert with intraday_time = CTime, so the row is
    marked as a non-final snapshot.
  - After 13:45 the same endpoint keeps returning the final value (CTime
    = "134500" indefinitely). Running this scraper post-close is safe;
    it will just keep upserting the same close value.
  - The daily scraper (scrapers.vix_tw) runs ~14:30 with the official
    minute-level file and sets intraday_time = NULL, marking the row as
    settled.
"""

from __future__ import annotations

import json
from datetime import date
from typing import Optional

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from utils.http_client import get_session

API_URL = "https://mis.taifex.com.tw/futures/api/getQuoteListVIX"

HEADERS = {
    "Accept":       "application/json, text/plain, */*",
    "Content-Type": "application/json;charset=UTF-8",
    "Origin":       "https://mis.taifex.com.tw",
    "Referer":      "https://mis.taifex.com.tw/futures/VolatilityQuotes/",
}


def fetch_snapshot() -> Optional[dict]:
    """POST to the MIS endpoint and return the parsed QuoteList[0] dict
    on success, or None on any failure (network, parse, RtCode != "0").
    """
    session = get_session()
    try:
        resp = session.post(API_URL, headers=HEADERS, data="{}", timeout=15)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        print(f"  TWVIX intraday: fetch failed: {e}")
        return None

    if payload.get("RtCode") != "0":
        print(f"  TWVIX intraday: RtCode={payload.get('RtCode')} msg={payload.get('RtMsg')}")
        return None

    quotes = (payload.get("RtData") or {}).get("QuoteList") or []
    if not quotes:
        print("  TWVIX intraday: empty QuoteList")
        return None

    return quotes[0]


def _save(trade_date: date, close: float, intraday_time: str) -> int:
    """Upsert one intraday row. Setting intraday_time to a non-NULL
    HHMMSS string marks this value as a session snapshot, not the
    official close — the daily scraper will overwrite intraday_time
    back to NULL once it runs."""
    sql = """
        INSERT INTO tw.vix_tw (trade_date, close, intraday_time)
        VALUES (%(trade_date)s, %(close)s, %(intraday_time)s)
        ON CONFLICT (trade_date) DO UPDATE SET
            close         = EXCLUDED.close,
            intraday_time = EXCLUDED.intraday_time,
            fetched_at    = NOW()
    """
    with get_cursor() as cur:
        cur.execute(sql, {
            "trade_date":    trade_date,
            "close":         close,
            "intraday_time": intraday_time,
        })
    return 1


def scrape_date(trade_date: date) -> ScrapeResult:
    """trade_date argument is ignored — the MIS endpoint always returns
    'today as TAIFEX sees it'. We trust CDate from the payload."""
    snap = fetch_snapshot()
    if snap is None:
        return ScrapeResult(records=0, api_rows=0, parse_errors=1)

    try:
        date_str = snap["CDate"]
        time_str = snap["CTime"]
        close    = float(snap["CLastPrice"])
        td = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
    except (KeyError, ValueError, TypeError) as e:
        print(f"  TWVIX intraday: parse error {e} payload={json.dumps(snap)[:200]}")
        return ScrapeResult(records=0, api_rows=1, parse_errors=1)

    saved = _save(td, close, time_str)
    print(f"  TWVIX intraday: {td} {time_str} close={close} saved={saved}")
    return ScrapeResult(records=saved, api_rows=1, parse_errors=0)


if __name__ == "__main__":
    scrape_date(date.today())
