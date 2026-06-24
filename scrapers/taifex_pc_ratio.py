"""
TAIFEX TAIEX-options Put/Call ratio scraper (臺指選擇權 Put/Call 比).

Source: https://www.taifex.com.tw/cht/3/pcRatioDown (Big5 CSV).
  POST queryStartDate/queryEndDate. The form rejects ranges longer than ~1
  month, so scrape_range() is called per-month by the backfill.

CSV columns (7 + trailing empty):
  日期,賣權成交量,買權成交量,買賣權成交量比率%,賣權未平倉量,買權未平倉量,
  買賣權未平倉量比率%
"""

from __future__ import annotations

from datetime import date

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from scrapers.taifex_common import download, rows, num, integer, date_slash

URL = "https://www.taifex.com.tw/cht/3/pcRatioDown"

_SQL = """
    INSERT INTO tw.taifex_pc_ratio (
        trade_date, put_volume, call_volume, pc_volume_ratio,
        put_oi, call_oi, pc_oi_ratio
    ) VALUES (
        %(trade_date)s, %(put_volume)s, %(call_volume)s, %(pc_volume_ratio)s,
        %(put_oi)s, %(call_oi)s, %(pc_oi_ratio)s
    )
    ON CONFLICT (trade_date) DO UPDATE SET
        put_volume=EXCLUDED.put_volume, call_volume=EXCLUDED.call_volume,
        pc_volume_ratio=EXCLUDED.pc_volume_ratio,
        put_oi=EXCLUDED.put_oi, call_oi=EXCLUDED.call_oi,
        pc_oi_ratio=EXCLUDED.pc_oi_ratio, fetched_at=NOW()
"""


def _parse(text: str):
    out, api, errs = {}, 0, 0
    for r in rows(text):
        api += 1
        if len(r) < 7:
            errs += 1
            continue
        d = date_slash(r[0])
        if d is None:
            errs += 1
            continue
        out[d] = {
            "trade_date": d,
            "put_volume": integer(r[1]), "call_volume": integer(r[2]),
            "pc_volume_ratio": num(r[3]),
            "put_oi": integer(r[4]), "call_oi": integer(r[5]),
            "pc_oi_ratio": num(r[6]),
        }
    return list(out.values()), api, errs


def scrape_range(start: date, end: date) -> ScrapeResult:
    """Pull Put/Call ratio for [start, end] (keep ranges <= 1 month)."""
    text = download(URL, {
        "queryStartDate": start.strftime("%Y/%m/%d"),
        "queryEndDate": end.strftime("%Y/%m/%d"),
    })
    if text is None:
        print(f"  TAIFEX P/C ratio {start}~{end}: no data (range too long/error)")
        return ScrapeResult(records=0, api_rows=0, parse_errors=0)
    data, api, errs = _parse(text)
    if data:
        with get_cursor() as cur:
            from psycopg2.extras import execute_batch
            execute_batch(cur, _SQL, data, page_size=500)
    print(f"  TAIFEX P/C ratio {start}~{end}: saved {len(data)} "
          f"(api={api}, errors={errs})")
    return ScrapeResult(records=len(data), api_rows=api, parse_errors=errs)


def scrape_date(trade_date: date) -> ScrapeResult:
    return scrape_range(trade_date, trade_date)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 2:
        scrape_range(date.fromisoformat(sys.argv[1]), date.fromisoformat(sys.argv[2]))
    else:
        scrape_date(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today())
