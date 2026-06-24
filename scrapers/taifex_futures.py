"""
TAIFEX futures daily OHLC scraper (期貨每日行情, all products).

Source: https://www.taifex.com.tw/cht/3/futDataDown
  POST down_type=1, commodity_id=all, queryStartDate/queryEndDate (same day).
  Returns Big5 CSV of every futures product for the day, regular (一般) and
  after-hours (盤後) sessions. The download form rejects multi-month ranges,
  so this scraper queries one trading day at a time.

CSV columns (19):
  交易日期,契約,到期月份(週別),開盤價,最高價,最低價,收盤價,漲跌價,漲跌%,
  成交量,結算價,未沖銷契約數,最後最佳買價,最後最佳賣價,歷史最高價,歷史最低價,
  是否因訊息面暫停交易,交易時段,價差對單式委託成交量
"""

from __future__ import annotations

from datetime import date

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from scrapers.taifex_common import (
    download, rows, num, integer, date_slash, halted,
)

URL = "https://www.taifex.com.tw/cht/3/futDataDown"

# Only persist TAIEX index futures: TX (大台), MTX (小台), TMF (微型台指).
# commodity_id=all still fetches every product in one request; the full feed
# carries 300+ mostly-illiquid single-stock futures that we intentionally drop.
KEEP_CONTRACTS = {"TX", "MTX", "TMF"}

_SQL = """
    INSERT INTO tw.taifex_futures_daily (
        trade_date, contract, contract_month, session,
        open_price, high_price, low_price, close_price, change, change_pct,
        volume, settlement, open_interest, best_bid, best_ask,
        hist_high, hist_low, halted, spread_volume
    ) VALUES (
        %(trade_date)s, %(contract)s, %(contract_month)s, %(session)s,
        %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s,
        %(change)s, %(change_pct)s, %(volume)s, %(settlement)s,
        %(open_interest)s, %(best_bid)s, %(best_ask)s,
        %(hist_high)s, %(hist_low)s, %(halted)s, %(spread_volume)s
    )
    ON CONFLICT (trade_date, contract, contract_month, session) DO UPDATE SET
        open_price=EXCLUDED.open_price, high_price=EXCLUDED.high_price,
        low_price=EXCLUDED.low_price, close_price=EXCLUDED.close_price,
        change=EXCLUDED.change, change_pct=EXCLUDED.change_pct,
        volume=EXCLUDED.volume, settlement=EXCLUDED.settlement,
        open_interest=EXCLUDED.open_interest, best_bid=EXCLUDED.best_bid,
        best_ask=EXCLUDED.best_ask, hist_high=EXCLUDED.hist_high,
        hist_low=EXCLUDED.hist_low, halted=EXCLUDED.halted,
        spread_volume=EXCLUDED.spread_volume, fetched_at=NOW()
"""


def _parse(text: str) -> tuple[list[dict], int, int]:
    """Return (deduped rows, api_rows, parse_errors)."""
    out: dict[tuple, dict] = {}
    api_rows = 0
    parse_errors = 0
    for r in rows(text):
        api_rows += 1
        if len(r) < 19:
            parse_errors += 1
            continue
        d = date_slash(r[0])
        contract = r[1].strip()
        month = r[2].strip()
        session = r[17].strip()
        if contract not in KEEP_CONTRACTS:
            continue
        if d is None or not month or not session:
            parse_errors += 1
            continue
        out[(d, contract, month, session)] = {
            "trade_date": d, "contract": contract,
            "contract_month": month, "session": session,
            "open_price": num(r[3]), "high_price": num(r[4]),
            "low_price": num(r[5]), "close_price": num(r[6]),
            "change": num(r[7]), "change_pct": num(r[8]),
            "volume": integer(r[9]), "settlement": num(r[10]),
            "open_interest": integer(r[11]),
            "best_bid": num(r[12]), "best_ask": num(r[13]),
            "hist_high": num(r[14]), "hist_low": num(r[15]),
            "halted": halted(r[16]), "spread_volume": integer(r[18]),
        }
    return list(out.values()), api_rows, parse_errors


def scrape_date(trade_date: date) -> ScrapeResult:
    ymd = trade_date.strftime("%Y/%m/%d")
    text = download(URL, {
        "down_type": "1", "commodity_id": "all", "commodity_id2": "",
        "queryStartDate": ymd, "queryEndDate": ymd,
    })
    if text is None:
        print(f"  TAIFEX futures {trade_date}: no data (holiday/error)")
        return ScrapeResult(records=0, api_rows=0, parse_errors=0)

    data, api_rows, parse_errors = _parse(text)
    if data:
        with get_cursor() as cur:
            from psycopg2.extras import execute_batch
            execute_batch(cur, _SQL, data, page_size=500)
    print(f"  TAIFEX futures {trade_date}: saved {len(data)} "
          f"(api={api_rows}, errors={parse_errors})")
    return ScrapeResult(records=len(data), api_rows=api_rows, parse_errors=parse_errors)


if __name__ == "__main__":
    import sys
    scrape_date(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today())
