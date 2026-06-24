"""
TAIFEX three-major-institutions scraper (三大法人, futures + options).

Sources (Big5 CSV, support multi-day ranges up to ~1 year):
  期貨   : https://www.taifex.com.tw/cht/3/futContractsDateDown
  選擇權 : https://www.taifex.com.tw/cht/3/callsAndPutsDateDown
  POST queryStartDate/queryEndDate, commodityId='' (all products).

scrape_date(d) pulls a single day (used by daily_update). scrape_range(s, e)
pulls a whole range in one request (used by the backfill, per-year batches).

Futures CSV columns (15):
  日期,商品名稱,身份別,多方交易口數,多方交易契約金額(千元),空方交易口數,
  空方交易契約金額(千元),多空交易口數淨額,多空交易契約金額淨額(千元),
  多方未平倉口數,多方未平倉契約金額(千元),空方未平倉口數,空方未平倉契約金額(千元),
  多空未平倉口數淨額,多空未平倉契約金額淨額(千元)

Options CSV columns (16): same shape but with 買賣權別 after 商品名稱 and
buy/sell (買方/賣方) labelling instead of 多方/空方.
"""

from __future__ import annotations

from datetime import date

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from scrapers.taifex_common import download, rows, integer, date_slash, call_put

FUT_URL = "https://www.taifex.com.tw/cht/3/futContractsDateDown"
OPT_URL = "https://www.taifex.com.tw/cht/3/callsAndPutsDateDown"

_FUT_SQL = """
    INSERT INTO tw.taifex_inst_futures (
        trade_date, product, investor,
        long_volume, long_amount, short_volume, short_amount,
        net_volume, net_amount, long_oi, long_oi_amount,
        short_oi, short_oi_amount, net_oi, net_oi_amount
    ) VALUES (
        %(trade_date)s, %(product)s, %(investor)s,
        %(long_volume)s, %(long_amount)s, %(short_volume)s, %(short_amount)s,
        %(net_volume)s, %(net_amount)s, %(long_oi)s, %(long_oi_amount)s,
        %(short_oi)s, %(short_oi_amount)s, %(net_oi)s, %(net_oi_amount)s
    )
    ON CONFLICT (trade_date, product, investor) DO UPDATE SET
        long_volume=EXCLUDED.long_volume, long_amount=EXCLUDED.long_amount,
        short_volume=EXCLUDED.short_volume, short_amount=EXCLUDED.short_amount,
        net_volume=EXCLUDED.net_volume, net_amount=EXCLUDED.net_amount,
        long_oi=EXCLUDED.long_oi, long_oi_amount=EXCLUDED.long_oi_amount,
        short_oi=EXCLUDED.short_oi, short_oi_amount=EXCLUDED.short_oi_amount,
        net_oi=EXCLUDED.net_oi, net_oi_amount=EXCLUDED.net_oi_amount,
        fetched_at=NOW()
"""

_OPT_SQL = """
    INSERT INTO tw.taifex_inst_options (
        trade_date, product, call_put, investor,
        buy_volume, buy_amount, sell_volume, sell_amount,
        net_volume, net_amount, buy_oi, buy_oi_amount,
        sell_oi, sell_oi_amount, net_oi, net_oi_amount
    ) VALUES (
        %(trade_date)s, %(product)s, %(call_put)s, %(investor)s,
        %(buy_volume)s, %(buy_amount)s, %(sell_volume)s, %(sell_amount)s,
        %(net_volume)s, %(net_amount)s, %(buy_oi)s, %(buy_oi_amount)s,
        %(sell_oi)s, %(sell_oi_amount)s, %(net_oi)s, %(net_oi_amount)s
    )
    ON CONFLICT (trade_date, product, call_put, investor) DO UPDATE SET
        buy_volume=EXCLUDED.buy_volume, buy_amount=EXCLUDED.buy_amount,
        sell_volume=EXCLUDED.sell_volume, sell_amount=EXCLUDED.sell_amount,
        net_volume=EXCLUDED.net_volume, net_amount=EXCLUDED.net_amount,
        buy_oi=EXCLUDED.buy_oi, buy_oi_amount=EXCLUDED.buy_oi_amount,
        sell_oi=EXCLUDED.sell_oi, sell_oi_amount=EXCLUDED.sell_oi_amount,
        net_oi=EXCLUDED.net_oi, net_oi_amount=EXCLUDED.net_oi_amount,
        fetched_at=NOW()
"""


def _parse_fut(text: str):
    out, api, errs = {}, 0, 0
    for r in rows(text):
        api += 1
        if len(r) < 15:
            errs += 1
            continue
        d = date_slash(r[0])
        product = r[1].strip()
        investor = r[2].strip()
        if d is None or not product or not investor:
            errs += 1
            continue
        out[(d, product, investor)] = {
            "trade_date": d, "product": product, "investor": investor,
            "long_volume": integer(r[3]), "long_amount": integer(r[4]),
            "short_volume": integer(r[5]), "short_amount": integer(r[6]),
            "net_volume": integer(r[7]), "net_amount": integer(r[8]),
            "long_oi": integer(r[9]), "long_oi_amount": integer(r[10]),
            "short_oi": integer(r[11]), "short_oi_amount": integer(r[12]),
            "net_oi": integer(r[13]), "net_oi_amount": integer(r[14]),
        }
    return list(out.values()), api, errs


def _parse_opt(text: str):
    out, api, errs = {}, 0, 0
    for r in rows(text):
        api += 1
        if len(r) < 16:
            errs += 1
            continue
        d = date_slash(r[0])
        product = r[1].strip()
        cp = call_put(r[2])
        investor = r[3].strip()
        if d is None or not product or cp is None or not investor:
            errs += 1
            continue
        out[(d, product, cp, investor)] = {
            "trade_date": d, "product": product, "call_put": cp,
            "investor": investor,
            "buy_volume": integer(r[4]), "buy_amount": integer(r[5]),
            "sell_volume": integer(r[6]), "sell_amount": integer(r[7]),
            "net_volume": integer(r[8]), "net_amount": integer(r[9]),
            "buy_oi": integer(r[10]), "buy_oi_amount": integer(r[11]),
            "sell_oi": integer(r[12]), "sell_oi_amount": integer(r[13]),
            "net_oi": integer(r[14]), "net_oi_amount": integer(r[15]),
        }
    return list(out.values()), api, errs


def _run(url: str, sql: str, parse, start: date, end: date) -> ScrapeResult:
    text = download(url, {
        "queryStartDate": start.strftime("%Y/%m/%d"),
        "queryEndDate": end.strftime("%Y/%m/%d"),
        "commodityId": "",
    })
    if text is None:
        return ScrapeResult(records=0, api_rows=0, parse_errors=0)
    data, api, errs = parse(text)
    if data:
        with get_cursor() as cur:
            from psycopg2.extras import execute_batch
            execute_batch(cur, sql, data, page_size=500)
    return ScrapeResult(records=len(data), api_rows=api, parse_errors=errs)


def scrape_range(start: date, end: date) -> ScrapeResult:
    """Pull futures + options institutional data for [start, end] (<= ~1yr)."""
    f = _run(FUT_URL, _FUT_SQL, _parse_fut, start, end)
    o = _run(OPT_URL, _OPT_SQL, _parse_opt, start, end)
    print(f"  TAIFEX inst {start}~{end}: fut={f.records} opt={o.records} "
          f"(errors fut={f.parse_errors} opt={o.parse_errors})")
    return ScrapeResult(records=f.records + o.records,
                        api_rows=f.api_rows + o.api_rows,
                        parse_errors=f.parse_errors + o.parse_errors)


def scrape_date(trade_date: date) -> ScrapeResult:
    return scrape_range(trade_date, trade_date)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 2:
        scrape_range(date.fromisoformat(sys.argv[1]), date.fromisoformat(sys.argv[2]))
    else:
        scrape_date(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today())
