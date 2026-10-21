"""
Stock futures contract -> underlying stock (tw.stock_futures_contracts).

Source: https://www.taifex.com.tw/cht/2/stockLists (GET, HTML table of the
current stock futures / options underlyings). Each row with a futures
underlying gives contract = 商品代碼 + "F", the stock id, and the shares per
lot: 2000 (ETF 10000) is the standard contract, 100 / 1000 the minis.

The table feeds the short-sale constraint of the backtest (signal_backtest.
short_constraints, margin_futures): a short goes through futures where the
stock's standard contracts traded >= 50 lots a day over the 20 days before
(ruling 2026-10-21). Delisted codes were imported once with source
'price_match'; this scraper only upserts the current list and stamps
last_listed (never moved back, so a run for an old date leaves it).

Reported as failed sources (checks on tw.taifex_futures_daily, written by
scrapers.taifex_futures earlier in the run):
  - a listed code whose stock differs from the table's: the table keeps its
    stock, as a reused code would move the code's history to another stock;
  - a three-letter F code that traded today and that the table does not
    know: recorded once as kind 'unknown' (source 'feed', never counted as a
    stock), so it is reported on the first day only; the list turns it into
    'stock' if it is one;
  - a day on which fewer than FEED_MIN_SHARE of the listed standard contracts
    have a day-session row: the feed lost them, and the backtest would read
    the day as no futures traded.
"""

from __future__ import annotations

from datetime import date

from bs4 import BeautifulSoup

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from utils.http_client import fetch

URL = "https://www.taifex.com.tw/cht/2/stockLists"
STANDARD_LOTS = (2000, 10000)
# 262-270 of the 270 listed standard contracts had day-session rows on every
# day of 2026 to 10-20, traded or not
FEED_MIN_SHARE = 0.9

_UPSERT = """
    INSERT INTO tw.stock_futures_contracts AS t
        (contract, kind, stock_id, shares_per_lot, standard, source, last_listed, updated_at)
    VALUES (%(contract)s, 'stock', %(stock_id)s, %(lot)s, %(standard)s, 'stockLists', %(d)s, NOW())
    ON CONFLICT (contract) DO UPDATE SET
        kind='stock', stock_id=EXCLUDED.stock_id, shares_per_lot=EXCLUDED.shares_per_lot,
        standard=EXCLUDED.standard, source='stockLists',
        last_listed=GREATEST(t.last_listed, EXCLUDED.last_listed), updated_at=NOW()
    WHERE t.stock_id IS NULL OR t.stock_id = EXCLUDED.stock_id
"""
_UNKNOWN = """
    INSERT INTO tw.stock_futures_contracts (contract, kind, standard, source)
    VALUES (%s, 'unknown', FALSE, 'feed') ON CONFLICT (contract) DO NOTHING
"""


def parse(html: str) -> tuple[list[dict], int, int] | None:
    """(rows, api_rows, parse_errors), or None when the page carries no
    such table. Cells are read as text, so the code "NA" (穩懋) stays a code;
    lots come as "2,000"."""
    for table in BeautifulSoup(html, "html.parser").find_all("table"):
        head = [c.get_text(" ", strip=True) for c in table.find_all("th")]
        if len(head) >= 12 and "商品代碼" in head[0].replace(" ", "") and "證券代號" in head[2]:
            break
    else:
        return None
    out, api_rows, errors = [], 0, 0
    for tr in table.find_all("tr"):
        r = [c.get_text(" ", strip=True) for c in tr.find_all("td")]
        if not r:
            continue
        api_rows += 1
        if len(r) < 12:
            errors += 1
            continue
        if "股票期貨" not in r[4]:
            continue                      # an options-only underlying
        prefix, stock_id, lot = r[0], r[2], r[11].replace(",", "")
        if not prefix or not stock_id or not lot.isdigit():
            errors += 1
            continue
        out.append({"contract": f"{prefix}F", "stock_id": stock_id, "lot": int(lot),
                    "standard": int(lot) in STANDARD_LOTS})
    return out, api_rows, errors


def scrape_date(trade_date: date) -> ScrapeResult:
    resp = fetch(URL)
    if resp is None:
        return ScrapeResult(0, 0, 0, failed_sources=("期交所個股期清單（沒有回應）",))
    parsed = parse(resp.content.decode("utf-8", errors="replace"))
    if parsed is None or not parsed[0]:
        return ScrapeResult(0, 0, 0, failed_sources=("期交所個股期清單（回應讀不懂）",))
    data, api_rows, errors = parsed
    standard = [r["contract"] for r in data if r["standard"]]
    with get_cursor() as cur:
        from psycopg2.extras import execute_batch
        cur.execute("SELECT contract, stock_id FROM tw.stock_futures_contracts "
                    "WHERE contract = ANY(%s) AND stock_id IS NOT NULL", ([r["contract"] for r in data],))
        held = {r["contract"]: r["stock_id"] for r in cur.fetchall()}
        moved = [(r["contract"], held[r["contract"]], r["stock_id"]) for r in data
                 if held.get(r["contract"], r["stock_id"]) != r["stock_id"]]
        execute_batch(cur, _UPSERT, [dict(r, d=trade_date) for r in data], page_size=500)
        cur.execute("""SELECT DISTINCT f.contract FROM tw.taifex_futures_daily f
                       WHERE f.trade_date = %s AND f.contract ~ '^[A-Z]{2}F$' AND f.volume > 0
                         AND NOT EXISTS (SELECT 1 FROM tw.stock_futures_contracts c
                                         WHERE c.contract = f.contract)
                       ORDER BY 1""", (trade_date,))
        unknown = [r["contract"] for r in cur.fetchall()]
        for c in unknown:
            cur.execute(_UNKNOWN, (c,))
        cur.execute("""SELECT count(DISTINCT contract) AS n FROM tw.taifex_futures_daily
                       WHERE trade_date = %s AND contract = ANY(%s) AND session = '一般'
                         AND contract_month NOT LIKE '%%/%%'""", (trade_date, standard))
        in_feed = cur.fetchone()["n"]
    failed = [f"期貨代碼 {c} 改對應 {a}→{b}（未更新）" for c, a, b in moved]
    failed += [f"未知期貨代碼 {c}（已登記 unknown）" for c in unknown]
    if in_feed < FEED_MIN_SHARE * len(standard):
        failed.append(f"個股期行情只有 {in_feed}/{len(standard)} 檔標準型契約")
    print(f"  stock futures contracts {trade_date}: {len(data)} listed (standard {len(standard)}, "
          f"{in_feed} in the feed), unknown traded codes {unknown}, moved {moved}")
    return ScrapeResult(records=len(data), api_rows=api_rows, parse_errors=errors,
                        failed_sources=tuple(failed))


if __name__ == "__main__":
    import sys
    scrape_date(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today())
