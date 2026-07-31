"""Taiwan hog auction prices — the feed-cost / protein-price anchor for
1210 大成, 1215 卜蜂, 1219 福壽.

Source: 農業部「田邊好幫手」毛豬交易行情, the JSON the page's own grid calls.

    POST https://m.moa.gov.tw/Transaction/AnimalTrans/IndexPost
    body: NowPage=1&PageSize=100

It answers with one row per market per trading day (19 markets), newest
first, and reports the full row count in `total` — 49,128 rows as of
2026-07-30, roughly ten years. Unlike everything in market_html.py this one
IS backfillable, by walking NowPage.

The card is the national average, weighted by head count:

    Σ(BarAvgPrice × BarAmount) ÷ Σ(BarAmount)

which is what 中央畜產會 quotes; a plain mean over markets would let 台東
(174 head) count as much as 新北 (2,118 head). Prices are TWD per kg.

Note the sibling endpoint /Transaction/PoultryTrans/IndexPost (broiler and
egg prices) is NOT wired up: it answers 200 with total:0 for every parameter
set tried, and its field names are not discoverable from the page.

Usage:
  python -m scrapers.market_hog             # last ~5 trading days
  python -m scrapers.market_hog backfill    # every page the source has
"""

from __future__ import annotations

import sys
from datetime import date

from scrapers.market_quote import save_rows
from utils.format_shift import ScrapeResult
from utils.http_client import post_json

INDEX_URL = "https://m.moa.gov.tw/Transaction/AnimalTrans/Index"
POST_URL  = "https://m.moa.gov.tw/Transaction/AnimalTrans/IndexPost"

PAGE_SIZE = 500

# Same shape as market_quote.SYMBOLS — export/generate.py reads display
# metadata off this dict.
HOG_SYMBOLS = {
    "hog": {"name": "毛豬", "category": "agri", "unit": "TWD/公斤",
            "dp": 2, "freq": "daily"},
}


def _roc_date(s: str) -> date | None:
    """'115/07/30' -> date(2026, 7, 30). The source prints ROC years only."""
    try:
        y, m, d = s.split("/")
        return date(int(y) + 1911, int(m), int(d))
    except (AttributeError, ValueError):
        return None


def _fetch_page(page: int) -> tuple[list[dict], int]:
    """One page of market×day rows, plus the source's total row count."""
    data = post_json(POST_URL, data={"NowPage": page, "PageSize": PAGE_SIZE},
                     timeout=60)
    if not data:
        return [], 0
    return data.get("rows") or [], data.get("total") or 0


def _daily_average(rows: list[dict]) -> dict[date, float]:
    """Head-count-weighted national average per trading day."""
    acc: dict[date, list[float]] = {}
    for r in rows:
        d = _roc_date(r.get("TransDate"))
        price = r.get("BarAvgPrice")
        head = r.get("BarAmount") or r.get("ImportAmount")
        if d is None or not price or not head:
            continue
        agg = acc.setdefault(d, [0.0, 0.0])
        agg[0] += float(price) * float(head)
        agg[1] += float(head)
    return {d: round(num / den, 2) for d, (num, den) in acc.items() if den}


def scrape(pages: int | None = 1) -> ScrapeResult:
    """Pull `pages` pages (None = everything) and upsert the daily average.

    Rows are collected across every page BEFORE averaging, because a page
    boundary lands mid-day: averaging page by page would publish a national
    average computed from whichever markets happened to fall on that page.
    For the same reason a run that stops early drops its oldest day — that
    one is the truncated half.
    """
    collected: list[dict] = []
    page = 1
    total = 0
    while True:
        rows, total = _fetch_page(page)
        if not rows:
            break
        collected.extend(rows)
        exhausted = len(collected) >= total
        if exhausted or (pages is not None and page >= pages):
            break
        page += 1

    if not collected:
        raise RuntimeError(
            "market_hog: source returned no rows — endpoint or its parameter "
            "names changed, or we are being blocked"
        )

    daily = _daily_average(collected)
    if daily and len(collected) < total:
        del daily[min(daily)]

    rows_out = [("hog", d, v) for d, v in sorted(daily.items())]
    saved = save_rows(rows_out)
    span = f"{rows_out[0][1]} .. {rows_out[-1][1]}" if rows_out else "none"
    print(f"  market_hog: saved {saved} days from {len(collected)}/{total} "
          f"rows ({span})")
    return ScrapeResult(records=saved, api_rows=len(collected), parse_errors=0)


def scrape_date(trade_date: date) -> ScrapeResult:
    """daily_update entry point. trade_date is unused: the source stamps its
    own trading day, and one page already covers several of them, which is
    what heals a short outage without a special backfill run."""
    return scrape(pages=1)


if __name__ == "__main__":
    scrape(pages=None if "backfill" in sys.argv else 1)
