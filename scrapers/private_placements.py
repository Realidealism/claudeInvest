"""
Company private-placement scraper (私募有價證券, MOPS ajax_t116sb01 一覽表).

One market-wide request per market (sii/otc) returns the FULL cumulative table of
every private-placement case. Each row's drill-down onclick embeds co_id +
decide_date (董事會決議日 = the date the market first learns) + stock_kind, so the
list alone yields the event without drilling into the detail form.

  fetch(market) -> list[dict]
  save_placements(rows) -> int
  scrape_date(trade_date)  — daily hook (sii + otc; new decide_date rows upsert in)

NOTE: UTF-8 response; cumulative table so re-running is idempotent via upsert.
"""

import re
from datetime import date

from db.connection import get_cursor
from scrapers.insider_holdings import _get_mops_session, _mops_rate_limit
from utils.format_shift import ScrapeResult

MOPS_URL = "https://mopsov.twse.com.tw/mops/web/ajax_t116sb01"
_REFERER = "https://mopsov.twse.com.tw/mops/web/t116sb01"


def _roc_ymd(s: str) -> date | None:
    s = s.strip()
    if len(s) not in (6, 7):
        return None
    try:
        return date(int(s[:-4]) + 1911, int(s[-4:-2]), int(s[-2:]))
    except ValueError:
        return None


def fetch(market: str) -> list[dict]:
    """Full cumulative placement list for one market. market = 'sii' | 'otc'."""
    payload = {
        "step": "1", "firstin": "true", "off": "1", "TYPEK": market, "co_id": "",
        "queryName": "co_id", "inpuType": "co_id", "isnew": "false",
    }
    _mops_rate_limit()
    s = _get_mops_session()
    try:
        r = s.post(MOPS_URL, data=payload, headers={"Referer": _REFERER}, timeout=60)
        r.raise_for_status()
    except Exception as e:
        print(f"  [placements] {market} request error: {e}")
        return []

    html = r.content.decode("utf-8", "replace")
    rows: list[dict] = []
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S | re.I):
        m = re.search(r"co_id\.value='(\w+)';.*?decide_date\.value='(\d{6,7})';"
                      r".*?stock_kind\.value='(\w*)'", row, re.S)
        if not m:
            continue
        dd = _roc_ymd(m.group(2))
        if dd is None:
            continue
        cells = [re.sub(r"<[^>]+>", "", c).replace("&nbsp;", " ").strip()
                 for c in re.findall(r"<td[^>]*>(.*?)</td>", row, re.S | re.I)]
        rows.append({
            "stock_id":      m.group(1),
            "decide_date":   dd,
            "security_kind": cells[2] if len(cells) > 2 else "",
            "market":        market,
        })
    return rows


_UPSERT = """
    INSERT INTO tw.private_placements (
        stock_id, decide_date, security_kind, market, updated_at
    ) VALUES (%s, %s, %s, %s, NOW())
    ON CONFLICT (stock_id, decide_date, security_kind)
    DO UPDATE SET market = EXCLUDED.market, updated_at = NOW()
"""


def save_placements(rows: list[dict]) -> int:
    if not rows:
        return 0
    seen = set()
    with get_cursor() as cur:
        for r in rows:
            key = (r["stock_id"], r["decide_date"], r["security_kind"])
            if key in seen:
                continue
            seen.add(key)
            cur.execute(_UPSERT, (r["stock_id"], r["decide_date"],
                                  r["security_kind"], r["market"]))
    return len(seen)


def scrape_date(trade_date: date) -> ScrapeResult:
    """Daily hook: fetch full cumulative list for sii + otc, upsert (dedupes)."""
    total = 0
    for market in ("sii", "otc"):
        total += save_placements(fetch(market))
    print(f"  [placements] sii+otc, {total} cases upserted.")
    return ScrapeResult(records=total, api_rows=total, parse_errors=0)


if __name__ == "__main__":
    scrape_date(date.today())
