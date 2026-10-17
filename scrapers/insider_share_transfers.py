"""
Insider pre-declared share-transfer scraper (內部人持股轉讓事前申報, MOPS ajax_t56sb21).

Event-grained: one row per pre-declared transfer filing, keeping 轉讓方式 so the
'洽特定人' (block-sale to a specific buyer) tradeable signal is filterable downstream.
Market-wide query (co_id empty + TYPEK=sii/otc) returns all filings for a month; the
monthly query itself is the discovery mechanism (no separate index needed).

  fetch(market, roc_year, month) -> list[dict]
  save_transfers(rows) -> int
  scrape_date(trade_date)  — daily hook (current + previous calendar month, sii+otc)

NOTE: this endpoint returns UTF-8 (not Big5 like STAMAK03_1); decode accordingly.
"""

import re
from datetime import date

from db.connection import get_cursor
from scrapers.insider_holdings import _get_mops_session, _mops_rate_limit, _parse_int
from utils.format_shift import EMPTY, NO_RESPONSE, OK, UNREADABLE, ScrapeResult, failed_legs

MOPS_URL = "https://mopsov.twse.com.tw/mops/web/ajax_t56sb21"
_REFERER = "https://mopsov.twse.com.tw/mops/web/t56sb21_q1"


def _roc_to_ad(s: str) -> date | None:
    s = re.sub(r"<[^>]+>", "", str(s)).replace("&nbsp;", "").strip().split("\n")[0].strip()
    m = re.match(r"^(\d{2,3})/(\d{1,2})/(\d{1,2})$", s)
    if not m:
        return None
    try:
        return date(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _clean(c: str) -> str:
    return re.sub(r"<[^>]+>", "", str(c)).replace("&nbsp;", " ").strip()


def fetch(market: str, roc_year: int, month: int) -> list[dict]:
    """Market-wide filings for one ROC year/month. market = 'sii' | 'otc'."""
    return fetch_state(market, roc_year, month)[0]


# What MOPS answers for a month nobody filed in (probed 2026-10-17).
_NO_FILINGS_TEXT = "無持股轉讓之情形"


def fetch_state(market: str, roc_year: int, month: int) -> tuple[list[dict], str]:
    """fetch() plus how the request went: NO_RESPONSE, UNREADABLE, EMPTY (MOPS
    said nobody filed) or OK."""
    payload = {
        "step": "1", "TYPEK": market, "co_id": "", "year": str(roc_year),
        "smonth": f"{month:02d}", "emonth": f"{month:02d}",
        "isnew": "", "sstep": "1", "firstin": "true", "run": "",
    }
    _mops_rate_limit()
    s = _get_mops_session()
    try:
        r = s.post(MOPS_URL, data=payload, headers={"Referer": _REFERER}, timeout=40)
        r.raise_for_status()
    except Exception as e:
        print(f"  [transfers] {market} {roc_year}/{month} request error: {e}")
        return [], NO_RESPONSE

    html = r.content.decode("utf-8", "replace")
    rows: list[dict] = []
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S | re.I):
        cells = [_clean(c) for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S | re.I)]
        if len(cells) < 15 or not re.match(r"^\d{4}[A-Z]?$", cells[2]):
            continue
        rd = _roc_to_ad(cells[1])
        if rd is None:
            continue
        rows.append({
            "stock_id":        cells[2],
            "report_date":     rd,
            "insider_role":    cells[4],
            "insider_name":    cells[5],
            "transfer_method": cells[6],
            "transfer_shares": _parse_int(cells[7]),
            "planned_shares":  _parse_int(cells[10]) if len(cells) > 10 else 0,
            "transfer_period": cells[16] if len(cells) > 16 else "",
        })
    if rows:
        return rows, OK
    if _NO_FILINGS_TEXT in html:
        return rows, EMPTY
    print(f"  [transfers] {market} {roc_year}/{month}: no rows and no "
          f"'{_NO_FILINGS_TEXT}' ({len(r.content)} bytes)")
    return rows, UNREADABLE


_UPSERT = """
    INSERT INTO tw.insider_share_transfers (
        stock_id, report_date, insider_role, insider_name, transfer_method,
        transfer_shares, planned_shares, transfer_period, updated_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (stock_id, report_date, insider_name, transfer_method,
                 transfer_shares, planned_shares)
    DO UPDATE SET insider_role    = EXCLUDED.insider_role,
                  transfer_period = EXCLUDED.transfer_period,
                  updated_at      = NOW()
"""


def save_transfers(rows: list[dict]) -> int:
    if not rows:
        return 0
    with get_cursor() as cur:
        for r in rows:
            cur.execute(_UPSERT, (
                r["stock_id"], r["report_date"], r["insider_role"], r["insider_name"],
                r["transfer_method"], r["transfer_shares"], r["planned_shares"],
                r["transfer_period"],
            ))
    return len(rows)


def scrape_date(trade_date: date) -> ScrapeResult:
    """Daily hook: current + previous calendar month, sii + otc."""
    months = []
    y, m = trade_date.year, trade_date.month
    months.append((y, m))
    pm_y, pm_m = (y - 1, 12) if m == 1 else (y, m - 1)
    months.append((pm_y, pm_m))

    total = 0
    legs = {}
    for cy, cm in months:
        for market in ("sii", "otc"):
            rows, state = fetch_state(market, cy - 1911, cm)
            legs[f"{'上市' if market == 'sii' else '上櫃'} {cy}-{cm:02d}"] = state
            total += save_transfers(rows)
    print(f"  [transfers] {len(months)*2} queries, {total} filings upserted; legs {legs}")
    return ScrapeResult(records=total, api_rows=total, parse_errors=0,
                        failed_sources=failed_legs(
                            {f"內部人轉讓 {k}": v for k, v in legs.items()}))


if __name__ == "__main__":
    scrape_date(date.today())
