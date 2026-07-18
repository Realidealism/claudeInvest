"""
Insider pledge / release EVENT scraper (內部人設質解質公告, MOPS STAMAK03_1).

Event-grained: each pledge-change filing (設質 / 解質) is one row, including the
release side (解質股數) and the exact 質設異動發生日期 — complementary to
scrapers/insider_holdings.py (monthly cumulative snapshot, no release events).

One wide-range query per stock returns the whole history in one shot (cost is
independent of range width). Reuses the MOPS session / rate-limit / helpers from
insider_holdings.py; does NOT touch that module or tw.insider_holdings.

  fetch_events(stock_id, roc_b_date, roc_e_date) -> list[dict]
  save_events(events) -> int
"""

import re
from datetime import date, timedelta

import requests

from db.connection import get_cursor
from scrapers.insider_holdings import (
    _get_mops_session,
    _mops_rate_limit,
    _parse_int,
)
from utils.format_shift import ScrapeResult
from utils.http_client import DEFAULT_HEADERS

MOPS_URL = "https://mopsov.twse.com.tw/mops/web/ajax_STAMAK03_1"

# MoneyDJ market-wide daily 董監質設異動清單 (Big5). Lists all listed+OTC pledge/
# release filings for the trailing ~2 weeks, one <tr> per filing, with the stock
# code embedded in a GenLink2stk('AS<code>',...) call. We use it only to discover
# WHICH stocks changed recently, then re-query MOPS STAMAK03_1 per stock for the
# authoritative exact-share values — keeping the daily feed same-source (MOPS,
# 股 not 張) and consistent with the backfill's UNIQUE key.
ZEU_URL = "https://www.moneydj.com/Z/ZE/ZEU/ZEU.djhtm"
ZEU_LOOKBACK_DAYS = 45  # MOPS re-query window; comfortably covers ZEU's ~2wk span


def _roc_to_ad_date(s: str) -> date | None:
    """Convert a ROC date string '113/11/05' to a python date (2024-11-05).

    Returns None for empty / unparseable values.
    """
    s = re.sub(r"<[^>]+>", "", str(s)).replace("&nbsp;", "").strip()
    m = re.match(r"^(\d{2,3})/(\d{1,2})/(\d{1,2})$", s)
    if not m:
        return None
    roc_y, mo, dy = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return date(roc_y + 1911, mo, dy)
    except ValueError:
        return None


def _clean_text(s: str) -> str:
    return re.sub(r"<[^>]+>", "", str(s)).replace("&nbsp;", "").strip()


def fetch_events(stock_id: str, roc_b_date: str, roc_e_date: str) -> list[dict]:
    """POST one stock over a ROC date range and parse pledge-change events.

    roc_b_date / roc_e_date are ROC-format yyyymmdd strings, e.g. '1130101'.
    Returns a list of event dicts (raw role/name preserved, no aggregation).
    Returns [] on no-data / request failure.
    """
    payload = {
        "encodeURIComponent": "1",
        "step": "1",
        "firstin": "1",
        "off": "1",
        "queryName": "co_id",
        "inpuType": "co_id",
        "TYPEK": "all",
        "co_id": stock_id,
        "b_date": roc_b_date,
        "e_date": roc_e_date,
    }
    _mops_rate_limit()
    s = _get_mops_session()
    try:
        r = s.post(MOPS_URL, data=payload, timeout=30)
        r.raise_for_status()
    except Exception as e:  # requests.RequestException + any transport issue
        print(f"  [{stock_id}] request error: {e}")
        return []

    html = r.text
    rows = re.findall(r"<tr class='(?:odd|even)'>(.*?)</tr>", html, re.S | re.I)
    if not rows:
        return []

    events: list[dict] = []
    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.S | re.I)
        if len(cells) < 11:
            continue
        # cells: [0]代號 [1]名稱 [2]身份別 [3]姓名 [4]異動日期
        #        [5]設質股數 [6]解質股數 [7]累積質設 [8]質權人 [9]備註 [10]申報日期
        events.append({
            "stock_id":           _clean_text(cells[0]) or stock_id,
            "insider_role":       _clean_text(cells[2]),
            "insider_name":       _clean_text(cells[3]),
            "change_date":        _roc_to_ad_date(cells[4]),
            "pledged_shares":     _parse_int(_clean_text(cells[5])),
            "released_shares":    _parse_int(_clean_text(cells[6])),
            "cumulative_pledged": _parse_int(_clean_text(cells[7])),
            "pledgee_name":       _clean_text(cells[8]),
            "remark":             _clean_text(cells[9]),
            "report_date":        _roc_to_ad_date(cells[10]),
        })
    return events


_UPSERT_SQL = """
    INSERT INTO tw.insider_pledge_events (
        stock_id, insider_role, insider_name, change_date,
        pledged_shares, released_shares, cumulative_pledged,
        pledgee_name, remark, report_date, updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (stock_id, change_date, insider_name, pledged_shares,
                 released_shares, cumulative_pledged, report_date)
    DO UPDATE SET
        insider_role = EXCLUDED.insider_role,
        pledgee_name = EXCLUDED.pledgee_name,
        remark       = EXCLUDED.remark,
        updated_at   = NOW()
"""


def save_events(events: list[dict]) -> int:
    """Upsert events into tw.insider_pledge_events. Returns rows attempted."""
    if not events:
        return 0
    with get_cursor() as cur:
        for e in events:
            cur.execute(_UPSERT_SQL, (
                e["stock_id"], e["insider_role"], e["insider_name"], e["change_date"],
                e["pledged_shares"], e["released_shares"], e["cumulative_pledged"],
                e["pledgee_name"], e["remark"], e["report_date"],
            ))
    return len(events)


# ---------------------------------------------------------------------------
# Daily hook: MoneyDJ ZEU (discovery) → MOPS STAMAK03_1 (authoritative values)
# ---------------------------------------------------------------------------

def _roc(d: date) -> str:
    """python date → ROC yyyymmdd string, e.g. 2024-11-05 → '1131105'."""
    return f"{d.year - 1911}{d.month:02d}{d.day:02d}"


def fetch_zeu_codes() -> list[str]:
    """Fetch MoneyDJ ZEU and return the unique stock codes with recent pledge/
    release activity, in page order. Codes are embedded as GenLink2stk('AS<code>',
    '<name>'). Returns [] on any fetch failure (caller treats as no-op)."""
    try:
        r = requests.get(ZEU_URL, headers=DEFAULT_HEADERS, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"  [ZEU] fetch error: {e}")
        return []
    html = r.content.decode("big5", "replace")
    seen: set[str] = set()
    codes: list[str] = []
    for c in re.findall(r"GenLink2stk\('AS(\d+)'", html):
        if c not in seen:
            seen.add(c)
            codes.append(c)
    return codes


_run_completed = False


def scrape_date(trade_date: date) -> ScrapeResult:
    """Daily hook. Discovers recently-active stocks from ZEU, then re-queries
    each via MOPS STAMAK03_1 over a trailing window for authoritative values.

    trade_date is ignored (ZEU always shows the latest ~2 weeks). Idempotent:
    per-stock upserts dedupe. Guarded so a date-range run only fetches once."""
    global _run_completed
    if _run_completed:
        return ScrapeResult(records=0, api_rows=0, parse_errors=0)

    codes = fetch_zeu_codes()
    if not codes:
        print("  [pledge-events] ZEU returned no codes; nothing to do.")
        _run_completed = True
        return ScrapeResult(records=0, api_rows=0, parse_errors=0)

    today = date.today()
    b_date = _roc(today - timedelta(days=ZEU_LOOKBACK_DAYS))
    e_date = _roc(today)

    total = 0
    for sid in codes:
        total += save_events(fetch_events(sid, b_date, e_date))

    print(f"  [pledge-events] {len(codes)} active stocks from ZEU, "
          f"{total} events upserted.")
    _run_completed = True
    return ScrapeResult(records=total, api_rows=len(codes), parse_errors=0)


if __name__ == "__main__":
    scrape_date(date.today())
