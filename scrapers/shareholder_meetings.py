"""Shareholder-meeting (股東常會/臨時會) date scraper.

Daily hook: TWSE OpenAPI t187ap41_L + TPEx t187ap41_O — current-year all-market
  開會日期 board (refreshes as companies announce), 公司代號 / 開會日期 / 股東常(臨時)會.

Backfill: MOPS ajax_t108sb31 per ROC year × market (2017+, history reaches 2007).
  The query needs MONTH='all' + SK='1' + encodeURIComponent=1 (empty MONTH/SK returns
  a JS shell). Column count varies by year, so parse by regex-locking code + date:
  cell[0]=公司代號, cell[4]=開會日期 (ROC). Reuses the MOPS session/rate-limit from
  insider_holdings. MOPS now serves UTF-8 (not Big5).

Feeds tw.shareholder_meetings; the covering-date (融券最後回補日) is DERIVED from
meeting_date separately into tw.short_cover_calendar.

  scrape_date(date)               -- daily OpenAPI hook
  backfill(start_roc, end_roc)    -- one-time historical (e.g. 106..115)
"""

import re
from datetime import date

import requests

from db.connection import get_cursor
from scrapers.insider_holdings import _get_mops_session, _mops_rate_limit
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json_retry

TWSE_OPENAPI = "https://openapi.twse.com.tw/v1/opendata/t187ap41_L"
TPEX_OPENAPI = "https://www.tpex.org.tw/openapi/v1/t187ap41_O"
MOPS_URL = "https://mopsov.twse.com.tw/mops/web/ajax_t108sb31"


def _roc_to_date(roc_str) -> date | None:
    """'1150522' or '106/06/28' -> date. Strips separators; last 4 = MMDD."""
    digits = re.sub(r"\D", "", str(roc_str))
    if len(digits) < 7:
        return None
    try:
        return date(int(digits[:-4]) + 1911, int(digits[-4:-2]), int(digits[-2:]))
    except ValueError:
        return None


def _norm_type(val) -> str | None:
    s = str(val or "")
    if "臨時" in s:
        return "臨時會"
    if "常會" in s or "常" in s:
        return "常會"
    return None


# ── Daily: OpenAPI current-year board ────────────────────────────────────────

def _fetch_openapi() -> list[dict]:
    out: list[dict] = []
    for url, market in ((TWSE_OPENAPI, "TWSE"), (TPEX_OPENAPI, "TPEx")):
        data = fetch_json_retry(url, validate=lambda d: isinstance(d, list))
        if not isinstance(data, list):
            continue
        for row in data:
            try:
                sid = str(row.get("公司代號", "")).strip()
                mdate = _roc_to_date(row.get("開會日期"))
                if not sid or not sid[0].isdigit() or mdate is None:
                    continue
                out.append({
                    "stock_id": sid,
                    "meeting_date": mdate,
                    "meeting_type": _norm_type(row.get("股東常(臨時)會")),
                    "market": market,
                    "source": "openapi_t187",
                })
            except (AttributeError, ValueError, TypeError):
                continue
    return out


# ── Backfill: MOPS historical per year × market ──────────────────────────────

def _fetch_mops(typek: str, roc_year: str) -> list[dict]:
    """One ROC year, one market ('sii'/'otc'). Returns meeting dicts."""
    payload = {
        "encodeURIComponent": "1", "run": "", "step": "1", "TYPEK": typek,
        "YEAR": roc_year, "co_id1": "", "co_id2": "", "MONTH": "all",
        "SDAY": "", "EDAY": "", "SK": "1", "firstin": "true",
    }
    _mops_rate_limit()
    s = _get_mops_session()
    try:
        r = s.post(MOPS_URL, data=payload, timeout=40)
        r.raise_for_status()
    except Exception as e:
        print(f"  [{typek}/{roc_year}] request error: {e}")
        return []
    html = r.content.decode("utf-8", errors="replace")
    market = "TWSE" if typek == "sii" else "TPEx"
    out: list[dict] = []
    for row in re.findall(r"<tr class='(?:odd|even)'>(.*?)</tr>", html, re.S | re.I):
        cells = [re.sub(r"<[^>]+>", "", c).replace("&nbsp;", "").strip()
                 for c in re.findall(r"<td[^>]*>(.*?)</td>", row, re.S | re.I)]
        # column count varies by year -> lock on code + ROC date, not position count
        if len(cells) < 5 or not re.match(r"^\d{4,6}$", cells[0]) \
                or not re.match(r"^\d{2,3}/\d{1,2}/\d{1,2}$", cells[4]):
            continue
        mdate = _roc_to_date(cells[4])
        if mdate is None:
            continue
        mtype = next((_norm_type(c) for c in cells[2:5] if _norm_type(c)), None)
        out.append({
            "stock_id": cells[0], "meeting_date": mdate, "meeting_type": mtype,
            "market": market, "source": "mops_t108",
        })
    return out


_UPSERT_SQL = """
    INSERT INTO tw.shareholder_meetings (stock_id, meeting_date, meeting_type, market, source)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (stock_id, meeting_date) DO UPDATE SET
        meeting_type = COALESCE(EXCLUDED.meeting_type, tw.shareholder_meetings.meeting_type),
        market       = EXCLUDED.market,
        source       = EXCLUDED.source,
        updated_at   = NOW()
"""


def _save(records: list[dict]) -> int:
    saved = 0
    with get_cursor() as cur:
        for r in records:
            if not r["meeting_date"]:
                continue
            cur.execute(_UPSERT_SQL, (r["stock_id"], r["meeting_date"],
                                      r["meeting_type"], r["market"], r["source"]))
            saved += 1
    return saved


def scrape_date(trade_date: date) -> ScrapeResult:
    """Daily hook: refresh current-year AGM board from OpenAPI."""
    print(f"Fetching shareholder meetings ({trade_date}) ...")
    records = _fetch_openapi()
    saved = _save(records)
    print(f"  Shareholder meetings (OpenAPI): {len(records)} rows, saved={saved}")
    return ScrapeResult(records=saved, api_rows=len(records), parse_errors=0)


def backfill(start_roc: int, end_roc: int) -> int:
    """One-time historical backfill via MOPS, ROC years [start_roc, end_roc]."""
    total = 0
    for yr in range(start_roc, end_roc + 1):
        for typek in ("sii", "otc"):
            recs = _fetch_mops(typek, str(yr))
            n = _save(recs)
            total += n
            print(f"  MOPS {typek}/{yr}: {len(recs)} meetings, saved={n}")
    print(f"Backfill total saved={total}")
    return total


if __name__ == "__main__":
    scrape_date(date.today())
