"""
Treasury stock buyback scraper (庫藏股買回).

Source: MOPS t35sc09 bulk summary by date range + market type.
Fetches all programs within a year for each market (sii/otc/rotc/pub).
Very low volume (~300 programs/year total), so full backfill is fast.
"""

import re
import time
import random
from datetime import date

import requests

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from utils.http_client import DEFAULT_HEADERS

MOPS_URL = "https://mopsov.twse.com.tw/mops/web/ajax_t35sc09"
MOPS_PAGE = "https://mopsov.twse.com.tw/mops/web/t35sc09"
MARKETS = ["sii", "otc", "rotc", "pub"]


def _parse_int(val) -> int | None:
    s = str(val).replace(",", "").replace("&nbsp;", "").strip()
    if not s or s in ("--", "---"):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _parse_float(val) -> float | None:
    s = str(val).replace(",", "").replace("&nbsp;", "").strip()
    if not s or s in ("--", "---"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _roc_to_date(roc_str: str) -> date | None:
    """Convert ROC date string like '115/01/02' or '1150102' to AD date."""
    s = roc_str.strip().replace("/", "")
    if not s or len(s) < 7:
        return None
    try:
        roc_year = int(s[:-4])
        month = int(s[-4:-2])
        day = int(s[-2:])
        return date(roc_year + 1911, month, day)
    except (ValueError, IndexError):
        return None


def _parse_rows(html: str) -> list[dict]:
    """Parse t35sc09 HTML into list of program dicts."""
    all_trs = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S | re.I)
    results = []
    for tr in all_trs:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S | re.I)
        if len(cells) < 18:
            continue
        clean = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
        # [0]序號 [1]代號 [2]名稱 [3]董事會日 [4]目的 [5]已發行總數
        # [6]預定數量 [7]價格低 [8]價格高 [9]起日 [10]迄日
        # [11]完成? [12]標準等 [13]已買回數量 [14]轉讓數量 [15]執行率%
        # [16]已買回金額 [17]平均價 [18]佔已發行% [19]補充
        stock_id = clean[1].strip()
        if not stock_id or not stock_id[0].isdigit():
            continue
        results.append({
            "stock_id":           stock_id,
            "board_date":         _roc_to_date(clean[3]),
            "purpose":            _parse_int(clean[4]),
            "shares_outstanding": _parse_int(clean[5]),
            "shares_planned":     _parse_int(clean[6]),
            "price_min":          _parse_float(clean[7]),
            "price_max":          _parse_float(clean[8]),
            "period_start":       _roc_to_date(clean[9]),
            "period_end":         _roc_to_date(clean[10]),
            "completed":          clean[11].upper() == "Y",
            "shares_bought":      _parse_int(clean[13]),
            "shares_transferred": _parse_int(clean[14]),
            "execution_rate":     _parse_float(clean[15]),
            "total_cost":         _parse_int(clean[16]),
            "avg_price":          _parse_float(clean[17]),
            "pct_outstanding":    _parse_float(clean[18]),
            "note":               clean[19] if len(clean) > 19 and clean[19] else None,
        })
    return results


_session: requests.Session | None = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        s = requests.Session()
        s.headers.update(DEFAULT_HEADERS)
        s.get(MOPS_PAGE, timeout=30)
        _session = s
    return _session


def fetch_year(ad_year: int, market: str = "sii") -> list[dict]:
    """Fetch all treasury stock programs for one year + one market."""
    roc = ad_year - 1911
    s = _get_session()
    payload = {
        "encodeURIComponent": "1",
        "step": "1",
        "firstin": "1",
        "off": "1",
        "TYPEK": market,
        "d1": f"{roc}0101",
        "d2": f"{roc}1231",
        "RD": "1",
    }
    time.sleep(1.5 + random.uniform(0, 1))
    r = s.post(MOPS_URL, data=payload, timeout=60)
    r.raise_for_status()
    return _parse_rows(r.text)


def fetch_range(start_roc: str, end_roc: str, market: str = "sii") -> list[dict]:
    """Fetch programs for arbitrary ROC date range."""
    s = _get_session()
    payload = {
        "encodeURIComponent": "1",
        "step": "1",
        "firstin": "1",
        "off": "1",
        "TYPEK": market,
        "d1": start_roc,
        "d2": end_roc,
        "RD": "1",
    }
    time.sleep(1.5 + random.uniform(0, 1))
    r = s.post(MOPS_URL, data=payload, timeout=60)
    r.raise_for_status()
    return _parse_rows(r.text)


# ---------------------------------------------------------------------------
# DB save
# ---------------------------------------------------------------------------

_UPSERT_SQL = """
    INSERT INTO tw.treasury_stock (
        stock_id, board_date, purpose,
        shares_outstanding, shares_planned,
        price_min, price_max,
        period_start, period_end,
        completed, shares_bought, shares_transferred,
        execution_rate, total_cost, avg_price, pct_outstanding, note
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (stock_id, board_date, purpose) DO UPDATE SET
        shares_outstanding = EXCLUDED.shares_outstanding,
        shares_planned     = EXCLUDED.shares_planned,
        price_min          = EXCLUDED.price_min,
        price_max          = EXCLUDED.price_max,
        period_start       = EXCLUDED.period_start,
        period_end         = EXCLUDED.period_end,
        completed          = EXCLUDED.completed,
        shares_bought      = EXCLUDED.shares_bought,
        shares_transferred = EXCLUDED.shares_transferred,
        execution_rate     = EXCLUDED.execution_rate,
        total_cost         = EXCLUDED.total_cost,
        avg_price          = EXCLUDED.avg_price,
        pct_outstanding    = EXCLUDED.pct_outstanding,
        note               = EXCLUDED.note
"""


def save_programs(programs: list[dict]) -> int:
    if not programs:
        return 0
    saved = 0
    with get_cursor() as cur:
        for p in programs:
            if not p["board_date"] or not p["purpose"]:
                continue
            cur.execute(_UPSERT_SQL, (
                p["stock_id"], p["board_date"], p["purpose"],
                p["shares_outstanding"], p["shares_planned"],
                p["price_min"], p["price_max"],
                p["period_start"], p["period_end"],
                p["completed"], p["shares_bought"], p["shares_transferred"],
                p["execution_rate"], p["total_cost"], p["avg_price"],
                p["pct_outstanding"], p["note"],
            ))
            saved += 1
    return saved


# ---------------------------------------------------------------------------
# Daily hook
# ---------------------------------------------------------------------------

_run_completed = False


def scrape_date(trade_date: date) -> ScrapeResult:
    """
    Daily hook. Fetches the current year's treasury stock programs
    for all markets. Idempotent via UPSERT.
    """
    global _run_completed
    if _run_completed:
        return ScrapeResult(records=0, api_rows=0, parse_errors=0)

    roc = trade_date.year - 1911
    total = 0
    api_rows = 0
    for market in MARKETS:
        programs = fetch_range(f"{roc}0101", trade_date.strftime(f"{roc}%m%d"), market)
        api_rows += len(programs)
        saved = save_programs(programs)
        if saved:
            print(f"  [treasury] {market}: {saved} programs")
        total += saved

    _run_completed = True
    return ScrapeResult(records=total, api_rows=api_rows, parse_errors=0)


if __name__ == "__main__":
    scrape_date(date.today())
