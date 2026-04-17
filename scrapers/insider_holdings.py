"""
Monthly director / supervisor / manager shareholding scraper (董監經理人持股餘額).

Two modes:
  scrape_date(trade_date)   — daily hook. Fetches the latest month's bulk CSV
                              from TWSE OpenAPI (one shot for all companies).
                              Skips if DB already has that year_month.
  fetch_mops_one(stock_id, year, month) — per-stock POST to MOPS.
                              Used for historical backfill (see backfill_insider_holdings.py).

Aggregation: raw data is per-person; we aggregate by 職稱 into director /
supervisor / manager. 持股 10% 以上股東 are excluded (not employees).
"""

import re
import time
import random
from datetime import date

import requests

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from utils.http_client import DEFAULT_HEADERS

TWSE_OPENAPI_L = "https://openapi.twse.com.tw/v1/opendata/t187ap11_L"  # 上市
TPEX_OPENAPI_O = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap11_O"  # 上櫃
TPEX_OPENAPI_R = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap11_R"  # 興櫃
MOPS_URL       = "https://mopsov.twse.com.tw/mops/web/ajax_stapap1"

# Field name aliases across endpoints (OpenAPI _R uses English keys).
_STOCK_ID_KEYS  = ("公司代號", "SecuritiesCompanyCode")
_YM_KEYS        = ("資料年月",)
_TITLE_KEYS     = ("職稱",)
_SHARES_KEYS    = ("目前持股",)
_PLEDGED_KEYS   = ("設質股數",)


def _pick(d: dict, keys: tuple) -> str:
    for k in keys:
        if k in d:
            return str(d[k]).strip()
    return ""

# MOPS is less aggressive than TWSE/TPEx. 1.5-3s per request is safe.
MOPS_MIN_INTERVAL = 1.5
MOPS_MAX_JITTER   = 1.5


def _parse_int(val) -> int:
    s = str(val).replace(",", "").strip()
    if not s or s in ("--", "---"):
        return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


def _classify_role(title: str) -> str | None:
    """Return 'director' / 'supervisor' / 'manager' / None."""
    if "監察人" in title:
        return "supervisor"
    if "董事" in title:
        return "director"
    if ("總經理" in title or "副總" in title or "經理" in title
            or "協理" in title or "財務主管" in title
            or "會計主管" in title or "稽核" in title):
        return "manager"
    return None


def _aggregate(rows: list) -> dict:
    """
    Input: list of dicts from OpenAPI (per-person).
    Output: {stock_id: {director_shares, director_pledged, ..., insider_shares, insider_pledged}}
    """
    by_stock: dict[str, dict] = {}
    for r in rows:
        stock_id = _pick(r, _STOCK_ID_KEYS)
        title    = _pick(r, _TITLE_KEYS)
        role = _classify_role(title)
        if not stock_id or not role:
            continue
        shares  = _parse_int(_pick(r, _SHARES_KEYS))
        pledged = _parse_int(_pick(r, _PLEDGED_KEYS))

        agg = by_stock.setdefault(stock_id, {
            "director_shares": 0, "director_pledged": 0,
            "supervisor_shares": 0, "supervisor_pledged": 0,
            "manager_shares": 0, "manager_pledged": 0,
        })
        agg[f"{role}_shares"]  += shares
        agg[f"{role}_pledged"] += pledged

    for agg in by_stock.values():
        agg["insider_shares"]  = agg["director_shares"] + agg["supervisor_shares"] + agg["manager_shares"]
        agg["insider_pledged"] = agg["director_pledged"] + agg["supervisor_pledged"] + agg["manager_pledged"]
    return by_stock


# ---------------------------------------------------------------------------
# Mode 1: TWSE OpenAPI (current month, full market)
# ---------------------------------------------------------------------------

def fetch_openapi() -> tuple[str | None, dict]:
    """
    Fetch current month from TWSE (上市) + TPEx (上櫃) + TPEx (興櫃).
    Returns (year_month, {stock_id: aggregate_dict}).
    """
    all_rows = []
    year_month = None

    for url, label in [(TWSE_OPENAPI_L, "TWSE 上市"),
                       (TPEX_OPENAPI_O, "TPEx 上櫃"),
                       (TPEX_OPENAPI_R, "ESB 興櫃")]:
        print(f"  Fetching {label} OpenAPI ...")
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=60)
        r.raise_for_status()
        rows = r.json()
        if rows and year_month is None:
            year_month = _pick(rows[0], _YM_KEYS)
        print(f"    {label}: {len(rows)} person-rows")
        all_rows.extend(rows)

    by_stock = _aggregate(all_rows)
    print(f"  Aggregated to {len(by_stock)} companies")
    return year_month, by_stock


# ---------------------------------------------------------------------------
# Mode 2: MOPS per-stock (historical)
# ---------------------------------------------------------------------------

_mops_session: requests.Session | None = None
_mops_last_request: float = 0.0


def _get_mops_session() -> requests.Session:
    global _mops_session
    if _mops_session is None:
        s = requests.Session()
        s.headers.update(DEFAULT_HEADERS)
        _mops_session = s
    return _mops_session


def _mops_rate_limit():
    """Simple per-session rate limit for MOPS."""
    global _mops_last_request
    now = time.time()
    elapsed = now - _mops_last_request
    needed = MOPS_MIN_INTERVAL + random.uniform(0, MOPS_MAX_JITTER)
    if elapsed < needed:
        time.sleep(needed - elapsed)
    _mops_last_request = time.time()


def _roc_year(ad_year: int) -> str:
    """Convert AD year to ROC year (民國)."""
    return str(ad_year - 1911)


def fetch_mops_one(stock_id: str, ad_year: int, month: int) -> dict | None:
    """
    POST one (stock_id, year, month) to MOPS and parse HTML table.
    Returns aggregate dict or None on failure/no-data.
    """
    payload = {
        "step": "2",
        "firstin": "true",
        "off": "1",
        "TYPEK": "all",
        "isnew": "false",
        "co_id": stock_id,
        "year":  _roc_year(ad_year),
        "month": f"{month:02d}",
    }
    _mops_rate_limit()
    s = _get_mops_session()
    try:
        r = s.post(MOPS_URL, data=payload, timeout=30)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"  [{stock_id}@{ad_year}-{month:02d}] request error: {e}")
        return None

    html = r.text
    if "查詢無資料" in html or "無資料" in html:
        return None

    # Find all data rows: <TR class='odd'|'even'>...<TD>...</TD>...</TR>
    rows = re.findall(r"<tr class='(?:odd|even)'>(.*?)</tr>", html, re.S | re.I)
    if not rows:
        return None

    agg = {
        "director_shares": 0, "director_pledged": 0,
        "supervisor_shares": 0, "supervisor_pledged": 0,
        "manager_shares": 0, "manager_pledged": 0,
    }
    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.S | re.I)
        if len(cells) < 5:
            continue
        title = re.sub(r"<[^>]+>", "", cells[0]).strip()
        role = _classify_role(title)
        if not role:
            continue
        # cells: [0]職稱 [1]姓名 [2]選任時持股 [3]目前持股 [4]設質股數
        shares  = _parse_int(re.sub(r"<[^>]+>", "", cells[3]))
        pledged = _parse_int(re.sub(r"<[^>]+>", "", cells[4]))
        agg[f"{role}_shares"]  += shares
        agg[f"{role}_pledged"] += pledged

    agg["insider_shares"]  = agg["director_shares"] + agg["supervisor_shares"] + agg["manager_shares"]
    agg["insider_pledged"] = agg["director_pledged"] + agg["supervisor_pledged"] + agg["manager_pledged"]

    # No insider data at all → treat as no-data rather than zero
    if agg["insider_shares"] == 0 and agg["insider_pledged"] == 0:
        return None
    return agg


# ---------------------------------------------------------------------------
# DB save
# ---------------------------------------------------------------------------

_UPSERT_SQL = """
    INSERT INTO tw.insider_holdings (
        stock_id, year_month,
        director_shares, director_pledged,
        supervisor_shares, supervisor_pledged,
        manager_shares, manager_pledged,
        insider_shares, insider_pledged,
        updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (stock_id, year_month) DO UPDATE SET
        director_shares    = EXCLUDED.director_shares,
        director_pledged   = EXCLUDED.director_pledged,
        supervisor_shares  = EXCLUDED.supervisor_shares,
        supervisor_pledged = EXCLUDED.supervisor_pledged,
        manager_shares     = EXCLUDED.manager_shares,
        manager_pledged    = EXCLUDED.manager_pledged,
        insider_shares     = EXCLUDED.insider_shares,
        insider_pledged    = EXCLUDED.insider_pledged,
        updated_at         = NOW()
"""


def save_one(stock_id: str, year_month: str, agg: dict):
    with get_cursor() as cur:
        cur.execute(_UPSERT_SQL, (
            stock_id, year_month,
            agg["director_shares"], agg["director_pledged"],
            agg["supervisor_shares"], agg["supervisor_pledged"],
            agg["manager_shares"], agg["manager_pledged"],
            agg["insider_shares"], agg["insider_pledged"],
        ))


def save_bulk(by_stock: dict, year_month: str) -> int:
    if not by_stock:
        return 0
    with get_cursor() as cur:
        for stock_id, agg in by_stock.items():
            cur.execute(_UPSERT_SQL, (
                stock_id, year_month,
                agg["director_shares"], agg["director_pledged"],
                agg["supervisor_shares"], agg["supervisor_pledged"],
                agg["manager_shares"], agg["manager_pledged"],
                agg["insider_shares"], agg["insider_pledged"],
            ))
    return len(by_stock)


# ---------------------------------------------------------------------------
# Daily hook (uses OpenAPI, idempotent per-month)
# ---------------------------------------------------------------------------

_run_completed = False


def scrape_date(trade_date: date) -> ScrapeResult:
    """
    Daily hook. Fetches the latest month's full market via TWSE OpenAPI.
    trade_date is ignored (data is published monthly, only current available).
    Skips if the OpenAPI's year_month is already in DB.
    """
    global _run_completed
    if _run_completed:
        return ScrapeResult(records=0, api_rows=0, parse_errors=0)

    year_month, by_stock = fetch_openapi()
    if not year_month:
        print("  [insider] OpenAPI returned no data.")
        return ScrapeResult(records=0, api_rows=0, parse_errors=0)

    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT 1 FROM tw.insider_holdings WHERE year_month = %s LIMIT 1",
            (year_month,),
        )
        if cur.fetchone():
            print(f"  [insider] {year_month} already in DB, skipping.")
            _run_completed = True
            return ScrapeResult(records=0, api_rows=len(by_stock), parse_errors=0)

    print(f"  [insider] Saving {len(by_stock)} companies for {year_month} ...")
    saved = save_bulk(by_stock, year_month)
    print(f"  [insider] Saved {saved} rows.")
    _run_completed = True
    return ScrapeResult(records=saved, api_rows=len(by_stock), parse_errors=0)


if __name__ == "__main__":
    scrape_date(date.today())
