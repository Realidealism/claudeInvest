"""
TDCC weekly shareholder distribution scraper (集保戶股權分散表).

Two modes:
  scrape_date(trade_date)   — daily hook. Fetches the latest week's OpenData CSV
                              (one shot, all stocks). Skips if DB already has that week.
  scrape_week_portal(scaDate, stock_ids) — per-stock POST to the qryStock portal.
                              Used for historical backfill (see backfill_shareholder.py).
"""

import re
import time
import random
from datetime import date, datetime

import requests

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from utils.http_client import _wait_for_rate_limit, DEFAULT_HEADERS

OPENDATA_URL = "https://opendata.tdcc.com.tw/getOD.ashx"
PORTAL_URL   = "https://www.tdcc.com.tw/portal/zh/smWeb/qryStock"

TIER_COUNT = 17


def _parse_int(val) -> int | None:
    s = str(val).replace(",", "").strip()
    if not s or s in ("--", "---"):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _parse_float(val) -> float | None:
    s = str(val).replace(",", "").strip()
    if not s or s in ("--", "---"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Mode 1: OpenData CSV (current week, entire market in one request)
# ---------------------------------------------------------------------------

def fetch_opendata() -> tuple[date | None, dict]:
    """
    Fetch the current week's CSV. Returns (data_date, {stock_id: [tier_rows]}).
    Each tier_row is (tier, holders, shares, pct).
    """
    _wait_for_rate_limit("opendata.tdcc.com.tw")
    r = requests.get(OPENDATA_URL, params={"id": "1-5"},
                     headers=DEFAULT_HEADERS, timeout=60)
    r.raise_for_status()
    r.encoding = "utf-8-sig"

    lines = r.text.replace("\r\n", "\n").split("\n")
    # header: 資料日期,證券代號,持股分級,人數,股數,占集保庫存數比例%
    by_stock: dict[str, list] = {}
    data_date = None
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) < 6:
            continue
        d_str = parts[0].strip()
        stock_id = parts[1].strip()
        tier = _parse_int(parts[2])
        holders = _parse_int(parts[3])
        shares = _parse_int(parts[4])
        pct = _parse_float(parts[5])
        if not d_str or not stock_id or tier is None:
            continue
        if data_date is None:
            data_date = datetime.strptime(d_str, "%Y%m%d").date()
        by_stock.setdefault(stock_id, []).append((tier, holders, shares, pct))

    return data_date, by_stock


# ---------------------------------------------------------------------------
# Mode 2: Portal POST per-stock (historical)
# ---------------------------------------------------------------------------

_portal_session: requests.Session | None = None
_portal_token: tuple[str, str] | None = None


def _get_portal_session() -> tuple[requests.Session, str, str]:
    """Lazy-init session with SYNCHRONIZER_TOKEN from the qryStock landing page."""
    global _portal_session, _portal_token
    if _portal_session is None:
        s = requests.Session()
        s.headers.update(DEFAULT_HEADERS)
        s.headers["Referer"] = PORTAL_URL
        r = s.get(PORTAL_URL, timeout=30)
        r.raise_for_status()
        tok = re.search(r'SYNCHRONIZER_TOKEN[^>]*value="([^"]+)"', r.text).group(1)
        uri = re.search(r'SYNCHRONIZER_URI[^>]*value="([^"]+)"', r.text).group(1)
        _portal_session = s
        _portal_token = (tok, uri)
    return _portal_session, _portal_token[0], _portal_token[1]


def get_available_dates() -> list[date]:
    """Return the list of scaDate values the portal currently offers (newest first)."""
    s, _, _ = _get_portal_session()
    r = s.get(PORTAL_URL, timeout=30)
    dates = re.findall(r'<option[^>]*value="(\d{8})"', r.text)
    return [datetime.strptime(d, "%Y%m%d").date() for d in dates]


def fetch_portal(stock_id: str, sca_date: date) -> list | None:
    """
    POST one (stock_id, scaDate) and parse the 17-row distribution table.
    Returns list of (tier, holders, shares, pct) or None on failure.
    """
    s, tok, uri = _get_portal_session()
    d_str = sca_date.strftime("%Y%m%d")
    payload = {
        "SYNCHRONIZER_TOKEN": tok,
        "SYNCHRONIZER_URI":   uri,
        "method":     "submit",
        "firDate":    d_str,
        "scaDate":    d_str,
        "sqlMethod":  "StockNo",
        "stockNo":    stock_id,
        "stockName":  "",
    }

    _wait_for_rate_limit("www.tdcc.com.tw")
    try:
        r = s.post(PORTAL_URL, data=payload, timeout=60)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"  [{stock_id}@{sca_date}] request error: {e}")
        return None

    # Table has header row + 17 data rows (tiers 1–17).
    tables = re.findall(r'<table[^>]*class="table"[^>]*>(.*?)</table>',
                        r.text, re.S)
    if not tables:
        return None

    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tables[0], re.S)
    results = []
    for row in rows[1:]:  # skip header
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.S)
        if len(cells) < 5:
            continue
        clean = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
        tier = _parse_int(clean[0])
        holders = _parse_int(clean[2])
        shares = _parse_int(clean[3])
        pct = _parse_float(clean[4])
        if tier is None:
            continue
        results.append((tier, holders, shares, pct))

    return results if len(results) == TIER_COUNT else None


# ---------------------------------------------------------------------------
# DB save
# ---------------------------------------------------------------------------

def _build_upsert_query() -> tuple[str, list[str]]:
    """Build INSERT ... ON CONFLICT statement with 17 tiers × 3 cols."""
    cols = ["stock_id", "data_date"]
    for t in range(1, TIER_COUNT + 1):
        cols += [f"t{t}_holders", f"t{t}_shares", f"t{t}_pct"]
    placeholders = ", ".join(["%s"] * len(cols))
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols[2:])
    sql = f"""
        INSERT INTO tw.shareholder_distribution ({', '.join(cols)})
        VALUES ({placeholders})
        ON CONFLICT (stock_id, data_date) DO UPDATE SET
            {update_set}
    """
    return sql, cols


_UPSERT_SQL, _UPSERT_COLS = _build_upsert_query()


def _row_values(stock_id: str, data_date: date, tiers: list) -> tuple:
    """Convert list of (tier, holders, shares, pct) into the column tuple."""
    by_tier = {t[0]: t for t in tiers}
    vals = [stock_id, data_date]
    for t in range(1, TIER_COUNT + 1):
        row = by_tier.get(t)
        if row is None:
            vals += [None, None, None]
        else:
            vals += [row[1], row[2], row[3]]
    return tuple(vals)


def save_records(records: dict, data_date: date) -> int:
    """records: {stock_id: [(tier, holders, shares, pct), ...]}. Returns saved count."""
    if not records:
        return 0
    with get_cursor() as cur:
        for stock_id, tiers in records.items():
            cur.execute(_UPSERT_SQL, _row_values(stock_id, data_date, tiers))
    return len(records)


# ---------------------------------------------------------------------------
# Daily hook (uses OpenData CSV)
# ---------------------------------------------------------------------------

# Avoids re-fetching the CSV on every trade_date when historical_update.py
# loops over a range — the CSV only ever carries the latest week's snapshot.
_run_completed = False


def scrape_date(trade_date: date) -> ScrapeResult:
    """
    Daily hook. Fetches the latest weekly CSV from OpenData. Trade_date is
    ignored — TDCC data only exists weekly and only the latest snapshot
    is available. Skips if the CSV's data_date is already in DB.
    """
    global _run_completed
    if _run_completed:
        return ScrapeResult(records=0, api_rows=0, parse_errors=0)
    data_date, records = fetch_opendata()
    if data_date is None:
        print("  [shareholder] OpenData returned no data.")
        return ScrapeResult(records=0, api_rows=0, parse_errors=0)

    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT 1 FROM tw.shareholder_distribution "
            "WHERE data_date = %s LIMIT 1",
            (data_date,),
        )
        if cur.fetchone():
            print(f"  [shareholder] {data_date} already in DB, skipping.")
            return ScrapeResult(records=0, api_rows=len(records), parse_errors=0)

    print(f"  [shareholder] Saving {len(records)} stocks for {data_date} ...")
    saved = save_records(records, data_date)
    print(f"  [shareholder] Saved {saved} rows.")
    _run_completed = True
    return ScrapeResult(records=saved, api_rows=len(records), parse_errors=0)


if __name__ == "__main__":
    scrape_date(date.today())
