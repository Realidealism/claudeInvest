"""
Price limits scraper (漲跌停價 / 參考價).

Both APIs return NEXT-DAY limits for a given query date, so to populate the
limits that apply ON trade_date T, we query the previous trading day and
save the result to T's row. The previous trading day is looked up from the
TAIEX calendar in tw.index_prices, which historical_update populates first.

Sources:
  TWSE: rwd/zh/variation/TWT84U (GET, selectType=ALL)
        fields: [2]=漲停價, [3]=開盤競價基準(參考價), [4]=跌停價
  TPEx: /www/zh-tw/afterTrading/dailyQuotes (GET, date=YYYY/MM/DD&response=json)
        fields: [16]=次日參考價, [17]=次日漲停價, [18]=次日跌停價
"""

from datetime import date, timedelta

from db.connection import get_cursor
from utils.classifier import classify_tw_security
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json_retry

TWSE_URL = "https://www.twse.com.tw/rwd/zh/variation/TWT84U"
TPEX_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes"


def _to_ad_date(d: date) -> str:
    return d.strftime("%Y/%m/%d")


def _parse_price(val) -> float | None:
    s = str(val).replace(",", "").strip()
    if not s or s in ("--", "---", " ", ""):
        return None
    try:
        return float(s)
    except ValueError:
        return None



# ---------------------------------------------------------------------------
# TWSE
# ---------------------------------------------------------------------------

def _parse_twse_limits(rows: list) -> tuple:
    """Returns (records, parse_errors)."""
    results = []
    errors = 0
    for row in rows:
        try:
            stock_id = str(row[0]).strip()
            security_type = classify_tw_security(stock_id)
            if not stock_id or not security_type:
                continue
            results.append({
                "stock_id":      stock_id,
                "name":          str(row[1]).strip(),
                "security_type": security_type,
                "limit_up":      _parse_price(row[2]),
                "ref_price":     _parse_price(row[3]),
                "limit_down":    _parse_price(row[4]),
            })
        except (IndexError, ValueError, TypeError) as e:
            print(f"  Skipping TWSE limits row: {e}")
            errors += 1
            continue
    return results, errors


def fetch_twse_limits(trade_date: date) -> tuple:
    """
    Fetch TWSE price limits for trade_date.
    Returns (records, api_rows, parse_errors).
    """
    date_str = trade_date.strftime("%Y%m%d")
    print(f"Fetching TWSE price limits for {trade_date} ...")

    data = fetch_json_retry(TWSE_URL, params={"date": date_str, "selectType": "ALL", "response": "json"},
                            validate=lambda d: d.get("stat") == "OK")
    if not data or data.get("stat") != "OK":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return [], 0, 0

    # Verify the API returned data for the requested date.
    api_date = str(data.get("date", "")).strip()
    if api_date and api_date != date_str:
        print(f"  Date mismatch: requested {date_str}, API returned {api_date} — skipping")
        return [], 0, 0

    rows = data.get("data", [])
    print(f"  Found {len(rows)} records.")
    records, errors = _parse_twse_limits(rows)
    return records, len(rows), errors


# ---------------------------------------------------------------------------
# TPEx
# ---------------------------------------------------------------------------

def _parse_tpex_limits(rows: list, fields: list) -> tuple:
    """
    Returns (records, parse_errors). Locates next-day reference / limit-up /
    limit-down columns by field name, since TPEx changed the column count
    around 2020-2021 (added 最後買量/賣量 columns, shifting subsequent indices).
    """
    def _find_idx(*names):
        for i, f in enumerate(fields):
            normalized = f.replace(" ", "").replace("　", "")
            for n in names:
                if n in normalized:
                    return i
        return -1

    ref_idx  = _find_idx("次日參考價")
    up_idx   = _find_idx("次日漲停價")
    down_idx = _find_idx("次日跌停價")

    if min(ref_idx, up_idx, down_idx) < 0:
        print(f"  [WARN] TPEx limits: required columns not found in fields={fields}")
        return [], 0

    results = []
    errors = 0
    for row in rows:
        try:
            stock_id = str(row[0]).strip()
            security_type = classify_tw_security(stock_id)
            if not stock_id or not security_type:
                continue
            results.append({
                "stock_id":      stock_id,
                "name":          str(row[1]).strip(),
                "security_type": security_type,
                "ref_price":     _parse_price(row[ref_idx]),
                "limit_up":      _parse_price(row[up_idx]),
                "limit_down":    _parse_price(row[down_idx]),
            })
        except (IndexError, ValueError, TypeError) as e:
            print(f"  Skipping TPEx limits row: {e}")
            errors += 1
            continue
    return results, errors


def fetch_tpex_limits(trade_date: date) -> tuple:
    """
    Fetch TPEx price limits for trade_date.
    Returns (records, api_rows, parse_errors).
    """
    ad = _to_ad_date(trade_date)
    print(f"Fetching TPEx price limits for {trade_date} ...")

    data = fetch_json_retry(TPEX_URL, params={"date": ad, "response": "json"},
                            validate=lambda d: d.get("stat") == "ok")
    if not data or data.get("stat") != "ok":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return [], 0, 0

    # Verify the API returned data for the requested date.
    api_date = str(data.get("date", "")).strip()
    expected = trade_date.strftime("%Y%m%d")
    if api_date and api_date != expected:
        print(f"  Date mismatch: requested {expected}, API returned {api_date} — skipping")
        return [], 0, 0

    tables = data.get("tables", [])
    if not tables:
        print("  Unexpected response structure.")
        return [], 0, 0

    fields = tables[0].get("fields", [])
    rows = tables[0].get("data", [])
    print(f"  Found {len(rows)} records.")
    records, errors = _parse_tpex_limits(rows, fields)
    return records, len(rows), errors


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

def _upsert_stocks(cur, records: list, market: str):
    """Ensure stocks exist before upserting price data."""
    for r in records:
        if not r.get("security_type"):
            continue
        name = r.get("name") or r["stock_id"]  # fallback to stock_id if name is empty
        cur.execute("""
            INSERT INTO tw.stocks (stock_id, name, market, security_type)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (stock_id) DO UPDATE SET name = EXCLUDED.name, updated_at = NOW()
        """, (r["stock_id"], name, market, r["security_type"]))


def save_price_limits(twse: list, tpex: list, trade_date: date):
    """Upsert price limit data into tw.daily_prices."""
    with get_cursor() as cur:
        _upsert_stocks(cur, twse, "TWSE")
        _upsert_stocks(cur, tpex, "TPEx")

        for r in twse + tpex:
            cur.execute("""
                INSERT INTO tw.daily_prices (stock_id, trade_date, ref_price, limit_up, limit_down)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (stock_id, trade_date) DO UPDATE SET
                    ref_price   = COALESCE(EXCLUDED.ref_price,  tw.daily_prices.ref_price),
                    limit_up    = COALESCE(EXCLUDED.limit_up,   tw.daily_prices.limit_up),
                    limit_down  = COALESCE(EXCLUDED.limit_down, tw.daily_prices.limit_down)
            """, (
                r["stock_id"], trade_date,
                r.get("ref_price"), r.get("limit_up"), r.get("limit_down"),
            ))

    print(f"Saved price limits: TWSE={len(twse)}, TPEx={len(tpex)} (total={len(twse)+len(tpex)})")


def fill_ref_price_from_prev_close(trade_date: date):
    """
    For ESB (興櫃) stocks on trade_date where ref_price is NULL,
    fill it with the most recent close_price before trade_date.
    TWSE/TPEx stocks are excluded — they already receive ref_price from the API.
    """
    with get_cursor() as cur:
        cur.execute("""
            UPDATE tw.daily_prices AS today
            SET ref_price = prev.close_price
            FROM (
                SELECT DISTINCT ON (stock_id)
                    stock_id, close_price
                FROM tw.daily_prices
                WHERE trade_date < %(d)s
                  AND close_price IS NOT NULL
                ORDER BY stock_id, trade_date DESC
            ) AS prev
            JOIN tw.stocks s ON s.stock_id = prev.stock_id AND s.market = 'ESB'
            WHERE today.stock_id   = prev.stock_id
              AND today.trade_date = %(d)s
              AND today.ref_price  IS NULL
              AND prev.close_price IS NOT NULL
        """, {"d": trade_date})
        updated = cur.rowcount
    print(f"  Auto-filled ESB ref_price from prev close: {updated} records.")


def _get_prev_trading_day(trade_date: date) -> date | None:
    """Find the most recent trading day strictly before trade_date (TAIEX calendar)."""
    with get_cursor() as cur:
        cur.execute("""
            SELECT MAX(trade_date) AS d FROM tw.index_prices
            WHERE index_id = 'TAIEX' AND trade_date < %s
        """, (trade_date,))
        row = cur.fetchone()
        return row["d"] if row and row["d"] else None


def scrape_date(trade_date: date) -> ScrapeResult:
    """
    Fetch price limits applicable ON trade_date T.

    Both APIs publish NEXT-DAY limits, so we query the previous trading day
    (which returns T's limits) and save them under trade_date=T.
    """
    prev = _get_prev_trading_day(trade_date)
    if not prev:
        print(f"  [SKIP] No previous trading day in DB before {trade_date} — cannot fetch limits.")
        return ScrapeResult(records=0, api_rows=0, parse_errors=0)

    print(f"  Querying {prev} to obtain limits for {trade_date}")
    twse, twse_api, twse_err = fetch_twse_limits(prev)
    tpex, tpex_api, tpex_err = fetch_tpex_limits(prev)
    save_price_limits(twse, tpex, trade_date)
    return ScrapeResult(
        records=len(twse) + len(tpex),
        api_rows=twse_api + tpex_api,
        parse_errors=twse_err + tpex_err,
    )


def scrape_date_range(start_date: date, end_date: date):
    """Fetch and save price limits for a date range (skips weekends)."""
    current = start_date
    total = 0
    while current <= end_date:
        if current.weekday() < 5:
            result = scrape_date(current)
            total += result.records
        current += timedelta(days=1)
    print(f"Done. Total records saved: {total}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 2:
        start = date.fromisoformat(sys.argv[1])
        end = date.fromisoformat(sys.argv[2]) if len(sys.argv) >= 3 else start
        scrape_date_range(start, end)
    else:
        scrape_date(date.today())
