"""
Price limits scraper (漲跌停價 / 參考價).

Both APIs return NEXT-DAY limits for a given query date.
To get limits for trade_date T, query with the previous business day.

Sources:
  TWSE: rwd/zh/variation/TWT84U (GET, selectType=ALL)
        fields: [2]=漲停價, [3]=開盤競價基準(參考價), [4]=跌停價
  TPEx: /www/zh-tw/afterTrading/dailyQuotes (GET, d=YYY/MM/DD&o=json)
        fields: [16]=次日參考價, [17]=次日漲停價, [18]=次日跌停價
"""

from datetime import date, timedelta

from db.connection import get_cursor
from utils.classifier import classify_tw_security
from utils.http_client import fetch_json

TWSE_URL = "https://www.twse.com.tw/rwd/zh/variation/TWT84U"
TPEX_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes"


def _to_roc_date(d: date) -> str:
    return f"{d.year - 1911}/{d.strftime('%m/%d')}"


def _parse_price(val) -> float | None:
    s = str(val).replace(",", "").strip()
    if not s or s in ("--", "---", " ", ""):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _prev_business_day(d: date) -> date:
    """Return the previous business day (skips weekends)."""
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    return prev


# ---------------------------------------------------------------------------
# TWSE
# ---------------------------------------------------------------------------

def _parse_twse_limits(rows: list) -> list:
    results = []
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
        except (IndexError, ValueError, TypeError):
            continue
    return results


def fetch_twse_limits(trade_date: date) -> list:
    """Fetch TWSE price limits for trade_date by querying previous business day."""
    query_date = _prev_business_day(trade_date)
    date_str = query_date.strftime("%Y%m%d")
    print(f"Fetching TWSE price limits for {trade_date} (querying {query_date}) ...")

    data = fetch_json(TWSE_URL, params={"date": date_str, "selectType": "ALL", "response": "json"})
    if not data or data.get("stat") != "OK":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return []

    rows = data.get("data", [])
    print(f"  Found {len(rows)} records.")
    return _parse_twse_limits(rows)


# ---------------------------------------------------------------------------
# TPEx
# ---------------------------------------------------------------------------

def _parse_tpex_limits(rows: list) -> list:
    results = []
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
                "ref_price":     _parse_price(row[16]),
                "limit_up":      _parse_price(row[17]),
                "limit_down":    _parse_price(row[18]),
            })
        except (IndexError, ValueError, TypeError):
            continue
    return results


def fetch_tpex_limits(trade_date: date) -> list:
    """Fetch TPEx price limits for trade_date by querying previous business day."""
    query_date = _prev_business_day(trade_date)
    roc = _to_roc_date(query_date)
    print(f"Fetching TPEx price limits for {trade_date} (querying {query_date}) ...")

    data = fetch_json(TPEX_URL, params={"d": roc, "o": "json"})
    if not data or data.get("stat") != "ok":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return []

    tables = data.get("tables", [])
    if not tables:
        print("  Unexpected response structure.")
        return []

    rows = tables[0].get("data", [])
    print(f"  Found {len(rows)} records.")
    return _parse_tpex_limits(rows)


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
                    ref_price   = EXCLUDED.ref_price,
                    limit_up    = EXCLUDED.limit_up,
                    limit_down  = EXCLUDED.limit_down
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


def scrape_date(trade_date: date):
    """Fetch and save price limits for a single trade date."""
    twse = fetch_twse_limits(trade_date)
    tpex = fetch_tpex_limits(trade_date)
    save_price_limits(twse, tpex, trade_date)


def scrape_date_range(start_date: date, end_date: date):
    """Fetch and save price limits for a date range (skips weekends)."""
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            scrape_date(current)
        current += timedelta(days=1)
    print("Done.")


if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 2:
        start = date.fromisoformat(sys.argv[1])
        end = date.fromisoformat(sys.argv[2]) if len(sys.argv) >= 3 else start
        scrape_date_range(start, end)
    else:
        scrape_date(date.today())