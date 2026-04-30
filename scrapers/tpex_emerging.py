"""
TPEx emerging market (興櫃) daily trading data scraper.

Source: /www/zh-tw/emerging/latest (GET, d=YYY/MM/DD&o=json)

Fields (by index):
  [0]  代號       stock code
  [1]  名稱       stock name
  [2]  前日均價   previous day avg price
  [7]  日最高     daily high
  [8]  日最低     daily low
  [9]  日均價     daily avg price (used as close price)
  [10] 成交       last transaction price
  [13] 成交量     volume (shares)

Notes:
  - Emerging market stocks have no price limits, no margin trading.
  - Volume is in shares (股), not lots (張).
  - '-' means no trading occurred.
"""

from datetime import date, timedelta

from db.connection import get_cursor
from utils.classifier import classify_tw_security
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json_retry
from scrapers.price_limits import fill_ref_price_from_prev_close

BASE_URL = "https://www.tpex.org.tw/www/zh-tw/emerging/latest"


def _to_ad_date(d: date) -> str:
    return d.strftime("%Y/%m/%d")


def _parse_num(val) -> float | None:
    s = str(val).replace(",", "").strip()
    if not s or s in ("-", "--", "---", ""):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def fetch_emerging(trade_date: date) -> tuple:
    """
    Fetch TPEx emerging market daily data for a given date.

    NOTE: The /emerging/latest endpoint only returns the latest trading day's
    data and ignores any date parameter. Historical emerging data requires
    a separate per-stock endpoint, which is not implemented here.
    To avoid corrupting historical rows, this scraper only writes data when
    the requested date is within 3 days of today.

    Returns (records, api_rows, parse_errors).
    """
    ad = _to_ad_date(trade_date)
    print(f"Fetching TPEx emerging market for {trade_date} ...")

    # Refuse historical dates: API ignores date parameter
    days_old = (date.today() - trade_date).days
    if days_old > 3:
        print(f"  Skipped: emerging /latest endpoint cannot fetch historical dates "
              f"(requested {trade_date}, {days_old} days old).")
        return [], 0, 0

    data = fetch_json_retry(BASE_URL, params={"date": ad, "response": "json"},
                            validate=lambda d: d.get("stat") == "ok")
    if not data or data.get("stat") != "ok":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return [], 0, 0

    tables = data.get("tables", [])
    if not tables:
        print("  Unexpected response structure.")
        return [], 0, 0

    rows = tables[0].get("data", [])
    print(f"  Found {len(rows)} records.")

    api_rows = len(rows)
    parse_errors = 0
    results = []
    for row in rows:
        try:
            stock_id = str(row[0]).strip()
            # Strip HTML anchor tags that may wrap the stock_id
            if "<" in stock_id:
                import re
                stock_id = re.sub(r"<[^>]+>", "", stock_id).strip()

            security_type = classify_tw_security(stock_id)
            if not stock_id or not security_type:
                continue

            volume = _parse_num(row[13])
            if not volume:
                continue

            results.append({
                "stock_id":      stock_id,
                "name":          str(row[1]).strip(),
                "security_type": security_type,
                "close_price":   _parse_num(row[9]),   # daily avg price
                "high_price":    _parse_num(row[7]),
                "low_price":     _parse_num(row[8]),
                "volume":        int(volume),
            })
        except (IndexError, ValueError, TypeError) as e:
            print(f"  Skipping row: {e}")
            parse_errors += 1
            continue

    return results, api_rows, parse_errors


def save_emerging(records: list, trade_date: date):
    """Upsert emerging market data into tw.stocks and tw.daily_prices."""
    if not records:
        print("  No records to save.")
        return

    with get_cursor() as cur:
        # Ensure stocks exist with market = 'ESB'
        for r in records:
            cur.execute("""
                INSERT INTO tw.stocks (stock_id, name, market, security_type)
                VALUES (%s, %s, 'ESB', %s)
                ON CONFLICT (stock_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    updated_at = NOW()
            """, (r["stock_id"], r["name"], r["security_type"]))

        for r in records:
            cur.execute("""
                INSERT INTO tw.daily_prices (
                    stock_id, trade_date,
                    close_price, high_price, low_price, volume
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_id, trade_date) DO UPDATE SET
                    close_price = COALESCE(EXCLUDED.close_price, tw.daily_prices.close_price),
                    high_price  = COALESCE(EXCLUDED.high_price,  tw.daily_prices.high_price),
                    low_price   = COALESCE(EXCLUDED.low_price,   tw.daily_prices.low_price),
                    volume      = COALESCE(EXCLUDED.volume,      tw.daily_prices.volume)
            """, (
                r["stock_id"], trade_date,
                r.get("close_price"), r.get("high_price"),
                r.get("low_price"),   r.get("volume"),
            ))

    print(f"Saved {len(records)} emerging market records.")
    fill_ref_price_from_prev_close(trade_date)


def scrape_date(trade_date: date) -> ScrapeResult:
    records, api_rows, errors = fetch_emerging(trade_date)
    save_emerging(records, trade_date)
    return ScrapeResult(records=len(records), api_rows=api_rows, parse_errors=errors)


def scrape_date_range(start_date: date, end_date: date) -> int:
    current, total = start_date, 0
    while current <= end_date:
        if current.weekday() < 5:
            result = scrape_date(current)
            total += result.records
        current += timedelta(days=1)
    print(f"\nDone. Total records saved: {total}")
    return total


if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 2:
        start = date.fromisoformat(sys.argv[1])
        end = date.fromisoformat(sys.argv[2]) if len(sys.argv) >= 3 else start
        scrape_date_range(start, end)
    else:
        scrape_date(date.today())
