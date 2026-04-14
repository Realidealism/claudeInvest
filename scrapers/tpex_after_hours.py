from datetime import date, timedelta

from db.connection import get_cursor
from utils.classifier import classify_tw_security
from utils.format_shift import ScrapeResult
from utils.http_client import get_session, _wait_for_rate_limit, _get_domain, MAX_RETRIES, BACKOFF_BASE

BASE_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/fixPricing"


def _to_ad_date(d: date) -> str:
    """Convert date to AD slash format (e.g. 2026/04/02)."""
    return d.strftime("%Y/%m/%d")


def _post(url: str, data: dict) -> dict | None:
    """POST request with rate limiting and retry (including soft failures)."""
    import time, random
    session = get_session()
    domain = _get_domain(url)

    for attempt in range(MAX_RETRIES):
        try:
            _wait_for_rate_limit(domain)
            resp = session.post(url, data=data, timeout=30)
            resp.raise_for_status()
            result = resp.json()
            if result.get("stat") == "ok":
                return result
            # Soft failure — stat != "ok", retry
            print(f"  Soft failure (stat={result.get('stat')}), attempt {attempt + 1}/{MAX_RETRIES}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(BACKOFF_BASE + random.uniform(1, 3))
        except Exception as e:
            print(f"  Attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(BACKOFF_BASE * (2 ** attempt) + random.uniform(1, 3))
    return None


def fetch_after_hours(trade_date: date) -> tuple:
    """
    Fetch TPEx after-hours fixed-price trading data for a given date.
    Source: /www/zh-tw/afterTrading/fixPricing (POST)
    Fields: [0] code, [1] name, [2] bid_count, [3] bid_lots, [4] ask_count, [5] ask_lots,
            [6] price, [7] tx_count, [8] volume(lots), [9] turnover, ...
    Volume is reported in lots (張); multiply by 1000 to convert to shares (股).
    Returns (records, api_rows, parse_errors).
    """
    ad_date = _to_ad_date(trade_date)
    print(f"Fetching TPEx after-hours for {trade_date} ...")

    data = _post(BASE_URL, {"date": ad_date, "response": "json"})
    if not data:
        print("  No data returned.")
        return [], 0, 0

    # Verify the API returned data for the requested date.
    api_date = str(data.get("date", "")).strip()
    expected = trade_date.strftime("%Y%m%d")
    if api_date and api_date != expected:
        print(f"  Date mismatch: requested {expected}, API returned {api_date} — skipping")
        return [], 0, 0

    rows = data["tables"][0].get("data", [])
    print(f"  Found {len(rows)} records.")

    api_rows = len(rows)
    parse_errors = 0
    results = []
    for row in rows:
        try:
            stock_id = str(row[0]).strip()
            if not stock_id or not classify_tw_security(stock_id):
                continue

            def parse_num(val):
                s = str(val).replace(",", "").strip()
                return None if not s or s == "0" else float(s)

            volume_lots = parse_num(row[8])  # 成交張數
            if not volume_lots:
                continue

            results.append({
                "stock_id": stock_id,
                "name": str(row[1]).strip(),
                "security_type": classify_tw_security(stock_id),
                "ah_price": parse_num(row[6]),
                "ah_volume": int(volume_lots * 1000),        # convert lots to shares
                "ah_tx_count": int(parse_num(row[7]) or 0),  # 成交筆數
                "ah_turnover": int(parse_num(row[9]) or 0),  # 成交金額
            })
        except (ValueError, TypeError, IndexError) as e:
            print(f"  Skipping parse error: {e}")
            parse_errors += 1
            continue

    return results, api_rows, parse_errors


def save_after_hours(records: list, trade_date: date):
    """Upsert TPEx after-hours data into tw.daily_prices."""
    if not records:
        print("No after-hours records to save.")
        return

    with get_cursor() as cur:
        # Ensure stock exists
        for r in records:
            cur.execute("""
                INSERT INTO tw.stocks (stock_id, name, market, security_type)
                VALUES (%s, %s, 'TPEx', %s)
                ON CONFLICT (stock_id) DO UPDATE SET name = EXCLUDED.name, updated_at = NOW()
            """, (r["stock_id"], r["name"], r["security_type"]))

        for r in records:
            cur.execute("""
                INSERT INTO tw.daily_prices (stock_id, trade_date, ah_price, ah_volume, ah_turnover, ah_tx_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_id, trade_date)
                DO UPDATE SET
                    ah_price    = COALESCE(EXCLUDED.ah_price,    tw.daily_prices.ah_price),
                    ah_volume   = COALESCE(EXCLUDED.ah_volume,   tw.daily_prices.ah_volume),
                    ah_turnover = COALESCE(EXCLUDED.ah_turnover, tw.daily_prices.ah_turnover),
                    ah_tx_count = COALESCE(EXCLUDED.ah_tx_count, tw.daily_prices.ah_tx_count)
            """, (
                r["stock_id"], trade_date,
                r.get("ah_price"), r.get("ah_volume"),
                r.get("ah_turnover"), r.get("ah_tx_count"),
            ))

    print(f"Saved {len(records)} TPEx after-hours records.")


def scrape_date(trade_date: date) -> ScrapeResult:
    records, api_rows, errors = fetch_after_hours(trade_date)
    save_after_hours(records, trade_date)
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
