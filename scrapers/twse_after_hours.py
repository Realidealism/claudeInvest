from datetime import date

from db.connection import get_cursor
from utils.classifier import classify_tw_security
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json, fetch_json_retry

# TWSE Open Data API (latest day only, no date param)
OPENDATA_URL = "https://openapi.twse.com.tw/v1/exchangeReport/BFT41U"

# TWSE rwd API (supports historical date query)
RWD_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/BFT41U"


def fetch_after_hours_latest():
    """
    Fetch TWSE after-hours fixed-price data from Open Data.
    Only returns the most recent trading day (no date parameter).
    """
    print("Fetching TWSE after-hours (latest) from Open Data ...")
    data = fetch_json(OPENDATA_URL)
    if not data or not isinstance(data, list):
        print("  No data returned.")
        return []

    print(f"  Found {len(data)} records.")
    return _parse_opendata(data)


def fetch_after_hours_by_date(trade_date: date) -> tuple:
    """
    Fetch TWSE after-hours fixed-price data for a specific date.
    Returns (records, api_rows, parse_errors).
    """
    date_str = trade_date.strftime("%Y%m%d")
    print(f"Fetching TWSE after-hours for {trade_date} ...")

    data = fetch_json_retry(
        RWD_URL,
        params={"date": date_str, "selectType": "ALL", "response": "json"},
        validate=lambda d: d.get("stat") == "OK",
    )
    if not data or data.get("stat") != "OK":
        print(f"  API returned stat={data.get('stat') if data else 'no response'}")
        return [], 0, 0

    # Verify the API returned data for the requested date.
    api_date = str(data.get("date", "")).strip()
    if api_date and api_date != date_str:
        print(f"  Date mismatch: requested {date_str}, API returned {api_date} — skipping")
        return [], 0, 0

    rows = data.get("data", [])
    fields = data.get("fields", [])
    if not rows:
        print("  No data for this date.")
        return [], 0, 0

    print(f"  Found {len(rows)} records.")
    records, errors = _parse_rwd(rows, fields)
    return records, len(rows), errors


def _parse_opendata(data: list) -> list:
    """Parse Open Data format (list of dicts)."""
    results = []
    for record in data:
        try:
            stock_id = record.get("Code", "").strip()
            if not stock_id or not classify_tw_security(stock_id):
                continue

            def parse_num(val):
                if not val or str(val).strip() in ("", "--", "---"):
                    return None
                return float(str(val).replace(",", "").strip())

            volume = parse_num(record.get("TradeVolume"))
            if not volume:
                continue

            results.append({
                "stock_id": stock_id,
                "ah_price": parse_num(record.get("TradePrice")),
                "ah_volume": int(volume),
                "ah_turnover": int(parse_num(record.get("TradeValue")) or 0),
                "ah_tx_count": int(parse_num(record.get("Transaction")) or 0),
            })
        except (ValueError, TypeError):
            continue
    return results


def _parse_rwd(rows: list, fields: list) -> tuple:
    """
    Parse rwd API format (list of lists).
    Fields: 證券代號, 證券名稱, 成交數量, 成交筆數, 成交金額, 成交價, ...
    Returns (records, parse_errors).
    """
    results = []
    errors = 0
    for row in rows:
        try:
            record = dict(zip(fields, row))
            stock_id = record.get("證券代號", "").strip()
            security_type = classify_tw_security(stock_id)
            if not stock_id or not security_type:
                continue

            def parse_num(val):
                if not val or str(val).strip() in ("", "--", "---"):
                    return None
                return float(str(val).replace(",", "").strip())

            volume = parse_num(record.get("成交數量"))
            if not volume:
                continue

            results.append({
                "stock_id": stock_id,
                "name":     record.get("證券名稱", "").strip(),
                "security_type": security_type,
                "ah_price": parse_num(record.get("成交價")),
                "ah_volume": int(volume),
                "ah_turnover": int(parse_num(record.get("成交金額")) or 0),
                "ah_tx_count": int(parse_num(record.get("成交筆數")) or 0),
            })
        except (ValueError, TypeError) as e:
            print(f"  Skipping row parse error: {e}")
            errors += 1
            continue
    return results, errors


def save_after_hours(records: list, trade_date: date):
    """Upsert after-hours data into tw.daily_prices."""
    if not records:
        print("No after-hours records to save.")
        return

    with get_cursor() as cur:
        for r in records:
            if r.get("name") and r.get("security_type"):
                cur.execute("""
                    INSERT INTO tw.stocks (stock_id, name, market, security_type)
                    VALUES (%s, %s, 'TWSE', %s)
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

    print(f"Saved {len(records)} TWSE after-hours records.")


def scrape_latest():
    """Fetch and save the latest after-hours data (today)."""
    records = fetch_after_hours_latest()
    if records:
        save_after_hours(records, date.today())
    return len(records)


def scrape_date(trade_date: date) -> ScrapeResult:
    """Fetch and save after-hours data for a specific date."""
    records, api_rows, errors = fetch_after_hours_by_date(trade_date)
    if records:
        save_after_hours(records, trade_date)
    return ScrapeResult(records=len(records), api_rows=api_rows, parse_errors=errors)


if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 2:
        scrape_date(date.fromisoformat(sys.argv[1]))
    else:
        scrape_latest()
