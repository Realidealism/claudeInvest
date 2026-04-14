from datetime import date, timedelta

from db.connection import get_cursor
from utils.classifier import classify_tw_security
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json_retry

BASE_URL = "https://www.twse.com.tw/rwd/zh/afterTrading"


def fetch_daily_prices(trade_date: date) -> tuple:
    """
    Fetch all TWSE-listed stock prices for a given date.
    Source: MI_INDEX endpoint (每日收盤行情)
    Returns (records, api_rows, parse_errors).
    """
    date_str = trade_date.strftime("%Y%m%d")
    print(f"Fetching TWSE daily prices for {trade_date} ...")

    data = fetch_json_retry(
        f"{BASE_URL}/MI_INDEX",
        params={"date": date_str, "type": "ALLBUT0999", "response": "json"},
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

    # Find the table containing stock price data
    tables = data.get("tables", [])
    price_table = None
    for table in tables:
        title = table.get("title", "")
        if "每日收盤行情" in title:
            price_table = table
            break

    if not price_table:
        print("  Could not find price table in response.")
        return [], 0, 0

    fields = price_table.get("fields", [])
    rows = price_table.get("data", [])
    print(f"  Found {len(rows)} records.")

    api_rows = len(rows)
    parse_errors = 0
    results = []
    for row in rows:
        record = dict(zip(fields, row))
        try:
            stock_id = record.get("證券代號", "").strip()
            name = record.get("證券名稱", "").strip()

            # Classify security type; skip warrants, ETN, TDR, etc.
            security_type = classify_tw_security(stock_id)
            if not stock_id or not security_type:
                continue

            # Parse numeric fields. Tolerates dashes, blanks, HTML tags, and
            # non-numeric strings (e.g. '除息', '<p>除權息</p>') used by the API
            # on ex-dividend days.
            def parse_num(val):
                import re
                s = re.sub(r"<[^>]+>", "", str(val))
                s = s.replace(",", "").replace("X", "").strip()
                if not s or set(s) == {"-"}:
                    return None
                try:
                    return float(s)
                except ValueError:
                    return None

            # Skip rows with no trading volume (no useful OHLC data either).
            volume = int(parse_num(record.get("成交股數", "0")) or 0)
            if volume == 0:
                continue

            # Determine price change. On ex-dividend days the API returns
            # '除息'/'除權息' here, in which case change_val is None and
            # change_pct should be computed later from ref_price - close_price.
            change_sign = record.get("漲跌(+/-)", "").strip()
            change_val = parse_num(record.get("漲跌價差"))
            if change_val is not None and change_sign == "-":
                change_val = -change_val

            close_price = parse_num(record.get("收盤價"))
            change_pct = None
            if close_price and change_val and (close_price - change_val) != 0:
                change_pct = round(
                    change_val / (close_price - change_val) * 100, 4
                )

            results.append({
                "stock_id": stock_id,
                "name": name,
                "security_type": security_type,
                "trade_date": trade_date,
                "open_price": parse_num(record.get("開盤價")),
                "high_price": parse_num(record.get("最高價")),
                "low_price": parse_num(record.get("最低價")),
                "close_price": close_price,
                "volume": volume,
                "turnover": int(parse_num(record.get("成交金額", "0")) or 0),
                "transaction_count": int(parse_num(record.get("成交筆數", "0")) or 0),
                "change": change_val,
                "change_pct": change_pct,
            })
        except (ValueError, TypeError) as e:
            print(f"  Skipping row parse error: {e}")
            parse_errors += 1
            continue

    return results, api_rows, parse_errors


def save_daily_prices(records: list):
    """Upsert stock info and daily prices into database."""
    if not records:
        print("No records to save.")
        return

    with get_cursor() as cur:
        # Upsert stocks
        for r in records:
            cur.execute("""
                INSERT INTO tw.stocks (stock_id, name, market, security_type)
                VALUES (%s, %s, 'TWSE', %s)
                ON CONFLICT (stock_id)
                DO UPDATE SET name = EXCLUDED.name, security_type = EXCLUDED.security_type, updated_at = NOW()
            """, (r["stock_id"], r["name"], r["security_type"]))

        # Upsert daily prices
        for r in records:
            cur.execute("""
                INSERT INTO tw.daily_prices
                    (stock_id, trade_date, open_price, high_price, low_price,
                     close_price, volume, turnover, transaction_count,
                     change, change_pct)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_id, trade_date)
                DO UPDATE SET
                    open_price = COALESCE(EXCLUDED.open_price, tw.daily_prices.open_price),
                    high_price = COALESCE(EXCLUDED.high_price, tw.daily_prices.high_price),
                    low_price = COALESCE(EXCLUDED.low_price, tw.daily_prices.low_price),
                    close_price = COALESCE(EXCLUDED.close_price, tw.daily_prices.close_price),
                    volume = COALESCE(EXCLUDED.volume, tw.daily_prices.volume),
                    turnover = COALESCE(EXCLUDED.turnover, tw.daily_prices.turnover),
                    transaction_count = COALESCE(EXCLUDED.transaction_count, tw.daily_prices.transaction_count),
                    change = COALESCE(EXCLUDED.change, tw.daily_prices.change),
                    change_pct = COALESCE(EXCLUDED.change_pct, tw.daily_prices.change_pct)
            """, (
                r["stock_id"], r["trade_date"],
                r["open_price"], r["high_price"], r["low_price"],
                r["close_price"], r["volume"], r["turnover"],
                r["transaction_count"], r["change"], r["change_pct"],
            ))

    print(f"Saved {len(records)} records to database.")


def scrape_date(trade_date: date) -> ScrapeResult:
    """Fetch and save daily prices for a single date."""
    records, api_rows, errors = fetch_daily_prices(trade_date)
    save_daily_prices(records)
    return ScrapeResult(records=len(records), api_rows=api_rows, parse_errors=errors)


def scrape_date_range(start_date: date, end_date: date):
    """Fetch and save daily prices for a date range (skips weekends)."""
    current = start_date
    total = 0

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
        # Usage: python -m scrapers.twse 2026-04-07
        #        python -m scrapers.twse 2026-04-01 2026-04-07
        start = date.fromisoformat(sys.argv[1])
        end = date.fromisoformat(sys.argv[2]) if len(sys.argv) >= 3 else start
        scrape_date_range(start, end)
    else:
        scrape_date(date.today())