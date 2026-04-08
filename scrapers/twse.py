from datetime import date, timedelta

from db.connection import get_cursor
from utils.classifier import classify_tw_security
from utils.http_client import fetch_json

BASE_URL = "https://www.twse.com.tw/rwd/zh/afterTrading"


def fetch_daily_prices(trade_date: date):
    """
    Fetch all TWSE-listed stock prices for a given date.
    Source: MI_INDEX endpoint (每日收盤行情)
    """
    date_str = trade_date.strftime("%Y%m%d")
    print(f"Fetching TWSE daily prices for {trade_date} ...")

    data = fetch_json(
        f"{BASE_URL}/MI_INDEX",
        params={"date": date_str, "type": "ALLBUT0999", "response": "json"},
    )
    if not data or data.get("stat") != "OK":
        print(f"  API returned stat={data.get('stat') if data else 'no response'}")
        return []

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
        return []

    fields = price_table.get("fields", [])
    rows = price_table.get("data", [])
    print(f"  Found {len(rows)} records.")

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

            # Parse numeric fields (remove commas)
            def parse_num(val):
                if not val or val == "--" or val == "---":
                    return None
                return float(val.replace(",", "").replace("X", "").strip())

            volume_str = record.get("成交股數", "0")
            turnover_str = record.get("成交金額", "0")
            tx_count_str = record.get("成交筆數", "0")

            # Determine price change
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
                "volume": int(parse_num(volume_str) or 0),
                "turnover": int(parse_num(turnover_str) or 0),
                "transaction_count": int(parse_num(tx_count_str) or 0),
                "change": change_val,
                "change_pct": change_pct,
            })
        except (ValueError, TypeError) as e:
            print(f"  Skipping row parse error: {e}")
            continue

    return results


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
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    turnover = EXCLUDED.turnover,
                    transaction_count = EXCLUDED.transaction_count,
                    change = EXCLUDED.change,
                    change_pct = EXCLUDED.change_pct
            """, (
                r["stock_id"], r["trade_date"],
                r["open_price"], r["high_price"], r["low_price"],
                r["close_price"], r["volume"], r["turnover"],
                r["transaction_count"], r["change"], r["change_pct"],
            ))

    print(f"Saved {len(records)} records to database.")


def scrape_date(trade_date: date):
    """Fetch and save daily prices for a single date."""
    records = fetch_daily_prices(trade_date)
    save_daily_prices(records)
    return len(records)


def scrape_date_range(start_date: date, end_date: date):
    """Fetch and save daily prices for a date range (skips weekends)."""
    current = start_date
    total = 0

    while current <= end_date:
        if current.weekday() < 5:
            count = scrape_date(current)
            total += count
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