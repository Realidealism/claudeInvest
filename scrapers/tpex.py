from datetime import date, timedelta

from db.connection import get_cursor
from utils.classifier import classify_tw_security
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json_retry

BASE_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes"


def _to_ad_date(d: date) -> str:
    """Convert date to AD slash format (e.g. 2026/04/02)."""
    return d.strftime("%Y/%m/%d")


def fetch_daily_prices(trade_date: date) -> tuple:
    """
    Fetch all TPEx-listed (OTC) stock prices for a given date.
    Source: dailyQuotes (上櫃每日收盤行情)
    Returns (records, api_rows, parse_errors).
    """
    ad_date = _to_ad_date(trade_date)
    print(f"Fetching TPEx daily prices for {trade_date} ...")

    data = fetch_json_retry(
        BASE_URL,
        params={"date": ad_date, "response": "json"},
        validate=lambda d: d.get("stat") == "ok" and d.get("tables"),
    )
    if not data or data.get("stat") != "ok" or not data.get("tables"):
        print(f"  API returned stat={data.get('stat') if data else 'no response'}")
        return [], 0, 0

    # Verify the API returned data for the requested date (TPEx silently returns
    # today's data if it doesn't recognize the date format).
    api_date = str(data.get("date", "")).strip()
    expected = trade_date.strftime("%Y%m%d")
    if api_date and api_date != expected:
        print(f"  Date mismatch: requested {expected}, API returned {api_date} — skipping")
        return [], 0, 0

    # Data is in tables[0]
    table = data["tables"][0]
    rows = table.get("data", [])
    print(f"  Found {len(rows)} records.")

    api_rows = len(rows)
    parse_errors = 0
    results = []
    for row in rows:
        try:
            stock_id = str(row[0]).strip()
            name = str(row[1]).strip()

            # Classify security type; skip warrants, ETN, TDR, etc.
            security_type = classify_tw_security(stock_id)
            if not stock_id or not security_type:
                continue

            def parse_num(val):
                """Return float or None. Tolerates dashes, blanks, and non-numeric
                strings (e.g. '除息', '除權息') used by the API on ex-div days."""
                s = str(val).replace(",", "").strip()
                if not s or set(s) == {"-"}:
                    return None
                try:
                    return float(s)
                except ValueError:
                    return None

            # Skip rows with no trading volume — those rows have OHLC = '---'
            # and offer no useful data beyond the previous-day reference price.
            volume = int(parse_num(row[8]) or 0)
            if volume == 0:
                continue

            close_price = parse_num(row[2])
            change_val = parse_num(row[3])  # may be None on ex-dividend days
            open_price = parse_num(row[4])
            high_price = parse_num(row[5])
            low_price = parse_num(row[6])
            # [7]=均價, [8]=成交股數, [9]=成交金額, [10]=成交筆數
            turnover = int(parse_num(row[9]) or 0)
            tx_count = int(parse_num(row[10]) or 0)

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
                "open_price": open_price,
                "high_price": high_price,
                "low_price": low_price,
                "close_price": close_price,
                "volume": volume,
                "turnover": turnover,
                "transaction_count": tx_count,
                "change": change_val,
                "change_pct": change_pct,
            })
        except (ValueError, TypeError, IndexError) as e:
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
        for r in records:
            cur.execute("""
                INSERT INTO tw.stocks (stock_id, name, market, security_type)
                VALUES (%s, %s, 'TPEx', %s)
                ON CONFLICT (stock_id)
                DO UPDATE SET name = EXCLUDED.name, security_type = EXCLUDED.security_type, updated_at = NOW()
            """, (r["stock_id"], r["name"], r["security_type"]))

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
        start = date.fromisoformat(sys.argv[1])
        end = date.fromisoformat(sys.argv[2]) if len(sys.argv) >= 3 else start
        scrape_date_range(start, end)
    else:
        scrape_date(date.today())
