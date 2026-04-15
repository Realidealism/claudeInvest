"""
Odd-lot trading data scraper (零股交易).

Sources:
  TWSE regular session  : rwd/zh/afterTrading/TWTC7U (GET, selectType=ALL)
  TWSE after-hours      : rwd/zh/afterTrading/TWT53U (GET, selectType=ALL)
  TPEx regular session  : /www/zh-tw/afterTrading/oddQuote (POST)
  TPEx after-hours      : /www/zh-tw/afterTrading/odd      (POST, type=Daily)
"""

from datetime import date, timedelta

from db.connection import get_cursor
from utils.classifier import classify_tw_security
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json_retry, post_json_retry

# TWSE
TWSE_OL_URL    = "https://www.twse.com.tw/rwd/zh/afterTrading/TWTC7U"
TWSE_OL_AH_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/TWT53U"

# TPEx
TPEX_OL_URL    = "https://www.tpex.org.tw/www/zh-tw/afterTrading/oddQuote"
TPEX_OL_AH_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/odd"


def _to_ad_date(d: date) -> str:
    return d.strftime("%Y/%m/%d")


def _post(url: str, data: dict) -> dict | None:
    """POST with rate limiting and retry (including soft failures)."""
    return post_json_retry(url, data=data, validate=lambda d: d.get("stat") == "ok")


def _parse_num(val) -> float | None:
    s = str(val).replace(",", "").strip()
    if not s or s in ("0", "--", "---"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# TWSE
# ---------------------------------------------------------------------------

def _parse_twse_ol(rows: list, fields: list) -> tuple:
    """
    TWTC7U fields: 證券代號, 證券名稱, 成交股數, 成交筆數, 成交金額,
                   成交均價, 最後揭示買價, ..., 最後揭示賣價, ...
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
            volume = _parse_num(record.get("成交股數"))
            if not volume:
                continue
            results.append({
                "stock_id": stock_id,
                "name":     record.get("證券名稱", "").strip(),
                "security_type": security_type,
                "ol_price":    _parse_num(record.get("成交均價")),
                "ol_volume":   int(volume),
                "ol_turnover": int(_parse_num(record.get("成交金額")) or 0),
                "ol_tx_count": int(_parse_num(record.get("成交筆數")) or 0),
            })
        except (ValueError, TypeError) as e:
            print(f"  Skipping TWSE odd-lot row: {e}")
            errors += 1
            continue
    return results, errors


def _parse_twse_ol_ah(rows: list, fields: list) -> tuple:
    """
    TWT53U fields: 證券代號, 證券名稱, 成交股數, 成交筆數, 成交金額,
                   成交價格, 最後揭示買價, ...
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
            volume = _parse_num(record.get("成交股數"))
            if not volume:
                continue
            results.append({
                "stock_id":      stock_id,
                "name":          record.get("證券名稱", "").strip(),
                "security_type": security_type,
                "ol_ah_price":    _parse_num(record.get("成交價格")),
                "ol_ah_volume":   int(volume),
                "ol_ah_turnover": int(_parse_num(record.get("成交金額")) or 0),
                "ol_ah_tx_count": int(_parse_num(record.get("成交筆數")) or 0),
            })
        except (ValueError, TypeError) as e:
            print(f"  Skipping TWSE odd-lot AH row: {e}")
            errors += 1
            continue
    return results, errors


def fetch_twse_odd_lot(trade_date: date) -> tuple:
    """
    Fetch TWSE odd-lot regular + after-hours for a date.
    Returns (ol, ol_ah, api_rows, parse_errors).
    """
    date_str = trade_date.strftime("%Y%m%d")
    params = {"date": date_str, "selectType": "ALL", "response": "json"}

    print(f"Fetching TWSE odd-lot (regular) for {trade_date} ...")
    d1 = fetch_json_retry(TWSE_OL_URL, params=params,
                          validate=lambda d: d.get("stat") == "OK")
    ol, ol_rows, ol_errors = [], 0, 0
    if d1 and d1.get("stat") == "OK" and d1.get("data"):
        api_date = str(d1.get("date", "")).strip()
        if api_date and api_date != date_str:
            print(f"  Date mismatch: requested {date_str}, API returned {api_date} — skipping")
        else:
            ol_rows = len(d1["data"])
            print(f"  Found {ol_rows} records.")
            ol, ol_errors = _parse_twse_ol(d1["data"], d1.get("fields", []))

    print(f"Fetching TWSE odd-lot (after-hours) for {trade_date} ...")
    d2 = fetch_json_retry(TWSE_OL_AH_URL, params=params,
                          validate=lambda d: d.get("stat") == "OK")
    ol_ah, ah_rows, ah_errors = [], 0, 0
    if d2 and d2.get("stat") == "OK" and d2.get("data"):
        api_date = str(d2.get("date", "")).strip()
        if api_date and api_date != date_str:
            print(f"  Date mismatch: requested {date_str}, API returned {api_date} — skipping")
        else:
            ah_rows = len(d2["data"])
            print(f"  Found {ah_rows} records.")
            ol_ah, ah_errors = _parse_twse_ol_ah(d2["data"], d2.get("fields", []))

    return ol, ol_ah, ol_rows + ah_rows, ol_errors + ah_errors


# ---------------------------------------------------------------------------
# TPEx
# ---------------------------------------------------------------------------

def _parse_tpex_ol(rows: list) -> tuple:
    """
    oddQuote fields (by index):
    [0] code, [1] name, [2] close, [3] change, [4] open, [5] high, [6] low,
    [7] volume, [8] turnover, [9] tx_count, ...
    Returns (records, parse_errors).
    """
    results = []
    errors = 0
    for row in rows:
        try:
            stock_id = str(row[0]).strip()
            if not stock_id or not classify_tw_security(stock_id):
                continue
            volume = _parse_num(row[7])
            if not volume:
                continue
            results.append({
                "stock_id":    stock_id,
                "name":        str(row[1]).strip(),
                "security_type": classify_tw_security(stock_id),
                "ol_price":    _parse_num(row[2]),
                "ol_volume":   int(volume),
                "ol_turnover": int(_parse_num(row[8]) or 0),
                "ol_tx_count": int(_parse_num(row[9]) or 0),
            })
        except (ValueError, TypeError, IndexError) as e:
            print(f"  Skipping TPEx odd-lot row: {e}")
            errors += 1
            continue
    return results, errors


def _parse_tpex_ol_ah(rows: list) -> tuple:
    """
    odd fields (by index):
    [0] code, [1] name, [2] volume, [3] tx_count, [4] turnover, [5] price, ...
    Returns (records, parse_errors).
    """
    results = []
    errors = 0
    for row in rows:
        try:
            stock_id = str(row[0]).strip()
            if not stock_id or not classify_tw_security(stock_id):
                continue
            volume = _parse_num(row[2])
            if not volume:
                continue
            results.append({
                "stock_id":      stock_id,
                "name":          str(row[1]).strip(),
                "security_type": classify_tw_security(stock_id),
                "ol_ah_price":    _parse_num(row[5]),
                "ol_ah_volume":   int(volume),
                "ol_ah_turnover": int(_parse_num(row[4]) or 0),
                "ol_ah_tx_count": int(_parse_num(row[3]) or 0),
            })
        except (ValueError, TypeError, IndexError) as e:
            print(f"  Skipping TPEx odd-lot AH row: {e}")
            errors += 1
            continue
    return results, errors


def fetch_tpex_odd_lot(trade_date: date) -> tuple:
    """
    Fetch TPEx odd-lot regular + after-hours for a date.
    Returns (ol, ol_ah, api_rows, parse_errors).
    """
    ad = _to_ad_date(trade_date)
    expected = trade_date.strftime("%Y%m%d")

    print(f"Fetching TPEx odd-lot (regular) for {trade_date} ...")
    d1 = _post(TPEX_OL_URL, {"date": ad, "response": "json"})
    ol, ol_rows, ol_errors = [], 0, 0
    if d1 and d1.get("tables"):
        api_date = str(d1.get("date", "")).strip()
        if api_date and api_date != expected:
            print(f"  Date mismatch: requested {expected}, API returned {api_date} — skipping")
        else:
            rows = d1["tables"][0].get("data", [])
            ol_rows = len(rows)
            print(f"  Found {ol_rows} records.")
            ol, ol_errors = _parse_tpex_ol(rows)

    print(f"Fetching TPEx odd-lot (after-hours) for {trade_date} ...")
    d2 = _post(TPEX_OL_AH_URL, {"date": ad, "type": "Daily", "response": "json"})
    ol_ah, ah_rows, ah_errors = [], 0, 0
    if d2 and d2.get("tables"):
        api_date = str(d2.get("date", "")).strip()
        if api_date and api_date != expected:
            print(f"  Date mismatch: requested {expected}, API returned {api_date} — skipping")
        else:
            rows = d2["tables"][0].get("data", [])
            ah_rows = len(rows)
            print(f"  Found {ah_rows} records.")
            ol_ah, ah_errors = _parse_tpex_ol_ah(rows)

    return ol, ol_ah, ol_rows + ah_rows, ol_errors + ah_errors


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

def _upsert_stocks(cur, records: list, market: str):
    """Ensure stocks exist before upserting prices."""
    for r in records:
        if r.get("name") and r.get("security_type"):
            cur.execute("""
                INSERT INTO tw.stocks (stock_id, name, market, security_type)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (stock_id) DO UPDATE SET name = EXCLUDED.name, updated_at = NOW()
            """, (r["stock_id"], r["name"], market, r["security_type"]))


def save_odd_lot(
    twse_ol: list, twse_ol_ah: list,
    tpex_ol: list, tpex_ol_ah: list,
    trade_date: date,
):
    """Upsert all odd-lot data into tw.daily_prices."""
    with get_cursor() as cur:
        _upsert_stocks(cur, twse_ol, "TWSE")
        _upsert_stocks(cur, twse_ol_ah, "TWSE")
        _upsert_stocks(cur, tpex_ol, "TPEx")
        _upsert_stocks(cur, tpex_ol_ah, "TPEx")

        for r in twse_ol + tpex_ol:
            cur.execute("""
                INSERT INTO tw.daily_prices (stock_id, trade_date, ol_price, ol_volume, ol_turnover, ol_tx_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_id, trade_date)
                DO UPDATE SET
                    ol_price    = COALESCE(EXCLUDED.ol_price,    tw.daily_prices.ol_price),
                    ol_volume   = COALESCE(EXCLUDED.ol_volume,   tw.daily_prices.ol_volume),
                    ol_turnover = COALESCE(EXCLUDED.ol_turnover, tw.daily_prices.ol_turnover),
                    ol_tx_count = COALESCE(EXCLUDED.ol_tx_count, tw.daily_prices.ol_tx_count)
            """, (
                r["stock_id"], trade_date,
                r.get("ol_price"), r.get("ol_volume"),
                r.get("ol_turnover"), r.get("ol_tx_count"),
            ))

        for r in twse_ol_ah + tpex_ol_ah:
            cur.execute("""
                INSERT INTO tw.daily_prices (stock_id, trade_date, ol_ah_price, ol_ah_volume, ol_ah_turnover, ol_ah_tx_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_id, trade_date)
                DO UPDATE SET
                    ol_ah_price    = COALESCE(EXCLUDED.ol_ah_price,    tw.daily_prices.ol_ah_price),
                    ol_ah_volume   = COALESCE(EXCLUDED.ol_ah_volume,   tw.daily_prices.ol_ah_volume),
                    ol_ah_turnover = COALESCE(EXCLUDED.ol_ah_turnover, tw.daily_prices.ol_ah_turnover),
                    ol_ah_tx_count = COALESCE(EXCLUDED.ol_ah_tx_count, tw.daily_prices.ol_ah_tx_count)
            """, (
                r["stock_id"], trade_date,
                r.get("ol_ah_price"), r.get("ol_ah_volume"),
                r.get("ol_ah_turnover"), r.get("ol_ah_tx_count"),
            ))

    total = len(twse_ol) + len(twse_ol_ah) + len(tpex_ol) + len(tpex_ol_ah)
    print(f"Saved odd-lot: TWSE regular={len(twse_ol)}, TWSE after={len(twse_ol_ah)}, "
          f"TPEx regular={len(tpex_ol)}, TPEx after={len(tpex_ol_ah)} (total={total})")


def scrape_date(trade_date: date) -> ScrapeResult:
    """Fetch and save all odd-lot data for a single date."""
    twse_ol, twse_ol_ah, twse_api, twse_err = fetch_twse_odd_lot(trade_date)
    tpex_ol, tpex_ol_ah, tpex_api, tpex_err = fetch_tpex_odd_lot(trade_date)
    save_odd_lot(twse_ol, twse_ol_ah, tpex_ol, tpex_ol_ah, trade_date)
    total_records = len(twse_ol) + len(twse_ol_ah) + len(tpex_ol) + len(tpex_ol_ah)
    return ScrapeResult(
        records=total_records,
        api_rows=twse_api + tpex_api,
        parse_errors=twse_err + tpex_err,
    )


def scrape_date_range(start_date: date, end_date: date):
    """Fetch and save odd-lot data for a date range (skips weekends)."""
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
