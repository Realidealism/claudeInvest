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
from utils.http_client import fetch_json, get_session, _wait_for_rate_limit, _get_domain, MAX_RETRIES, BACKOFF_BASE

# TWSE
TWSE_OL_URL    = "https://www.twse.com.tw/rwd/zh/afterTrading/TWTC7U"
TWSE_OL_AH_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/TWT53U"

# TPEx
TPEX_OL_URL    = "https://www.tpex.org.tw/www/zh-tw/afterTrading/oddQuote"
TPEX_OL_AH_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/odd"


def _to_roc_date(d: date) -> str:
    return f"{d.year - 1911}/{d.strftime('%m/%d')}"


def _post(url: str, data: dict) -> dict | None:
    """POST with rate limiting and retry."""
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
            return None
        except Exception as e:
            print(f"  Attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(BACKOFF_BASE * (2 ** attempt) + random.uniform(1, 3))
    return None


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

def _parse_twse_ol(rows: list, fields: list) -> list:
    """
    TWTC7U fields: 證券代號, 證券名稱, 成交股數, 成交筆數, 成交金額,
                   成交均價, 最後揭示買價, ..., 最後揭示賣價, ...
    """
    results = []
    for row in rows:
        try:
            record = dict(zip(fields, row))
            stock_id = record.get("證券代號", "").strip()
            if not stock_id or not classify_tw_security(stock_id):
                continue
            volume = _parse_num(record.get("成交股數"))
            if not volume:
                continue
            results.append({
                "stock_id": stock_id,
                "ol_price":    _parse_num(record.get("成交均價")),
                "ol_volume":   int(volume),
                "ol_turnover": int(_parse_num(record.get("成交金額")) or 0),
                "ol_tx_count": int(_parse_num(record.get("成交筆數")) or 0),
            })
        except (ValueError, TypeError):
            continue
    return results


def _parse_twse_ol_ah(rows: list, fields: list) -> list:
    """
    TWT53U fields: 證券代號, 證券名稱, 成交股數, 成交筆數, 成交金額,
                   成交價格, 最後揭示買價, ...
    """
    results = []
    for row in rows:
        try:
            record = dict(zip(fields, row))
            stock_id = record.get("證券代號", "").strip()
            if not stock_id or not classify_tw_security(stock_id):
                continue
            volume = _parse_num(record.get("成交股數"))
            if not volume:
                continue
            results.append({
                "stock_id":      stock_id,
                "ol_ah_price":    _parse_num(record.get("成交價格")),
                "ol_ah_volume":   int(volume),
                "ol_ah_turnover": int(_parse_num(record.get("成交金額")) or 0),
                "ol_ah_tx_count": int(_parse_num(record.get("成交筆數")) or 0),
            })
        except (ValueError, TypeError):
            continue
    return results


def fetch_twse_odd_lot(trade_date: date) -> tuple[list, list]:
    """Fetch TWSE odd-lot regular + after-hours for a date."""
    date_str = trade_date.strftime("%Y%m%d")
    params = {"date": date_str, "selectType": "ALL", "response": "json"}

    print(f"Fetching TWSE odd-lot (regular) for {trade_date} ...")
    d1 = fetch_json(TWSE_OL_URL, params=params)
    ol = []
    if d1 and d1.get("stat") == "OK" and d1.get("data"):
        print(f"  Found {len(d1['data'])} records.")
        ol = _parse_twse_ol(d1["data"], d1.get("fields", []))

    print(f"Fetching TWSE odd-lot (after-hours) for {trade_date} ...")
    d2 = fetch_json(TWSE_OL_AH_URL, params=params)
    ol_ah = []
    if d2 and d2.get("stat") == "OK" and d2.get("data"):
        print(f"  Found {len(d2['data'])} records.")
        ol_ah = _parse_twse_ol_ah(d2["data"], d2.get("fields", []))

    return ol, ol_ah


# ---------------------------------------------------------------------------
# TPEx
# ---------------------------------------------------------------------------

def _parse_tpex_ol(rows: list) -> list:
    """
    oddQuote fields (by index):
    [0] code, [1] name, [2] close, [3] change, [4] open, [5] high, [6] low,
    [7] volume, [8] turnover, [9] tx_count, ...
    """
    results = []
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
        except (ValueError, TypeError, IndexError):
            continue
    return results


def _parse_tpex_ol_ah(rows: list) -> list:
    """
    odd fields (by index):
    [0] code, [1] name, [2] volume, [3] tx_count, [4] turnover, [5] price, ...
    """
    results = []
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
        except (ValueError, TypeError, IndexError):
            continue
    return results


def fetch_tpex_odd_lot(trade_date: date) -> tuple[list, list]:
    """Fetch TPEx odd-lot regular + after-hours for a date."""
    roc = _to_roc_date(trade_date)

    print(f"Fetching TPEx odd-lot (regular) for {trade_date} ...")
    d1 = _post(TPEX_OL_URL, {"d": roc})
    ol = []
    if d1 and d1.get("tables"):
        rows = d1["tables"][0].get("data", [])
        print(f"  Found {len(rows)} records.")
        ol = _parse_tpex_ol(rows)

    print(f"Fetching TPEx odd-lot (after-hours) for {trade_date} ...")
    d2 = _post(TPEX_OL_AH_URL, {"date": roc, "type": "Daily"})
    ol_ah = []
    if d2 and d2.get("tables"):
        rows = d2["tables"][0].get("data", [])
        print(f"  Found {len(rows)} records.")
        ol_ah = _parse_tpex_ol_ah(rows)

    return ol, ol_ah


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

def _upsert_tpex_stocks(cur, records: list):
    """Ensure TPEx stocks exist before upserting prices."""
    for r in records:
        if r.get("name") and r.get("security_type"):
            cur.execute("""
                INSERT INTO tw.stocks (stock_id, name, market, security_type)
                VALUES (%s, %s, 'TPEx', %s)
                ON CONFLICT (stock_id) DO UPDATE SET name = EXCLUDED.name, updated_at = NOW()
            """, (r["stock_id"], r["name"], r["security_type"]))


def save_odd_lot(
    twse_ol: list, twse_ol_ah: list,
    tpex_ol: list, tpex_ol_ah: list,
    trade_date: date,
):
    """Upsert all odd-lot data into tw.daily_prices."""
    with get_cursor() as cur:
        _upsert_tpex_stocks(cur, tpex_ol)
        _upsert_tpex_stocks(cur, tpex_ol_ah)

        for r in twse_ol + tpex_ol:
            cur.execute("""
                INSERT INTO tw.daily_prices (stock_id, trade_date, ol_price, ol_volume, ol_turnover, ol_tx_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_id, trade_date)
                DO UPDATE SET
                    ol_price    = EXCLUDED.ol_price,
                    ol_volume   = EXCLUDED.ol_volume,
                    ol_turnover = EXCLUDED.ol_turnover,
                    ol_tx_count = EXCLUDED.ol_tx_count
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
                    ol_ah_price    = EXCLUDED.ol_ah_price,
                    ol_ah_volume   = EXCLUDED.ol_ah_volume,
                    ol_ah_turnover = EXCLUDED.ol_ah_turnover,
                    ol_ah_tx_count = EXCLUDED.ol_ah_tx_count
            """, (
                r["stock_id"], trade_date,
                r.get("ol_ah_price"), r.get("ol_ah_volume"),
                r.get("ol_ah_turnover"), r.get("ol_ah_tx_count"),
            ))

    total = len(twse_ol) + len(twse_ol_ah) + len(tpex_ol) + len(tpex_ol_ah)
    print(f"Saved odd-lot: TWSE regular={len(twse_ol)}, TWSE after={len(twse_ol_ah)}, "
          f"TPEx regular={len(tpex_ol)}, TPEx after={len(tpex_ol_ah)} (total={total})")


def scrape_date(trade_date: date):
    """Fetch and save all odd-lot data for a single date."""
    twse_ol, twse_ol_ah = fetch_twse_odd_lot(trade_date)
    tpex_ol, tpex_ol_ah = fetch_tpex_odd_lot(trade_date)
    save_odd_lot(twse_ol, twse_ol_ah, tpex_ol, tpex_ol_ah, trade_date)


def scrape_date_range(start_date: date, end_date: date):
    """Fetch and save odd-lot data for a date range (skips weekends)."""
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