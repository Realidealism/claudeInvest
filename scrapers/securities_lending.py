"""
Securities Borrowing and Lending (SBL / 借券賣出) per-stock daily scraper.

Sources:
  TWSE: rwd/zh/marginTrading/TWT93U (GET, date=YYYYMMDD, response=json)
        Fields: 15 cols. [0-1] stock, [2-7] 融券, [8-13] 借券, [14] note.
  TPEx: www/zh-tw/margin/sbl (GET, date=YYYY/MM/DD, response=json)
        tables[0].data has identical 15-col structure.

Only fields [8-13] (借券) are stored — 融券 is already handled by margin.py.
All values are in shares (股), not lots (張).
"""

from datetime import date, timedelta

from db.connection import get_cursor
from utils.classifier import classify_tw_security
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json_retry

TWSE_URL = "https://www.twse.com.tw/rwd/zh/marginTrading/TWT93U"
TPEX_URL = "https://www.tpex.org.tw/www/zh-tw/margin/sbl"


def _parse_int(val) -> int | None:
    s = str(val).replace(",", "").strip()
    if not s or s in ("--", "---", " ", ""):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# TWSE
# ---------------------------------------------------------------------------
# Fields (15 cols):
# [0] 代號, [1] 名稱,
# [2] 融券前日餘額, [3] 融券賣出, [4] 融券買進, [5] 現券, [6] 融券今日餘額, [7] 融券次一營業日限額,
# [8] 借券前日餘額, [9] 借券當日賣出, [10] 借券當日還券, [11] 借券當日調整, [12] 借券當日餘額, [13] 借券次一營業日可限額,
# [14] 備註

def _parse_sbl_rows(rows: list, source: str) -> tuple[list, int]:
    """Extract borrow-lending (fields 8-13) from the shared 15-col table."""
    results = []
    errors = 0
    for row in rows:
        try:
            stock_id = str(row[0]).strip()
            if not stock_id or not classify_tw_security(stock_id):
                continue
            results.append({
                "stock_id":         stock_id,
                "name":             str(row[1]).strip(),
                "sbl_prev_balance": _parse_int(row[8]),
                "sbl_sell":         _parse_int(row[9]),
                "sbl_return":       _parse_int(row[10]),
                "sbl_adjust":       _parse_int(row[11]),
                "sbl_balance":      _parse_int(row[12]),
                "sbl_limit":        _parse_int(row[13]),
            })
        except (IndexError, ValueError, TypeError) as e:
            print(f"  Skipping {source} SBL row: {e}")
            errors += 1
    return results, errors


def fetch_twse_sbl(trade_date: date) -> tuple[list, int, int]:
    """Returns (records, api_rows, parse_errors)."""
    date_str = trade_date.strftime("%Y%m%d")
    print(f"Fetching TWSE SBL for {trade_date} ...")
    data = fetch_json_retry(TWSE_URL, params={"date": date_str, "response": "json"},
                            validate=lambda d: d.get("stat") == "OK")
    if not data or data.get("stat") != "OK":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return [], 0, 0

    api_date = str(data.get("date", "")).strip()
    if api_date and api_date != date_str:
        print(f"  Date mismatch: requested {date_str}, API returned {api_date} — skipping")
        return [], 0, 0

    rows = data.get("data", [])
    print(f"  Found {len(rows)} records.")
    records, errors = _parse_sbl_rows(rows, "TWSE")
    return records, len(rows), errors


# ---------------------------------------------------------------------------
# TPEx
# ---------------------------------------------------------------------------

def fetch_tpex_sbl(trade_date: date) -> tuple[list, int, int]:
    """Returns (records, api_rows, parse_errors)."""
    ad = trade_date.strftime("%Y/%m/%d")
    print(f"Fetching TPEx SBL for {trade_date} ...")
    data = fetch_json_retry(TPEX_URL, params={"date": ad, "response": "json"},
                            validate=lambda d: d.get("stat") == "ok")
    if not data or data.get("stat") != "ok":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return [], 0, 0

    api_date = str(data.get("date", "")).strip()
    expected = trade_date.strftime("%Y%m%d")
    if api_date and api_date != expected:
        print(f"  Date mismatch: requested {expected}, API returned {api_date} — skipping")
        return [], 0, 0

    tables = data.get("tables", [])
    if not tables:
        print("  Unexpected response structure.")
        return [], 0, 0

    rows = tables[0].get("data", [])
    print(f"  Found {len(rows)} records.")
    records, errors = _parse_sbl_rows(rows, "TPEx")
    return records, len(rows), errors


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

def save_sbl(twse: list, tpex: list, trade_date: date):
    """Upsert SBL data into tw.daily_prices using COALESCE to preserve other columns."""
    with get_cursor() as cur:
        for r in twse + tpex:
            cur.execute("""
                INSERT INTO tw.daily_prices (
                    stock_id, trade_date,
                    sbl_prev_balance, sbl_sell, sbl_return,
                    sbl_adjust, sbl_balance, sbl_limit
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_id, trade_date) DO UPDATE SET
                    sbl_prev_balance = COALESCE(EXCLUDED.sbl_prev_balance, tw.daily_prices.sbl_prev_balance),
                    sbl_sell         = COALESCE(EXCLUDED.sbl_sell,         tw.daily_prices.sbl_sell),
                    sbl_return       = COALESCE(EXCLUDED.sbl_return,       tw.daily_prices.sbl_return),
                    sbl_adjust       = COALESCE(EXCLUDED.sbl_adjust,       tw.daily_prices.sbl_adjust),
                    sbl_balance      = COALESCE(EXCLUDED.sbl_balance,      tw.daily_prices.sbl_balance),
                    sbl_limit        = COALESCE(EXCLUDED.sbl_limit,        tw.daily_prices.sbl_limit)
            """, (
                r["stock_id"], trade_date,
                r.get("sbl_prev_balance"), r.get("sbl_sell"), r.get("sbl_return"),
                r.get("sbl_adjust"), r.get("sbl_balance"), r.get("sbl_limit"),
            ))
    print(f"Saved SBL: TWSE={len(twse)}, TPEx={len(tpex)} (total={len(twse)+len(tpex)})")


def scrape_date(trade_date: date) -> ScrapeResult:
    """Fetch and save SBL data for a single trading date."""
    twse, twse_api, twse_err = fetch_twse_sbl(trade_date)
    tpex, tpex_api, tpex_err = fetch_tpex_sbl(trade_date)
    save_sbl(twse, tpex, trade_date)
    return ScrapeResult(
        records=len(twse) + len(tpex),
        api_rows=twse_api + tpex_api,
        parse_errors=twse_err + tpex_err,
    )


def scrape_date_range(start_date: date, end_date: date):
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
