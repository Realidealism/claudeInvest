"""
Day trading (當日沖銷) per-stock and market-level scraper.

Sources:
  TWSE: rwd/zh/dayTrading/TWTB4U (GET, date=YYYYMMDD, response=json)
        tables[0] = market summary (1 row, 6 cols)
        tables[1] = per-stock (代號, 名稱, 暫停註記, 成交股數, 買進金額, 賣出金額)
  TPEx: www/zh-tw/intraday/stat (POST, type=Daily&date=YYYY/MM/DD&id=&response=json)
        tables[0] = market summary, tables[1] = per-stock (same 6 cols)

All amounts are in NTD, volumes in shares (股).
"""

from datetime import date

from db.connection import get_cursor
from utils.classifier import classify_tw_security
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json_retry, post_json_retry

TWSE_URL = "https://www.twse.com.tw/rwd/zh/dayTrading/TWTB4U"
TPEX_URL = "https://www.tpex.org.tw/www/zh-tw/intraday/stat"


def _parse_int(val) -> int | None:
    s = str(val).replace(",", "").replace("%", "").strip()
    if not s or s in ("--", "---", " ", ""):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# TWSE
# ---------------------------------------------------------------------------

def fetch_twse(trade_date: date) -> tuple[dict | None, list, int, int]:
    """Returns (market_summary, per_stock_records, api_rows, parse_errors)."""
    date_str = trade_date.strftime("%Y%m%d")
    print(f"Fetching TWSE day trading for {trade_date} ...")
    data = fetch_json_retry(
        TWSE_URL,
        params={"date": date_str, "response": "json"},
        validate=lambda d: d.get("stat") == "OK" and d.get("tables"),
    )
    if not data or data.get("stat") != "OK" or not data.get("tables"):
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return None, [], 0, 0

    tables = data["tables"]

    # Table 0: market summary
    summary = None
    if tables and tables[0].get("data"):
        row = tables[0]["data"][0]
        summary = {
            "dt_volume":     _parse_int(row[0]),
            "dt_buy_amount": _parse_int(row[2]),
            "dt_sell_amount": _parse_int(row[4]),
        }

    # Table 1: per-stock
    records = []
    errors = 0
    if len(tables) > 1:
        rows = tables[1].get("data", [])
        print(f"  Found {len(rows)} records.")
        for row in rows:
            try:
                stock_id = str(row[0]).strip()
                if not stock_id or not classify_tw_security(stock_id):
                    continue
                records.append({
                    "stock_id":       stock_id,
                    "dt_volume":      _parse_int(row[3]),
                    "dt_buy_amount":  _parse_int(row[4]),
                    "dt_sell_amount": _parse_int(row[5]),
                })
            except (IndexError, ValueError, TypeError) as e:
                print(f"  Skipping TWSE day-trade row: {e}")
                errors += 1
        return summary, records, len(rows), errors

    return summary, records, 0, errors


# ---------------------------------------------------------------------------
# TPEx
# ---------------------------------------------------------------------------

def fetch_tpex(trade_date: date) -> tuple[dict | None, list, int, int]:
    """Returns (market_summary, per_stock_records, api_rows, parse_errors)."""
    ad = trade_date.strftime("%Y/%m/%d")
    print(f"Fetching TPEx day trading for {trade_date} ...")
    data = post_json_retry(
        TPEX_URL,
        data={"type": "Daily", "date": ad, "id": "", "response": "json"},
        validate=lambda d: d.get("tables") and len(d["tables"]) > 1,
    )
    if not data or not data.get("tables") or len(data["tables"]) < 2:
        print(f"  No data")
        return None, [], 0, 0

    tables = data["tables"]

    # Table 0: market summary
    summary = None
    if tables[0].get("data"):
        row = tables[0]["data"][0]
        summary = {
            "dt_volume":     _parse_int(row[0]),
            "dt_buy_amount": _parse_int(row[2]),
            "dt_sell_amount": _parse_int(row[4]),
        }

    # Table 1: per-stock
    records = []
    errors = 0
    rows = tables[1].get("data", [])
    print(f"  Found {len(rows)} records.")
    for row in rows:
        try:
            stock_id = str(row[0]).strip()
            if not stock_id or not classify_tw_security(stock_id):
                continue
            records.append({
                "stock_id":       stock_id,
                "dt_volume":      _parse_int(row[3]),
                "dt_buy_amount":  _parse_int(row[4]),
                "dt_sell_amount": _parse_int(row[5]),
            })
        except (IndexError, ValueError, TypeError) as e:
            print(f"  Skipping TPEx day-trade row: {e}")
            errors += 1

    return summary, records, len(rows), errors


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

def _save_market_summary(trade_date: date, twse_summary: dict | None, tpex_summary: dict | None):
    """Upsert market-level day trading totals into tw.index_prices."""
    with get_cursor() as cur:
        for index_id, summary in [("TAIEX", twse_summary), ("TPEx", tpex_summary)]:
            if not summary:
                continue
            cur.execute("""
                UPDATE tw.index_prices
                SET dt_volume = %s, dt_buy_amount = %s, dt_sell_amount = %s
                WHERE index_id = %s AND trade_date = %s
            """, (
                summary["dt_volume"], summary["dt_buy_amount"], summary["dt_sell_amount"],
                index_id, trade_date,
            ))
            if cur.rowcount:
                print(f"  [day-trade] {index_id} market summary saved")


def _save_per_stock(trade_date: date, records: list):
    """Upsert per-stock day trading into tw.daily_prices."""
    with get_cursor() as cur:
        for r in records:
            cur.execute("""
                INSERT INTO tw.daily_prices (stock_id, trade_date, dt_volume, dt_buy_amount, dt_sell_amount)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (stock_id, trade_date) DO UPDATE SET
                    dt_volume     = COALESCE(EXCLUDED.dt_volume,     tw.daily_prices.dt_volume),
                    dt_buy_amount = COALESCE(EXCLUDED.dt_buy_amount, tw.daily_prices.dt_buy_amount),
                    dt_sell_amount = COALESCE(EXCLUDED.dt_sell_amount, tw.daily_prices.dt_sell_amount)
            """, (
                r["stock_id"], trade_date,
                r["dt_volume"], r["dt_buy_amount"], r["dt_sell_amount"],
            ))


def scrape_date(trade_date: date) -> ScrapeResult:
    """Fetch and save day trading data for a single trading date."""
    twse_summary, twse_records, twse_api, twse_err = fetch_twse(trade_date)
    tpex_summary, tpex_records, tpex_api, tpex_err = fetch_tpex(trade_date)

    _save_market_summary(trade_date, twse_summary, tpex_summary)
    _save_per_stock(trade_date, twse_records + tpex_records)

    total = len(twse_records) + len(tpex_records)
    print(f"Saved day trading: TWSE={len(twse_records)}, TPEx={len(tpex_records)} (total={total})")

    return ScrapeResult(
        records=total,
        api_rows=twse_api + tpex_api,
        parse_errors=twse_err + tpex_err,
    )


if __name__ == "__main__":
    import sys
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) >= 2 else date.today()
    scrape_date(d)
