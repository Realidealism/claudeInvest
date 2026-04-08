"""
Margin trading data scraper (融資融券).

Sources:
  TWSE: rwd/zh/marginTrading/MI_MARGN (GET, selectType=ALL)
        tables[1] = per-stock margin/short data
  TPEx: /www/zh-tw/margin/balance (GET, d=YYY/MM/DD&s=0,asc&o=json)
        tables[0] = per-stock margin/short data
"""

from datetime import date, timedelta

from db.connection import get_cursor
from utils.classifier import classify_tw_security
from utils.http_client import fetch_json

TWSE_URL = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
TPEX_URL = "https://www.tpex.org.tw/www/zh-tw/margin/balance"


def _to_roc_date(d: date) -> str:
    return f"{d.year - 1911}/{d.strftime('%m/%d')}"


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
# tables[1] fields (by index):
# [0] 代號, [1] 名稱,
# [2] 融資買進, [3] 融資賣出, [4] 現金償還, [5] 融資前日餘額, [6] 融資今日餘額, [7] 次一營業日限額,
# [8] 融券買進, [9] 融券賣出, [10] 現券償還, [11] 融券前日餘額, [12] 融券今日餘額, [13] 次一營業日限額,
# [14] 資券互抵, [15] 註記
# All volume fields are in lots (張); multiply by 1000 to convert to shares (股).

def _lots_to_shares(val) -> int | None:
    n = _parse_int(val)
    return n * 1000 if n is not None else None


def _parse_twse_margin(rows: list) -> list:
    results = []
    for row in rows:
        try:
            stock_id = str(row[0]).strip()
            if not stock_id or not classify_tw_security(stock_id):
                continue
            results.append({
                "stock_id":            stock_id,
                "margin_buy":          _lots_to_shares(row[2]),
                "margin_sell":         _lots_to_shares(row[3]),
                "margin_cash_repay":   _lots_to_shares(row[4]),
                "margin_prev_balance": _lots_to_shares(row[5]),
                "margin_balance":      _lots_to_shares(row[6]),
                "short_buy":           _lots_to_shares(row[8]),
                "short_sell":          _lots_to_shares(row[9]),
                "short_repay":         _lots_to_shares(row[10]),
                "short_prev_balance":  _lots_to_shares(row[11]),
                "short_balance":       _lots_to_shares(row[12]),
                "margin_short_offset": _lots_to_shares(row[14]),
            })
        except (IndexError, ValueError, TypeError):
            continue
    return results


def fetch_twse_margin(trade_date: date) -> list:
    date_str = trade_date.strftime("%Y%m%d")
    print(f"Fetching TWSE margin trading for {trade_date} ...")
    data = fetch_json(TWSE_URL, params={"date": date_str, "selectType": "ALL", "response": "json"})

    if not data or data.get("stat") != "OK":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return []

    tables = data.get("tables", [])
    if len(tables) < 2:
        print("  Unexpected response structure.")
        return []

    rows = tables[1].get("data", [])
    print(f"  Found {len(rows)} records.")
    return _parse_twse_margin(rows)


# ---------------------------------------------------------------------------
# TPEx
# ---------------------------------------------------------------------------
# tables[0] fields (by index):
# [0] 代號, [1] 名稱,
# [2] 前資餘額(張), [3] 資買, [4] 資賣, [5] 現償, [6] 資餘額,
# [7] 資屬證金, [8] 資使用率(%), [9] 資限額,
# [10] 前券餘額(張), [11] 券賣, [12] 券買, [13] 券償, [14] 券餘額,
# [15] 券屬證金, [16] 券使用率(%), [17] 券限額,
# [18] 資券相抵(張), [19] 備註
# All volume fields are in lots (張); multiply by 1000 to convert to shares (股).

def _parse_tpex_margin(rows: list) -> list:
    results = []
    for row in rows:
        try:
            stock_id = str(row[0]).strip()
            if not stock_id or not classify_tw_security(stock_id):
                continue
            results.append({
                "stock_id":            stock_id,
                "name":                str(row[1]).strip(),
                "security_type":       classify_tw_security(stock_id),
                "margin_prev_balance": _lots_to_shares(row[2]),
                "margin_buy":          _lots_to_shares(row[3]),
                "margin_sell":         _lots_to_shares(row[4]),
                "margin_cash_repay":   _lots_to_shares(row[5]),
                "margin_balance":      _lots_to_shares(row[6]),
                "short_prev_balance":  _lots_to_shares(row[10]),
                "short_sell":          _lots_to_shares(row[11]),
                "short_buy":           _lots_to_shares(row[12]),
                "short_repay":         _lots_to_shares(row[13]),
                "short_balance":       _lots_to_shares(row[14]),
                "margin_short_offset": _lots_to_shares(row[18]),
            })
        except (IndexError, ValueError, TypeError):
            continue
    return results


def fetch_tpex_margin(trade_date: date) -> list:
    roc = _to_roc_date(trade_date)
    print(f"Fetching TPEx margin trading for {trade_date} ...")
    data = fetch_json(TPEX_URL, params={"d": roc, "s": "0,asc", "o": "json"})

    if not data or data.get("stat") != "ok":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return []

    tables = data.get("tables", [])
    if not tables:
        print("  Unexpected response structure.")
        return []

    rows = tables[0].get("data", [])
    print(f"  Found {len(rows)} records.")
    return _parse_tpex_margin(rows)


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


def save_margin(twse: list, tpex: list, trade_date: date):
    """Upsert margin trading data into tw.daily_prices."""
    with get_cursor() as cur:
        _upsert_tpex_stocks(cur, tpex)

        for r in twse + tpex:
            cur.execute("""
                INSERT INTO tw.daily_prices (
                    stock_id, trade_date,
                    margin_buy, margin_sell, margin_cash_repay,
                    margin_prev_balance, margin_balance,
                    short_sell, short_buy, short_repay,
                    short_prev_balance, short_balance,
                    margin_short_offset
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_id, trade_date) DO UPDATE SET
                    margin_buy          = EXCLUDED.margin_buy,
                    margin_sell         = EXCLUDED.margin_sell,
                    margin_cash_repay   = EXCLUDED.margin_cash_repay,
                    margin_prev_balance = EXCLUDED.margin_prev_balance,
                    margin_balance      = EXCLUDED.margin_balance,
                    short_sell          = EXCLUDED.short_sell,
                    short_buy           = EXCLUDED.short_buy,
                    short_repay         = EXCLUDED.short_repay,
                    short_prev_balance  = EXCLUDED.short_prev_balance,
                    short_balance       = EXCLUDED.short_balance,
                    margin_short_offset = EXCLUDED.margin_short_offset
            """, (
                r["stock_id"], trade_date,
                r.get("margin_buy"),    r.get("margin_sell"),    r.get("margin_cash_repay"),
                r.get("margin_prev_balance"), r.get("margin_balance"),
                r.get("short_sell"),    r.get("short_buy"),      r.get("short_repay"),
                r.get("short_prev_balance"),  r.get("short_balance"),
                r.get("margin_short_offset"),
            ))

    print(f"Saved margin: TWSE={len(twse)}, TPEx={len(tpex)} (total={len(twse)+len(tpex)})")


def scrape_date(trade_date: date):
    """Fetch and save all margin trading data for a single date."""
    twse = fetch_twse_margin(trade_date)
    tpex = fetch_tpex_margin(trade_date)
    save_margin(twse, tpex, trade_date)


def scrape_date_range(start_date: date, end_date: date):
    """Fetch and save margin trading data for a date range (skips weekends)."""
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