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
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json_retry

TWSE_URL = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
TPEX_URL = "https://www.tpex.org.tw/www/zh-tw/margin/balance"


def _to_ad_date(d: date) -> str:
    return d.strftime("%Y/%m/%d")


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


def _parse_twse_margin(rows: list) -> tuple:
    """Returns (records, parse_errors)."""
    results = []
    errors = 0
    for row in rows:
        try:
            stock_id = str(row[0]).strip()
            security_type = classify_tw_security(stock_id)
            if not stock_id or not security_type:
                continue
            results.append({
                "stock_id":            stock_id,
                "name":                str(row[1]).strip(),
                "security_type":       security_type,
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
        except (IndexError, ValueError, TypeError) as e:
            print(f"  Skipping TWSE margin row: {e}")
            errors += 1
            continue
    return results, errors


def _parse_margin_summary(rows: list) -> dict | None:
    """
    Parse tables[0] '信用交易統計' into a summary dict.
    Rows: 融資(交易單位), 融券(交易單位), 融資金額(仟元).
    Fields: [1]=買進, [2]=賣出, [3]=現金(券)償還, [4]=前日餘額, [5]=今日餘額.
    """
    result = {}
    for row in rows:
        label = str(row[0]).strip()
        vals = [_parse_int(row[i]) for i in range(1, 6)]
        if "融資" in label and "金額" not in label:
            result["margin_buy"], result["margin_sell"], result["margin_repay"] = vals[0], vals[1], vals[2]
            result["margin_prev_balance"], result["margin_balance"] = vals[3], vals[4]
        elif "融券" in label:
            result["short_buy"], result["short_sell"], result["short_repay"] = vals[0], vals[1], vals[2]
            result["short_prev_balance"], result["short_balance"] = vals[3], vals[4]
        elif "融資" in label and "金額" in label:
            result["margin_buy_value"], result["margin_sell_value"], result["margin_repay_value"] = vals[0], vals[1], vals[2]
            result["margin_prev_value"], result["margin_balance_value"] = vals[3], vals[4]
    return result if result else None


def fetch_twse_margin(trade_date: date) -> tuple:
    """Returns (records, summary, api_rows, parse_errors)."""
    date_str = trade_date.strftime("%Y%m%d")
    print(f"Fetching TWSE margin trading for {trade_date} ...")
    data = fetch_json_retry(TWSE_URL, params={"date": date_str, "selectType": "ALL", "response": "json"},
                            validate=lambda d: d.get("stat") == "OK")

    if not data or data.get("stat") != "OK":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return [], None, 0, 0

    # Verify the API returned data for the requested date.
    api_date = str(data.get("date", "")).strip()
    if api_date and api_date != date_str:
        print(f"  Date mismatch: requested {date_str}, API returned {api_date} — skipping")
        return [], None, 0, 0

    tables = data.get("tables", [])
    if len(tables) < 2:
        print("  Unexpected response structure.")
        return [], None, 0, 0

    # tables[0] = market-wide summary, tables[1] = per-stock data
    summary = _parse_margin_summary(tables[0].get("data", []))

    rows = tables[1].get("data", [])
    print(f"  Found {len(rows)} records.")
    records, errors = _parse_twse_margin(rows)
    return records, summary, len(rows), errors


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

def _parse_tpex_margin(rows: list) -> tuple:
    """Returns (records, parse_errors)."""
    results = []
    errors = 0
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
        except (IndexError, ValueError, TypeError) as e:
            print(f"  Skipping TPEx margin row: {e}")
            errors += 1
            continue
    return results, errors


def fetch_tpex_margin(trade_date: date) -> tuple:
    """Returns (records, api_rows, parse_errors)."""
    ad = _to_ad_date(trade_date)
    print(f"Fetching TPEx margin trading for {trade_date} ...")
    data = fetch_json_retry(TPEX_URL, params={"date": ad, "s": "0,asc", "response": "json"},
                            validate=lambda d: d.get("stat") == "ok")

    if not data or data.get("stat") != "ok":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return [], 0, 0

    # Verify the API returned data for the requested date.
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
    records, errors = _parse_tpex_margin(rows)
    return records, len(rows), errors


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


def save_margin_summary(summary: dict, trade_date: date):
    """Upsert market-wide margin summary into tw.margin_summary."""
    if not summary:
        return
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO tw.margin_summary (
                trade_date,
                margin_buy, margin_sell, margin_repay,
                margin_prev_balance, margin_balance,
                short_buy, short_sell, short_repay,
                short_prev_balance, short_balance,
                margin_buy_value, margin_sell_value, margin_repay_value,
                margin_prev_value, margin_balance_value
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_date) DO UPDATE SET
                margin_buy          = COALESCE(EXCLUDED.margin_buy,          tw.margin_summary.margin_buy),
                margin_sell         = COALESCE(EXCLUDED.margin_sell,         tw.margin_summary.margin_sell),
                margin_repay        = COALESCE(EXCLUDED.margin_repay,        tw.margin_summary.margin_repay),
                margin_prev_balance = COALESCE(EXCLUDED.margin_prev_balance, tw.margin_summary.margin_prev_balance),
                margin_balance      = COALESCE(EXCLUDED.margin_balance,      tw.margin_summary.margin_balance),
                short_buy           = COALESCE(EXCLUDED.short_buy,           tw.margin_summary.short_buy),
                short_sell          = COALESCE(EXCLUDED.short_sell,          tw.margin_summary.short_sell),
                short_repay         = COALESCE(EXCLUDED.short_repay,         tw.margin_summary.short_repay),
                short_prev_balance  = COALESCE(EXCLUDED.short_prev_balance,  tw.margin_summary.short_prev_balance),
                short_balance       = COALESCE(EXCLUDED.short_balance,       tw.margin_summary.short_balance),
                margin_buy_value    = COALESCE(EXCLUDED.margin_buy_value,    tw.margin_summary.margin_buy_value),
                margin_sell_value   = COALESCE(EXCLUDED.margin_sell_value,   tw.margin_summary.margin_sell_value),
                margin_repay_value  = COALESCE(EXCLUDED.margin_repay_value,  tw.margin_summary.margin_repay_value),
                margin_prev_value   = COALESCE(EXCLUDED.margin_prev_value,   tw.margin_summary.margin_prev_value),
                margin_balance_value= COALESCE(EXCLUDED.margin_balance_value,tw.margin_summary.margin_balance_value)
        """, (
            trade_date,
            summary.get("margin_buy"),    summary.get("margin_sell"),    summary.get("margin_repay"),
            summary.get("margin_prev_balance"), summary.get("margin_balance"),
            summary.get("short_buy"),     summary.get("short_sell"),     summary.get("short_repay"),
            summary.get("short_prev_balance"),  summary.get("short_balance"),
            summary.get("margin_buy_value"),    summary.get("margin_sell_value"),    summary.get("margin_repay_value"),
            summary.get("margin_prev_value"),   summary.get("margin_balance_value"),
        ))
    print(f"  Saved margin summary: balance={summary.get('margin_balance')} lots, "
          f"short={summary.get('short_balance')} lots")


def save_margin(twse: list, tpex: list, trade_date: date):
    """Upsert margin trading data into tw.daily_prices."""
    with get_cursor() as cur:
        _upsert_stocks(cur, twse, "TWSE")
        _upsert_stocks(cur, tpex, "TPEx")

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
                    margin_buy          = COALESCE(EXCLUDED.margin_buy,          tw.daily_prices.margin_buy),
                    margin_sell         = COALESCE(EXCLUDED.margin_sell,         tw.daily_prices.margin_sell),
                    margin_cash_repay   = COALESCE(EXCLUDED.margin_cash_repay,   tw.daily_prices.margin_cash_repay),
                    margin_prev_balance = COALESCE(EXCLUDED.margin_prev_balance, tw.daily_prices.margin_prev_balance),
                    margin_balance      = COALESCE(EXCLUDED.margin_balance,      tw.daily_prices.margin_balance),
                    short_sell          = COALESCE(EXCLUDED.short_sell,          tw.daily_prices.short_sell),
                    short_buy           = COALESCE(EXCLUDED.short_buy,           tw.daily_prices.short_buy),
                    short_repay         = COALESCE(EXCLUDED.short_repay,         tw.daily_prices.short_repay),
                    short_prev_balance  = COALESCE(EXCLUDED.short_prev_balance,  tw.daily_prices.short_prev_balance),
                    short_balance       = COALESCE(EXCLUDED.short_balance,       tw.daily_prices.short_balance),
                    margin_short_offset = COALESCE(EXCLUDED.margin_short_offset, tw.daily_prices.margin_short_offset)
            """, (
                r["stock_id"], trade_date,
                r.get("margin_buy"),    r.get("margin_sell"),    r.get("margin_cash_repay"),
                r.get("margin_prev_balance"), r.get("margin_balance"),
                r.get("short_sell"),    r.get("short_buy"),      r.get("short_repay"),
                r.get("short_prev_balance"),  r.get("short_balance"),
                r.get("margin_short_offset"),
            ))

    print(f"Saved margin: TWSE={len(twse)}, TPEx={len(tpex)} (total={len(twse)+len(tpex)})")


def scrape_date(trade_date: date) -> ScrapeResult:
    """Fetch and save all margin trading data for a single date."""
    twse, summary, twse_api, twse_err = fetch_twse_margin(trade_date)
    tpex, tpex_api, tpex_err = fetch_tpex_margin(trade_date)
    save_margin(twse, tpex, trade_date)
    save_margin_summary(summary, trade_date)
    return ScrapeResult(
        records=len(twse) + len(tpex),
        api_rows=twse_api + tpex_api,
        parse_errors=twse_err + tpex_err,
    )


def scrape_date_range(start_date: date, end_date: date):
    """Fetch and save margin trading data for a date range (skips weekends)."""
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
