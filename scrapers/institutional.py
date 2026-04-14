"""
Institutional investors (三大法人) daily buy/sell data scraper.

Sources:
  TWSE: rwd/zh/fund/T86 (GET, selectType=ALL)
    [2-4]   外陸資(不含外資自營商) buy/sell/net
    [5-7]   外資自營商 buy/sell/net
    [8-10]  投信 buy/sell/net
    [11]    自營商買賣超合計
    [12-14] 自營商(自行) buy/sell/net
    [15-17] 自營商(避險) buy/sell/net
    [18]    三大法人買賣超合計

  TPEx: /www/zh-tw/insti/dailyTrade (GET, date=YYYYMMDD&type=Daily)
    [2-4]   外資(不含外資自營商) buy/sell/net
    [5-7]   外資自營商 buy/sell/net
    [8-10]  外陸資合計 buy/sell/net
    [11-13] 投信 buy/sell/net
    [14-16] 自營商(自行) buy/sell/net
    [17-19] 自營商(避險) buy/sell/net
    [20-22] 自營商合計 buy/sell/net
    [23]    三大法人買賣超合計

All values are in shares (股).
"""

from datetime import date, timedelta

from db.connection import get_cursor
from utils.classifier import classify_tw_security
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json_retry

TWSE_URL = "https://www.twse.com.tw/rwd/zh/fund/T86"
TPEX_URL = "https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade"


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
# 2018+  (19 cols): 外陸資(不含自營) + 外資自營商 separated, then 投信, 自營商合計, 自行, 避險, 三大法人
# 2017-  (16 cols): 外資 combined (no 自營商 split), then 投信, 自營商合計, 自行, 避險, 三大法人

def _find_col(fields, *keywords):
    """Find column index where field name contains all keywords. Returns -1 if not found."""
    for i, f in enumerate(fields):
        if all(k in f for k in keywords):
            return i
    return -1


def _parse_twse(rows: list, fields: list) -> tuple:
    """Returns (records, parse_errors). Handles both 16-col (pre-2018) and 19-col formats."""
    has_foreign_dealer = _find_col(fields, "外資自營商", "買進") >= 0

    if has_foreign_dealer:
        # 19-col format (2018+)
        fb_idx  = _find_col(fields, "外陸資", "買進")
        fs_idx  = _find_col(fields, "外陸資", "賣出")
        fn_idx  = _find_col(fields, "外陸資", "買賣超")
        fdb_idx = _find_col(fields, "外資自營商", "買進")
        fds_idx = _find_col(fields, "外資自營商", "賣出")
        fdn_idx = _find_col(fields, "外資自營商", "買賣超")
    else:
        # 16-col format (pre-2018): single foreign column
        fb_idx  = _find_col(fields, "外資", "買進")
        fs_idx  = _find_col(fields, "外資", "賣出")
        fn_idx  = _find_col(fields, "外資", "買賣超")
        fdb_idx = fds_idx = fdn_idx = -1

    tb_idx  = _find_col(fields, "投信", "買進")
    ts_idx  = _find_col(fields, "投信", "賣出")
    tn_idx  = _find_col(fields, "投信", "買賣超")
    dn_idx  = _find_col(fields, "自營商買賣超股數")  # dealer net total
    dbb_idx = _find_col(fields, "自行買賣", "買進")   # 自行 buy (not 自營商買賣超)
    dbs_idx = _find_col(fields, "自行買賣", "賣出")
    dhb_idx = _find_col(fields, "避險", "買進")
    dhs_idx = _find_col(fields, "避險", "賣出")
    in_idx  = _find_col(fields, "三大法人")

    results = []
    errors = 0
    for row in rows:
        try:
            stock_id = str(row[0]).strip()
            security_type = classify_tw_security(stock_id)
            if not stock_id or not security_type:
                continue

            fb = (_parse_int(row[fb_idx]) or 0) if fb_idx >= 0 else 0
            fs = (_parse_int(row[fs_idx]) or 0) if fs_idx >= 0 else 0
            fdb = (_parse_int(row[fdb_idx]) or 0) if fdb_idx >= 0 else 0
            fds = (_parse_int(row[fds_idx]) or 0) if fds_idx >= 0 else 0

            fn = (_parse_int(row[fn_idx]) or 0) if fn_idx >= 0 else 0
            fdn = (_parse_int(row[fdn_idx]) or 0) if fdn_idx >= 0 else 0

            db_self = (_parse_int(row[dbb_idx]) or 0) if dbb_idx >= 0 else 0
            ds_self = (_parse_int(row[dbs_idx]) or 0) if dbs_idx >= 0 else 0
            db_hedge = (_parse_int(row[dhb_idx]) or 0) if dhb_idx >= 0 else 0
            ds_hedge = (_parse_int(row[dhs_idx]) or 0) if dhs_idx >= 0 else 0

            results.append({
                "stock_id":     stock_id,
                "name":         str(row[1]).strip(),
                "security_type": security_type,
                "foreign_buy":  fb + fdb,
                "foreign_sell": fs + fds,
                "foreign_net":  fn + fdn,
                "trust_buy":    _parse_int(row[tb_idx]) if tb_idx >= 0 else None,
                "trust_sell":   _parse_int(row[ts_idx]) if ts_idx >= 0 else None,
                "trust_net":    _parse_int(row[tn_idx]) if tn_idx >= 0 else None,
                "dealer_buy":   db_self + db_hedge,
                "dealer_sell":  ds_self + ds_hedge,
                "dealer_net":   _parse_int(row[dn_idx]) if dn_idx >= 0 else None,
                "inst_net":     _parse_int(row[in_idx]) if in_idx >= 0 else None,
            })
        except (IndexError, ValueError, TypeError) as e:
            print(f"  Skipping TWSE institutional row: {e}")
            errors += 1
            continue
    return results, errors


def fetch_twse_institutional(trade_date: date) -> tuple:
    """Returns (records, api_rows, parse_errors)."""
    date_str = trade_date.strftime("%Y%m%d")
    print(f"Fetching TWSE institutional for {trade_date} ...")
    data = fetch_json_retry(TWSE_URL, params={"date": date_str, "selectType": "ALL", "response": "json"},
                            validate=lambda d: d.get("stat") == "OK")

    if not data or data.get("stat") != "OK":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return [], 0, 0

    # Verify the API returned data for the requested date.
    api_date = str(data.get("date", "")).strip()
    if api_date and api_date != date_str:
        print(f"  Date mismatch: requested {date_str}, API returned {api_date} — skipping")
        return [], 0, 0

    fields = data.get("fields", [])
    rows = data.get("data", [])
    print(f"  Found {len(rows)} records ({len(fields)} fields).")
    records, errors = _parse_twse(rows, fields)
    return records, len(rows), errors


# ---------------------------------------------------------------------------
# TPEx
# ---------------------------------------------------------------------------
# foreign total = [8-10] (外陸資合計, already includes 外資+外資自營商)
# dealer total  = [20-22] (自營商合計)

def _parse_tpex(rows: list) -> tuple:
    """Returns (records, parse_errors)."""
    results = []
    errors = 0
    for row in rows:
        try:
            stock_id = str(row[0]).strip()
            if not stock_id or not classify_tw_security(stock_id):
                continue

            results.append({
                "stock_id":    stock_id,
                "name":        str(row[1]).strip(),
                "security_type": classify_tw_security(stock_id),
                "foreign_buy":  _parse_int(row[8]),
                "foreign_sell": _parse_int(row[9]),
                "foreign_net":  _parse_int(row[10]),
                "trust_buy":    _parse_int(row[11]),
                "trust_sell":   _parse_int(row[12]),
                "trust_net":    _parse_int(row[13]),
                "dealer_buy":   _parse_int(row[20]),
                "dealer_sell":  _parse_int(row[21]),
                "dealer_net":   _parse_int(row[22]),
                "inst_net":     _parse_int(row[23]),
            })
        except (IndexError, ValueError, TypeError) as e:
            print(f"  Skipping TPEx institutional row: {e}")
            errors += 1
            continue
    return results, errors


def _to_ad_date(d: date) -> str:
    return d.strftime("%Y/%m/%d")


def fetch_tpex_institutional(trade_date: date) -> tuple:
    """Returns (records, api_rows, parse_errors)."""
    ad = _to_ad_date(trade_date)
    print(f"Fetching TPEx institutional for {trade_date} ...")
    data = fetch_json_retry(TPEX_URL, params={"date": ad, "type": "Daily", "response": "json"},
                            validate=lambda d: d.get("stat") == "ok")

    if not data or data.get("stat") != "ok":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return [], 0, 0

    # Verify the API returned data for the requested date (TPEx silently returns
    # today's data if it doesn't recognize the date format).
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
    records, errors = _parse_tpex(rows)
    return records, len(rows), errors


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

def _upsert_stocks(cur, records: list, market: str):
    for r in records:
        if r.get("name") and r.get("security_type"):
            cur.execute("""
                INSERT INTO tw.stocks (stock_id, name, market, security_type)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (stock_id) DO UPDATE SET name = EXCLUDED.name, updated_at = NOW()
            """, (r["stock_id"], r["name"], market, r["security_type"]))


def save_institutional(twse: list, tpex: list, trade_date: date):
    with get_cursor() as cur:
        _upsert_stocks(cur, twse, "TWSE")
        _upsert_stocks(cur, tpex, "TPEx")

        for r in twse + tpex:
            cur.execute("""
                INSERT INTO tw.daily_prices (
                    stock_id, trade_date,
                    foreign_buy, foreign_sell, foreign_net,
                    trust_buy,   trust_sell,   trust_net,
                    dealer_buy,  dealer_sell,  dealer_net,
                    inst_net
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_id, trade_date) DO UPDATE SET
                    foreign_buy  = COALESCE(EXCLUDED.foreign_buy,  tw.daily_prices.foreign_buy),
                    foreign_sell = COALESCE(EXCLUDED.foreign_sell, tw.daily_prices.foreign_sell),
                    foreign_net  = COALESCE(EXCLUDED.foreign_net,  tw.daily_prices.foreign_net),
                    trust_buy    = COALESCE(EXCLUDED.trust_buy,    tw.daily_prices.trust_buy),
                    trust_sell   = COALESCE(EXCLUDED.trust_sell,   tw.daily_prices.trust_sell),
                    trust_net    = COALESCE(EXCLUDED.trust_net,    tw.daily_prices.trust_net),
                    dealer_buy   = COALESCE(EXCLUDED.dealer_buy,   tw.daily_prices.dealer_buy),
                    dealer_sell  = COALESCE(EXCLUDED.dealer_sell,  tw.daily_prices.dealer_sell),
                    dealer_net   = COALESCE(EXCLUDED.dealer_net,   tw.daily_prices.dealer_net),
                    inst_net     = COALESCE(EXCLUDED.inst_net,     tw.daily_prices.inst_net)
            """, (
                r["stock_id"], trade_date,
                r.get("foreign_buy"),  r.get("foreign_sell"),  r.get("foreign_net"),
                r.get("trust_buy"),    r.get("trust_sell"),    r.get("trust_net"),
                r.get("dealer_buy"),   r.get("dealer_sell"),   r.get("dealer_net"),
                r.get("inst_net"),
            ))

    print(f"Saved institutional: TWSE={len(twse)}, TPEx={len(tpex)} (total={len(twse)+len(tpex)})")


def scrape_date(trade_date: date) -> ScrapeResult:
    twse, twse_api, twse_err = fetch_twse_institutional(trade_date)
    tpex, tpex_api, tpex_err = fetch_tpex_institutional(trade_date)
    save_institutional(twse, tpex, trade_date)
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
