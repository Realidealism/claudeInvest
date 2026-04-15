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

def _find_col(fields, *keywords, exclude=()):
    """Find column index where field name contains all keywords and none of exclude."""
    for i, f in enumerate(fields):
        if all(k in f for k in keywords) and not any(e in f for e in exclude):
            return i
    return -1


def _parse_twse(rows: list, fields: list) -> tuple:
    """
    Returns (records, parse_errors). Handles both 16-col (pre-2018) and 19-col formats.
    Stores foreign / foreign_dealer / dealer_self / dealer_hedge separately;
    pre-2018 rows leave foreign_dealer_* and dealer_hedge_* as None.
    """
    has_foreign_dealer = _find_col(fields, "外資自營商", "買進") >= 0

    if has_foreign_dealer:
        # 外陸資* and 外資自營商* both contain "外資自營商" as substring via
        # the "(不含外資自營商)" suffix — disambiguate with exclude=("不含",).
        fb_idx  = _find_col(fields, "外陸資", "買進")
        fs_idx  = _find_col(fields, "外陸資", "賣出")
        fn_idx  = _find_col(fields, "外陸資", "買賣超")
        fdb_idx = _find_col(fields, "外資自營商", "買進", exclude=("不含",))
        fds_idx = _find_col(fields, "外資自營商", "賣出", exclude=("不含",))
        fdn_idx = _find_col(fields, "外資自營商", "買賣超", exclude=("不含",))
    else:
        # Pre-2018: single foreign column, no dealer split.
        fb_idx  = _find_col(fields, "外資", "買進")
        fs_idx  = _find_col(fields, "外資", "賣出")
        fn_idx  = _find_col(fields, "外資", "買賣超")
        fdb_idx = fds_idx = fdn_idx = -1

    tb_idx  = _find_col(fields, "投信", "買進")
    ts_idx  = _find_col(fields, "投信", "賣出")
    tn_idx  = _find_col(fields, "投信", "買賣超")
    dsb_idx = _find_col(fields, "自行買賣", "買進")
    dss_idx = _find_col(fields, "自行買賣", "賣出")
    dsn_idx = _find_col(fields, "自行買賣", "買賣超")
    dhb_idx = _find_col(fields, "避險", "買進")
    dhs_idx = _find_col(fields, "避險", "賣出")
    dhn_idx = _find_col(fields, "避險", "買賣超")
    in_idx  = _find_col(fields, "三大法人")

    def _get(row, idx):
        return _parse_int(row[idx]) if idx >= 0 else None

    results = []
    errors = 0
    for row in rows:
        try:
            stock_id = str(row[0]).strip()
            security_type = classify_tw_security(stock_id)
            if not stock_id or not security_type:
                continue

            results.append({
                "stock_id":     stock_id,
                "name":         str(row[1]).strip(),
                "security_type": security_type,
                "foreign_buy":         _get(row, fb_idx),
                "foreign_sell":        _get(row, fs_idx),
                "foreign_net":         _get(row, fn_idx),
                "foreign_dealer_buy":  _get(row, fdb_idx),
                "foreign_dealer_sell": _get(row, fds_idx),
                "foreign_dealer_net":  _get(row, fdn_idx),
                "trust_buy":           _get(row, tb_idx),
                "trust_sell":          _get(row, ts_idx),
                "trust_net":           _get(row, tn_idx),
                "dealer_buy":          _get(row, dsb_idx),
                "dealer_sell":         _get(row, dss_idx),
                "dealer_net":          _get(row, dsn_idx),
                "dealer_hedge_buy":    _get(row, dhb_idx),
                "dealer_hedge_sell":   _get(row, dhs_idx),
                "dealer_hedge_net":    _get(row, dhn_idx),
                "inst_net":            _get(row, in_idx),
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
# [2-4]   外資不含自營商 → foreign_*
# [5-7]   外資自營商       → foreign_dealer_*
# [11-13] 投信             → trust_*
# [14-16] 自營商 (自行)    → dealer_*
# [17-19] 自營商 (避險)    → dealer_hedge_*
# [23]    三大法人合計     → inst_net

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
                "foreign_buy":         _parse_int(row[2]),
                "foreign_sell":        _parse_int(row[3]),
                "foreign_net":         _parse_int(row[4]),
                "foreign_dealer_buy":  _parse_int(row[5]),
                "foreign_dealer_sell": _parse_int(row[6]),
                "foreign_dealer_net":  _parse_int(row[7]),
                "trust_buy":           _parse_int(row[11]),
                "trust_sell":          _parse_int(row[12]),
                "trust_net":           _parse_int(row[13]),
                "dealer_buy":          _parse_int(row[14]),
                "dealer_sell":         _parse_int(row[15]),
                "dealer_net":          _parse_int(row[16]),
                "dealer_hedge_buy":    _parse_int(row[17]),
                "dealer_hedge_sell":   _parse_int(row[18]),
                "dealer_hedge_net":    _parse_int(row[19]),
                "inst_net":            _parse_int(row[23]),
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

        # Semantics changed in migration 023: foreign_* / dealer_* are now
        # "不含自營商 / 自行買賣" only (not combined). Use plain assignment
        # (not COALESCE) so old combined values get overwritten with the new
        # split values during historical backfill.
        for r in twse + tpex:
            cur.execute("""
                INSERT INTO tw.daily_prices (
                    stock_id, trade_date,
                    foreign_buy, foreign_sell, foreign_net,
                    foreign_dealer_buy, foreign_dealer_sell, foreign_dealer_net,
                    trust_buy,   trust_sell,   trust_net,
                    dealer_buy,  dealer_sell,  dealer_net,
                    dealer_hedge_buy, dealer_hedge_sell, dealer_hedge_net,
                    inst_net
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_id, trade_date) DO UPDATE SET
                    foreign_buy         = EXCLUDED.foreign_buy,
                    foreign_sell        = EXCLUDED.foreign_sell,
                    foreign_net         = EXCLUDED.foreign_net,
                    foreign_dealer_buy  = EXCLUDED.foreign_dealer_buy,
                    foreign_dealer_sell = EXCLUDED.foreign_dealer_sell,
                    foreign_dealer_net  = EXCLUDED.foreign_dealer_net,
                    trust_buy           = EXCLUDED.trust_buy,
                    trust_sell          = EXCLUDED.trust_sell,
                    trust_net           = EXCLUDED.trust_net,
                    dealer_buy          = EXCLUDED.dealer_buy,
                    dealer_sell         = EXCLUDED.dealer_sell,
                    dealer_net          = EXCLUDED.dealer_net,
                    dealer_hedge_buy    = EXCLUDED.dealer_hedge_buy,
                    dealer_hedge_sell   = EXCLUDED.dealer_hedge_sell,
                    dealer_hedge_net    = EXCLUDED.dealer_hedge_net,
                    inst_net            = EXCLUDED.inst_net
            """, (
                r["stock_id"], trade_date,
                r.get("foreign_buy"),         r.get("foreign_sell"),         r.get("foreign_net"),
                r.get("foreign_dealer_buy"),  r.get("foreign_dealer_sell"),  r.get("foreign_dealer_net"),
                r.get("trust_buy"),           r.get("trust_sell"),           r.get("trust_net"),
                r.get("dealer_buy"),          r.get("dealer_sell"),          r.get("dealer_net"),
                r.get("dealer_hedge_buy"),    r.get("dealer_hedge_sell"),    r.get("dealer_hedge_net"),
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
