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
from utils.http_client import fetch_json

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
# foreign total = [2-4] (外陸資不含自營) + [5-7] (外資自營商)
# dealer total  = [12-14] (自行) + [15-17] (避險); net = [11]

def _parse_twse(rows: list) -> list:
    results = []
    for row in rows:
        try:
            stock_id = str(row[0]).strip()
            if not stock_id or not classify_tw_security(stock_id):
                continue

            fb = _parse_int(row[2]) or 0
            fs = _parse_int(row[3]) or 0
            fdb = _parse_int(row[5]) or 0
            fds = _parse_int(row[6]) or 0

            db = (_parse_int(row[12]) or 0) + (_parse_int(row[15]) or 0)
            ds = (_parse_int(row[13]) or 0) + (_parse_int(row[16]) or 0)

            results.append({
                "stock_id":    stock_id,
                "foreign_buy":  fb + fdb,
                "foreign_sell": fs + fds,
                "foreign_net":  _parse_int(row[4]) or 0 + (_parse_int(row[7]) or 0),
                "trust_buy":    _parse_int(row[8]),
                "trust_sell":   _parse_int(row[9]),
                "trust_net":    _parse_int(row[10]),
                "dealer_buy":   db,
                "dealer_sell":  ds,
                "dealer_net":   _parse_int(row[11]),
                "inst_net":     _parse_int(row[18]),
            })
        except (IndexError, ValueError, TypeError):
            continue
    return results


def fetch_twse_institutional(trade_date: date) -> list:
    date_str = trade_date.strftime("%Y%m%d")
    print(f"Fetching TWSE institutional for {trade_date} ...")
    data = fetch_json(TWSE_URL, params={"date": date_str, "selectType": "ALL", "response": "json"})

    if not data or data.get("stat") != "OK":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return []

    rows = data.get("data", [])
    print(f"  Found {len(rows)} records.")
    return _parse_twse(rows)


# ---------------------------------------------------------------------------
# TPEx
# ---------------------------------------------------------------------------
# foreign total = [8-10] (外陸資合計, already includes 外資+外資自營商)
# dealer total  = [20-22] (自營商合計)

def _parse_tpex(rows: list) -> list:
    results = []
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
        except (IndexError, ValueError, TypeError):
            continue
    return results


def fetch_tpex_institutional(trade_date: date) -> list:
    date_str = trade_date.strftime("%Y%m%d")
    print(f"Fetching TPEx institutional for {trade_date} ...")
    data = fetch_json(TPEX_URL, params={"date": date_str, "type": "Daily", "o": "json"})

    if not data or data.get("stat") != "ok":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return []

    tables = data.get("tables", [])
    if not tables:
        print("  Unexpected response structure.")
        return []

    rows = tables[0].get("data", [])
    print(f"  Found {len(rows)} records.")
    return _parse_tpex(rows)


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

def _upsert_tpex_stocks(cur, records: list):
    for r in records:
        if r.get("name") and r.get("security_type"):
            cur.execute("""
                INSERT INTO tw.stocks (stock_id, name, market, security_type)
                VALUES (%s, %s, 'TPEx', %s)
                ON CONFLICT (stock_id) DO UPDATE SET name = EXCLUDED.name, updated_at = NOW()
            """, (r["stock_id"], r["name"], r["security_type"]))


def save_institutional(twse: list, tpex: list, trade_date: date):
    with get_cursor() as cur:
        _upsert_tpex_stocks(cur, tpex)

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
                    foreign_buy  = EXCLUDED.foreign_buy,
                    foreign_sell = EXCLUDED.foreign_sell,
                    foreign_net  = EXCLUDED.foreign_net,
                    trust_buy    = EXCLUDED.trust_buy,
                    trust_sell   = EXCLUDED.trust_sell,
                    trust_net    = EXCLUDED.trust_net,
                    dealer_buy   = EXCLUDED.dealer_buy,
                    dealer_sell  = EXCLUDED.dealer_sell,
                    dealer_net   = EXCLUDED.dealer_net,
                    inst_net     = EXCLUDED.inst_net
            """, (
                r["stock_id"], trade_date,
                r.get("foreign_buy"),  r.get("foreign_sell"),  r.get("foreign_net"),
                r.get("trust_buy"),    r.get("trust_sell"),    r.get("trust_net"),
                r.get("dealer_buy"),   r.get("dealer_sell"),   r.get("dealer_net"),
                r.get("inst_net"),
            ))

    print(f"Saved institutional: TWSE={len(twse)}, TPEx={len(tpex)} (total={len(twse)+len(tpex)})")


def scrape_date(trade_date: date):
    twse = fetch_twse_institutional(trade_date)
    tpex = fetch_tpex_institutional(trade_date)
    save_institutional(twse, tpex, trade_date)


def scrape_date_range(start_date: date, end_date: date):
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
