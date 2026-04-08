"""
Market index daily price scraper (指數日行情).

Sources:
  TAIEX (加權指數):
    - TWSE rwd/zh/TAIEX/MI_5MINS_HIST (GET, date=YYYYMMDD)
      Returns all trading days of the queried month.
      Fields: [0]日期, [1]開盤, [2]最高, [3]最低, [4]收盤
    - TWSE rwd/zh/afterTrading/FMTQIK (GET, date=YYYYMMDD)
      Returns all trading days of the queried month.
      Fields: [0]日期, [1]成交股數, [2]成交金額, [3]成交筆數, [4]收盤指數, [5]漲跌點數

  TPEx Composite Index (櫃買指數):
    TPEx /www/zh-tw/indexInfo/inx (GET, d=YYY/MM/DD)
    Returns ~30 days before query date.
    Fields: [0]日期, [1]開盤, [2]最高, [3]最低, [4]收盤, [5]漲跌
    Note: volume/turnover not available from this endpoint.
"""

from datetime import date, timedelta

from db.connection import get_cursor
from utils.http_client import fetch_json

TWSE_HIST_URL  = "https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST"
TWSE_STAT_URL  = "https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK"
TPEX_INDEX_URL = "https://www.tpex.org.tw/www/zh-tw/indexInfo/inx"


def _parse_num(val) -> float | None:
    s = str(val).replace(",", "").strip()
    if not s or s in ("--", "---", ""):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_int(val) -> int | None:
    n = _parse_num(val)
    return int(n) if n is not None else None


def _roc_to_date(roc_str: str) -> date | None:
    """Convert ROC date string (e.g. '115/04/02' or '2026/04/02') to date."""
    parts = roc_str.strip().replace("-", "/").split("/")
    if len(parts) != 3:
        return None
    try:
        year = int(parts[0])
        if year < 1000:
            year += 1911
        return date(year, int(parts[1]), int(parts[2]))
    except ValueError:
        return None


def _to_roc_date(d: date) -> str:
    return f"{d.year - 1911}/{d.strftime('%m/%d')}"


# ---------------------------------------------------------------------------
# TWSE TAIEX — price data (MI_5MINS_HIST)
# ---------------------------------------------------------------------------

def _fetch_taiex_price_month(year: int, month: int) -> dict:
    """Returns {date: {open, high, low, close}} for the month."""
    date_str = f"{year}{month:02d}01"
    data = fetch_json(TWSE_HIST_URL, params={"date": date_str, "response": "json"})
    if not data or data.get("stat") != "OK":
        return {}
    result = {}
    for row in data.get("data", []):
        try:
            d = _roc_to_date(str(row[0]))
            if not d:
                continue
            result[d] = {
                "open_price":  _parse_num(row[1]),
                "high_price":  _parse_num(row[2]),
                "low_price":   _parse_num(row[3]),
                "close_price": _parse_num(row[4]),
            }
        except (IndexError, ValueError, TypeError):
            continue
    return result


# ---------------------------------------------------------------------------
# TWSE TAIEX — volume/turnover data (FMTQIK)
# ---------------------------------------------------------------------------

def _fetch_taiex_stat_month(year: int, month: int) -> dict:
    """Returns {date: {volume, turnover, tx_count, close_price, change}} for the month."""
    date_str = f"{year}{month:02d}01"
    data = fetch_json(TWSE_STAT_URL, params={"date": date_str, "response": "json"})
    if not data or data.get("stat") != "OK":
        return {}
    result = {}
    for row in data.get("data", []):
        try:
            d = _roc_to_date(str(row[0]))
            if not d:
                continue
            close_p = _parse_num(row[4])
            change  = _parse_num(row[5])
            change_pct = None
            if change is not None and close_p and (close_p - change) != 0:
                change_pct = round(change / (close_p - change) * 100, 4)
            result[d] = {
                "volume":      _parse_int(row[1]),
                "turnover":    _parse_int(row[2]),
                "tx_count":    _parse_int(row[3]),
                "close_price": close_p,
                "change":      change,
                "change_pct":  change_pct,
            }
        except (IndexError, ValueError, TypeError):
            continue
    return result


def fetch_taiex(trade_date: date) -> list:
    print(f"Fetching TAIEX for {trade_date.year}/{trade_date.month:02d} ...")
    price_map = _fetch_taiex_price_month(trade_date.year, trade_date.month)
    stat_map  = _fetch_taiex_stat_month(trade_date.year, trade_date.month)

    if trade_date not in price_map and trade_date not in stat_map:
        print("  No data.")
        return []

    p = price_map.get(trade_date, {})
    s = stat_map.get(trade_date, {})

    record = {
        "index_id":    "TAIEX",
        "trade_date":  trade_date,
        "open_price":  p.get("open_price"),
        "high_price":  p.get("high_price"),
        "low_price":   p.get("low_price"),
        "close_price": s.get("close_price") or p.get("close_price"),
        "change":      s.get("change"),
        "change_pct":  s.get("change_pct"),
        "volume":      s.get("volume"),
        "turnover":    s.get("turnover"),
        "tx_count":    s.get("tx_count"),
    }
    print(f"  Found 1 record.")
    return [record]


# ---------------------------------------------------------------------------
# TPEx Composite Index
# ---------------------------------------------------------------------------

def fetch_tpex_index(trade_date: date) -> list:
    """Fetch TPEx composite index. Volume/turnover not available from this endpoint."""
    roc = _to_roc_date(trade_date)
    print(f"Fetching TPEx index for {trade_date} ...")
    data = fetch_json(TPEX_INDEX_URL, params={"d": roc, "o": "json"})

    if not data or data.get("stat") != "ok":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return []

    tables = data.get("tables", [])
    if not tables:
        return []

    results = []
    for row in tables[0].get("data", []):
        try:
            trade_d = _roc_to_date(str(row[0]))
            if not trade_d or trade_d != trade_date:
                continue
            close_p = _parse_num(row[4])
            change  = _parse_num(row[5])
            change_pct = None
            if change is not None and close_p and (close_p - change) != 0:
                change_pct = round(change / (close_p - change) * 100, 4)
            results.append({
                "index_id":    "TPEx",
                "trade_date":  trade_d,
                "open_price":  _parse_num(row[1]),
                "high_price":  _parse_num(row[2]),
                "low_price":   _parse_num(row[3]),
                "close_price": close_p,
                "change":      change,
                "change_pct":  change_pct,
                "volume":      None,
                "turnover":    None,
                "tx_count":    None,
            })
        except (IndexError, ValueError, TypeError):
            continue

    print(f"  Found {len(results)} record(s).")
    return results


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

def save_indices(records: list):
    if not records:
        return
    with get_cursor() as cur:
        for r in records:
            cur.execute("""
                INSERT INTO tw.index_prices (
                    index_id, trade_date,
                    open_price, high_price, low_price, close_price,
                    change, change_pct,
                    volume, turnover, tx_count
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (index_id, trade_date) DO UPDATE SET
                    open_price  = EXCLUDED.open_price,
                    high_price  = EXCLUDED.high_price,
                    low_price   = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    change      = EXCLUDED.change,
                    change_pct  = EXCLUDED.change_pct,
                    volume      = COALESCE(EXCLUDED.volume,   tw.index_prices.volume),
                    turnover    = COALESCE(EXCLUDED.turnover, tw.index_prices.turnover),
                    tx_count    = COALESCE(EXCLUDED.tx_count, tw.index_prices.tx_count)
            """, (
                r["index_id"], r["trade_date"],
                r.get("open_price"), r.get("high_price"),
                r.get("low_price"),  r.get("close_price"),
                r.get("change"),     r.get("change_pct"),
                r.get("volume"),     r.get("turnover"),
                r.get("tx_count"),
            ))
    print(f"Saved {len(records)} index record(s).")


def scrape_date(trade_date: date):
    records = fetch_taiex(trade_date) + fetch_tpex_index(trade_date)
    save_indices(records)


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
