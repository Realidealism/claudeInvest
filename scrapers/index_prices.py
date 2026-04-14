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
    TPEx /www/zh-tw/indexInfo/inx (GET, date=YYY/MM)
    Returns all trading days of the queried month (ROC year/month).
    Fields: [0]日期, [1]開市, [2]最高, [3]最低, [4]收市, [5]漲/跌
    Note: volume/turnover not available from this endpoint.
"""

from datetime import date, timedelta

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json, fetch_json_retry

TWSE_HIST_URL      = "https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST"
TWSE_STAT_URL      = "https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK"
TWSE_MI_INDEX_URL  = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
TPEX_INDEX_URL     = "https://www.tpex.org.tw/www/zh-tw/indexInfo/inx"
TPEX_HIGHLIGHT_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/highlight"


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
# Trading days calendar (derived from TAIEX data)
# ---------------------------------------------------------------------------

def get_trading_days_and_save_index(start: date, end: date) -> list:
    """
    Fetch all actual trading days between start and end (inclusive)
    by querying TAIEX and TPEx monthly data. Saves all index data to DB
    in the same pass (avoids redundant per-day API calls).
    Returns sorted list of trading dates.
    """
    trading_days = set()
    all_records = []

    current_year, current_month = start.year, start.month
    end_year, end_month = end.year, end.month

    failed_months = []

    while (current_year, current_month) <= (end_year, end_month):
        print(f"Fetching index data for {current_year}/{current_month:02d} ...")

        # TAIEX: price + stat for the whole month (retry each up to 2 extra times)
        price_map, _, _ = _fetch_taiex_price_month(current_year, current_month)
        if not price_map:
            import time
            print(f"  [RETRY] MI_5MINS_HIST empty for {current_year}/{current_month:02d}, retrying ...")
            time.sleep(8)
            price_map, _, _ = _fetch_taiex_price_month(current_year, current_month)

        stat_map, _, _ = _fetch_taiex_stat_month(current_year, current_month)
        if not stat_map and price_map:
            import time
            print(f"  [RETRY] FMTQIK empty for {current_year}/{current_month:02d}, retrying ...")
            time.sleep(8)
            stat_map, _, _ = _fetch_taiex_stat_month(current_year, current_month)

        if price_map and not stat_map:
            failed_months.append(f"{current_year}/{current_month:02d} (FMTQIK)")
            print(f"  [WARN] FMTQIK still empty for {current_year}/{current_month:02d} — volume/change will be null")
        elif not price_map:
            failed_months.append(f"{current_year}/{current_month:02d} (MI_5MINS_HIST)")
            print(f"  [WARN] MI_5MINS_HIST empty for {current_year}/{current_month:02d} — skipping month")

        # Merge TAIEX price + stat per day
        all_dates = set(price_map.keys()) | set(stat_map.keys())
        for d in all_dates:
            if not (start <= d <= end):
                continue
            trading_days.add(d)
            p = price_map.get(d, {})
            s = stat_map.get(d, {})
            all_records.append({
                "index_id":    "TAIEX",
                "trade_date":  d,
                "open_price":  p.get("open_price"),
                "high_price":  p.get("high_price"),
                "low_price":   p.get("low_price"),
                "close_price": s.get("close_price") or p.get("close_price"),
                "change":      s.get("change"),
                "change_pct":  s.get("change_pct"),
                "volume":      s.get("volume"),
                "turnover":    s.get("turnover"),
                "tx_count":    s.get("tx_count"),
            })

        # TPEx index for the same month (query by ROC year/month).
        # NOTE: this endpoint only accepts ROC format; AD format triggers a
        # silent fallback to the current month.
        roc_month = f"{current_year - 1911}/{current_month:02d}"
        expected_ym = f"{current_year}{current_month:02d}"
        tpex_data = fetch_json_retry(TPEX_INDEX_URL, params={"date": roc_month, "o": "json"},
                                     validate=lambda d: d.get("stat") == "ok")
        api_date = str(tpex_data.get("date", "")).strip() if tpex_data else ""
        if api_date and not api_date.startswith(expected_ym):
            print(f"  [WARN] TPEx index date mismatch: requested {expected_ym}, "
                  f"API returned {api_date} — skipping this month")
            tpex_data = None
        if tpex_data and tpex_data.get("stat") == "ok" and tpex_data.get("tables"):
            for row in tpex_data["tables"][0].get("data", []):
                try:
                    trade_d = _roc_to_date(str(row[0]))
                    if not trade_d or not (start <= trade_d <= end):
                        continue
                    trading_days.add(trade_d)
                    close_p = _parse_num(row[4])
                    change  = _parse_num(row[5])
                    change_pct = None
                    if change is not None and close_p and (close_p - change) != 0:
                        change_pct = round(change / (close_p - change) * 100, 4)
                    all_records.append({
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

        # Next month
        if current_month == 12:
            current_year += 1
            current_month = 1
        else:
            current_month += 1

    # Save all index records at once
    print(f"Saving {len(all_records)} index records ...")
    save_indices(all_records)

    if failed_months:
        print(f"\n  [WARN] Failed months ({len(failed_months)}):")
        for m in failed_months:
            print(f"    - {m}")
        print("  These can be fixed by re-running with --force\n")

    result = sorted(trading_days)
    print(f"Found {len(result)} trading days in {start} ~ {end}")
    return result


# ---------------------------------------------------------------------------
# TWSE TAIEX — price data (MI_5MINS_HIST)
# ---------------------------------------------------------------------------

def _fetch_taiex_price_month(year: int, month: int) -> tuple:
    """
    Returns ({date: {open, high, low, close}}, api_rows, parse_errors) for the month.
    """
    date_str = f"{year}{month:02d}01"
    data = fetch_json_retry(TWSE_HIST_URL, params={"date": date_str, "response": "json"},
                            validate=lambda d: d.get("stat") == "OK")
    if not data or data.get("stat") != "OK":
        return {}, 0, 0
    rows = data.get("data", [])
    result = {}
    errors = 0
    for row in rows:
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
            errors += 1
            continue
    return result, len(rows), errors


# ---------------------------------------------------------------------------
# TWSE TAIEX — volume/turnover data (FMTQIK)
# ---------------------------------------------------------------------------

def _fetch_taiex_stat_month(year: int, month: int) -> tuple:
    """
    Returns ({date: {volume, turnover, tx_count, close_price, change}}, api_rows, parse_errors).
    """
    date_str = f"{year}{month:02d}01"
    data = fetch_json_retry(TWSE_STAT_URL, params={"date": date_str, "response": "json"},
                            validate=lambda d: d.get("stat") == "OK")
    if not data or data.get("stat") != "OK":
        return {}, 0, 0
    rows = data.get("data", [])
    result = {}
    errors = 0
    for row in rows:
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
            errors += 1
            continue
    return result, len(rows), errors


def _parse_count_with_limit(val: str) -> tuple:
    """
    Parse '8,084(208)' → (8084, 208) or '691' → (691, None).
    Returns (count, limit_count).
    """
    import re
    s = str(val).replace(",", "").strip()
    m = re.match(r"(\d+)\((\d+)\)", s)
    if m:
        return int(m.group(1)), int(m.group(2))
    n = _parse_int(s)
    return (n, None) if n is not None else (None, None)


def _fetch_twse_advance_decline(trade_date: date) -> dict | None:
    """
    Fetch TWSE advance/decline counts from MI_INDEX (Table '漲跌證券數合計').
    Uses the '股票' column. Returns dict or None.
    """
    date_str = trade_date.strftime("%Y%m%d")
    print(f"Fetching TWSE advance/decline for {trade_date} ...")
    data = fetch_json_retry(
        TWSE_MI_INDEX_URL,
        params={"date": date_str, "type": "ALLBUT0999", "response": "json"},
        validate=lambda d: d.get("stat") == "OK",
    )
    if not data or data.get("stat") != "OK":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return None

    api_date = str(data.get("date", "")).strip()
    if api_date and api_date != date_str:
        print(f"  Date mismatch: requested {date_str}, API returned {api_date} — skipping")
        return None

    # Find the advance/decline table by title
    for table in data.get("tables", []):
        if "漲跌證券數" in table.get("title", ""):
            fields = table.get("fields", [])
            rows = table.get("data", [])
            # Find the '股票' column index
            stock_col = None
            for i, f in enumerate(fields):
                if "股票" in f:
                    stock_col = i
                    break
            if stock_col is None:
                print("  [WARN] '股票' column not found")
                return None

            result = {}
            for row in rows:
                label = str(row[0]).strip()
                val = str(row[stock_col]).strip()
                if "上漲" in label:
                    result["advance"], result["advance_limit"] = _parse_count_with_limit(val)
                elif "下跌" in label:
                    result["decline"], result["decline_limit"] = _parse_count_with_limit(val)
                elif "持平" in label:
                    result["unchanged"], _ = _parse_count_with_limit(val)
                elif "未成交" in label:
                    result["no_trade"], _ = _parse_count_with_limit(val)
            print(f"  advance={result.get('advance')} decline={result.get('decline')}")
            return result

    print("  advance/decline table not found")
    return None


def _fetch_tpex_advance_decline(trade_date: date) -> dict | None:
    """
    Fetch TPEx advance/decline counts from /afterTrading/highlight.
    Returns dict or None.
    """
    ad = trade_date.strftime("%Y/%m/%d")
    expected = trade_date.strftime("%Y%m%d")
    print(f"Fetching TPEx advance/decline for {trade_date} ...")
    data = fetch_json_retry(TPEX_HIGHLIGHT_URL, params={"date": ad, "response": "json"},
                            validate=lambda d: d.get("stat") == "ok")

    if not data or data.get("stat") != "ok":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return None

    api_date = str(data.get("date", "")).strip()
    if api_date and api_date != expected:
        print(f"  Date mismatch: requested {expected}, API returned {api_date} — skipping")
        return None

    tables = data.get("tables", [])
    if not tables:
        return None

    row = tables[0].get("data", [[]])[0]
    fields = tables[0].get("fields", [])
    if not row or len(row) < 13:
        print("  Unexpected row length")
        return None

    result = {
        "advance":       _parse_int(row[7]),
        "advance_limit": _parse_int(row[8]),
        "decline":       _parse_int(row[9]),
        "decline_limit": _parse_int(row[10]),
        "unchanged":     _parse_int(row[11]),
        "no_trade":      _parse_int(row[12]),
    }
    print(f"  advance={result.get('advance')} decline={result.get('decline')}")
    return result


def fetch_taiex(trade_date: date) -> tuple:
    """Returns (records, api_rows, parse_errors)."""
    print(f"Fetching TAIEX for {trade_date.year}/{trade_date.month:02d} ...")
    price_map, p_rows, p_err = _fetch_taiex_price_month(trade_date.year, trade_date.month)
    stat_map,  s_rows, s_err = _fetch_taiex_stat_month(trade_date.year, trade_date.month)

    if trade_date not in price_map and trade_date not in stat_map:
        print("  No data.")
        return [], p_rows + s_rows, p_err + s_err

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
    return [record], p_rows + s_rows, p_err + s_err


# ---------------------------------------------------------------------------
# TPEx Composite Index
# ---------------------------------------------------------------------------

def fetch_tpex_index(trade_date: date) -> tuple:
    """
    Fetch TPEx composite index. Volume/turnover not available from this endpoint.
    Returns (records, api_rows, parse_errors).
    """
    roc_month = f"{trade_date.year - 1911}/{trade_date.month:02d}"
    expected_ym = f"{trade_date.year}{trade_date.month:02d}"
    print(f"Fetching TPEx index for {trade_date} ...")
    data = fetch_json_retry(TPEX_INDEX_URL, params={"date": roc_month, "o": "json"},
                            validate=lambda d: d.get("stat") == "ok")

    if not data or data.get("stat") != "ok":
        print(f"  No data (stat={data.get('stat') if data else 'none'})")
        return [], 0, 0

    # Verify API returned the requested month (this endpoint silently falls
    # back to the current month if the date format is unrecognised).
    api_date = str(data.get("date", "")).strip()
    if api_date and not api_date.startswith(expected_ym):
        print(f"  Month mismatch: requested {expected_ym}, API returned {api_date} — skipping")
        return [], 0, 0

    tables = data.get("tables", [])
    if not tables:
        return [], 0, 0

    rows = tables[0].get("data", [])
    api_rows = len(rows)
    results = []
    errors = 0
    for row in rows:
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
        except (IndexError, ValueError, TypeError) as e:
            print(f"  Skipping TPEx index row: {e}")
            errors += 1
            continue

    print(f"  Found {len(results)} record(s).")
    return results, api_rows, errors


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
                    volume, turnover, tx_count,
                    advance, advance_limit, decline, decline_limit,
                    unchanged, no_trade
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (index_id, trade_date) DO UPDATE SET
                    open_price    = COALESCE(EXCLUDED.open_price,    tw.index_prices.open_price),
                    high_price    = COALESCE(EXCLUDED.high_price,    tw.index_prices.high_price),
                    low_price     = COALESCE(EXCLUDED.low_price,     tw.index_prices.low_price),
                    close_price   = COALESCE(EXCLUDED.close_price,   tw.index_prices.close_price),
                    change        = COALESCE(EXCLUDED.change,        tw.index_prices.change),
                    change_pct    = COALESCE(EXCLUDED.change_pct,    tw.index_prices.change_pct),
                    volume        = COALESCE(EXCLUDED.volume,        tw.index_prices.volume),
                    turnover      = COALESCE(EXCLUDED.turnover,      tw.index_prices.turnover),
                    tx_count      = COALESCE(EXCLUDED.tx_count,      tw.index_prices.tx_count),
                    advance       = COALESCE(EXCLUDED.advance,       tw.index_prices.advance),
                    advance_limit = COALESCE(EXCLUDED.advance_limit, tw.index_prices.advance_limit),
                    decline       = COALESCE(EXCLUDED.decline,       tw.index_prices.decline),
                    decline_limit = COALESCE(EXCLUDED.decline_limit, tw.index_prices.decline_limit),
                    unchanged     = COALESCE(EXCLUDED.unchanged,     tw.index_prices.unchanged),
                    no_trade      = COALESCE(EXCLUDED.no_trade,      tw.index_prices.no_trade)
            """, (
                r["index_id"], r["trade_date"],
                r.get("open_price"),    r.get("high_price"),
                r.get("low_price"),     r.get("close_price"),
                r.get("change"),        r.get("change_pct"),
                r.get("volume"),        r.get("turnover"),
                r.get("tx_count"),
                r.get("advance"),       r.get("advance_limit"),
                r.get("decline"),       r.get("decline_limit"),
                r.get("unchanged"),     r.get("no_trade"),
            ))
    print(f"Saved {len(records)} index record(s).")


def scrape_date(trade_date: date) -> ScrapeResult:
    taiex, t_api, t_err = fetch_taiex(trade_date)
    tpex,  p_api, p_err = fetch_tpex_index(trade_date)

    # Merge advance/decline data into index records
    twse_ad = _fetch_twse_advance_decline(trade_date)
    tpex_ad = _fetch_tpex_advance_decline(trade_date)
    if twse_ad and taiex:
        taiex[0].update(twse_ad)
    if tpex_ad and tpex:
        tpex[0].update(tpex_ad)

    records = taiex + tpex
    save_indices(records)
    return ScrapeResult(
        records=len(records),
        api_rows=t_api + p_api,
        parse_errors=t_err + p_err,
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
