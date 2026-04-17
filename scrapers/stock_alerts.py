"""
Stock alerts scraper: attention (注意) and disposal (處置) announcements.

Sources:
  TWSE attention: GET /rwd/zh/announcement/notice  (querytype=1)
  TWSE disposal:  GET /rwd/zh/announcement/punish  (querytype=3)
  TPEx attention: POST /www/zh-tw/bulletin/attention
  TPEx disposal:  POST /www/zh-tw/bulletin/disposal

Fields:
  TWSE notice:  [0]編號 [1]證券代號 [2]證券名稱 [3]累計次數 [4]注意交易資訊 [5]日期 [6]收盤價 [7]本益比
  TWSE punish:  [0]編號 [1]公布日期 [2]證券代號 [3]證券名稱 [4]累計 [5]處置條件 [6]處置起迄時間 [7]處置措施 [8]處置內容 [9]備註
  TPEx attention: same 8-col structure as TWSE notice
  TPEx disposal:  [0]編號 [1]公布日期 [2]證券代號 [3]證券名稱 [4]累計 [5]處置起訖時間 [6]處置條件 [7]處置內容 [8]收盤價 [9]本益比
"""

import re
from datetime import date

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json_retry, post_json_retry

TWSE_NOTICE_URL = "https://www.twse.com.tw/rwd/zh/announcement/notice"
TWSE_PUNISH_URL = "https://www.twse.com.tw/rwd/zh/announcement/punish"
TPEX_ATTENTION_URL = "https://www.tpex.org.tw/www/zh-tw/bulletin/attention"
TPEX_DISPOSAL_URL = "https://www.tpex.org.tw/www/zh-tw/bulletin/disposal"


def _parse_float(val) -> float | None:
    s = str(val).replace(",", "").strip()
    if not s or s in ("--", "---", "-----", " ", ""):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_int(val) -> int | None:
    s = str(val).replace(",", "").strip()
    if not s or s in ("--", "---", " ", ""):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _roc_to_date(roc_str: str) -> date | None:
    """Convert ROC date like '115/04/16', '115.04.16', or '1150416' to AD date."""
    s = roc_str.strip().replace("/", "").replace(".", "")
    if not s or len(s) < 7:
        return None
    try:
        roc_year = int(s[:-4])
        month = int(s[-4:-2])
        day = int(s[-2:])
        return date(roc_year + 1911, month, day)
    except (ValueError, IndexError):
        return None


def _parse_period(period_str: str) -> tuple[date | None, date | None]:
    """Parse '115/04/17～115/04/30' into (start, end) dates."""
    parts = re.split(r"[～~]", period_str.strip())
    start = _roc_to_date(parts[0]) if len(parts) >= 1 else None
    end = _roc_to_date(parts[1]) if len(parts) >= 2 else None
    return start, end


# ---------------------------------------------------------------------------
# TWSE
# ---------------------------------------------------------------------------

def _fetch_twse_notice(start: date, end: date) -> list[dict]:
    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")
    data = fetch_json_retry(
        TWSE_NOTICE_URL,
        params={
            "querytype": "1", "stockNo": "", "selectType": "",
            "startDate": start_str, "endDate": end_str,
            "sortKind": "STKNO", "response": "json",
        },
        validate=lambda d: d.get("stat") == "OK",
    )
    if not data or data.get("stat") != "OK":
        return []

    results = []
    for row in data.get("data", []):
        try:
            stock_id = str(row[1]).strip()
            if not stock_id or not stock_id[0].isdigit():
                continue
            results.append({
                "stock_id": stock_id,
                "alert_date": _roc_to_date(str(row[5])),
                "alert_type": "attention",
                "market": "TWSE",
                "cumulative": _parse_int(row[3]),
                "reason": str(row[4]).strip() or None,
                "period_start": None,
                "period_end": None,
                "measure": None,
                "close_price": _parse_float(row[6]),
                "pe_ratio": _parse_float(row[7]),
            })
        except (IndexError, ValueError, TypeError):
            continue
    return results


def _fetch_twse_punish(start: date, end: date) -> list[dict]:
    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")
    data = fetch_json_retry(
        TWSE_PUNISH_URL,
        params={
            "querytype": "3", "stockNo": "", "selectType": "",
            "proceType": "", "remarkType": "",
            "startDate": start_str, "endDate": end_str,
            "sortKind": "STKNO", "response": "json",
        },
        validate=lambda d: d.get("stat") == "OK",
    )
    if not data or data.get("stat") != "OK":
        return []

    results = []
    for row in data.get("data", []):
        try:
            stock_id = str(row[2]).strip()
            if not stock_id or not stock_id[0].isdigit():
                continue
            p_start, p_end = _parse_period(str(row[6]))
            results.append({
                "stock_id": stock_id,
                "alert_date": _roc_to_date(str(row[1])),
                "alert_type": "disposal",
                "market": "TWSE",
                "cumulative": _parse_int(row[4]),
                "reason": str(row[5]).strip() or None,
                "period_start": p_start,
                "period_end": p_end,
                "measure": str(row[7]).strip() or None,
                "close_price": None,
                "pe_ratio": None,
            })
        except (IndexError, ValueError, TypeError):
            continue
    return results


# ---------------------------------------------------------------------------
# TPEx
# ---------------------------------------------------------------------------

def _tpex_date(d: date) -> str:
    return d.strftime("%Y/%m/%d")


def _fetch_tpex_attention(start: date, end: date) -> list[dict]:
    data = post_json_retry(
        TPEX_ATTENTION_URL,
        data={
            "startDate": _tpex_date(start), "endDate": _tpex_date(end),
            "code": "", "cate": "", "order": "date",
            "id": "", "response": "json",
        },
        validate=lambda d: d.get("tables") is not None,
    )
    if not data or not data.get("tables"):
        return []

    results = []
    rows = data["tables"][0].get("data", []) if data["tables"] else []
    for row in rows:
        try:
            stock_id = str(row[1]).strip()
            if not stock_id or not stock_id[0].isdigit():
                continue
            results.append({
                "stock_id": stock_id,
                "alert_date": _roc_to_date(str(row[5])),
                "alert_type": "attention",
                "market": "TPEx",
                "cumulative": _parse_int(row[3]),
                "reason": str(row[4]).strip() or None,
                "period_start": None,
                "period_end": None,
                "measure": None,
                "close_price": _parse_float(row[6]),
                "pe_ratio": _parse_float(row[7]),
            })
        except (IndexError, ValueError, TypeError):
            continue
    return results


def _fetch_tpex_disposal(start: date, end: date) -> list[dict]:
    data = post_json_retry(
        TPEX_DISPOSAL_URL,
        data={
            "startDate": _tpex_date(start), "endDate": _tpex_date(end),
            "code": "", "cate": "", "type": "all",
            "reason": "-1", "measure": "-1", "order": "date",
            "id": "", "response": "json",
        },
        validate=lambda d: d.get("tables") is not None,
    )
    if not data or not data.get("tables"):
        return []

    results = []
    rows = data["tables"][0].get("data", []) if data["tables"] else []
    for row in rows:
        try:
            stock_id = str(row[2]).strip()
            if not stock_id or not stock_id[0].isdigit():
                continue
            p_start, p_end = _parse_period(str(row[5]))
            results.append({
                "stock_id": stock_id,
                "alert_date": _roc_to_date(str(row[1])),
                "alert_type": "disposal",
                "market": "TPEx",
                "cumulative": _parse_int(row[4]),
                "reason": str(row[6]).strip() or None,
                "period_start": p_start,
                "period_end": p_end,
                "measure": str(row[7]).strip() or None,
                "close_price": _parse_float(row[8]),
                "pe_ratio": _parse_float(row[9]),
            })
        except (IndexError, ValueError, TypeError):
            continue
    return results


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

_UPSERT_SQL = """
    INSERT INTO tw.stock_alerts (
        stock_id, alert_date, alert_type, market,
        cumulative, reason, period_start, period_end,
        measure, close_price, pe_ratio
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (stock_id, alert_date, alert_type, market) DO UPDATE SET
        cumulative   = EXCLUDED.cumulative,
        reason       = EXCLUDED.reason,
        period_start = EXCLUDED.period_start,
        period_end   = EXCLUDED.period_end,
        measure      = EXCLUDED.measure,
        close_price  = EXCLUDED.close_price,
        pe_ratio     = EXCLUDED.pe_ratio
"""


def _save(records: list[dict]) -> int:
    if not records:
        return 0
    saved = 0
    with get_cursor() as cur:
        for r in records:
            if not r["alert_date"]:
                continue
            cur.execute(_UPSERT_SQL, (
                r["stock_id"], r["alert_date"], r["alert_type"], r["market"],
                r["cumulative"], r["reason"], r["period_start"], r["period_end"],
                r["measure"], r["close_price"], r["pe_ratio"],
            ))
            saved += 1
    return saved


# ---------------------------------------------------------------------------
# Daily hook
# ---------------------------------------------------------------------------

def scrape_date(trade_date: date) -> ScrapeResult:
    """Fetch attention + disposal alerts for a single date from TWSE and TPEx."""
    print(f"Fetching stock alerts for {trade_date} ...")

    twse_att = _fetch_twse_notice(trade_date, trade_date)
    twse_dis = _fetch_twse_punish(trade_date, trade_date)
    tpex_att = _fetch_tpex_attention(trade_date, trade_date)
    tpex_dis = _fetch_tpex_disposal(trade_date, trade_date)

    all_records = twse_att + twse_dis + tpex_att + tpex_dis
    saved = _save(all_records)

    print(f"  Alerts: TWSE att={len(twse_att)} dis={len(twse_dis)}, "
          f"TPEx att={len(tpex_att)} dis={len(tpex_dis)}, saved={saved}")

    return ScrapeResult(records=saved, api_rows=len(all_records), parse_errors=0)


# ---------------------------------------------------------------------------
# Range fetch (for backfill)
# ---------------------------------------------------------------------------

def scrape_range(start: date, end: date) -> int:
    """Fetch all alerts in a date range (bulk, not per-day)."""
    print(f"Fetching stock alerts {start} ~ {end} ...")

    twse_att = _fetch_twse_notice(start, end)
    twse_dis = _fetch_twse_punish(start, end)
    tpex_att = _fetch_tpex_attention(start, end)
    tpex_dis = _fetch_tpex_disposal(start, end)

    all_records = twse_att + twse_dis + tpex_att + tpex_dis
    saved = _save(all_records)

    print(f"  TWSE att={len(twse_att)} dis={len(twse_dis)}, "
          f"TPEx att={len(tpex_att)} dis={len(tpex_dis)}, saved={saved}")
    return saved


if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 3:
        scrape_range(date.fromisoformat(sys.argv[1]), date.fromisoformat(sys.argv[2]))
    elif len(sys.argv) >= 2:
        scrape_date(date.fromisoformat(sys.argv[1]))
    else:
        scrape_date(date.today())
