"""Short-sale suspension / 融券最後回補日 calendar scraper (停券預告表).

Forward-looking announcement board (rolling ~5-week window, no history, no date
param), so each run re-upserts the whole board — same shape as dividend_calendar.

Sources:
  TWSE: GET /rwd/zh/marginTrading/BFI84U?response=json
        fields [0]股票代號 [1]股票名稱 [2]停券起日(最後回補日) [3]停券迄日 [4]原因
        ROC dotted dates ('115.07.15').
  TPEx: GET /openapi/v1/tpex_margin_trading_term  (JSON array)
        keys SecuritiesCompanyCode / CompanyName / ShortSaleSuspensionStartDate
             / ShortSaleSuspensionEndDate / Reason   (ROC 'yyyymmdd')

停券起日 == 融券最後回補日 (the tradeable anchor). Writes tw.short_cover_calendar
with source in ('BFI84U','tpex_term'), is_derived=FALSE. 股東會 rows appear only in
AGM season (May–Jun). Historical AGM covering dates are backfilled separately
(mops_agm_derived, is_derived=TRUE).
"""

import re
from datetime import date

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json_retry

TWSE_URL = "https://www.twse.com.tw/rwd/zh/marginTrading/BFI84U"
TPEX_URL = "https://www.tpex.org.tw/openapi/v1/tpex_margin_trading_term"


def _roc_to_date(roc_str) -> date | None:
    """'115.07.15' or '1150715' -> date(2026, 7, 15)."""
    digits = re.sub(r"\D", "", str(roc_str))
    if len(digits) < 7:
        return None
    try:
        return date(int(digits[:-4]) + 1911, int(digits[-4:-2]), int(digits[-2:]))
    except ValueError:
        return None


def _norm_reason(val) -> str:
    """Collapse the raw reason string to a small stable set (keeps AGM detection
    reliable for the covering-squeeze study)."""
    s = str(val or "").strip()
    if not s:
        return "其他"
    if "股東" in s:
        return "股東會"
    if "減資" in s:
        return "減資"
    if "增資" in s or "現增" in s:
        return "現增"
    if "除權息" in s:
        return "除權息"
    if "除權" in s:
        return "除權"
    if "除息" in s or "分配收益" in s or "配息" in s:
        return "除息"
    return s


def _fetch_twse() -> list[dict]:
    data = fetch_json_retry(
        TWSE_URL,
        params={"response": "json"},
        validate=lambda d: isinstance(d, dict) and d.get("stat") == "OK",
    )
    if not isinstance(data, dict) or data.get("stat") != "OK":
        return []
    results = []
    for row in data.get("data", []):
        try:
            stock_id = str(row[0]).strip()
            if not stock_id or not stock_id[0].isdigit():
                continue
            results.append({
                "stock_id": stock_id,
                "last_cover_date": _roc_to_date(row[2]),
                "suspension_end": _roc_to_date(row[3]),
                "reason": _norm_reason(row[4]),
                "market": "TWSE",
                "source": "BFI84U",
            })
        except (IndexError, ValueError, TypeError):
            continue
    return results


def _fetch_tpex() -> list[dict]:
    data = fetch_json_retry(TPEX_URL, validate=lambda d: isinstance(d, list))
    if not isinstance(data, list):
        return []
    results = []
    for row in data:
        try:
            stock_id = str(row.get("SecuritiesCompanyCode", "")).strip()
            if not stock_id or not stock_id[0].isdigit():
                continue
            results.append({
                "stock_id": stock_id,
                "last_cover_date": _roc_to_date(row.get("ShortSaleSuspensionStartDate")),
                "suspension_end": _roc_to_date(row.get("ShortSaleSuspensionEndDate")),
                "reason": _norm_reason(row.get("Reason")),
                "market": "TPEx",
                "source": "tpex_term",
            })
        except (AttributeError, ValueError, TypeError):
            continue
    return results


_UPSERT_SQL = """
    INSERT INTO tw.short_cover_calendar (
        stock_id, last_cover_date, suspension_end, reason, market, source, is_derived
    )
    VALUES (%s, %s, %s, %s, %s, %s, FALSE)
    ON CONFLICT (stock_id, last_cover_date, reason) DO UPDATE SET
        suspension_end = EXCLUDED.suspension_end,
        market         = EXCLUDED.market,
        source         = EXCLUDED.source,
        is_derived     = FALSE,
        updated_at     = NOW()
"""


def _save(records: list[dict]) -> int:
    saved = 0
    with get_cursor() as cur:
        for r in records:
            if not r["last_cover_date"]:
                continue
            cur.execute(_UPSERT_SQL, (
                r["stock_id"], r["last_cover_date"], r["suspension_end"],
                r["reason"], r["market"], r["source"],
            ))
            saved += 1
    return saved


def scrape_date(trade_date: date) -> ScrapeResult:
    """Refresh the whole 停券預告 board. trade_date is ignored (source only serves
    the current rolling window) — kept for the daily_update registry signature."""
    print(f"Fetching short-cover calendar ({trade_date}) ...")
    twse = _fetch_twse()
    tpex = _fetch_tpex()
    records = twse + tpex
    saved = _save(records)
    print(f"  Short-cover calendar: TWSE={len(twse)} TPEx={len(tpex)}, saved={saved}")
    return ScrapeResult(records=saved, api_rows=len(records), parse_errors=0)


if __name__ == "__main__":
    scrape_date(date.today())
