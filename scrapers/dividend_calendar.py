"""
Ex-dividend / ex-rights calendar scraper (除權除息預告表).

Forward-looking announcement board — both sources publish a rolling window
covering the next ~5 weeks, so each run re-upserts the whole board rather
than fetching a single date.

Sources:
  TWSE: GET  /rwd/zh/exRight/TWT48U?response=json
        fields [0]除權除息日期(ROC) [1]股票代號 [2]名稱 [3]除權息 [4]無償配股率
               [5]現增配股率 [6]現增認購價 [7]現金股利 ...
        現金股利 is an HTML blurb ("需公告實際配息金額") until the amount is
        declared — parsed to NULL, not 0.
  TPEx: GET  /openapi/v1/tpex_exright_prepost   (JSON array)
        keys ExRrightsExDividendDate(ROC) / SecuritiesCompanyCode / CompanyName
             / ExRrightsExDividend / StockDividendRatio / CashDividend

Writes tw.dividend_calendar. Feeds the Telegram morning-brief ex-dividend
alert; tw.dividends (FinMind, historical) is a separate table and untouched.
"""

import re
from datetime import date

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json_retry

TWSE_URL = "https://www.twse.com.tw/rwd/zh/exRight/TWT48U"
TPEX_URL = "https://www.tpex.org.tw/openapi/v1/tpex_exright_prepost"

_TAG_RE = re.compile(r"<[^>]+>")


def _parse_float(val) -> float | None:
    """Numeric cell -> float. Returns None for blanks, dashes, and the HTML
    placeholder TWSE uses before the payout is declared."""
    s = _TAG_RE.sub("", str(val)).replace(",", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _roc_to_date(roc_str: str) -> date | None:
    """'115年07月09日' or '1150709' -> date(2026, 7, 9)."""
    digits = re.sub(r"\D", "", str(roc_str))
    if len(digits) < 7:
        return None
    try:
        return date(int(digits[:-4]) + 1911, int(digits[-4:-2]), int(digits[-2:]))
    except ValueError:
        return None


def _norm_kind(val) -> str | None:
    """TWSE says 息/權/權息; TPEx says 除息/除權/除權息. Normalise to the former."""
    s = str(val).strip().lstrip("除")
    return s or None


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
            stock_id = str(row[1]).strip()
            if not stock_id or not stock_id[0].isdigit():
                continue
            results.append({
                "stock_id": stock_id,
                "ex_date": _roc_to_date(row[0]),
                "market": "TWSE",
                "name": str(row[2]).strip() or None,
                "kind": _norm_kind(row[3]),
                "cash_dividend": _parse_float(row[7]),
                "stock_ratio": _parse_float(row[4]),
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
                "ex_date": _roc_to_date(row.get("ExRrightsExDividendDate")),
                "market": "TPEx",
                "name": str(row.get("CompanyName", "")).strip() or None,
                "kind": _norm_kind(row.get("ExRrightsExDividend")),
                "cash_dividend": _parse_float(row.get("CashDividend")),
                "stock_ratio": _parse_float(row.get("StockDividendRatio")),
            })
        except (AttributeError, ValueError, TypeError):
            continue
    return results


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

_UPSERT_SQL = """
    INSERT INTO tw.dividend_calendar (
        stock_id, ex_date, market, name, kind, cash_dividend, stock_ratio
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (stock_id, ex_date) DO UPDATE SET
        market        = EXCLUDED.market,
        name          = EXCLUDED.name,
        kind          = EXCLUDED.kind,
        cash_dividend = EXCLUDED.cash_dividend,
        stock_ratio   = EXCLUDED.stock_ratio,
        updated_at    = NOW()
"""


def _save(records: list[dict]) -> int:
    if not records:
        return 0
    saved = 0
    with get_cursor() as cur:
        for r in records:
            if not r["ex_date"]:
                continue
            cur.execute(_UPSERT_SQL, (
                r["stock_id"], r["ex_date"], r["market"], r["name"],
                r["kind"], r["cash_dividend"], r["stock_ratio"],
            ))
            saved += 1
    return saved


# ---------------------------------------------------------------------------
# Daily hook
# ---------------------------------------------------------------------------

def scrape_date(trade_date: date) -> ScrapeResult:
    """Refresh the whole announcement board. trade_date is ignored (both
    sources only serve the current rolling window) and kept for the
    daily_update registry's uniform scrape_date(date) signature."""
    print(f"Fetching dividend calendar ({trade_date}) ...")

    twse = _fetch_twse()
    tpex = _fetch_tpex()
    records = twse + tpex
    saved = _save(records)

    print(f"  Dividend calendar: TWSE={len(twse)} TPEx={len(tpex)}, saved={saved}")
    return ScrapeResult(records=saved, api_rows=len(records), parse_errors=0)


if __name__ == "__main__":
    scrape_date(date.today())
