"""Exchange holiday schedule scraper (TWSE OpenAPI holidaySchedule).

Source: GET https://openapi.twse.com.tw/v1/holidaySchedule/holidaySchedule
        JSON array, keys Name / Date (ROC 'yyyymmdd') / Weekday / Description.

The source serves the current year only and takes no date parameter, so each
run re-upserts the whole list and rows from earlier years stay in the table.

The list is not only closures: it also carries markers for trading days
('國曆新年開始交易日', '農曆春節前最後交易日', '農曆春節後開始交易日'). Those are
stored with is_closed=FALSE. Everything else is a closure, including
'市場無交易，僅辦理結算交割作業'. Unscheduled closures (typhoon days) are not in
the source.

Writes tw.market_holidays, read by the trading-day gate in daily_update.py and
by the daily health check.
"""

import re
from datetime import date

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json_retry

URL = "https://openapi.twse.com.tw/v1/holidaySchedule/holidaySchedule"

# An entry whose name carries this is a trading day, not a closure.
TRADING_DAY_MARK = "交易日"


def _roc_to_date(roc_str) -> date | None:
    """'1150101' -> date(2026, 1, 1)."""
    digits = re.sub(r"\D", "", str(roc_str))
    if len(digits) < 7:
        return None
    try:
        return date(int(digits[:-4]) + 1911, int(digits[-4:-2]), int(digits[-2:]))
    except ValueError:
        return None


def parse(rows: list[dict], near_year: int | None = None) -> tuple[dict[date, dict], int]:
    """Collapse the source list to one record per date.

    Returns ({date: {"name", "description", "is_closed"}}, parse_errors).
    When a date is listed more than once, any closure entry makes it closed.
    With near_year, a date more than a year away from it is a parse error: a
    change of date format (a Gregorian '20260101' reads as ROC year 2026) must
    not land in the table as a closure in the year 3937.
    """
    by_date: dict[date, dict] = {}
    errors = 0
    for row in rows:
        try:
            name = str(row["Name"]).strip()
            d = _roc_to_date(row["Date"])
        except (KeyError, TypeError):
            errors += 1
            continue
        if d is None or not name:
            errors += 1
            continue
        if near_year is not None and abs(d.year - near_year) > 1:
            errors += 1
            continue
        description = re.sub(r"<br\s*/?>", "", str(row.get("Description") or "")).strip()
        record = {
            "name": name,
            "description": description or None,
            "is_closed": TRADING_DAY_MARK not in name,
        }
        seen = by_date.get(d)
        if seen is None or (record["is_closed"] and not seen["is_closed"]):
            by_date[d] = record
    return by_date, errors


_UPSERT_SQL = """
    INSERT INTO tw.market_holidays (holiday_date, name, description, is_closed, source)
    VALUES (%s, %s, %s, %s, 'twse_calendar')
    ON CONFLICT (holiday_date) DO UPDATE SET
        name        = EXCLUDED.name,
        description = EXCLUDED.description,
        is_closed   = EXCLUDED.is_closed,
        source      = EXCLUDED.source,
        updated_at  = NOW()
"""


def _save(by_date: dict[date, dict]) -> int:
    with get_cursor() as cur:
        for d, r in sorted(by_date.items()):
            cur.execute(_UPSERT_SQL, (d, r["name"], r["description"], r["is_closed"]))
    return len(by_date)


def scrape_date(trade_date: date) -> ScrapeResult:
    """Refresh the holiday schedule. trade_date is ignored (the source only
    serves the current year) — kept for the daily_update registry signature."""
    print(f"Fetching market holidays ({trade_date}) ...")
    data = fetch_json_retry(URL, validate=lambda d: isinstance(d, list) and len(d) > 0)
    rows = data if isinstance(data, list) else []
    this_year = date.today().year
    by_date, errors = parse(rows, near_year=this_year)
    saved = _save(by_date)
    closed = sum(r["is_closed"] for r in by_date.values())
    print(f"  Market holidays: api={len(rows)}, saved={saved} (closed={closed})")
    # The source answering with last year's list is a full, parseable response
    # that leaves this year without a calendar. Name it, so the nightly summary
    # shows a failure instead of 27 rows saved.
    stale = by_date and max(d.year for d in by_date) < this_year
    return ScrapeResult(
        records=saved, api_rows=len(rows), parse_errors=errors,
        failed_sources=(f"{this_year} 年休市日曆（來源仍是舊年度）",) if stale else (),
    )


if __name__ == "__main__":
    scrape_date(date.today())
