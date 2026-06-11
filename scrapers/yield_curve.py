"""
US Treasury yield curve scraper.

Source: U.S. Department of the Treasury daily yield curve CSV.

    https://home.treasury.gov/resource-center/data-chart-center/interest-rates/
        daily-treasury-rates.csv/{year}/all
        ?type=daily_treasury_yield_curve&field_tdr_date_value={year}&page&_format=csv

This is the upstream FRED itself republishes — going direct skips
FRED's CDN, avoids the periodic fredgraph 504s, and yields one file
per calendar year with every maturity on every row.

CSV format (first three lines example):
    Date,"1 Mo","1.5 Month","2 Mo","3 Mo","4 Mo","6 Mo","1 Yr","2 Yr",...,"10 Yr",...
    06/10/2026,3.69,3.70,3.72,3.79,3.80,3.82,3.90,4.13,...,4.55,...
    06/09/2026,...

We only persist the three maturities the dashboard needs (DGS-style
column names from the original FRED-based plan: 3 Mo → dgs3mo,
2 Yr → dgs2, 10 Yr → dgs10) and ignore the rest. Empty cells (rare —
Treasury occasionally leaves a maturity blank) become NULL.

Every run pulls the current calendar year plus the previous one, so we
always have ≥252 trading days available regardless of when in the year
we run for the first time.
"""

from __future__ import annotations

import csv
import io
from datetime import date
from typing import Optional

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from utils.http_client import fetch

CSV_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
    "daily-treasury-rates.csv/{year}/all"
    "?type=daily_treasury_yield_curve&field_tdr_date_value={year}&page&_format=csv"
)

# Treasury CSV column header → DB column name. Whatever else the CSV
# carries gets dropped silently.
COLUMN_MAP = {
    "3 Mo":  "dgs3mo",
    "2 Yr":  "dgs2",
    "10 Yr": "dgs10",
}


def _fetch_year(year: int) -> Optional[str]:
    """Pull one year of daily yield-curve CSV. Returns the decoded text
    or None on fetch failure."""
    url = CSV_URL.format(year=year)
    resp = fetch(url, timeout=30)
    if resp is None:
        print(f"  Treasury {year}: fetch failed")
        return None
    return resp.text


def _parse_csv(text: str) -> dict[date, dict[str, float | None]]:
    """Parse one yearly CSV. Returns {trade_date: {col: val}} keeping
    only the maturities in COLUMN_MAP."""
    out: dict[date, dict[str, float | None]] = {}
    reader = csv.reader(io.StringIO(text))
    header = next(reader, None)
    if not header or header[0].strip().lower() != "date":
        print(f"  Treasury: unexpected header {header}")
        return out

    # Build index of which CSV column to read into which DB column.
    wanted: list[tuple[int, str]] = []
    for idx, name in enumerate(header):
        col = COLUMN_MAP.get(name.strip())
        if col is not None:
            wanted.append((idx, col))

    if not wanted:
        print(f"  Treasury: no target maturities found in header {header}")
        return out

    for row in reader:
        if not row or not row[0].strip():
            continue
        try:
            m, d, y = row[0].strip().split("/")
            trade_date = date(int(y), int(m), int(d))
        except (ValueError, IndexError):
            continue
        vals: dict[str, float | None] = {}
        for idx, col in wanted:
            if idx >= len(row):
                vals[col] = None
                continue
            raw = row[idx].strip()
            try:
                vals[col] = float(raw) if raw else None
            except ValueError:
                vals[col] = None
        out[trade_date] = vals
    return out


def save_rows(per_date: dict[date, dict[str, float | None]]) -> int:
    """Upsert one row per trade_date. The COALESCE-in-update keeps
    columns we already have if Treasury later serves a row with one
    maturity missing."""
    if not per_date:
        return 0
    sql = """
        INSERT INTO tw.yield_curve (trade_date, dgs10, dgs2, dgs3mo)
        VALUES (%(trade_date)s, %(dgs10)s, %(dgs2)s, %(dgs3mo)s)
        ON CONFLICT (trade_date) DO UPDATE SET
            dgs10      = COALESCE(EXCLUDED.dgs10,  tw.yield_curve.dgs10),
            dgs2       = COALESCE(EXCLUDED.dgs2,   tw.yield_curve.dgs2),
            dgs3mo     = COALESCE(EXCLUDED.dgs3mo, tw.yield_curve.dgs3mo),
            fetched_at = NOW()
    """
    rows = [
        {
            "trade_date": d,
            "dgs10":  vals.get("dgs10"),
            "dgs2":   vals.get("dgs2"),
            "dgs3mo": vals.get("dgs3mo"),
        }
        for d, vals in sorted(per_date.items())
    ]
    with get_cursor() as cur:
        from psycopg2.extras import execute_batch
        execute_batch(cur, sql, rows, page_size=500)
    return len(rows)


def scrape_date(trade_date: date) -> ScrapeResult:
    """Pull the current calendar year plus the previous one; both
    upserts are idempotent. trade_date informs which year we ask for
    but the daily run window is the past two calendar years."""
    years = [trade_date.year, trade_date.year - 1]
    per_date: dict[date, dict[str, float | None]] = {}
    api_rows = 0
    parse_errors = 0

    for year in years:
        text = _fetch_year(year)
        if text is None:
            parse_errors += 1
            continue
        parsed = _parse_csv(text)
        if not parsed:
            parse_errors += 1
            continue
        api_rows += len(parsed)
        per_date.update(parsed)

    saved = save_rows(per_date)
    print(f"  Treasury yield_curve: saved {saved} dates "
          f"(api={api_rows}, year_errors={parse_errors})")
    return ScrapeResult(records=saved, api_rows=api_rows, parse_errors=parse_errors)


if __name__ == "__main__":
    scrape_date(date.today())
