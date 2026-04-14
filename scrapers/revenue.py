"""
Monthly revenue scraper for TWSE/TPEx listed companies.

Data source: MOPS (公開資訊觀測站)
URL pattern: https://emops.twse.com.tw/nas/t21/{market}/t21sc03_{roc_year}_{month}_0.html

Revenue is reported in thousands of NTD (千元).
Companies must publish previous month's revenue by the 10th of each month.
"""

import re
from datetime import date

from db.connection import get_cursor
from utils.http_client import fetch


MOPS_URL_NEW = "https://emops.twse.com.tw/nas/t21/{market}/t21sc03_{year}_{month}_0.html"
MOPS_URL_OLD = "https://emops.twse.com.tw/nas/t21/{market}/t21sc03_{year}_{month}.html"

# ROC year 98 (2009) and earlier use the old URL without trailing _0
OLD_FORMAT_CUTOFF = 98

# market slug → tw.stocks.market value
MARKET_MAP = {
    "sii": "TWSE",
    "otc": "TPEx",
}


def _parse_num(val: str):
    """Parse a numeric string with commas. Returns None for blanks/dashes."""
    s = val.replace(",", "").strip()
    if not s or s == "-" or s == "N/A":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _fetch_revenue(roc_year: int, month: int, market: str) -> list[dict]:
    """
    Fetch monthly revenue for one market (sii or otc) from MOPS.
    Returns list of parsed records.
    """
    if roc_year <= OLD_FORMAT_CUTOFF:
        url = MOPS_URL_OLD.format(market=market, year=roc_year, month=month)
    else:
        url = MOPS_URL_NEW.format(market=market, year=roc_year, month=month)
    print(f"Fetching {MARKET_MAP[market]} revenue for {roc_year + 1911}-{month:02d} ...")

    resp = fetch(url, timeout=30)
    if resp is None:
        print("  No response from MOPS.")
        return []

    text = resp.content.decode("big5", errors="replace")

    # Extract all <tr>...</tr> blocks
    trs = re.findall(r"<tr[^>]*>(.*?)</tr>", text, re.DOTALL)
    if not trs:
        print("  No table rows found.")
        return []

    records = []
    for tr_html in trs:
        cells = [
            re.sub(r"<[^>]+>", "", c).strip()
            for c in re.findall(r"<td[^>]*>(.*?)</td>", tr_html, re.DOTALL)
        ]
        # Valid data rows have 10 cells: code, name, rev, prev_rev, yago_rev,
        # mom%, yoy%, ytd_rev, yago_ytd_rev, pct_change, note
        if len(cells) < 9:
            continue

        stock_id = cells[0].strip()
        # Skip non-stock rows (headers, totals, etc.)
        if not re.match(r"^\d{4,6}$", stock_id):
            continue

        revenue = _parse_num(cells[2])
        if revenue is None:
            continue

        mom_pct = _parse_num(cells[5])
        # yoy% is not in the data rows; compute from revenue vs last year
        last_year_rev = _parse_num(cells[4])
        if last_year_rev and last_year_rev != 0:
            yoy_pct = round((revenue - last_year_rev) / last_year_rev * 100, 2)
        else:
            yoy_pct = None

        note = cells[9].strip() if len(cells) > 9 else None
        if note == "-":
            note = None

        records.append({
            "stock_id": stock_id,
            "revenue": int(revenue),
            "mom_pct": mom_pct,
            "yoy_pct": yoy_pct,
            "note": note,
        })

    print(f"  Parsed {len(records)} records.")
    return records


def fetch_monthly_revenue(year: int, month: int) -> tuple[list[dict], int]:
    """
    Fetch monthly revenue for all markets (TWSE + TPEx).
    year/month are in Western calendar (e.g. 2026, 3).
    Returns (records, total_api_rows).
    """
    roc_year = year - 1911
    all_records = []

    for market in ("sii", "otc"):
        records = _fetch_revenue(roc_year, month, market)
        all_records.extend(records)

    return all_records, len(all_records)


def save_monthly_revenue(records: list[dict], year: int, month: int):
    """Upsert monthly revenue records into tw.monthly_revenue."""
    if not records:
        print("No revenue records to save.")
        return

    year_month = f"{year}-{month:02d}"

    with get_cursor() as cur:
        for r in records:
            cur.execute("""
                INSERT INTO tw.monthly_revenue
                    (stock_id, year_month, revenue, mom_pct, yoy_pct, note)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_id, year_month)
                DO UPDATE SET
                    revenue = EXCLUDED.revenue,
                    mom_pct = EXCLUDED.mom_pct,
                    yoy_pct = EXCLUDED.yoy_pct,
                    note = EXCLUDED.note
            """, (r["stock_id"], year_month, r["revenue"],
                  r["mom_pct"], r["yoy_pct"], r["note"]))

    print(f"Saved {len(records)} revenue records for {year_month}.")


def scrape_month(year: int, month: int):
    """Fetch and save monthly revenue for a given year/month."""
    records, _ = fetch_monthly_revenue(year, month)
    save_monthly_revenue(records, year, month)
    return len(records)


def scrape_range(start_year: int, start_month: int,
                 end_year: int, end_month: int):
    """Fetch and save monthly revenue for a range of months."""
    y, m = start_year, start_month
    total = 0

    while (y, m) <= (end_year, end_month):
        count = scrape_month(y, m)
        total += count
        # Advance to next month
        m += 1
        if m > 12:
            m = 1
            y += 1

    print(f"\nDone. Total revenue records saved: {total}")
    return total


if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 3:
        # Usage: python -m scrapers.revenue 2026 3
        #        python -m scrapers.revenue 2024 1 2026 3  (range)
        sy, sm = int(sys.argv[1]), int(sys.argv[2])
        if len(sys.argv) >= 5:
            ey, em = int(sys.argv[3]), int(sys.argv[4])
            scrape_range(sy, sm, ey, em)
        else:
            scrape_month(sy, sm)
    else:
        # Default: fetch last month's revenue
        today = date.today()
        m = today.month - 1
        y = today.year
        if m == 0:
            m = 12
            y -= 1
        scrape_month(y, m)
