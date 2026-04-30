"""
Industry classification scraper — populate tw.stocks.industry.

Data source: TWSE ISIN service (official), which lists every listed security
with its industry category (產業別). Encoding is Big5.

URLs:
  strMode=2 → 上市 (TWSE listed)
  strMode=4 → 上櫃 (TPEx listed)

Usage:
  python -m scrapers.industry
"""

import re

import requests

from db.connection import get_cursor


ISIN_URL = "https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode}"


def _fetch_mode(mode: str) -> list[tuple[str, str, str]]:
    """Return list of (stock_id, name, industry) for one ISIN mode."""
    url = ISIN_URL.format(mode=mode)
    r = requests.get(url, timeout=60)
    r.encoding = "big5"
    text = r.text

    rows = re.findall(r"<tr>(.*?)</tr>", text, re.DOTALL)
    result = []
    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
        if len(cells) < 6:
            continue
        code_name = re.sub(r"<[^>]+>", "", cells[0]).strip()
        if "\u3000" not in code_name:
            continue
        parts = code_name.split("\u3000", 1)
        if len(parts) != 2:
            continue
        code = parts[0].strip()
        name = parts[1].strip()
        # 4-digit codes = ordinary stocks; skip warrants/ETF/TDR/etc.
        if not re.match(r"^\d{4}$", code):
            continue
        industry = re.sub(r"<[^>]+>", "", cells[4]).strip()
        if not industry:
            continue
        result.append((code, name, industry))
    return result


def scrape_industries() -> int:
    """Fetch industry classifications for TWSE + TPEx and update tw.stocks."""
    all_records: dict[str, tuple[str, str]] = {}   # stock_id → (name, industry)
    for mode, market in (("2", "TWSE"), ("4", "TPEx")):
        records = _fetch_mode(mode)
        print(f"  {market}: {len(records)} stocks with industry")
        for code, name, industry in records:
            all_records[code] = (name, industry)

    if not all_records:
        print("No records fetched.")
        return 0

    updated = 0
    with get_cursor() as cur:
        for sid, (name, industry) in all_records.items():
            cur.execute(
                "UPDATE tw.stocks SET industry = %s, updated_at = NOW() "
                "WHERE stock_id = %s AND (industry IS NULL OR industry <> %s)",
                (industry, sid, industry),
            )
            updated += cur.rowcount
    print(f"Updated industry for {updated} stocks (from {len(all_records)} records).")
    return updated


if __name__ == "__main__":
    scrape_industries()
