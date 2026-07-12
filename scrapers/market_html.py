"""
Market quotes that only exist as scraped HTML — shipping rates and memory
spot prices. Everything with a real data feed lives in market_quote.py; this
module is the fragile half, deliberately kept separate.

Three unrelated sources, each wrapped in its own try/except so one site's
redesign can't take the other two down with it:

  bdi        TradingEconomics /commodity/baltic — the page's meta description
             carries the number ("Baltic Dry rose to 2,944 Index Points on
             July 10, 2026, up 1.17%"). Baltic Exchange itself is behind
             Akamai and every free history API is paywalled, so we can only
             read today's print — the series here grows one row per run
             rather than arriving with a backfill. BDRY (in market_quote.py)
             is the ETF proxy that carries long history in the meantime.
  fbx        Freightos FBX global container index, from a JSON blob embedded
             in the landing page. Stands in for SCFI, which sits behind the
             Shanghai Shipping Exchange member login.
  dram_ddr5  DRAMeXchange homepage spot tables (server-rendered). Session
  nand_mlc   average of the bellwether contract in each table. Same-day only.

All four accumulate history from first run onward; none can be backfilled.

Usage:
  python -m scrapers.market_html
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime

from db.connection import get_cursor
from scrapers.market_quote import save_rows
from utils.format_shift import ScrapeResult
from utils.http_client import fetch

BDI_URL  = "https://tradingeconomics.com/commodity/baltic"
FBX_URL  = "https://fbx.freightos.com/"
DRAM_URL = "https://www.dramexchange.com/"

# Same shape as market_quote.SYMBOLS — export/generate.py merges the two.
#
# freq is what stops the frontend from labelling a week-on-week move as a
# daily one: FBX only prints on Fridays, so its consecutive points are a week
# apart, not a day.
HTML_SYMBOLS = {
    "bdi":       {"name": "BDI 波羅的海",  "category": "shipping", "unit": "點",     "dp": 0, "freq": "daily"},
    "fbx":       {"name": "FBX 貨櫃運價",  "category": "shipping", "unit": "USD/FEU", "dp": 0, "freq": "weekly"},
    "dram_ddr5": {"name": "DRAM DDR5 16Gb", "category": "memory",  "unit": "USD",    "dp": 3, "freq": "daily"},
    "nand_mlc":  {"name": "NAND MLC 64Gb",  "category": "memory",  "unit": "USD",    "dp": 3, "freq": "daily"},
}

# The row each spot table is read from. DRAMeXchange lists several contracts;
# these are the mainstream ones the industry quotes.
DRAM_TABLE = ("tb_NationalDramSpotPrice",  "DDR5 16Gb (2Gx8) 4800/5600")
NAND_TABLE = ("tb_NationalFlashSpotPrice", "MLC 64Gb 8GBx8")


def _strip_tags(html: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", html)).strip()


def _scrape_bdi() -> tuple[date, float] | None:
    """Value + its own print date, both out of the meta description — the
    date matters because TradingEconomics keeps serving the last print on
    days the index doesn't publish, and we'd otherwise store a stale number
    under today's date."""
    resp = fetch(BDI_URL, timeout=30)
    if resp is None:
        return None
    m = re.search(
        r"([\d,]+(?:\.\d+)?)\s*Index Points on (\w+ \d{1,2},? \d{4})", resp.text
    )
    if not m:
        print("  BDI: number not found in page (source layout changed?)")
        return None
    value = float(m.group(1).replace(",", ""))
    try:
        d = datetime.strptime(m.group(2).replace(",", ""), "%B %d %Y").date()
    except ValueError:
        print(f"  BDI: unparsable date {m.group(2)!r}")
        return None
    return d, value


def _scrape_fbx() -> list[tuple[date, float]] | None:
    """FBX is weekly (Fridays). The page's chart data is a JSON array with
    one dated point per week, which is both a free backfill and the only
    trustworthy date: the big "$3,715" headline label next to it is the
    PREVIOUS week's print, so reading that and stamping it with today's date
    silently stores a stale number."""
    resp = fetch(FBX_URL, timeout=30)
    if resp is None:
        return None
    m = re.search(
        r"window\.frProductIntroChartData\[[^\]]+\]\s*=\s*(\[.*?\]);", resp.text, re.S
    )
    if not m:
        print("  FBX: chart data not found in page (source layout changed?)")
        return None
    try:
        points = json.loads(m.group(1))
    except ValueError:
        print("  FBX: chart data is not valid JSON")
        return None

    out = []
    for p in points:
        if p.get("ticker") != "FBX":
            continue
        try:
            out.append((date.fromisoformat(p["indexDate"]), float(p["value"])))
        except (KeyError, TypeError, ValueError):
            continue
    if not out:
        print("  FBX: chart data carried no usable FBX points")
        return None
    return out


def _table_date(html: str, anchor: int) -> date | None:
    """The 'Last Update: Jul.10 2026 18:10 (GMT+8)' stamp printed above a
    spot table. Taking the LAST one before the table's anchor is what binds
    the date to this table rather than to some other table further up the
    page — and reading it at all is what stops a source that hasn't
    refreshed from being written in under today's date."""
    stamps = list(re.finditer(r"Last Update:\s*(\w{3})\.(\d{1,2})\s+(\d{4})",
                              html[:anchor]))
    if not stamps:
        return None
    mon, day, year = stamps[-1].groups()
    try:
        return datetime.strptime(f"{mon} {day} {year}", "%b %d %Y").date()
    except ValueError:
        return None


def _spot_row(html: str, table_id: str, item: str) -> tuple[date, float] | None:
    """One row's 'Session Average' (6th cell) out of a DRAMeXchange spot
    table, stamped with that table's own Last Update date."""
    # Anchor on the id attribute, not the bare string — the table id also
    # appears earlier in the page's nav/script and matching that lands us
    # 30KB short of the actual table.
    anchor = html.find(f'id="{table_id}"')
    if anchor < 0:
        print(f"  {table_id}: table not found")
        return None
    d = _table_date(html, anchor)
    if d is None:
        print(f"  {table_id}: no Last Update stamp; refusing to guess the date")
        return None
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", html[anchor:anchor + 8000], re.S):
        cells = [_strip_tags(c) for c in
                 re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S)]
        cells = [c for c in cells if c]
        if cells and cells[0] == item:
            try:
                return d, float(cells[5])
            except (IndexError, ValueError):
                print(f"  {table_id}: row {item!r} has no parsable average: {cells}")
                return None
    print(f"  {table_id}: row {item!r} not present")
    return None


def _scrape_memory() -> dict[str, tuple[date, float]]:
    resp = fetch(DRAM_URL, timeout=30)
    if resp is None:
        return {}
    out = {}
    for symbol, (table_id, item) in (
        ("dram_ddr5", DRAM_TABLE),
        ("nand_mlc",  NAND_TABLE),
    ):
        row = _spot_row(resp.text, table_id, item)
        if row is not None:
            out[symbol] = row
    return out


def scrape_date(trade_date: date) -> ScrapeResult:
    """Each source is independent — one failing is counted and skipped so a
    dead site doesn't cost us the other quotes. All FOUR failing does raise:
    these are unofficial HTML scrapes that will eventually break, and a
    silent success would freeze the page with nobody noticing.

    trade_date is deliberately unused. Every source here stamps its own date
    (BDI's meta line, FBX's indexDate, DRAMeXchange's Last Update), because
    daily_update supports backfill runs — and writing today's spot price
    under a backfilled date would be permanent, unfixable corruption in
    series that cannot be re-fetched.
    """
    rows: list[tuple[str, date, float]] = []
    failed: list[str] = []

    try:
        bdi = _scrape_bdi()
        if bdi:
            rows.append(("bdi", bdi[0], bdi[1]))
        else:
            failed.append("bdi")
    except Exception as e:
        print(f"  BDI failed: {e}")
        failed.append("bdi")

    try:
        fbx = _scrape_fbx()
        if fbx:
            rows.extend(("fbx", d, v) for d, v in fbx)
        else:
            failed.append("fbx")
    except Exception as e:
        print(f"  FBX failed: {e}")
        failed.append("fbx")

    try:
        mem = _scrape_memory()
        rows.extend((s, d, v) for s, (d, v) in mem.items())
        failed.extend(s for s in ("dram_ddr5", "nand_mlc") if s not in mem)
    except Exception as e:
        print(f"  Memory spot failed: {e}")
        failed.extend(("dram_ddr5", "nand_mlc"))

    if len(failed) == len(HTML_SYMBOLS):
        raise RuntimeError(
            "market_html: every source failed "
            f"({', '.join(failed)}) — all three sites changed at once, or "
            "we are being blocked"
        )

    saved = save_rows(rows)
    print(f"  market_html: saved {saved} rows "
          f"(failed: {', '.join(failed) or 'none'})")
    return ScrapeResult(records=saved, api_rows=len(rows), parse_errors=len(failed))


if __name__ == "__main__":
    scrape_date(date.today())
