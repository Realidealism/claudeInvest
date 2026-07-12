"""
Market quotes that only exist as scraped HTML — shipping rates, memory and
panel spot prices, petrochemical feedstock. Everything with a real data feed
lives in market_quote.py; this module is the fragile half, deliberately kept
separate.

Five unrelated sources, each wrapped in its own try/except so one site's
redesign can't take the others down with it:

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
  panel_tv55 TrendForce Large Size Panel Price table. MONTHLY, not daily —
             the page says so ("Change(MoM.) would only be updated at the end
             of every month"), and it stamps its own Last Update date, so a
             daily run just re-upserts the same row until it moves.
  propylene  SunSirs (生意社) China spot, one product page each. Prices are
  styrene    RMB (the site's own "(Unit:RMB)" note; per-tonne is inferred from
  pvc        magnitude and trade convention, NOT stated on the page). These are
             China domestic spot, NOT the CFR North-East Asia landed price
             台塑 actually buys on — correlated, not the same number.
             Ethylene has no free live source: SunSirs' own ethylene series
             stopped updating in 2022 (verified 2026-07-12), so it is absent.

None of the five can be backfilled; they accumulate from the first run onward.
(SunSirs is the mild exception — each product page carries its last ~6 days,
which self-heals a short outage.)

Usage:
  python -m scrapers.market_html
"""

from __future__ import annotations

import html as html_lib
import json
import re
from datetime import date, datetime

from db.connection import get_cursor
from scrapers.market_quote import save_rows
from utils.format_shift import ScrapeResult
from utils.http_client import fetch

BDI_URL     = "https://tradingeconomics.com/commodity/baltic"
FBX_URL     = "https://fbx.freightos.com/"
DRAM_URL    = "https://www.dramexchange.com/"
PANEL_URL   = "https://www.trendforce.com/price/lcd/panel"
SUNSIRS_URL = "https://www.sunsirs.com/uk/prodetail-{pid}.html"

# Same shape as market_quote.SYMBOLS — export/generate.py merges the two.
#
# freq is what stops the frontend from labelling a week-on-week move as a
# daily one: FBX only prints on Fridays, so its consecutive points are a week
# apart, not a day.
#
# Only BDI has a `tv` (TradingView symbol the card links out to); freight rates
# and memory spot prices are not listed instruments anywhere, which is exactly
# why we scrape them off HTML in the first place.
HTML_SYMBOLS = {
    "bdi":        {"name": "BDI 波羅的海",   "category": "shipping", "unit": "點",       "dp": 0, "freq": "daily",   "tv": "INDEX:BDI"},
    "fbx":        {"name": "FBX 貨櫃運價",   "category": "shipping", "unit": "USD/FEU",  "dp": 0, "freq": "weekly"},
    "dram_ddr5":  {"name": "DRAM DDR5 16Gb", "category": "memory",   "unit": "USD",      "dp": 3, "freq": "daily"},
    "nand_mlc":   {"name": "NAND MLC 64Gb",  "category": "memory",   "unit": "USD",      "dp": 3, "freq": "daily"},
    "panel_tv55": {"name": "面板 55吋 UHD",  "category": "panel",    "unit": "USD",      "dp": 1, "freq": "monthly"},
    "propylene":  {"name": "丙烯",           "category": "petro",    "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    "styrene":    {"name": "苯乙烯 SM",      "category": "petro",    "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    "pvc":        {"name": "PVC",            "category": "petro",    "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
}

# The row each spot table is read from. DRAMeXchange lists several contracts;
# these are the mainstream ones the industry quotes.
DRAM_TABLE = ("tb_NationalDramSpotPrice",  "DDR5 16Gb (2Gx8) 4800/5600")
NAND_TABLE = ("tb_NationalFlashSpotPrice", "MLC 64Gb 8GBx8")

# TrendForce prints four tables; the one we want is Large Size Panel Price.
# The 32" number on that page belongs to the STREET PRICE table (retail TV
# sets), not to panels — anchoring on the open-cell spec keeps them apart.
PANEL_APP, PANEL_SPEC = "LCD TV", '55"W UHD Open-Cell'

# SunSirs product ids.
SUNSIRS_PRODUCTS = {"propylene": 505, "styrene": 168, "pvc": 107}


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


def _scrape_panel() -> tuple[date, float] | None:
    """Average price of the bellwether TV panel out of TrendForce's Large Size
    table, stamped with the 'Last Update YYYY-MM-DD' printed above it (the page
    only moves monthly, so writing it under today's date would invent a series
    that never happened)."""
    resp = fetch(PANEL_URL, timeout=30)
    if resp is None:
        return None
    page = resp.text

    for tbl in re.finditer(r"<table[^>]*>(.*?)</table>", page, re.S):
        for row in re.findall(r"<tr[^>]*>(.*?)</tr>", tbl.group(1), re.S):
            cells = [html_lib.unescape(_strip_tags(c)) for c in
                     re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S)]
            cells = [c for c in cells if c]
            if len(cells) < 6 or cells[0] != PANEL_APP or cells[1] != PANEL_SPEC:
                continue
            stamps = re.findall(r"Last Update\s*(\d{4}-\d{2}-\d{2})", page[:tbl.start()])
            if not stamps:
                print("  panel: no Last Update stamp; refusing to guess the date")
                return None
            try:
                # cells: App | Spec | App/Spec | Low | High | Average | ...
                return date.fromisoformat(stamps[-1]), float(cells[5])
            except ValueError:
                print(f"  panel: unparsable row {cells[:6]}")
                return None

    print(f"  panel: row {PANEL_APP} {PANEL_SPEC!r} not found (source layout changed?)")
    return None


def _scrape_sunsirs() -> dict[str, list[tuple[date, float]]]:
    """Each product page lists its last ~6 daily prints as
    <td>Propylene</td><td>Chemical</td><td>8011.00</td><td>2026-07-12</td>.
    Taking every row (not just the newest) is what lets a few days of downtime
    heal themselves — these series cannot be backfilled any other way."""
    out: dict[str, list[tuple[date, float]]] = {}
    for symbol, pid in SUNSIRS_PRODUCTS.items():
        resp = fetch(SUNSIRS_URL.format(pid=pid), timeout=30)
        if resp is None:
            continue
        rows = re.findall(
            r"<td>[^<]+</td>\s*<td>[^<]+</td>\s*<td>([\d.]+)</td>\s*"
            r"<td>(\d{4}-\d{2}-\d{2})</td>",
            resp.text,
        )
        pts = []
        for value, d in rows:
            try:
                pts.append((date.fromisoformat(d), float(value)))
            except ValueError:
                continue
        if pts:
            out[symbol] = pts
        else:
            print(f"  sunsirs {symbol}: no price rows (source layout changed?)")
    return out


def scrape_date(trade_date: date) -> ScrapeResult:
    """Each source is independent — one failing is counted and skipped so a
    dead site doesn't cost us the other quotes. All failing does raise:
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

    try:
        panel = _scrape_panel()
        if panel:
            rows.append(("panel_tv55", panel[0], panel[1]))
        else:
            failed.append("panel_tv55")
    except Exception as e:
        print(f"  Panel failed: {e}")
        failed.append("panel_tv55")

    try:
        petro = _scrape_sunsirs()
        rows.extend((s, d, v) for s, pts in petro.items() for d, v in pts)
        failed.extend(s for s in SUNSIRS_PRODUCTS if s not in petro)
    except Exception as e:
        print(f"  SunSirs failed: {e}")
        failed.extend(SUNSIRS_PRODUCTS)

    if len(failed) == len(HTML_SYMBOLS):
        raise RuntimeError(
            "market_html: every source failed "
            f"({', '.join(failed)}) — every site changed at once, or "
            "we are being blocked"
        )

    saved = save_rows(rows)
    print(f"  market_html: saved {saved} rows "
          f"(failed: {', '.join(failed) or 'none'})")
    return ScrapeResult(records=saved, api_rows=len(rows), parse_errors=len(failed))


if __name__ == "__main__":
    scrape_date(date.today())
