"""
Market quotes that only exist as scraped HTML — shipping rates, memory and
panel spot prices, petrochemical feedstock. Everything with a real data feed
lives in market_quote.py; this module is the fragile half, deliberately kept
separate.

Five unrelated sites, each wrapped in its own try/except so one site's
redesign can't take the others down with it:

  bdi        TradingEconomics /commodity/{slug} — the page's meta description
  metals     carries the number ("Baltic Dry rose to 2,944 Index Points on
  naphtha    July 10, 2026, up 1.17%"; "Nickel rose to 17,288.13 USD/T on
  iron ore   July 30, 2026"). Same shape for every commodity, hence one
  coking     parser for all of them. No free history anywhere on this source,
             so these series grow one row per run rather than arriving with a
             backfill. The base metals are here rather than in market_quote
             because LME has no free daily feed at all — Yahoo carries only
             aluminium (COMEX, thin) and FRED is monthly. BDRY (in
             market_quote.py) is the ETF proxy that carries long history for
             the BDI card in the meantime.
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
  pvc, pp    magnitude and trade convention, NOT stated on the page). These are
  pe x3      China domestic spot, NOT the CFR North-East Asia landed price
  benzene    台塑 actually buys on — correlated, not the same number.
  xylene     Ethylene has no free live source: SunSirs' own ethylene series
  pta, eg    stopped updating in 2022 (re-verified 2026-07-30, last print
             2022-07-23), so it is absent. VCM/EVA/EDC/PS/AN/CPL/butadiene/
             paraxylene are absent for a different reason: SunSirs' English
             product index is 102 items and carries none of them.

None of them can be backfilled; they accumulate from the first run onward.
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
from utils.http_client import fetch, get_session

TE_URL      = "https://tradingeconomics.com/commodity/{slug}"
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
    # 基本金屬 (LME 現貨等級的報價，經 TradingEconomics)
    "nickel":     {"name": "鎳",             "category": "metal",    "unit": "USD/噸",   "dp": 2, "freq": "daily"},
    "zinc":       {"name": "鋅",             "category": "metal",    "unit": "USD/噸",   "dp": 2, "freq": "daily"},
    "lead":       {"name": "鉛",             "category": "metal",    "unit": "USD/噸",   "dp": 2, "freq": "daily"},
    "tin":        {"name": "錫",             "category": "metal",    "unit": "USD/噸",   "dp": 2, "freq": "daily"},
    "aluminum":   {"name": "鋁",             "category": "metal",    "unit": "USD/噸",   "dp": 2, "freq": "daily"},
    "cobalt":     {"name": "鈷",             "category": "metal",    "unit": "USD/噸",   "dp": 2, "freq": "daily"},
    # 鋼鐵爐料端 (高爐的鐵礦砂+焦煤是中鋼的成本；螺紋鋼是電爐廠豐興/東鋼的
    # 產品端——台灣廠沒有公布盤價，中國現貨是唯一免費代理)
    "iron_ore":   {"name": "鐵礦砂",         "category": "steel",    "unit": "USD/噸",   "dp": 2, "freq": "daily"},
    "coking_coal":{"name": "焦煤",           "category": "steel",    "unit": "USD/噸",   "dp": 2, "freq": "daily"},
    "rebar":      {"name": "螺紋鋼",         "category": "steel",    "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    # 石化最上游的裂解原料
    "naphtha":    {"name": "石油腦",         "category": "petro",    "unit": "USD/噸",   "dp": 2, "freq": "daily"},
    "dram_ddr5":  {"name": "DRAM DDR5 16Gb", "category": "memory",   "unit": "USD",      "dp": 3, "freq": "daily"},
    "dram_ddr5e": {"name": "DRAM DDR5 eTT",  "category": "memory",   "unit": "USD",      "dp": 3, "freq": "daily"},
    "dram_ddr4":  {"name": "DRAM DDR4 16Gb", "category": "memory",   "unit": "USD",      "dp": 3, "freq": "daily"},
    "nand_mlc":   {"name": "NAND MLC 64Gb",  "category": "memory",   "unit": "USD",      "dp": 3, "freq": "daily"},
    "panel_tv55": {"name": "面板 55吋 UHD",  "category": "panel",    "unit": "USD",      "dp": 1, "freq": "monthly"},
    # 石化中間體 (SunSirs 中國內銷)
    "propylene":  {"name": "丙烯",           "category": "petro",    "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    "benzene":    {"name": "苯",             "category": "petro",    "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    "xylene":     {"name": "二甲苯",         "category": "petro",    "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    # 塑化 (台塑四寶/台聚族群的產品端)
    "styrene":    {"name": "苯乙烯 SM",      "category": "plastics", "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    "pvc":        {"name": "PVC",            "category": "plastics", "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    "pp":         {"name": "聚丙烯 PP",      "category": "plastics", "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    "hdpe":       {"name": "HDPE",           "category": "plastics", "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    "ldpe":       {"name": "LDPE",           "category": "plastics", "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    "lldpe":      {"name": "LLDPE",          "category": "plastics", "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    "pta":        {"name": "PTA",            "category": "plastics", "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    "eg":         {"name": "乙二醇 EG",      "category": "plastics", "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    # 橡膠 (天然膠是正新/建大的成本，SBR 是台橡的產品)
    "rubber_nat": {"name": "天然橡膠",       "category": "rubber",   "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    "sbr":        {"name": "丁苯橡膠 SBR",   "category": "rubber",   "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    # 新能源材料
    "polysilicon":{"name": "多晶矽",         "category": "newenergy","unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    "lithium":    {"name": "碳酸鋰",         "category": "newenergy","unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    # 動力煤是水泥/電力的燃料成本，跟煉鋼用的焦煤不是同一種煤
    "thermal_coal":{"name": "動力煤",        "category": "energy",   "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
    "urea":       {"name": "尿素",           "category": "agri",     "unit": "人民幣/噸", "dp": 0, "freq": "daily"},
}

# TradingEconomics slug + the unit string that follows the number in the page's
# meta description. The unit is what pins the match to the headline print
# rather than to some other number on the page, so it is per-commodity rather
# than one loose alternation.
TE_PRODUCTS = {
    "bdi":         ("baltic",      "Index Points"),
    "nickel":      ("nickel",      "USD/T"),
    "zinc":        ("zinc",        "USD/T"),
    "lead":        ("lead",        "USD/T"),
    "tin":         ("tin",         "USD/T"),
    "aluminum":    ("aluminum",    "USD/T"),
    "cobalt":      ("cobalt",      "USD/T"),
    "iron_ore":    ("iron-ore",    "USD/T"),
    "coking_coal": ("coking-coal", "USD/T"),
    "naphtha":     ("naphtha",     "USD/T"),
}

# The rows each spot table is read from. DRAMeXchange lists several contracts;
# these are the mainstream ones the industry quotes. The free tables carry no
# DDR3 row any more and no TLC row at all (TLC is contract-price only), which
# is why neither is here.
DRAM_TABLE = "tb_NationalDramSpotPrice"
NAND_TABLE = "tb_NationalFlashSpotPrice"
SPOT_ROWS = {
    "dram_ddr5":  (DRAM_TABLE, "DDR5 16Gb (2Gx8) 4800/5600"),
    "dram_ddr5e": (DRAM_TABLE, "DDR5 16Gb (2Gx8) eTT"),
    "dram_ddr4":  (DRAM_TABLE, "DDR4 16Gb (2Gx8) 3200"),
    "nand_mlc":   (NAND_TABLE, "MLC 64Gb 8GBx8"),
}

# TrendForce prints four tables; the one we want is Large Size Panel Price.
# The 32" number on that page belongs to the STREET PRICE table (retail TV
# sets), not to panels — anchoring on the open-cell spec keeps them apart.
PANEL_APP, PANEL_SPEC = "LCD TV", '55"W UHD Open-Cell'

# SunSirs product ids. The English site's full index is /uk/sectors.html and
# runs to 102 products; anything not in it (VCM, EVA, EDC, PS, AN, CPL,
# butadiene, paraxylene) has no free source here.
SUNSIRS_PRODUCTS = {
    "propylene": 505, "styrene": 168, "pvc": 107, "pp": 718,
    "hdpe": 295, "ldpe": 334, "lldpe": 435, "benzene": 120,
    "xylene": 1222, "pta": 356, "eg": 222,
    "rebar": 927, "thermal_coal": 369, "rubber_nat": 586, "sbr": 388,
    "polysilicon": 463, "urea": 89, "lithium": 1162,
}

# SunSirs answers a burst of requests with a ~600-byte JS interstitial that
# writes an HW_CHECK cookie and reloads itself. requests runs no JS, so once
# that trips, every product page parses as "no price rows" and all 18 symbols
# silently stop updating under a misleading error. The token is handed to us
# in cleartext in that page, so echoing it back as the cookie clears it.
SUNSIRS_CHALLENGE = re.compile(r'var\s+_0x2\s*=\s*"([0-9a-f]+)"')


def _strip_tags(html: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", html)).strip()


def _scrape_te(symbol: str) -> tuple[date, float] | None:
    """Value + its own print date, both out of the meta description — the
    date matters because TradingEconomics keeps serving the last print on
    days the commodity doesn't publish, and we'd otherwise store a stale
    number under today's date."""
    slug, unit = TE_PRODUCTS[symbol]
    resp = fetch(TE_URL.format(slug=slug), timeout=30)
    if resp is None:
        return None
    m = re.search(
        rf"([\d,]+(?:\.\d+)?)\s*{re.escape(unit)} on (\w+ \d{{1,2}},? \d{{4}})",
        resp.text,
    )
    if not m:
        print(f"  TE {symbol}: number not found in page (source layout changed?)")
        return None
    value = float(m.group(1).replace(",", ""))
    try:
        d = datetime.strptime(m.group(2).replace(",", ""), "%B %d %Y").date()
    except ValueError:
        print(f"  TE {symbol}: unparsable date {m.group(2)!r}")
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
    for symbol, (table_id, item) in SPOT_ROWS.items():
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


def _sunsirs_fetch(url: str):
    """fetch(), but solve the anti-bot interstitial if we hit it.

    The cookie goes on the shared session, so only the first challenged
    request in a run pays the extra round trip.
    """
    resp = fetch(url, timeout=30)
    if resp is None:
        return None
    m = SUNSIRS_CHALLENGE.search(resp.text)
    if m and len(resp.text) < 2000:
        get_session().cookies.set(
            "HW_CHECK", m.group(1), domain="www.sunsirs.com", path="/"
        )
        resp = fetch(url, timeout=30)
    return resp


def _scrape_sunsirs() -> dict[str, list[tuple[date, float]]]:
    """Each product page lists its last ~6 daily prints as
    <td>Propylene</td><td>Chemical</td><td>8011.00</td><td>2026-07-12</td>.
    Taking every row (not just the newest) is what lets a few days of downtime
    heal themselves — these series cannot be backfilled any other way."""
    out: dict[str, list[tuple[date, float]]] = {}
    for symbol, pid in SUNSIRS_PRODUCTS.items():
        resp = _sunsirs_fetch(SUNSIRS_URL.format(pid=pid))
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
    (TradingEconomics' meta line, FBX's indexDate, DRAMeXchange's Last Update)
    because
    daily_update supports backfill runs — and writing today's spot price
    under a backfilled date would be permanent, unfixable corruption in
    series that cannot be re-fetched.
    """
    rows: list[tuple[str, date, float]] = []
    failed: list[str] = []

    for symbol in TE_PRODUCTS:
        try:
            hit = _scrape_te(symbol)
            if hit:
                rows.append((symbol, hit[0], hit[1]))
            else:
                failed.append(symbol)
        except Exception as e:
            print(f"  TE {symbol} failed: {e}")
            failed.append(symbol)

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
        failed.extend(s for s in SPOT_ROWS if s not in mem)
    except Exception as e:
        print(f"  Memory spot failed: {e}")
        failed.extend(SPOT_ROWS)

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
    # parse_errors stays 0: `failed` counts whole source symbols, not
    # unparseable rows, so mixing it with api_rows (data points) would give
    # daily_update._result_problem a meaningless error rate. Failed sources
    # are reported by the print above and by the all-failed raise.
    return ScrapeResult(records=saved, api_rows=len(rows), parse_errors=0)


if __name__ == "__main__":
    scrape_date(date.today())
