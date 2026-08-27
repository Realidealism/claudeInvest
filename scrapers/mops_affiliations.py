"""
Parent -> subsidiary scraper (轉投資/子公司關係, MOPS server-java/t164sb01).

Source is the consolidated-financials note "列入合併財務報表之子公司", which is
the only MOPS view carrying BOTH the ownership percentage and the consolidation
status (appearing in that note IS the consolidation status). One POST returns a
company's whole note for a given year/season.

Two environment facts that differ from the ajax_* MOPS endpoints used elsewhere
in this repo:
  - the response is Big5, not UTF-8
  - a failed request answers 502 "Unreachable Server"; that is retryable, NOT
    "no data" (treating it as empty would silently under-report coverage)
  - it intermittently answers 307; the redirect target is empty, so requests are
    sent with allow_redirects=False

  fetch_one(stock_id, year, season) -> list[dict]
  save_rows(rows) -> int
  python -m scrapers.mops_affiliations --year 2025 --season 4 --stocks 2454,6462
  python -m scrapers.mops_affiliations --year 2025 --season 4 --all
"""

import argparse
import random
import re
import sys
import time

import requests

from db.connection import get_cursor
from utils.http_client import DEFAULT_HEADERS

MOPS_URL = "https://mopsov.twse.com.tw/server-java/t164sb01"
MIN_INTERVAL = 1.0
MAX_JITTER = 0.5
MAX_RETRIES = 3

_session = None
_last_request = 0.0
_parent_names: dict[str, str] = {}

TUPLE_RE = re.compile(
    r'<ix:tuple name="tifrs-notes:TheConsolidatedEntities">(.*?)</ix:tuple>', re.S
)
# The significant-influence note is parsed row-wise, not tuple-wise: its rows
# embed a nested <ix:tuple> (OriginalInvestmentAmount), so a non-greedy tuple
# match would stop at the inner close tag and lose the ownership columns.
INFLUENCE_ANCHOR = 'id="NamesLocationsAndRelatedInformationOfInvesteesOverWhichTheCompanyExercisesSignificantInfluence"'
ROW_RE = re.compile(r"<tr>(.*?)</tr>", re.S)
# MOPS repeats the holder above with a ditto mark in both notes.
DITTO = ("〃", "”", '"', "同上")
FIELD_RE = re.compile(r"<ix:(?:nonNumeric|nonFraction)\s([^>]*)>([^<]*)<", re.S)
ATTR_RE = re.compile(r'(\w+)="([^"]*)"')
# Company-form suffixes to strip before matching investee names to tw.stocks.
SUFFIX_RE = re.compile(
    r"(股份有限公司|有限公司|\(股\)公司|\(股\)有限公司|公司|股份|\(.*?\)|Co\.,? ?Ltd\.?|Inc\.?|Corp\.?|Corporation|Limited|Pte\.? ?Ltd\.?)",
    re.I,
)
# Trailing industry words that MOPS keeps but the board name drops
# (立錡科技(股)公司 vs 立錡). Stripped only as a second matching attempt.
INDUSTRY_TAIL_RE = re.compile(
    r"(?:科技|電子|半導體|光電|工業|實業|企業|國際|精密|材料|生技)+$"
)
# 投資/控股 arms reuse the listed company's name but are separate entities
# (景碩投資 is not 3189 景碩), and a region in parentheses marks an overseas arm
# (達發科技(蘇州) is not 6526 達發). Both must never resolve to a ticker.
NON_MATCH_TAIL_RE = re.compile(r"(?:投資|控股|開發|租賃|貿易|國際貿易)+$")
REGION_RE = re.compile(
    r"[（(](?:中國|大陸|香港|澳門|日本|韓國|美國|英國|德國|法國|荷蘭|新加坡|馬來西亞|越南|泰國|印度|菲律賓|"
    r"上海|北京|深圳|廣州|東莞|蘇州|南京|杭州|成都|重慶|廈門|合肥|天津|武漢|西安|寧波|無錫|昆山|"
    r"開曼|薩摩亞|英屬|模里西斯|汶萊|BVI|Cayman|Samoa)"
)


def _rate_limit():
    global _last_request
    elapsed = time.time() - _last_request
    needed = MIN_INTERVAL + random.uniform(0, MAX_JITTER)
    if elapsed < needed:
        time.sleep(needed - elapsed)
    _last_request = time.time()


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        s = requests.Session()
        s.headers.update(DEFAULT_HEADERS)
        _session = s
    return _session


def _normalize(name: str) -> str:
    """Strip company-form suffixes / punctuation so MOPS names match tw.stocks."""
    n = SUFFIX_RE.sub("", str(name))
    return re.sub(r"[\s　,.\-]", "", n)


def _load_name_index() -> dict:
    """normalized stock name -> stock_id (includes delisted companies)."""
    with get_cursor() as cur:
        cur.execute("SELECT stock_id, name FROM tw.stocks WHERE security_type = 'STOCK'")
        rows = cur.fetchall()
    index = {}
    for r in rows:
        index.setdefault(_normalize(r["name"]), []).append(r["stock_id"])
    return index


def _match_stock_id(investee_name: str, index: dict, parent_id: str | None = None) -> str | None:
    """Resolve an investee name to a stock_id, or None when it is not certain.

    MOPS writes the full legal name (立錡科技(股)公司) while tw.stocks holds the
    short board name (立錡), so an exact hit is tried first and a hit after
    stripping a trailing industry word second. Free prefix matching is NOT used:
    it collapses 聯發科中國 onto 1459 聯發. Self-matches are dropped, since a
    company is never its own subsidiary (the note repeats the parent's name in
    the investor column).
    """
    if REGION_RE.search(str(investee_name)):
        return None
    norm = _normalize(investee_name)
    if not norm or NON_MATCH_TAIL_RE.search(norm):
        return None
    hits = index.get(norm)
    if hits and len(hits) == 1:
        return None if hits[0] == parent_id else hits[0]
    # No exact hit: take the LONGEST board name that prefixes this one and whose
    # remainder is just an industry word (立錡科技 -> 立錡). Longest wins because
    # 聯發科技 also prefixes onto 聯發(1459); the longer 聯發科(2454) is the real
    # owner of that name and being the parent it correctly resolves to None
    # instead of silently linking MediaTek's China arm to an unrelated ticker.
    best = None
    for name, ids in index.items():
        if len(ids) != 1 or len(name) < 2 or not norm.startswith(name):
            continue
        tail = norm[len(name):]
        # Board names are truncated to four characters (福懋科技 -> 福懋科), so a
        # one-character remainder is a truncation artifact, not a different firm.
        if tail and len(tail) > 1 and not INDUSTRY_TAIL_RE.fullmatch(tail):
            continue
        if best is None or len(name) > len(best[0]):
            best = (name, ids[0])
    if best is None or best[1] == parent_id:
        return None
    return best[1]


def _parent_name(stock_id: str) -> str:
    """Board name of the filing company, used to fold 本公司 onto its own name."""
    if stock_id not in _parent_names:
        with get_cursor() as cur:
            cur.execute("SELECT name FROM tw.stocks WHERE stock_id = %s", (stock_id,))
            row = cur.fetchone()
        _parent_names[stock_id] = row["name"] if row else ""
    return _parent_names[stock_id]


def _merge_key(name: str) -> str:
    """Key used to fold the two notes' spellings of the same investee together.

    One note writes the full legal name (易威科技(股)公司), the other the short
    board name (易威), so the industry tail comes off for keying only.
    """
    norm = _normalize(name)
    # An overseas arm keeps its region in the key: 達發科技(蘇州) normalizes to the
    # same string as the listed 達發科技, and folding them let the 100%-held
    # Suzhou entity outrank (and hide) the 66%-held listed company.
    region = REGION_RE.search(str(name))
    if region:
        return f"{norm}@{region.group(0)}"
    return INDUSTRY_TAIL_RE.sub("", norm) or norm


def _to_number(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value.replace(",", "").replace("%", ""))
    except ValueError:
        return None


def _influence_rows(html: str, as_of: str) -> list[dict]:
    """Parse 「被投資公司名稱、所在地區…等相關資訊」 (subsidiaries + associates)."""
    start = html.find(INFLUENCE_ANCHOR)
    if start < 0:
        return []
    section = html[start:]
    numeric = {"PercentageOfOwnership1", "NumberOfShares1", "BookValue1",
               "InvestmentIncomeLossesRecognized1"}
    out = []
    last_investor = ""
    for block in ROW_RE.findall(section):
        if "CompanyNameOfTheInvestee" not in block:
            continue
        fields = {}
        for attrs, value in FIELD_RE.findall(block):
            attr = dict(ATTR_RE.findall(attrs))
            tag = attr.get("name", "").split(":")[-1]
            value = value.strip()
            if tag in numeric:
                if attr.get("contextRef") == as_of:
                    fields[tag] = _to_number(value)
            elif value:
                fields.setdefault(tag, value)
        if not fields.get("CompanyNameOfTheInvestee"):
            continue
        investor = fields.get("CompanyNameOfTheInvestor", "")
        if investor in DITTO:
            fields["CompanyNameOfTheInvestor"] = last_investor
        elif investor:
            last_investor = investor
        out.append(fields)
    return out


def fetch_one(stock_id: str, year: int, season: int) -> list[dict]:
    """Fetch and parse one company's consolidated-subsidiary note.

    Returns [] when the company filed no such note; raises RuntimeError when the
    endpoint stays unreachable, so callers can tell "no data" from "failed".
    """
    # Companies with no subsidiaries file no consolidated report (REPORT_ID=C
    # answers a 98-byte stub), but their INDIVIDUAL report (REPORT_ID=A) still
    # carries the significant-influence note, so fall back to it.
    for report_id in ("C", "A"):
        html = _fetch_report(stock_id, year, season, report_id)
        rows = _parse_report(html, stock_id, year, season, report_id)
        if rows:
            return rows
    return []


def _fetch_report(stock_id: str, year: int, season: int, report_id: str) -> str:
    payload = {
        "step": "1",
        "CO_ID": stock_id,
        "SYEAR": str(year),
        "SSEASON": str(season),
        "REPORT_ID": report_id,
    }
    html = None
    for attempt in range(MAX_RETRIES):
        _rate_limit()
        try:
            # allow_redirects=False on purpose: MOPS intermittently answers this
            # endpoint with a 307 whose target serves an empty page, so following
            # the redirect turns a good response into a phantom failure.
            r = _get_session().post(MOPS_URL, data=payload, timeout=60,
                                    allow_redirects=False)
            r.encoding = "big5"
            if r.status_code == 200 and "Unreachable Server" not in r.text:
                html = r.text
                break
        except requests.RequestException as e:
            print(f"  [{stock_id}] request error (attempt {attempt + 1}): {e}")
        time.sleep(2 * (attempt + 1))
    if html is None:
        raise RuntimeError(f"{stock_id}: endpoint unreachable after {MAX_RETRIES} tries")
    return html


def _parse_report(html: str, stock_id: str, year: int, season: int, report_id: str) -> list[dict]:
    quarter_end = {1: "0331", 2: "0630", 3: "0930", 4: "1231"}[season]
    as_of = f"AsOf{year}{quarter_end}"
    period = f"{year}Q{season}"
    # Keyed by the NORMALIZED investee name, so one relationship is one row: the
    # two notes spell both the holder and the investee differently (乾瞻科技(股)公司
    # vs 乾瞻科技股份有限公司; 安國 vs 安國、群峰), and a group may hold the same
    # investee through several arms. The largest stake wins as the headline row.
    merged: dict[str, dict] = {}

    def _merge(key: str, row: dict):
        old_row = merged.get(key)
        if old_row is None:
            merged[key] = row
            return
        if (row.get("holding_pct") or -1) > (old_row.get("holding_pct") or -1):
            row["consolidated"] = row.get("consolidated") or old_row.get("consolidated")
            for field in ("location", "shares_held", "book_value", "investment_income",
                          "main_business"):
                if row.get(field) is None:
                    row[field] = old_row.get(field)
            merged[key] = row
        else:
            old_row["consolidated"] = old_row.get("consolidated") or row.get("consolidated")
            for field in ("location", "shares_held", "book_value", "investment_income",
                          "main_business"):
                if old_row.get(field) is None:
                    old_row[field] = row.get(field)

    for fields in _influence_rows(html, as_of):
        investee = fields.get("CompanyNameOfTheInvestee")
        if not investee:
            continue
        investor = fields.get("CompanyNameOfTheInvestor", "")
        # The two notes name the filing company differently ("本公司" vs its board
        # name); fold them so one relationship is not stored as two rows.
        if investor in ("本公司", "母公司"):
            investor = _parent_name(stock_id)
        _merge(_merge_key(investee), {
            "parent_id": stock_id,
            "period": period,
            "investor_name": investor,
            "investee_name": investee,
            "main_business": fields.get("MainBusinessesAndProducts"),
            "location": fields.get("Location"),
            "holding_pct": fields.get("PercentageOfOwnership1"),
            "shares_held": fields.get("NumberOfShares1"),
            "book_value": fields.get("BookValue1"),
            "investment_income": fields.get("InvestmentIncomeLossesRecognized1"),
            "consolidated": False,
            "source": f"t164sb01:{report_id}",
        })

    last_investor = ""
    for block in TUPLE_RE.findall(html):
        fields = {}
        pct = None
        for attrs, value in FIELD_RE.findall(block):
            attr = dict(ATTR_RE.findall(attrs))
            tag = attr.get("name", "").split(":")[-1]
            value = value.strip()
            if tag == "PercentageOfOwnership4":
                # The note prints this period and the year-ago figure in two
                # columns; keep only the one whose context is this quarter end.
                if attr.get("contextRef") == as_of and value:
                    try:
                        pct = float(value.replace(",", "").replace("%", ""))
                    except ValueError:
                        pct = None
            elif value:
                fields.setdefault(tag, value)
        investee = fields.get("NameOfInvestee")
        if not investee:
            continue
        investor = fields.get("CompanyNameOfTheInvestor", "")
        # MOPS uses the ditto mark for "same holder as the row above".
        if investor in DITTO:
            investor = last_investor
        elif investor:
            last_investor = investor
        if investor in ("本公司", "母公司"):
            investor = _parent_name(stock_id)
        _merge(_merge_key(investee), {
            "parent_id": stock_id,
            "period": period,
            "investor_name": investor,
            "investee_name": investee,
            "main_business": fields.get("MainBusinesses"),
            "location": None,
            "holding_pct": pct,
            "shares_held": None,
            "book_value": None,
            "investment_income": None,
            "consolidated": True,
            "source": f"t164sb01:{report_id}",
        })
    return list(merged.values())


def save_rows(rows: list[dict], index: dict | None = None) -> int:
    if not rows:
        return 0
    if index is None:
        index = _load_name_index()
    saved = 0
    with get_cursor() as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO tw.company_affiliations
                    (parent_id, period, investor_name, investee_name, investee_id,
                     main_business, holding_pct, consolidated, location,
                     shares_held, book_value, investment_income, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (parent_id, period, investor_name, investee_name) DO UPDATE SET
                    investee_id = EXCLUDED.investee_id,
                    main_business = EXCLUDED.main_business,
                    holding_pct = EXCLUDED.holding_pct,
                    consolidated = EXCLUDED.consolidated,
                    source = EXCLUDED.source,
                    location = EXCLUDED.location,
                    shares_held = EXCLUDED.shares_held,
                    book_value = EXCLUDED.book_value,
                    investment_income = EXCLUDED.investment_income,
                    updated_at = NOW()
                """,
                (
                    row["parent_id"], row["period"], row["investor_name"],
                    row["investee_name"],
                    _match_stock_id(row["investee_name"], index, row["parent_id"]),
                    row["main_business"], row["holding_pct"],
                    row.get("consolidated", False), row.get("location"),
                    row.get("shares_held"), row.get("book_value"),
                    row.get("investment_income"),
                    row.get("source", "t164sb01"),
                ),
            )
            saved += 1
    return saved


def _target_stocks() -> list[str]:
    with get_cursor() as cur:
        cur.execute(
            "SELECT stock_id FROM tw.stocks "
            "WHERE is_active AND security_type = 'STOCK' ORDER BY stock_id"
        )
        return [r["stock_id"] for r in cur.fetchall()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--season", type=int, required=True, choices=[1, 2, 3, 4])
    ap.add_argument("--stocks", help="comma-separated stock ids")
    ap.add_argument("--all", action="store_true", help="every active listed stock")
    args = ap.parse_args()

    if args.stocks:
        targets = [s.strip() for s in args.stocks.split(",") if s.strip()]
    elif args.all:
        targets = _target_stocks()
    else:
        sys.exit("Pass --stocks 2454,6462 or --all")

    index = _load_name_index()
    total_rows = failed = no_data = 0
    for i, stock_id in enumerate(targets, 1):
        try:
            rows = fetch_one(stock_id, args.year, args.season)
        except RuntimeError as e:
            print(f"[{i}/{len(targets)}] {e}")
            failed += 1
            continue
        if not rows:
            no_data += 1
        else:
            total_rows += save_rows(rows, index)
        if i % 50 == 0 or i == len(targets):
            print(f"[{i}/{len(targets)}] rows={total_rows} no_data={no_data} failed={failed}")
    print(f"Done: {total_rows} rows, {no_data} without the note, {failed} unreachable.")


if __name__ == "__main__":
    main()
