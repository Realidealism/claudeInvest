"""
Product-line revenue mix scraper (主要商品(服務)之營業比重, 年報 PDF).

The structured MOPS endpoint for product revenue (ajax_t05st08) became voluntary
after IFRS and now covers ~226 companies, so the annual report is the only source
that spans every listed company. Chapter 肆 of every 年報 carries the table, but
each company chooses its own granularity: 台積電 discloses one bucket (晶圓),
神盾 discloses six with both amounts and percentages.

Pipeline (all three steps hit doc.twse.com.tw):
  1. step=1  list a company's shareholder-meeting filings for a ROC filing year
             -> pick F04 (中文年報); fall back to FE4 (英文年報)
  2. step=9  exchange the filename for an on-demand link with a fresh timestamp
             (the timestamp cannot be constructed, it must be requested)
  3. GET     download the PDF, locate the 營業比重 pages, parse

Encoding trap: this host answers UTF-8 sometimes and Big5 other times, so the
listing is decoded by trying UTF-8 first and falling back on replacement-char count.

  python -m scrapers.annual_report_revenue --year 2025 --stocks 6462,8299
  python -m scrapers.annual_report_revenue --year 2025 --all [--retry-failed]
"""

import argparse
import io
import random
import re
import sys
import time
import unicodedata
from datetime import date

import pdfplumber
import requests

from db.connection import get_cursor

BASE = "https://doc.twse.com.tw"
LIST_URL = f"{BASE}/server-java/t57sb01"
MIN_INTERVAL = 1.0
MAX_JITTER = 0.6
MAX_RETRIES = 3
HEADERS = {"User-Agent": "Mozilla/5.0"}

_session = None
_last_request = 0.0

FILE_RE = re.compile(r"(\d{4}_\d{4}_\d{8}F\w*\d*\.pdf)")
# The filing list prints the upload timestamp two cells after the filename:
#   ...F04.pdf</a></td><td align='right'>7,006,132</td><td>115/05/21 19:02:58</td>
FILE_DATE_RE = re.compile(
    r"(\d{4}_\d{4}_\d{8}F\w*\d*\.pdf)</a>.*?(\d{2,3})/(\d{2})/(\d{2})\s+\d{2}:\d{2}:\d{2}",
    re.S)
LINK_RE = re.compile(r"href='(/pdf/[^']+\.pdf)'")
# Chapter heading and table caption wording both vary; any hit marks a candidate page.
SECTION_HINTS = ("主要商品", "營業比重", "產品別營收", "主要產品之銷售")
PCT_RE = re.compile(r"(\d{1,3}(?:\.\d+)?)\s*%")
NUM_RE = re.compile(r"^[\d,]+(?:\.\d+)?$")


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
        s.headers.update(HEADERS)
        _session = s
    return _session


def _decode(content: bytes) -> str:
    text = content.decode("utf-8", errors="replace")
    if text.count("�") > 20:
        text = content.decode("big5", errors="replace")
    return text


def _post(data: dict) -> str | None:
    for attempt in range(MAX_RETRIES):
        _rate_limit()
        try:
            r = _get_session().post(LIST_URL, data=data, timeout=60)
            if r.status_code == 200:
                return _decode(r.content)
        except requests.RequestException:
            pass
        time.sleep(2 * (attempt + 1))
    return None


def list_filings(stock_id: str, fiscal_year: int) -> dict[str, "date | None"]:
    """Every filename on the filing list, mapped to the day it was uploaded.

    The listing is keyed by ROC *filing* year, which is the fiscal year + 1 in AD
    terms: FY2025 is filed in 2026 = ROC 115.

    The upload timestamp is the only public statement of when a report became
    readable, and known_date on a classification row needs exactly that — a
    backtest reading a classification dated to the fiscal year would otherwise
    be using knowledge from months before anyone had it (R021).

    An empty dict means the request failed OR the company filed nothing; the two
    are indistinguishable here, which is why callers must not read it as "no
    report" without a second look.
    """
    roc_filing_year = fiscal_year + 1 - 1911
    html = _post({"step": "1", "colorchg": "1", "co_id": stock_id,
                  "year": str(roc_filing_year), "mtype": "F"})
    if not html:
        return {}
    out: dict[str, date | None] = {}
    for name, roc_y, mm, dd in FILE_DATE_RE.findall(html):
        try:
            out.setdefault(name, date(int(roc_y) + 1911, int(mm), int(dd)))
        except ValueError:
            out.setdefault(name, None)
    # A row whose timestamp cell is malformed still counts as a filing.
    for name in FILE_RE.findall(html):
        out.setdefault(name, None)
    return out


def _pick_report(filings, stock_id: str, fiscal_year: int) -> str | None:
    prefix = f"{fiscal_year}_{stock_id}_"
    for suffix in ("F04", "FE4"):
        hits = sorted(n for n in filings if n.startswith(prefix) and suffix in n)
        if hits:
            return hits[-1]
    return None


def find_report(stock_id: str, fiscal_year: int) -> str | None:
    """Filename of the Chinese annual report for a fiscal year, or None."""
    return _pick_report(list_filings(stock_id, fiscal_year), stock_id, fiscal_year)


def download_pdf(stock_id: str, filename: str) -> bytes | None:
    html = _post({"step": "9", "kind": "F", "co_id": stock_id, "filename": filename})
    if not html:
        return None
    m = LINK_RE.search(html)
    if not m:
        return None
    for attempt in range(MAX_RETRIES):
        _rate_limit()
        try:
            r = _get_session().get(BASE + m.group(1), timeout=180)
            if r.status_code == 200 and r.content[:4] == b"%PDF":
                return r.content
        except requests.RequestException:
            pass
        time.sleep(2 * (attempt + 1))
    return None


def _clean(cell) -> str:
    """Normalise a cell for matching.

    Annual report PDFs embed CJK compatibility ideographs (幸福水泥 writes 年 as
    U+F98E, not U+5E74) and full-width digits, so a plain substring test for
    "114年" silently misses. NFKC folds both into their canonical forms.
    """
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", str(cell or "")))


# Words that only appear in narrative prose, never as a product name. The line
# and prose readers scan free text, so without this they lift fragments such as
# 「根據國際貨幣基金組織(IMF)於」 or 「百分比」 out of a market-commentary
# paragraph and store them as products.
PROSE_NOISE = ("百分比", "合計", "市場", "根據", "成長率", "年化", "表現", "預估",
               "分析", "報告", "指出", "表示", "占比", "比重", "營收為", "較去年",
               # totals and P&L captions sit in the same table but are not products
               "總額", "總計", "營業收入", "營業成本", "營業毛利", "營業利益",
               "營業費用", "銷貨收入淨額",
               # A share of what, where or when is not a share of which product.
               # These lifted 「北部地區 71%」, 「近視人口 60%」, 「國內約為 80%」
               # and 「本公司商品 100%」 into product mixes.
               "約佔", "約為", "佔全", "地區", "人口", "本公司", "營收", "目前", "年度")

# A fragment cut out of a sentence usually starts with the conjunction that
# joined it or ends on the preposition that continued it — 「其次為雜項紡織品(」,
# 「但由於直接輸美紡織成衣僅」, 「牌在整體冷藏果蔬汁市」.
PROSE_LEAD = re.compile(r"^(但|其次|以及|而|因|由於|雖|另|至於|\)|）|、|，|及)")
PROSE_TAIL = re.compile(r"[市牌之於在含約僅]$")
YEAR_LABEL = re.compile(r"^\d{2,4}年")


def _is_prose_noise(item: str) -> bool:
    # Compare without spaces: reports typeset labels as 「合 計」 with padding,
    # which slips past a plain substring test for 合計.
    compact = re.sub(r"\s+", "", item)
    if any(w in compact for w in PROSE_NOISE) or compact in ("合計", "總計", "小計"):
        return True
    if PROSE_LEAD.match(compact) or PROSE_TAIL.search(compact) or YEAR_LABEL.match(compact):
        return True
    # Unbalanced brackets mean the phrase was cut mid-parenthetical.
    if compact.count("(") + compact.count("（") != compact.count(")") + compact.count("）"):
        return True
    # A product name is a noun phrase; sentence punctuation means a fragment.
    return bool(re.search(r"[，。；！？,]", item)) or len(item) > 14


def _is_label(item: str) -> bool:
    """A product name must carry actual words.

    Some reports keep the product names in a footnote and leave the table cell as
    a marker (太欣: "(1)" "(2)"), which parses as a row but names nothing; storing
    those pollutes the mix with unidentifiable lines.
    """
    stripped = re.sub(r"[\d\s()（）.,、:：\-_/]", "", item)
    return len(stripped) >= 2


def _to_num(text: str) -> float | None:
    text = text.replace(",", "").replace("%", "").strip()
    try:
        return float(text)
    except ValueError:
        return None


REGION_WORDS = ("美國", "日本", "中國", "大陸", "台灣", "臺灣", "歐洲", "亞洲", "德國",
                "韓國", "新加坡", "外銷", "內銷", "營運地點", "其他地區")


def _header_columns(table, fiscal_year):
    """Locate (amount_col, pct_col) for the wanted year in a two-year mix table.

    Annual reports print 前一年度 and 本年度 side by side (神盾: 113年度 then
    114年度), so the year has to come from the header instead of taking the first
    numeric column, which silently yields last year's mix.
    """
    roc = fiscal_year - 1911
    year_tokens = (f"{roc}年", f"{fiscal_year}年", str(roc), str(fiscal_year))
    # Header rows only. A caption row can push the real header down (6547 高端:
    # row 0 is "2.主要產品之營業比重:"), so scan a few rows but stop at the first
    # data row — otherwise a data cell like "43.4%" is read as a column label
    # (1455 尼龍). A cell holding a number above 100 marks the data.
    head = []
    for row in table[:4]:
        if any((_to_num(_clean(c)) or 0) > 100 for c in row):
            break
        head.append(row)
    head = head or table[:1]
    year_col = None
    for row in head:
        for idx, cell in enumerate(row):
            text = _clean(cell)
            # Bare "2025"/"114" counts only as a whole cell: a substring test
            # would match an amount like 20,251 once the comma is stripped.
            if any(tok in text for tok in year_tokens[:2]) or text in year_tokens[2:]:
                year_col = idx if year_col is None else max(year_col, idx)
    if year_col is None:
        # Some reports label the column only as 佔營業額比重（％） with the year in
        # the surrounding heading (1326 台化). Treat that as the current year.
        for row in head:
            for idx, cell in enumerate(row):
                text = _clean(cell)
                if "比重" in text or "百分比" in text or "比率" in text:
                    return None, idx
        return None, None
    pct_cols, amount_cols = [], []
    for row in head:
        for idx, cell in enumerate(row):
            text = _clean(cell)
            if "比重" in text or "百分比" in text or "比率" in text:
                pct_cols.append(idx)
            elif "營業收入" in text or "金額" in text or "淨額" in text:
                amount_cols.append(idx)

    def pick(candidates):
        """Column for the wanted year: after its header when the table shows two
        years, otherwise the only one there is — 1455 尼龍 puts 營業比重 before
        114年度營收, so position alone cannot decide."""
        if not candidates:
            return None
        later = [c for c in sorted(set(candidates)) if c >= year_col]
        if later and len(set(candidates)) > 1:
            return later[0]
        return sorted(set(candidates))[0]

    pct_col = pick(pct_cols)
    amount_col = pick(amount_cols)
    if pct_col is None and amount_col is None:
        # Merged header with no sub-labels: the year column itself holds the value.
        amount_col = year_col
    return amount_col, pct_col


def _rows_from_transposed(table) -> list[dict]:
    """Handle the layout that lists items across the top instead of down the side.

    聯發科 prints 產品項目 | 晶片 | 其他 on one row and 營業比重 | 98.22% | 1.78%
    on the next, with the year only in the surrounding heading, so the column-based
    reader finds nothing.
    """
    if len(table) < 2:
        return []
    head = [_clean(c) for c in table[0]]
    body = [_clean(c) for c in table[1]]
    if not head or not body:
        return []
    if not any(k in head[0] for k in ("產品項目", "商品項目", "服務項目", "產品別")):
        return []
    if "比重" not in body[0]:
        return []
    out = []
    for item, value in zip(head[1:], body[1:]):
        pct = _to_num(value)
        if item and pct is not None and 0 <= pct <= 100:
            out.append({"item": item[:60], "revenue": None, "pct": pct})
    return out


def _rows_from_table(table, fiscal_year) -> list[dict]:
    """Pull (item, revenue, pct) for one fiscal year out of a pdfplumber table."""
    amount_col, pct_col = _header_columns(table, fiscal_year)
    if amount_col is None and pct_col is None:
        return []
    # A geography table (營業單位之營運地點 / 銷售地區) has the same year headers as
    # the product table, so reject it by looking at what the rows are named.
    labels = [_clean(row[0]) for row in table if row and _clean(row[0])]
    if labels and sum(any(w in x for w in REGION_WORDS) for x in labels) >= max(2, len(labels) // 3):
        return []
    out = []
    # 廣宇 numbers its rows, so column 0 holds "1"/"2" and the product name sits
    # one cell to the right; taking column 0 blindly stores the serial number.
    label_col = 0
    for row in table[1:]:
        cells = [_clean(c) for c in row]
        if len(cells) > 1 and cells[0] and _to_num(cells[0]) is not None and _is_label(cells[1]):
            label_col = 1
            break

    for row in table[1:]:
        cells = [_clean(c) for c in row]
        if not cells or len(cells) <= label_col or not cells[label_col]:
            continue
        item = cells[label_col]
        if not _is_label(item):
            continue
        if (len(item) > 30 or any(h in item for h in ("合計", "總計", "小計", "年度", "項目", "比重"))
                or any(w in item for w in REGION_WORDS)):
            continue
        pct = _to_num(cells[pct_col]) if pct_col is not None and pct_col < len(cells) else None
        amount = _to_num(cells[amount_col]) if amount_col is not None and amount_col < len(cells) else None
        if pct is None and amount is None:
            continue
        if pct is not None and not (0 <= pct <= 100):
            pct = None
        out.append({"item": item[:60], "revenue": amount, "pct": pct})
    total = sum(r["pct"] for r in out if r["pct"])
    if out and total and not (50 <= total <= 150):
        return []
    # One nameless row with no percentage is a stray cell, not a revenue mix.
    if len(out) == 1 and out[0]["pct"] is None:
        return []
    return out


# 業務範圍 lists the parent and then each subsidiary, every one of them with its
# own 「目前所營事業比重」. 中纖's section reads 「※本公司 …人造纖維、玻璃紙…」
# followed by 「2、台中銀行 …目前所營事業比重:銀行業100%」, and the reader took the
# bank's. When more than one such ratio is present the passage covers more than
# one company and none of them can be attributed to the filer.
def _covers_many_companies(text: str) -> bool:
    return len(re.findall(r"所營事業比重", text)) > 1


def _rows_from_prose(text: str) -> list[dict]:
    """Fallback for companies writing the mix as a sentence (上銀: 線性滑軌佔營收62%)."""
    out = []
    for match in re.finditer(r"([一-鿿A-Za-z0-9（）()／/\-]{2,20}?)\s*(?:占|佔|約占|約佔)\s*(?:營收|營業額|營業收入)?[^\d%]{0,4}(\d{1,3}(?:\.\d+)?)\s*%", text):
        item = re.sub(r"^(其中|另|及|與|和|，|、|：|:)+", "", match.group(1))
        item = re.sub(r"^\d{4}年(?:度)?", "", item)
        if item and not any(w in item for w in REGION_WORDS) and not _is_prose_noise(item):
            out.append({"item": item[:60], "revenue": None, "pct": float(match.group(2))})
    if not out:
        # 「營業比重（民國114年度）：人機介面應用晶片100%。」 — the colon comes before
        # the product and the percentage is glued to it, with no 占/佔 to anchor on.
        for match in re.finditer(r"[：:]\s*([一-鿿A-Za-z][^\d%：:，,。]{1,20}?)(\d{1,3}(?:\.\d+)?)\s*%", text):
            item = match.group(1).strip("（(）) 、")
            if item and not any(w in item for w in REGION_WORDS) and not _is_prose_noise(item):
                out.append({"item": item[:60], "revenue": None, "pct": float(match.group(2))})
    if not out:
        # 「營業比重：工業用紙：100%」 — a colon list rather than a sentence (1906 寶隆).
        for match in re.finditer(r"([一-鿿A-Za-z0-9（）()／/\-]{2,20}?)\s*[：:]\s*(\d{1,3}(?:\.\d+)?)\s*%", text):
            item = re.sub(r"^(營業比重|項目|產品|主要商品)+", "", match.group(1))
            if item and not any(w in item for w in REGION_WORDS) and not _is_prose_noise(item):
                out.append({"item": item[:60], "revenue": None, "pct": float(match.group(2))})
    return out


def _year_slot(lines, fiscal_year):
    """Which percentage on a data line belongs to the wanted year (0-based).

    Two-year tables print the years in either order (神盾: 113 then 114; 慧洋:
    2025 then 2024), so the header line decides. Returns 0 when only one year is
    shown or no header is found.
    """
    roc = fiscal_year - 1911
    for line in lines[:6]:
        years = re.findall(r"(?<!\d)(1\d{2}|20\d{2})(?!\d)", _clean(line))
        years = [y for y in years if y in (str(roc), str(roc - 1), str(fiscal_year), str(fiscal_year - 1))]
        if len(years) >= 2:
            for slot, y in enumerate(years):
                if y in (str(roc), str(fiscal_year)):
                    return slot
    return 0


def _rows_from_lines(text, fiscal_year):
    """Read the mix straight off the extracted text lines.

    pdfplumber only finds a table when the PDF has ruling lines or clean column
    alignment, but the text layer almost always reads "品項 金額 比重" in order
    (1789 生泰: 原料藥之製造銷售 2,715,888 86%). Whitespace is the column
    separator here, so unlike elsewhere it must NOT be stripped: joining the
    numbers turns "2,715,888 86%" into 271588886%.
    """
    start = text.find("營業比重")
    if start < 0:
        return []
    raw = [unicodedata.normalize("NFKC", l) for l in text[start:start + 2500].splitlines() if l.strip()]
    slot = _year_slot(raw, fiscal_year)
    out = []
    for line in raw[1:]:
        if any(w in line for w in REGION_WORDS) or line.strip().startswith(("合計", "總計", "小計")):
            continue
        label = re.match(r"^\s*([^\d%]{2,30}?)\s+(?=[\d(])", line)
        if not label:
            continue
        tokens = re.findall(r"(\d[\d,]*(?:\.\d+)?)\s*(%?)", line)
        tokens = [(t[0], t[1]) for t in tokens if t[0]]
        if not tokens:
            continue
        marked = [t for t in tokens if t[1]]
        if marked:
            picks = marked
        else:
            # 高端疫苗 prints the ratio column without a % sign, so fall back to the
            # trailing 0-100 values on the line.
            picks = [t for t in tokens if (_to_num(t[0]) or 999) <= 100][-2:]
        if not picks:
            continue
        chosen = picks[slot] if slot < len(picks) else picks[0]
        pct = _to_num(chosen[0])
        if pct is None or not (0 < pct <= 100):
            continue
        amounts = [_to_num(t[0]) for t in tokens if (_to_num(t[0]) or 0) > 100]
        item = label.group(1).strip("：:。、 ")
        if not _is_label(item) or _is_prose_noise(item):
            continue
        out.append({"item": item[:60], "revenue": amounts[0] if amounts else None, "pct": pct})
    total = sum(r["pct"] for r in out)
    if not out or not (50 <= total <= 150):
        return []
    return out


def parse_pdf(content: bytes, fiscal_year: int) -> tuple[list[dict], str]:
    """Return (rows, method): 'table', 'prose', or '' when the section is absent."""
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        pages = []
        for page in pdf.pages:
            text = page.extract_text() or ""
            if "營業比重" in text or "主要商品" in text:
                pages.append((page, text))
        amounts_only = None
        for page, _text in pages:
            for table in page.extract_tables():
                rows = _rows_from_table(table, fiscal_year)
                # A table read yielding amounts but no percentage means the ratio
                # column was missed (慧洋: merged year header). Keep it aside and
                # let the text lines try first — amounts alone still carry the mix,
                # so it is the fallback rather than a discard.
                if rows and any(r["pct"] is not None for r in rows):
                    return rows, "table"
                if rows and amounts_only is None:
                    amounts_only = rows
                transposed = _rows_from_transposed(table)
                if transposed:
                    return transposed, "table"
        for _page, text in pages:
            rows = _rows_from_lines(text, fiscal_year)
            if rows:
                return rows, "lines"
        for _page, text in pages:
            head = text[text.find("營業比重"):] if "營業比重" in text else text
            rows = [] if _covers_many_companies(head) else _rows_from_prose(head[:800])
            # Same completeness bar as the other readers: a lone "15%" sentence is
            # a fragment of narrative, not the company's revenue mix (3041 揚智).
            if rows and 50 <= sum(r["pct"] for r in rows) <= 150:
                return rows, "prose"
        if amounts_only:
            return amounts_only, "table-amounts"
    return [], ""


def save(stock_id: str, fiscal_year: int, rows: list[dict], method: str,
         source_file: str | None, status: str, detail: str | None = None):
    with get_cursor() as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO tw.product_revenue
                    (stock_id, fiscal_year, item, revenue, pct, source_file, method)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_id, fiscal_year, item) DO UPDATE SET
                    revenue = EXCLUDED.revenue, pct = EXCLUDED.pct,
                    source_file = EXCLUDED.source_file, method = EXCLUDED.method,
                    updated_at = NOW()
                """,
                (stock_id, fiscal_year, row["item"], row["revenue"], row["pct"],
                 source_file, method),
            )
        cur.execute(
            """
            INSERT INTO tw.product_revenue_status
                (stock_id, fiscal_year, status, detail, source_file)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (stock_id, fiscal_year) DO UPDATE SET
                status = EXCLUDED.status, detail = EXCLUDED.detail,
                source_file = EXCLUDED.source_file, updated_at = NOW()
            """,
            (stock_id, fiscal_year, status, detail, source_file),
        )


def process(stock_id: str, fiscal_year: int) -> str:
    filename = find_report(stock_id, fiscal_year)
    if not filename:
        save(stock_id, fiscal_year, [], "", None, "no_report")
        return "no_report"
    content = download_pdf(stock_id, filename)
    if not content:
        save(stock_id, fiscal_year, [], "", filename, "download_failed")
        return "download_failed"
    try:
        rows, method = parse_pdf(content, fiscal_year)
    except Exception as e:  # pdfplumber raises a variety of errors on odd files
        save(stock_id, fiscal_year, [], "", filename, "parse_failed", f"{type(e).__name__}: {e}")
        return "parse_failed"
    if not rows:
        save(stock_id, fiscal_year, [], "", filename, "no_section")
        return "no_section"
    save(stock_id, fiscal_year, rows, method, filename, "ok", f"{len(rows)} rows")
    return "ok"


def daily_sweep(today=None) -> int:
    """Pick up annual reports as they are filed.

    Reports trickle in from March to August rather than landing on one day, so a
    single full run leaves the late filers (47 companies were still unfiled when
    the FY2025 sweep ran). This re-checks anything without a result and anything
    previously marked no_report; no_section is skipped because a report that
    discloses no mix will not start disclosing one.
    """
    from datetime import date as _date
    today = today or _date.today()
    fiscal_year = today.year - 1
    # Filing runs March-August; outside it a company that has not filed is very
    # unlikely to file tomorrow, so unfiled ones are re-checked weekly instead of
    # daily. Companies with no result at all (new listings) are always tried.
    in_season = 3 <= today.month <= 8
    retry_gap = "INTERVAL '0 days'" if in_season else "INTERVAL '7 days'"
    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT s.stock_id FROM tw.stocks s
            LEFT JOIN tw.product_revenue_status st
                   ON st.stock_id = s.stock_id AND st.fiscal_year = %s
            WHERE s.is_active AND s.security_type = 'STOCK'
              AND (st.status IS NULL
                   OR (st.status IN ('download_failed', 'parse_failed'))
                   OR (st.status = 'no_report' AND st.updated_at < NOW() - {retry_gap}))
            ORDER BY s.stock_id
            """, (fiscal_year,))
        targets = [r["stock_id"] for r in cur.fetchall()]
    season = "filing season" if in_season else "off season (weekly retry)"
    print(f"Annual report sweep FY{fiscal_year} [{season}]: {len(targets)} pending")
    tally = {}
    for i, stock_id in enumerate(targets, 1):
        started = time.time()
        status = process(stock_id, fiscal_year)
        tally[status] = tally.get(status, 0) + 1
        spare = 30.0 - (time.time() - started)
        if spare > 0 and i < len(targets):
            time.sleep(spare)
        if i % 25 == 0:
            print(f"  [{i}/{len(targets)}] " + "  ".join(f"{k}={v}" for k, v in sorted(tally.items())))
    print("Sweep done: " + "  ".join(f"{k}={v}" for k, v in sorted(tally.items())))
    return tally.get("ok", 0)


def backfill_filing_dates(fiscal_year: int, stocks=None, pace: float = 6.0) -> dict:
    """Fill tw.product_revenue_status.filing_date from the MOPS filing list.

    Only rows that already name a source_file are touched: the filename is the
    key we look the timestamp up by, and re-deriving it here would repeat work
    the revenue sweep already did.

    Nothing is written when the listing comes back empty. doc.twse.com.tw
    answers a rate-limited request with the same empty page it uses for "this
    company filed nothing", so an empty result is reported as unreachable and
    left for a later run rather than being recorded as an absent date.

    pace is why the default is six seconds and not the module's one-second
    floor: at one second the listing endpoint returned empty for 277 of 385
    companies, and every one of them answered when retried more slowly. The
    limit is on sustained rate, so going faster does not finish sooner — it
    just converts answers into blanks.
    """
    with get_cursor() as cur:
        if stocks:
            cur.execute(
                "SELECT stock_id, source_file FROM tw.product_revenue_status "
                "WHERE fiscal_year = %s AND source_file IS NOT NULL "
                "  AND filing_date IS NULL AND stock_id = ANY(%s) ORDER BY stock_id",
                (fiscal_year, list(stocks)))
        else:
            cur.execute(
                "SELECT stock_id, source_file FROM tw.product_revenue_status "
                "WHERE fiscal_year = %s AND source_file IS NOT NULL "
                "  AND filing_date IS NULL ORDER BY stock_id", (fiscal_year,))
        targets = [(r["stock_id"], r["source_file"]) for r in cur.fetchall()]

    print(f"Filing-date backfill FY{fiscal_year}: {len(targets)} pending")
    tally = {}
    for i, (stock_id, source_file) in enumerate(targets, 1):
        started = time.time()
        filings = list_filings(stock_id, fiscal_year)
        if not filings:
            outcome = "unreachable"
        elif source_file not in filings:
            outcome = "not_listed"
        elif filings[source_file] is None:
            outcome = "no_timestamp"
        else:
            with get_cursor() as cur:
                cur.execute(
                    "UPDATE tw.product_revenue_status SET filing_date = %s "
                    "WHERE stock_id = %s AND fiscal_year = %s",
                    (filings[source_file], stock_id, fiscal_year))
            outcome = "ok"
        tally[outcome] = tally.get(outcome, 0) + 1
        spare = pace - (time.time() - started)
        if spare > 0 and i < len(targets):
            time.sleep(spare)
        if i % 50 == 0 or i == len(targets):
            print(f"  [{i}/{len(targets)}] " + "  ".join(f"{k}={v}" for k, v in sorted(tally.items())))
    print("Backfill done: " + "  ".join(f"{k}={v}" for k, v in sorted(tally.items())))
    return tally


def _targets(args) -> list[str]:
    if args.stocks:
        return [s.strip() for s in args.stocks.split(",") if s.strip()]
    with get_cursor() as cur:
        if args.retry_failed:
            cur.execute(
                "SELECT stock_id FROM tw.product_revenue_status "
                "WHERE fiscal_year = %s "
                "  AND status IN ('download_failed', 'parse_failed', 'no_section') "
                "ORDER BY stock_id", (args.year,))
        else:
            cur.execute(
                "SELECT s.stock_id FROM tw.stocks s "
                "WHERE s.is_active AND s.security_type = 'STOCK' "
                "  AND NOT EXISTS (SELECT 1 FROM tw.product_revenue_status st "
                "                  WHERE st.stock_id = s.stock_id AND st.fiscal_year = %s) "
                "ORDER BY s.stock_id", (args.year,))
        return [r["stock_id"] for r in cur.fetchall()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, help="fiscal year, e.g. 2025")
    ap.add_argument("--stocks", help="comma-separated stock ids")
    ap.add_argument("--all", action="store_true", help="every listed stock not done yet")
    ap.add_argument("--retry-failed", action="store_true")
    ap.add_argument("--backfill-filing-date", action="store_true",
                    help="fill filing_date on rows that already have a source_file")
    ap.add_argument("--daily", action="store_true",
                    help="scheduled sweep: pending + previously unfiled, current fiscal year")
    ap.add_argument("--pace", type=float, default=30.0,
                    help="minimum seconds per company; doc.twse.com.tw starts "
                         "answering malformed HTTP after sustained PDF pulls")
    args = ap.parse_args()
    if args.daily:
        daily_sweep()
        return
    if args.backfill_filing_date:
        if not args.year:
            sys.exit("--backfill-filing-date needs --year")
        stocks = [s.strip() for s in args.stocks.split(",") if s.strip()] if args.stocks else None
        backfill_filing_dates(args.year, stocks, args.pace if args.pace != 30.0 else 6.0)
        return
    if not (args.stocks or args.all or args.retry_failed):
        sys.exit("Pass --stocks, --all, --retry-failed or --daily")

    targets = _targets(args)
    print(f"{len(targets)} targets")
    tally = {}
    for i, stock_id in enumerate(targets, 1):
        started = time.time()
        status = process(stock_id, args.year)
        tally[status] = tally.get(status, 0) + 1
        spare = args.pace - (time.time() - started)
        if spare > 0 and i < len(targets):
            time.sleep(spare)
        if i % 25 == 0 or i == len(targets):
            print(f"[{i}/{len(targets)}] " + "  ".join(f"{k}={v}" for k, v in sorted(tally.items())))
    print("Done: " + "  ".join(f"{k}={v}" for k, v in sorted(tally.items())))


if __name__ == "__main__":
    main()
