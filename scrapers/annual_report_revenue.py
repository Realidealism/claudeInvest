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
import html as html_lib
import re
import sys
import time
import unicodedata
from datetime import date, datetime
from typing import NamedTuple

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


class Filing(NamedTuple):
    """One row of the MOPS filing list, in the source's own terms.

    description is what MOPS calls the document -- 「股東會年報(股東會後修訂本)」,
    「股東會年報(適用永續揭露準則)」, 「英文版-股東會年報…」. It was being
    discarded, and its absence is why the picker had nothing to decide with
    except the filename.
    """
    filed: "date | None"
    uploaded: "datetime | None"
    description: str
    revision_note: str = ""


_ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S)
_CELL_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S)
_STAMP_RE = re.compile(r"^(\d{2,3})/(\d{2})/(\d{2})\s+(\d{2}):(\d{2}):(\d{2})$")


def _cell_text(cell: str) -> str:
    # The empty cells arrive as the literal entity, not as the character, so
    # unescaping has to happen before the "last filled cell" test can skip them.
    text = html_lib.unescape(re.sub(r"<[^>]+>", " ", cell))
    return re.sub(r"\s+", " ", text.replace(" ", " ")).strip()


def _parse_stamp(text: str):
    m = _STAMP_RE.match(text)
    if not m:
        return None, None
    roc_y, mm, dd, hh, mi, ss = (int(x) for x in m.groups())
    try:
        stamp = datetime(roc_y + 1911, mm, dd, hh, mi, ss)
    except ValueError:
        return None, None
    return stamp.date(), stamp


def list_filings(stock_id: str, fiscal_year: int) -> dict[str, Filing]:
    """Every filename on the filing list, with what MOPS says it is and when.

    The listing is keyed by ROC *filing* year, which is the fiscal year + 1 in AD
    terms: FY2025 is filed in 2026 = ROC 115.

    The upload timestamp is the only public statement of when a report became
    readable, and known_date on a classification row needs exactly that — a
    backtest reading a classification dated to the fiscal year would otherwise
    be using knowledge from months before anyone had it (R021). It is also what
    orders a report against its own later revision, which is why the full
    timestamp is kept and not just the day.

    An empty dict means the request failed OR the company filed nothing; the two
    are indistinguishable here, which is why callers must not read it as "no
    report" without a second look.
    """
    roc_filing_year = fiscal_year + 1 - 1911
    html = _post({"step": "1", "colorchg": "1", "co_id": stock_id,
                  "year": str(roc_filing_year), "mtype": "F"})
    if not html:
        return {}
    out: dict[str, Filing] = {}
    for row in _ROW_RE.findall(html):
        if ".pdf" not in row:
            continue
        cells = [_cell_text(c) for c in _CELL_RE.findall(row)]
        idx = next((i for i, c in enumerate(cells) if c.endswith(".pdf")), None)
        if idx is None:
            continue
        name = cells[idx]
        # The row's columns are fixed: 公司代號 | 年度 | 資料類型 | | 股東會別 |
        # 資料細節說明 | 備註/更正說明 | 電子檔案 | 大小 | 上傳時間 | 更補正.
        # So the description sits two cells before the filename and the revision
        # note one. Taking "the last filled cell before the filename" instead
        # read the revision note as the description -- 3071 資通's revision is
        # described as 「修訂頁碼：4、71、80」 and 4989 榮科's as 「修訂永續發展
        # 相關表格」, so neither contained 年報 and both were struck from the
        # candidate list, handing the choice back to the superseded edition;
        # 6550 光聖 prints 「無」 in that column on every row and lost all four.
        desc = cells[idx - 2] if idx >= 2 else ""
        note = cells[idx - 1] if idx >= 1 else ""
        if note in ("無", "無更補正選項"):
            note = ""
        filed = uploaded = None
        for c in cells[idx + 1:]:
            filed, uploaded = _parse_stamp(c)
            if filed:
                break
        out.setdefault(name, Filing(filed, uploaded, desc, note))
    # A layout change must not silently return nothing: fall back to the old
    # filename-only scan, which yields filings with no description -- and the
    # picker refuses to choose among several of those rather than guess.
    for name, roc_y, mm, dd in FILE_DATE_RE.findall(html):
        try:
            out.setdefault(name, Filing(date(int(roc_y) + 1911, int(mm), int(dd)),
                                        None, ""))
        except ValueError:
            out.setdefault(name, Filing(None, None, ""))
    for name in FILE_RE.findall(html):
        out.setdefault(name, Filing(None, None, ""))
    return out


def _pick_report(filings, stock_id: str,
                 fiscal_year: int) -> tuple[str | None, str | None]:
    """The annual report among a company's filings for that year.

    Selection is by the `{fiscal_year}_{stock_id}_` prefix, which MOPS gives to
    the annual report and to nothing else on the list -- a company's other
    filings that year are all prefixed with the *filing* year.

    Among several candidates the choice is made on what MOPS says each document
    is, and on when it was uploaded. Not on the form code: F04, F18 and F19 are
    the same document under three sustainability-disclosure regimes
    (「尚未適用永續揭露準則」, 「適用永續揭露準則」, 「僅永續專章」) and F11 is
    any of them 「股東會後修訂本」 -- revised after the AGM. A whitelist of codes
    once recorded 4923 力士 and 6425 易發 as having filed nothing, and the rule
    that replaced it -- take the last filename alphabetically -- was not a rule
    at all. It happened to agree for the 46 companies filing F04 then F11,
    because a revision sorts after its original AND is uploaded later. It was
    wrong wherever those two came apart: 2412 中華電 and 2884 玉山金 filed F19
    then F18, so it took the superseded February edition over the May one, and
    8906 花王 filed F18, F04 and F11, so it took the oldest of three -- and read
    it successfully, storing a plausible mix from a document revised twice
    since. A stale source produces no symptom downstream; nothing but the
    selection itself can catch it.

    So: the latest upload among the fiscal year's Chinese annual reports. When
    that cannot be established -- a missing timestamp, or two candidates tied at
    the newest -- nothing is chosen. A guess here is worse than a gap, because
    the gap is visible and the guess is not.
    """
    prefix = f"{fiscal_year}_{stock_id}_"
    hits = {n: f for n, f in filings.items() if n.startswith(prefix)}
    if not hits:
        # Say which of the two ways this failed. Without it, no_report answers
        # "did the company file an annual report" with a value that also means
        # "our request came back empty".
        return None, ("the filing list came back empty (request failed, or the "
                      "company filed nothing at all)" if not filings else
                      "no %s file among the %d filings listed: %s"
                      % (prefix, len(filings), ", ".join(sorted(filings))))

    described = {n: f for n, f in hits.items() if f.description}
    if described:
        annual = {n: f for n, f in described.items() if "年報" in f.description}
        pool = {n: f for n, f in annual.items()
                if not f.description.startswith("英文版")}
        basis = "MOPS document description"
    else:
        annual = dict(hits)
        # The list came back without descriptions (a layout change, or the
        # filename-only fallback). Say so, so that a choice made on weaker
        # grounds is not read as a choice made on the source's own terms.
        pool = {n: f for n, f in hits.items() if not re.search(r"FE\d", n)}
        basis = "filename only; the filing list carried no document descriptions"

    if not pool:
        listed = ", ".join(sorted("%s (%s)" % (n, f.description or "?")
                                  for n, f in hits.items()))
        if annual:
            # The document exists and is an annual report; what it is not is
            # Chinese, and every heading the readers below look for is. Saying
            # "no annual report" would send the next reader hunting for a filing
            # that is on the list.
            return None, ("the only annual report filed under %s is in English, "
                          "and the readers need the Chinese edition: %s"
                          % (prefix, listed))
        return None, ("none of the %d filings under %s is an annual report: %s"
                      % (len(hits), prefix, listed))
    if len(pool) == 1:
        return next(iter(pool)), None

    listed = ", ".join(sorted("%s [%s]" % (n, f.description or "?") for n, f in pool.items()))
    if any(f.uploaded is None for f in pool.values()):
        return None, ("unable to uniquely select the annual report: upload "
                      "timestamp missing (basis: %s); candidates %s" % (basis, listed))
    newest = max(f.uploaded for f in pool.values())
    latest = [n for n, f in pool.items() if f.uploaded == newest]
    if len(latest) > 1:
        return None, ("unable to uniquely select the annual report: latest "
                      "upload timestamp tied (basis: %s); candidates %s"
                      % (basis, listed))
    chosen = latest[0]
    return chosen, ("selected the latest annual-report filing by upload "
                    "timestamp (basis: %s): %s uploaded %s; candidates %s"
                    % (basis, chosen, newest, listed))


def find_report(stock_id: str,
                fiscal_year: int) -> tuple[str | None, str | None]:
    """(filename, ambiguity note) for the Chinese annual report of a fiscal year.

    The note is None in the ordinary case. It exists so that a choice made among
    several candidates reaches the status row instead of being decided quietly
    inside the picker.
    """
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
               "依據", "年增", "產值", "銷售總值",
               # totals and P&L captions sit in the same table but are not products
               "總額", "總計", "營業收入", "營業成本", "營業毛利", "營業利益",
               "營業費用", "銷貨收入淨額",
               # A share of what, where or when is not a share of which product.
               # These lifted 「北部地區 71%」, 「近視人口 60%」, 「國內約為 80%」
               # and 「本公司商品 100%」 into product mixes.
               "約佔", "約為", "佔全", "地區", "人口", "本公司", "營收", "目前", "年度",
               # The tail of an amount, captured because the item class allows
               # digits: 2701 萬企 stored 「357仟元 87%」 and 「646仟元 13%」 as its
               # two product lines. _is_label passes them -- 仟元 is two non-digit
               # characters -- so the unit has to be named here.
               "仟元", "千元", "百萬元", "億元")

# A fragment cut out of a sentence usually starts with the conjunction that
# joined it or ends on the preposition that continued it — 「其次為雜項紡織品(」,
# 「但由於直接輸美紡織成衣僅」, 「牌在整體冷藏果蔬汁市」.
PROSE_LEAD = re.compile(r"^(但|其次|以及|而|因|由於|雖|另|至於|滿足|提供|包括|涵蓋|致力|依據|根據|\)|）|、|，|及)")
# 「率達到」, 「增長率為」, 「售企業零售額年增率微幅轉正至」 -- a product name
# does not end on the verb or preposition that carried the sentence on.
PROSE_TAIL = re.compile(r"[市牌之於在含約僅為至到達成]$")
YEAR_LABEL = re.compile(r"^\d{2,4}年")


# Row captions that name a total rather than a product. The line reader used to
# test only 合計/總計/小計 with startswith, so 1256 鮮活's 「銷貨淨額 4,119,789
# 100%」 was read as a fourth product: its three correct rows summed to 200% and
# the whole read was then discarded by the 50..150 completeness bar. This is a
# rejection defect, not a coverage one -- the parser had already read the mix.
AGGREGATE_ROW = ("合計", "總計", "小計", "總額", "銷貨淨額", "銷售淨額",
                 "收入淨額", "營收淨額", "營業淨額")
# Matched whole, not as a substring: 「Total Station」 (全站儀) is a product.
AGGREGATE_WORD = ("total", "subtotal", "sum")


# What the company does with a product is not the product. 1413 宏洲 and 1452
# 宏益 both write 「聚酯粒及聚酯原絲之製造、行銷所佔營業比重為 100%」: the noun
# phrase before 之 is the product and 製造、行銷 is the activity, so a reader
# that stops at the delimiter lands on the verb. Matched whole, never as a
# substring -- 「行銷服務」 and 「3C產品銷售收入」 are real revenue lines.
# Deliberately short. The first draft also held 租賃, 服務, 維修, 批發, 零售,
# 生產 and 加工 -- and the stored rows say those are real revenue lines: 1110
# 東泥 租賃 6.92%, 3147 大綜 服務 3.93%, 6843 維修 5.41%, 5301 批發及零售
# 94.35%. Selling a service is revenue; 製造 and 行銷 are what a company does
# to something else, and can only ever be a fragment of the noun phrase that
# named it. As listed here the vocabulary rejects nothing currently stored.
ACTIVITY_ONLY = ("製造", "行銷", "銷售", "買賣", "產銷", "製銷", "經銷")
_ACTIVITY_SPLIT = re.compile(r"[、,，/／]|及|與|和")


def _is_activity_only(item: str) -> bool:
    """True when every part of the label names an activity, not a thing.

    Split rather than enumerated: 1413 宏洲 writes 「製造、行銷」 and other
    filings write 「製造及銷售」, 「生產、銷售及服務」 -- listing every
    combination would be a growing tuple that still misses the next one.
    A single part must match whole, so 「行銷服務」 and 「IC設計」 stay products.
    """
    compact = re.sub(r"\s+", "", unicodedata.normalize("NFKC", item or ""))
    parts = [p for p in _ACTIVITY_SPLIT.split(compact) if p]
    return bool(parts) and all(p in ACTIVITY_ONLY for p in parts)


def _is_aggregate_row(item: str) -> bool:
    """Whitespace- and width-insensitive: reports typeset totals as 「合 計」."""
    compact = re.sub(r"\s+", "", unicodedata.normalize("NFKC", item or "")).casefold()
    return (any(w in compact for w in AGGREGATE_ROW)
            or compact.strip(":：") in AGGREGATE_WORD)


def _page_text(page) -> str:
    """The one place a page becomes text, so every reader gets the same text.

    _clean (tables) and _rows_from_lines already ran NFKC on their own input,
    while the page filter and _rows_from_prose read the raw layer -- so 1236
    宏亞's 「營業比重約71.63％」 reached a percent regex that only knows the
    half-width sign. Which paths normalise must not depend on which helper
    happened to remember to.
    """
    return unicodedata.normalize("NFKC", page.extract_text() or "")


def _is_prose_noise(item: str) -> bool:
    # Compare without spaces: reports typeset labels as 「合 計」 with padding,
    # which slips past a plain substring test for 合計.
    compact = re.sub(r"\s+", "", item)
    if any(w in compact for w in PROSE_NOISE) or compact in ("合計", "總計", "小計"):
        return True
    if _is_activity_only(compact):
        return True
    if PROSE_LEAD.match(compact) or PROSE_TAIL.search(compact) or YEAR_LABEL.match(compact):
        return True
    # Unbalanced brackets mean the phrase was cut mid-parenthetical.
    if compact.count("(") + compact.count("（") != compact.count(")") + compact.count("）"):
        return True
    # A product name is a noun phrase; sentence punctuation means a fragment.
    return bool(re.search(r"[，。；！？,]", item)) or len(item) > 14


# A revenue mix names products. These name the market the company sells into,
# the filing's own section headings, or lines from the income statement -- all of
# which sit in tables that look exactly like a product table and carry numbers in
# exactly the same columns.
#
# Every entry below is a real row this parser stored as a product:
#   8401 白紗科   市場佔有率(%) / 整體印刷業銷售總值(仟元) / 白紗公司營收淨額(仟元)
#   2206 三陽工業  國內市場佔有率 / 成長率 / 銷售量(出廠)
#   2883 凱基金   採用權益法認列子公司、關聯企業及合資損益之份額
#   1905 華紙     ㄧ、業務內容 紙張
#   6141 柏承     全球PCB銷售產值   <- and this one reached a Core weight of 100%
#
# 柏承 is why this is a gate and not a blacklist: that row became the company's
# weight_item, and the evidence sentence quoted it, so a parser artefact was
# still supporting a Core ruling weeks later. Nothing downstream could see it,
# because every check below this layer takes the revenue table as given.
CONTEXT_WORDS = (
    # a share of the market is not a share of the revenue
    "市場佔有", "市場占有", "市佔率", "市占率", "市場規模", "市場需求",
    "銷售總值", "產值", "產業排名", "同業比較",
    # rates of change describe the industry, not what was sold
    "成長率", "年增率", "增長率", "衰退率",
    # the filing's own headings, lifted when a section title shares a line
    "業務內容", "營運概況", "產業概況", "業務範圍", "計畫開發",
    # income-statement captions: real numbers, wrong question
    "採用權益法", "損益之份額", "利息淨收益", "兌換淨損益", "資產減損",
    "營收淨額", "營業收入淨額", "銷售量",
)

# 銀行業, 鋼鐵業, 印刷業 -- an industry name, not a product line. Written as a
# shape rather than a word list so the next one does not need a code change:
# 1718 中纖 (a fibre maker) had 「銀行業 100%」 stored from a subsidiary's
# paragraph, and a 銀行 blacklist would have let 鋼鐵業 through next time.
#
# 事業 is the exception the shape cannot see: 生產事業, 不動產開發事業 and
# 投資及其他事業 are 1402 遠東新's own segment names and belong in a mix, while
# nothing calls its own product line 銀行業.
_INDUSTRY_SHAPE = re.compile(r"^[一-鿿]{2,4}業$")


def _is_industry_name(item: str) -> bool:
    flat = re.sub(r"\s+", "", item or "")
    return bool(_INDUSTRY_SHAPE.match(flat)) and not flat.endswith("事業")


# The first product of a table often shares its text line with the chapter title
# above it, so the reader gets 「ㄧ、業務內容 紙張」 where the product is 紙張.
# Rejecting the row loses a real line -- 1905 華紙's 紙張 is 68.50% of the company,
# and dropping it took the remaining rows under the completeness bar, which failed
# the whole filing.
#
# Stripping is deliberately conditional: the prefix comes off only when what is
# left still reads as a product. That is what keeps this a normalisation rather
# than a rename -- if a company genuinely sells something called 業務內容, there
# is nothing after the heading and the item is returned untouched for the context
# gate to judge.
SECTION_HEADINGS = ("業務內容", "營運概況", "產業概況", "業務範圍", "營業比重",
                    "主要產品", "主要商品", "產品項目", "商品項目", "產銷概況")
_SECTION_PREFIX = re.compile(
    r"^[一二三四五六七八九十ㄧ壹貳參肆\d]{0,3}\s*[、.．\)）]?\s*(?:" +
    "|".join(SECTION_HEADINGS) + r")\s*[:：]?\s*")


def _strip_section_prefix(item: str) -> str:
    """Remove a leading chapter heading, but only if a product name survives it."""
    m = _SECTION_PREFIX.match(unicodedata.normalize("NFKC", item or ""))
    if not m:
        return item
    rest = item[m.end():].strip(" 　:：、.")
    # Nothing usable behind the heading, or what is behind it is context of its
    # own: leave the item as it stands and let the caller's gate reject it.
    if not rest or not _is_label(rest) or _is_context_item(rest):
        return item
    return rest


_RESIDUAL = re.compile(r"^(其他|其它|other|others)([(（][^)）]*[)）])?$", re.I)


def _only_residual(rows) -> bool:
    """A mix made entirely of the residual category is not a mix.

    3709 鑫聯大投控 read a major-customer table: 甲公司 12.94 and 其他 87.06.
    Rejecting the anonymised counterparty left 其他 alone, which still summed
    into range and still beat the company's real mix -- two lines of prose on
    another page. 「其他」 is what is left after the products are named, so a
    read that names nothing else has not found the products.
    """
    return bool(rows) and all(
        _RESIDUAL.match(re.sub(r"\s+", "", r.get("item") or "")) for r in rows)


_ANON_PARTY = re.compile(r"^[甲乙丙丁戊己庚辛壬癸](公司|客戶|廠商|供應商)$")


def _is_context_item(item: str) -> bool:
    """Whether a row label describes context rather than something that was sold."""
    compact = re.sub(r"\s+", "", unicodedata.normalize("NFKC", item or ""))
    if any(w in compact for w in CONTEXT_WORDS):
        return True
    # 3709 鑫聯大投控 stored 「甲公司 12.94」 as a product. Filings anonymise
    # counterparties as 甲/乙/丙公司 in the major-customer and related-party
    # tables, and those tables carry percentages of sales, so they read exactly
    # like a mix. The company's real mix was two lines of prose on another page.
    if _ANON_PARTY.match(compact):
        return True
    # 「ㄧ、業務內容 紙張」: a section number glued to the first product
    return bool(re.match(r"^[一二三四五六七八九十ㄧ壹貳參肆]{1,3}[、.]", compact))


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
    # R2-10: a leading $ directly followed by a digit is the filing's currency sign on an
    # amount (3057 「$551」, 4113 「$609,477」, 2371's whole 部門收入 column). Only that shape:
    # NT$, US$, ＄, a space after the sign and a parenthesised ($705,357) are not read.
    if text[:1] == "$" and text[1:2].isdigit():
        text = text[1:]
    try:
        return float(text)
    except ValueError:
        return None


REGION_WORDS = ("美國", "日本", "中國", "大陸", "台灣", "臺灣", "歐洲", "亞洲", "德國",
                "韓國", "新加坡", "外銷", "內銷", "營運地點", "其他地區")


def _is_region(text: str) -> bool:
    """Whether a row label names a place rather than a product.

    The test has to normalise first. pdfplumber returns a cell as it is laid
    out, and a two-character Chinese name in a wide column comes back spaced
    out — "美 國", not "美國" — so a plain substring test against REGION_WORDS
    silently passes it through. 嘉聯益's entire geography table was stored as
    its product mix that way, 中國大陸 43.01% dropped along with it, and the
    three surviving rows looked like a well-formed mix summing to 57%.
    """
    flat = re.sub(r"\s+", "", text or "")
    return any(w in flat for w in REGION_WORDS)


# ---- Gate 1: is this table a revenue mix at all? -------------------------
# Judged on the label column and applied to the WHOLE table. Rejecting rows one
# by one lets an ineligible table survive by losing its worst ones: 2897 王道
# 銀行's income statement kept three plausible rows after the rest were dropped,
# and won the page on those three.
_TABLE_AXIS = (
    ("income_statement",
     ("營業收入", "營業成本", "營業毛利", "營業毛損", "營業費用", "營業損益",
      "營業外", "稅前", "稅後", "所得稅", "淨利", "淨損", "損益", "毛利率",
      "資產報酬率", "股東權益報酬率", "負債比率")),
    ("geography",
     ("地區", "區域", "內銷", "外銷", "國內", "國外", "海外", "亞洲", "美洲",
      "歐洲", "非洲", "大洋洲", "中國", "大陸", "台灣", "臺灣", "日本", "韓國",
      "北美", "東南亞", "北部", "中部", "南部", "東部", "台北", "臺北", "新北",
      "桃園", "台中", "臺中", "台南", "臺南", "高雄", "基隆", "新竹", "嘉義",
      "宜蘭", "花蓮", "台東", "彰化", "南投", "雲林", "屏東", "苗栗")),
    ("change_analysis",
     ("增減", "變動比率", "成長率", "年增", "年減", "較前一年")),
)
_ANON_TABLE_PARTY = re.compile(
    r"^([甲乙丙丁戊己庚辛壬癸]|[A-Z])(公司|客戶|廠商|供應商)$|被投資公司")


def _label_col(table) -> int:
    """Which column holds the row's name.

    廣宇 numbers its rows, so column 0 holds "1"/"2" and the product name sits one
    cell to the right.

    **Gate 1 does not consume this yet, and that is a known defect.** _table_axis
    takes each row's first non-empty cell, which on a numbered table is the serial
    number: 2069 運錩's supplier table offers it 其他 and 進貨淨額 as its entire
    body, and the four A公司/B公司 rows are never counted, so a purchase
    concentration table is not recognised as one. The fix is not to bolt this
    function onto Gate 1 -- that swaps one positional heuristic for another. Gate 1
    should consume a label column decided once, with amount, pct and change, by the
    structure parser ahead of it. Recorded in db/PARSER_TABLE_GATES.md; the
    behaviour is pinned by test_defect_gate_1_does_not_consume_the_label_column.
    """
    for row in table[1:]:
        cells = [_clean(c) for c in row]
        if len(cells) > 1 and cells[0] and _to_num(cells[0]) is not None and _is_label(cells[1]):
            return 1
    return 0


def _table_axis(table) -> str | None:
    """The axis this table is cut on, when it is not products. None means eligible.

    A label is counted once. The test is a majority of the data rows, so one
    地區 line inside a product table does not condemn it -- 2545 皇翔's three
    rows are 台北市可售金額 / 新北市可售金額 / 皇翔建設可售金額, and 6451 訊芯's
    page-80 table is 亞洲 / 美洲 / 其他.
    """
    body = []
    for row in table:
        cells = [_clean(c) for c in row]
        # Data rows only. A header row carries no value, and counting its cells
        # as labels dilutes the majority: 2897 王道銀行's income statement has
        # three captions and the header word 項目, and 2 of 4 is not a majority.
        if not any(_to_num(c) is not None for c in cells[1:] if c):
            continue
        first = next((c for c in cells if c), "")
        if first and _is_label(first):
            body.append(first)
    if len(body) < 2:
        return None
    for name, words in _TABLE_AXIS:
        if sum(any(w in x for w in words) for x in body) > len(body) / 2:
            return name
    if sum(bool(_ANON_TABLE_PARTY.match(x)) for x in body) > len(body) / 2:
        return "counterparty"
    return None


_YEAR_CELL = re.compile(r"^(1\d{2}|20\d{2})\s*年?\s*度?$")
# What the column holds. 「%」 and 「比例」 are here because that is what the
# filings write; leaving them out is what made a disclosed ratio invisible.
_RATIO_WORDS = ("比重", "百分比", "比率", "比例", "占比", "佔比", "%", "％")
_AMOUNT_WORDS = ("金額", "營業收入", "淨額", "營收", "銷售額")
# A rate of change is not a share, and no numeric range can tell them apart:
# 3017 奇鋐's 「變動比率」 column holds 94.59, a perfectly legal percentage that
# is a growth rate. 變動比率 also contains 比率, so this must win on the same
# cell rather than merely exist alongside.
_CHANGE_WORDS = ("變動比率", "變動", "增減", "成長率", "年增", "年減",
                 "月增", "月減", "yoy", "較前一年", "增減比率", "增減百分比")


def _header_kind(text: str) -> str | None:
    """What a header cell says its column holds. CHANGE beats PCT, PCT beats AMOUNT."""
    low = _clean(text).casefold()
    if any(w in low for w in _CHANGE_WORDS):
        return "change"
    if any(w in low for w in _RATIO_WORDS):
        return "pct"
    if any(w in low for w in _AMOUNT_WORDS):
        return "amount"
    return None


# ---- Scope identity (P22 repair, step 0) ------------------------------------------
# A value column may be selected only once its scope is known: which period, which
# entity, which measure. These helpers establish that from the filing's own labels and
# establish nothing from position. Selection itself comes later; nothing here is on a
# read path yet. Contract: db/P22_REPAIR_DESIGN.md.

_CN_DIGITS = str.maketrans("〇○零一二三四五六七八九", "000123456789")
# 114年 / 114年度 / 114度 / 2025年 / 一一四年度 / 民國一一四年度. The character after the
# year is required, so an amount such as 1,140 is never read as a year.
_PERIOD_LABEL = re.compile(r"(?<![\d.,])(1\d{2}|20\d{2})(?=年|度)"
                           r"|(一[〇○零一二][〇○零一二三四五六七八九])(?=年|度)")
_BARE_YEAR = re.compile(r"^(1\d{2}|20\d{2})$")


def _periods(text) -> tuple:
    """The ROC years a header label names, in order. AD years are converted (2025 -> 114).

    2727 writes 民國一一三年度, 2465 writes 114度, 2637 heads a column with a bare 2025.
    A label naming no year returns an empty tuple, which means unknown -- not current.
    """
    s = _clean(text)
    bare = _BARE_YEAR.match(s)
    if bare:
        years = [int(bare.group(1))]
    else:
        years = [int(m.group(1)) if m.group(1) else int(m.group(2).translate(_CN_DIGITS))
                 for m in _PERIOD_LABEL.finditer(s)]
    return tuple(y - 1911 if y >= 1912 else y for y in years)


# Entity words. 本公司 is weak on purpose: 5487's 佔本公司營收比重 means the company
# itself, and it marks parent-only only beside a consolidated column (2332's 本公司 |
# 合併公司). Selection decides that; this only records what the label says.
_ENTITY_WORDS = (
    ("consolidated", ("合併", "全球", "集團")),
    ("parent", ("個體", "母公司", "單一", "個別")),
    ("parent_weak", ("本公司",)),
    ("total", ("合計", "總計")),
    ("region", ("內銷", "外銷")),
    ("segment", ("部門",)),
)
# A share of something other than the company's revenue: 5487's 佔台灣IC設計業產值比重.
_SHARE_OF_OTHER = ("產值", "市場", "市佔", "佔有率", "占有率", "進貨", "採購", "產量", "銷量", "數量")
_SCOPE_NOISE = re.compile(r"[\d%().（）:：、,，/\-]|仟元|千元|新台幣|新臺幣|單位|金額|淨額|收入|營業"
                          r"|比重|比率|比例|百分比|占|佔|全年度|年度|年|度|民國|累計|同期|額|銷值")
# Amount words Gate 2 does not carry: 4995 heads its amounts 營業額. They are added here,
# for scope identity only; _header_kind is shared by the current reader and is unchanged.
_SCOPE_AMOUNT_WORDS = ("營業額", "銷值", "銷貨")


class _ColumnScope(NamedTuple):
    col: int
    measure: str | None      # "amount" / "pct" / "change" / None, from _header_kind
    share_of: str | None     # for a share: "revenue" or "other"
    periods: tuple           # ROC years the label names; empty = unknown
    entity: frozenset        # kinds from _ENTITY_WORDS; empty = unknown
    residual: str            # what the label says beyond period, measure and entity
    label: str


def _scope_header_rows(table):
    """Rows above the first row carrying a value after the label column.

    Any value ends the header -- a single 「11%」 is data (1305) -- but a year is not a
    value, so 2739's 113年度 and 2637's 2025 stay header."""
    head = []
    for row in table[:6]:
        cells = [_clean(c) for c in row]
        if any(c and not _periods(c) and _to_num(c) is not None for c in cells[1:]):
            break
        head.append(cells)
    return head


_CELL_PLACEHOLDER = re.compile(r"^[-－—―]+%?$")
_CELL_TEXT_AMOUNT = re.compile(r"^\d[\d,.]*\s*[億萬仟佰千]")


def _value_like_column(body, c) -> bool:
    """Every non-empty cell of column c is a placeholder or an amount written in words."""
    cells = [_clean(r[c]) for r in body if c < len(r) and _clean(r[c])]
    return bool(cells) and all(_CELL_PLACEHOLDER.match(x) or _CELL_TEXT_AMOUNT.match(x) for x in cells)


def _column_scopes(table) -> list:
    """The scope identity of every data column, from the header rows alone.

    pdfplumber does not always put a header cell over its values: 5310 heads columns
    2/5/8/11 and prints its values at 1/4/7/10, and 2637 puts spacer columns between
    them. So a header row with fewer cells than there are data columns is a spanning
    title that covers the columns to its right, and a row with a cell for every data
    column labels each data column with its nearest cell.

    The measure comes from a column's own cells before any spanning one, because 7883's
    比重(%) is printed askew and spans onto an amount column. A share column that names
    no period or entity takes the missing parts from the amount column immediately to
    its left: 8996 prints 個體財報 over the amount and 營業比重(%) beside it, and 2332
    prints 114年度 only over the amounts. Two shares side by side never lend each other
    scope: 5487's company share and industry share are different things.
    """
    head = _scope_header_rows(table)
    body = table[len(head):]
    width = max((len(r) for r in table), default=0)
    # R2-3, ruled 2026-09-15: a column whose cells hold only placeholders (6827's 114 「- | -」)
    # or amounts written as text (5609's 「188億0仟4佰萬元」) is still a column; left out, its
    # year label slid onto the neighbouring 113 column. A column of descriptions is not added.
    data_cols = [c for c in range(1, width)
                 if any(c < len(r) and _to_num(_clean(r[c])) is not None for r in body)
                 or _value_like_column(body, c)]
    if not data_cols:
        return []
    labels = {dc: [] for dc in data_cols}          # dc -> [(depth, text, own)]
    for depth, row in enumerate(head):
        cells = [(c, t) for c, t in enumerate(row) if c >= 1 and t]
        if not cells:
            continue
        spanning = len(cells) < len(data_cols)
        for dc in data_cols:
            if spanning:
                left = [(c, t) for c, t in cells if c <= dc]
                if left:
                    c, t = left[-1]
                    labels[dc].append((depth, t, c == dc))
            else:
                c, t = min(cells, key=lambda ct: (abs(ct[0] - dc), -ct[0]))
                labels[dc].append((depth, t, True))

    scopes = []
    for dc in data_cols:
        parts = labels[dc]
        text = " ".join(t for _, t, _ in parts)
        measure = None
        for own in (True, False):
            for _, t, is_own in sorted(parts, key=lambda p: -p[0]):
                kind = _header_kind(t) or ("amount" if any(w in t for w in _SCOPE_AMOUNT_WORDS) else None)
                if is_own == own and kind:
                    measure = kind
                    break
            if measure:
                break
        periods = tuple(dict.fromkeys(y for _, t, _ in parts for y in _periods(t)))
        entity = frozenset(kind for kind, words in _ENTITY_WORDS
                           if any(w in _clean(text) for w in words))
        share_of = None
        if measure == "pct":
            share_of = "other" if any(w in _clean(text) for w in _SHARE_OF_OTHER) else "revenue"
        residual = _SCOPE_NOISE.sub("", _PERIOD_LABEL.sub("", _clean(text)))
        for _, words in _ENTITY_WORDS:
            for w in words:
                residual = residual.replace(w, "")
        scopes.append(_ColumnScope(dc, measure, share_of, periods, entity, residual, text))

    for i, s in enumerate(scopes):
        if s.measure != "pct" or i == 0:
            continue
        left = scopes[i - 1]
        # the column it pairs with is anything but another share: 8996's 個體財報 names
        # no measure at all, and 5487's two shares must not lend each other scope
        if left.measure == "pct":
            continue
        scopes[i] = s._replace(periods=s.periods or left.periods,
                               entity=s.entity or left.entity)
    return scopes


class ScopeUnresolved(Exception):
    """The reader cannot select a value uniquely from established scope, so it refuses.

    Ruled 2026-09-14 (D1): terminal. The section is not handed to a weaker reader to
    guess -- 2404 was re-guessed that way after its table was correctly declined. The
    message carries the reason and the header text that caused it. It is deliberately
    not parse_conflict, which asserts conflicting evidence was parsed and on which
    save() retracts."""

    def __init__(self, reason: str, header: str):
        super().__init__("%s | %s" % (reason, header))
        self.reason = reason
        self.header = header


def _period_selection(table, fiscal_year):
    """Which data columns the fiscal year owns, by period identity alone.

    ('target', cols)      columns whose labels name the fiscal year
    ('unlabelled', cols)  no column names any period; cols are the revenue-share candidates
    ('no_target', years)  periods are named and none is the fiscal year
    """
    target = fiscal_year - 1911
    scopes = _column_scopes(table)
    named = [s for s in scopes if s.periods]
    if not named:
        return ("unlabelled",
                [s.col for s in scopes if s.measure == "pct" and s.share_of == "revenue"], scopes)
    cols = [s.col for s in named if target in s.periods]
    if not cols:
        return "no_target", sorted({p for s in named for p in s.periods}), scopes
    return "target", cols, scopes


_WHOLE = frozenset({"consolidated", "total"})
_PART = frozenset({"parent", "segment", "region"})


def _entity_select(pool):
    """(the chosen share column, refusal reason) among candidates of one period -- P22-2.

    A revenue mix describes the whole company (P20), so the one consolidated or total
    column is the target. 本公司 is weak: beside a consolidated column it is the
    parent-only side and loses (2332); alone it is the company itself (5487). A pool
    printing only parent-only columns does not provide the target scope (D2). Position
    never decides between columns.
    """
    whole = [s for s in pool if s.entity & _WHOLE and not s.entity & _PART]
    if whole:
        if len(whole) == 1:
            return whole[0], None
        return None, "%d consolidated or total columns" % len(whole)
    if pool and all(s.entity & {"parent"} for s in pool):
        return None, "target consolidated scope unavailable"
    if any(s.entity & {"segment", "region"} for s in pool):
        return None, "segment or region columns without a total"
    if len(pool) == 1:
        return pool[0], None
    return None, "%d share columns and no period or entity to tell them apart" % len(pool)


def _period_refusal(table, fiscal_year):
    """(reason, header) when no column was selected for a reason of scope, else None."""
    return _select_columns(table, fiscal_year)[2]


def _header_columns(table, fiscal_year):
    """(amount_col, pct_col) for the wanted year. See _select_columns."""
    amount_col, pct_col, _ = _select_columns(table, fiscal_year)
    return amount_col, pct_col


def _select_columns(table, fiscal_year):
    """(amount_col, pct_col, refusal) for the wanted year — scope, then Gate 2 and Gate 3.

    refusal is (reason, header text) when no column could be selected because its scope
    could not be established, and None otherwise.

    Gate 2 asks what each header cell says its column holds; Gate 3 asks where
    in the data that column actually sits. The two are kept apart because the
    data can be silent: a percentage-only table has no value above 100 and a
    single-product table has one value, and neither silence means the header
    was wrong.
    """
    # Header rows only. A caption row can push the real header down (6547 高端:
    # row 0 is "2.主要產品之營業比重:"), so scan a few rows but stop at the first
    # data row -- otherwise a data cell like "43.4%" is read as a column label
    # (1455 尼龍).
    head = []
    for row in table[:4]:
        cells = [_clean(c) for c in row]
        # A bare year is not data: 2884 玉山金 heads its columns 2025 / 2024,
        # which parse as numbers above 100.
        nums = [_to_num(c) for c in cells if c and not _YEAR_CELL.match(c)]
        nums = [v for v in nums if v is not None]
        # Two tests, because either alone misses a shape. A value above 100 marks
        # the data in a table carrying amounts; a percentage-only table never has
        # one -- 1432 大魯閣's 「百貨商場 41% 59%」 -- so two numbers in a row with
        # a text label mark it there.
        if any(v > 100 for v in nums) or len(nums) >= 2:
            break
        head.append(row)
    if not head or len(head) == len(table):
        # Neither test fires on a percentage-only table whose rows carry a single
        # value each: 3054 立萬利's 「114年度營業比重(%)」 column holds 64.93 /
        # 35.07 / 100.00 -- never above 100, never two to a row -- so the loop
        # swallows the table and Gate 3 is left with nothing to look at. An empty
        # body then makes the named pct column look contradicted by its own data,
        # and the merged-header fallback hands that very column over as the
        # amount, storing the mix with NULL percentages (P19: 2301, 2426, 3054,
        # 4545). Whatever else is uncertain, a header is not the whole table.
        head = table[:1]

    # P22-1: which columns belong to the fiscal year is decided by period identity --
    # every form a filing writes the year in -- and never by position. The old span
    # read Arabic years only, so 一一三年度 | 一一四年度 looked like no year at all and
    # the first column was taken as current (2404, 2727, 7742).
    period, cols, scopes = _period_selection(table, fiscal_year)
    by_col = {s.col: s for s in scopes}
    header = " | ".join(s.label for s in scopes)[:300]
    scope_body = table[len(_scope_header_rows(table)):]

    def contradicted(s):
        # R2-7: a share label over values above 100 is contradicted by its own data (1467's
        # 營業比重 over 604,445 … beside 百分比(%) over 11.74 …); it is not a share, and it is not
        # made an amount either -- a label is not inferred back from magnitude
        vals = [_to_num(_clean(r[s.col])) for r in scope_body if s.col < len(r)]
        return s.measure == "pct" and any(v is not None and v > 100 for v in vals)

    def amount_partner(pool_scopes, chosen):
        # R2-1: the share's amount is the one amount column whose identity does not contradict
        # the share's -- the same stated entity, or an entity left unstated -- and it must be
        # unique. An exact match comes first; unknown is no conflict, never any value.
        amounts = [s for s in pool_scopes if s.measure == "amount"]
        exact = [s for s in amounts if chosen.entity and s.entity == chosen.entity]
        if exact:
            return exact[0].col if len(exact) == 1 else None
        unstated = [s for s in amounts if not s.entity]
        return unstated[0].col if len(unstated) == 1 else None
    if period == "no_target":
        # periods are named and none is the fiscal year (7907: 112 | 113)
        return None, None, ("no target period; the header names %s"
                            % ", ".join(map(str, cols)), header)
    if period == "unlabelled":
        # 1326 台化 labels the column only 佔營業額比重（％） and puts the year in the
        # surrounding heading. That is accepted only while it is unique -- one share
        # column, or one the entity words single out (P22-2: 2492's 全球, 4722's 合併).
        pool = [by_col[c] for c in cols]
        if len(pool) >= 2:
            pool = [s for s in pool if not contradicted(s)] or pool
        if not pool:
            ratio = [idx for row in head for idx, cell in enumerate(row)
                     if _header_kind(cell) == "pct"]
            return None, (ratio[0] if len(ratio) == 1 else None), None
        chosen, why = _entity_select(pool)
        if chosen is None:
            return None, None, (why, header)
        # the amount follows the share's entity (8996's 合併財報, 4131's 合計), or is the one
        # amount column leaving it unstated (2374, 9928, 3055, 4534, 1737); otherwise NULL (D4)
        return amount_partner(scopes, chosen), chosen.col, None

    target = [by_col[c] for c in cols]
    shares = [s for s in target if s.measure == "pct" and s.share_of == "revenue"]
    if len(shares) >= 2:
        shares = [s for s in shares if not contradicted(s)] or shares
    pool = shares or [s for s in target if s.measure is None]
    if any(s.entity for s in pool):
        # P22-2: several entities under the fiscal year. 2332's 本公司 | 合併公司 and
        # 8472's 個體 | 合併 both carry 114; the first of them was taken.
        chosen, why = _entity_select(pool)
        if chosen is None:
            return None, None, (why + " under the target period", header)
        # R2-1: the chosen entity's columns, plus its amount partner when that leaves the
        # entity unstated (1795's 114年度 金額 beside 佔合併營收比重)
        partner = amount_partner(target, chosen)
        cols = [s.col for s in target if s.entity == chosen.entity or s.col == partner]

    body = table[len(head):]
    allowed = set(cols)

    def in_period(idx):
        # A header cell may sit one column right of its values (5310, 2637), but only
        # over a column carrying no values of its own: 9930's 占銷售額% one column right
        # of its 114 consolidated values heads 113's parent-only values.
        return idx in allowed or ((idx - 1) in allowed and not values(idx))

    def named(kind):
        """Deepest header row wins: it labels the column, the year row labels a
        span. 1584 精剛's 「114年營收比重」 otherwise beats the 「百分比」 under it."""
        for row in reversed(head):
            hits = [idx for idx, cell in enumerate(row)
                    if in_period(idx) and _header_kind(cell) == kind]
            if hits:
                return hits[0]
        return None

    def values(idx):
        out = [_to_num(_clean(r[idx])) for r in body if idx < len(r)]
        return [v for v in out if v is not None]

    def locate(kind):
        """Find a column of that kind from the data alone — used only when the
        header named none, or named one the data contradicts."""
        best = None
        for idx in sorted(allowed):
            vals = values(idx)
            if len(vals) < 2:
                continue
            if kind == "pct":
                if any(v < 0 or v > 100 for v in vals):
                    continue
                full = sum(vals)
                # A 合計 row doubles the column, so either reading may be right;
                # the value dropped has to be the total, which is the largest.
                cands = [x for x in (full, full - max(vals)) if 40 <= x <= 160]
                if not cands:
                    continue
                score = min(abs(100 - x) for x in cands)
            else:
                if not any(v > 100 for v in vals):
                    continue
                score = -sum(1 for v in vals if v > 100)
            if best is None or score < best[0]:
                best = (score, idx, False)
            elif score == best[0]:
                best = (score, best[1], True)
        # A tie is not decided by position: 2739's four hotels all sum to 100 under
        # 114, and the first of them is no more the company's mix than the last.
        return best[1] if best and not best[2] else None

    # ---- Gate 3: data is veto evidence, not mandatory evidence -------------
    def placeholders_only(idx):
        cells = [_clean(r[idx]) for r in body if idx < len(r) and _clean(r[idx])]
        return bool(cells) and all(_CELL_PLACEHOLDER.match(x) for x in cells)

    pct_col = named("pct")
    if pct_col is not None and placeholders_only(pct_col):
        pass    # R2-3: the fiscal year's column holds no value; its shares are NULL, not relocated
    elif pct_col is not None:
        vals = values(pct_col)
        if not vals:
            # The column is empty across every data row. That is not the header
            # being wrong about a ratio existing -- it is the header index not
            # lining up with the data, which pdfplumber does when a table carries
            # spacer columns (2637 慧洋: 金額 at header index 4, amounts at data
            # index 3). Keep the claim, relocate it.
            pct_col = locate("pct")
        elif any(v < 0 or v > 100 for v in vals):
            pct_col = locate("pct")
        # else: values present and all within range -- the header stands, however
        # few of them there are. 3526 凡甲「連接器 100」 and 3588 通嘉
        # 「電源管理IC 100」 are one-line mixes, and one product is still a mix.
    else:
        pct_col = locate("pct")

    amount_col = named("amount")
    if amount_col is not None and placeholders_only(amount_col):
        pass    # R2-3, as for the share
    elif amount_col is not None and not values(amount_col):
        amount_col = locate("amount")
    elif amount_col is None:
        amount_col = locate("amount")

    if (pct_col is not None and placeholders_only(pct_col)
            and (amount_col is None or placeholders_only(amount_col))):
        return amount_col, pct_col, None    # every target value is a placeholder: no rows, no refusal
    if pct_col is None and amount_col is None:
        if len(allowed) == 1:
            # Merged header with no sub-labels: the year column itself holds the value.
            return cols[0], None, None
        # with several, no column is the fiscal year's by position
        return None, None, ("%d columns under the target period and none identified"
                            % len(allowed), header)
    return amount_col, pct_col, None


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


def _join_split_rows(table, label_col, value_cols):
    """Rejoin a logical row that pdfplumber returned as two physical ones.

    6533 晶心科's 權利金收入 row carries only the prior year's figures; the wanted
    year's 282,337 / 19 sit on the next row with an empty label. Each half alone
    fails the reader -- one has no value in the chosen columns, the other has no
    name -- so the line is lost and the table sums to 81 against its own 合計 100.

    Only a label-without-value followed by a value-without-label is joined, which
    is the shape of a split row and not the shape of two real rows.
    """
    cols = [c for c in value_cols if c is not None]

    def has_value(cells):
        return any(c < len(cells) and _to_num(cells[c]) is not None for c in cols)

    def has_label(cells):
        return len(cells) > label_col and bool(cells[label_col])

    out, i = [], 0
    while i < len(table):
        cells = [_clean(c) for c in table[i]]
        row = list(table[i])
        if has_label(cells) and not has_value(cells) and i + 1 < len(table):
            nxt = [_clean(c) for c in table[i + 1]]
            if not has_label(nxt) and has_value(nxt):
                width = max(len(table[i]), len(table[i + 1]))
                row = []
                for k in range(width):
                    a = table[i][k] if k < len(table[i]) else ""
                    b = table[i + 1][k] if k < len(table[i + 1]) else ""
                    row.append(a if _clean(a) else b)
                i += 1
        out.append(row)
        i += 1
    return out


def _declared_total(table, pct_col):
    """The percentage the table's own 合計 row claims, when it has one."""
    if pct_col is None:
        return None
    for row in table:
        cells = [_clean(c) for c in row]
        label = next((c for c in cells if c), "")
        if label and _is_aggregate_row(label) and pct_col < len(cells):
            value = _to_num(cells[pct_col])
            if value is not None and 50 <= value <= 150:
                return value
    return None


_ACCOUNTING_NEG = re.compile(r"^\((.+)\)$")


def _accounting_num(text):
    """A number as a filing prints it, negatives included.

    `_to_num` is deliberately strict and shared by every reader; widening it was
    measured over 90 filings and made two companies worse, so it stays. This is
    local to arithmetic that has to add a column up, where a number the filing
    printed but the reader cannot see is not the same as no number at all.
    """
    value = _to_num(text)
    if value is not None:
        return value
    s = _clean(text)
    if not s:
        return None
    negative = False
    m = _ACCOUNTING_NEG.match(s)
    if m:
        s, negative = m.group(1), True
    elif s[0] in "\u25b3\u25b2\u2212":        # \u25b3 \u25b2 \u2212
        s, negative = s[1:], True
    value = _to_num(s)
    if value is None:
        return None
    return -value if negative else value


def _unnamed_total_row(table, label_col, amount_col, pct_col):
    """The index of the row that is this table's total, when nothing says so.

    2897 王道銀行's income-statement table ends on a row called 淨收益 -- its own
    total, 6,690,492, exactly the sum of the eight rows above it -- and no word in
    it says 合計. So the reader emitted the total as a ninth product at 100%, the
    accepted rows came to 125, and 125 sits inside the 50..150 window that would
    otherwise have caught it. The table had no declared total to be checked
    against because the declared total was being read as a product.

    Arithmetic settles it without a word of vocabulary: a row whose amount equals
    what the other rows add up to is not one of them. The percentage guard is
    what keeps a coincidence from qualifying -- a part that happens to equal the
    rest still has to claim ~100% of the table to be mistaken for the whole.

    Being arithmetic, it is only entitled to run when the arithmetic is sound,
    which costs two refusals. Without a percentage column the guard above is
    vacuous and the inference is not made at all (P16). And an amount the filing
    printed but this reader cannot parse -- `(1,485,732)`, `\u25b3x` -- is not the
    same as a row carrying no amount: skipping it silently leaves the sum short
    and lets another row match by accident, so that also abandons the inference
    (P15). A row's identity is not its sign.
    """
    if amount_col is None:
        return None
    if pct_col is None:
        # P16. The percentage guard is what separates "this row is the whole" from
        # "this row happens to equal the rest". With no percentage column every
        # pct is None, the guard passes for free, and any product holding exactly
        # half a table qualifies -- 甲500 / 乙200 / 丙200 / 丁100 deleted 甲, the
        # largest real product, with no percentages left for the completeness
        # check to notice. No percentages means no evidence that any row claims to
        # be the whole, so there is nothing to infer from.
        return None
    seen = []
    for i, row in enumerate(table):
        if i == 0:
            continue             # the header, which _rows_from_table also skips
        cells = [_clean(c) for c in row]
        if len(cells) <= max(label_col, amount_col) or not cells[label_col]:
            continue
        if _is_aggregate_row(cells[label_col]):
            return None          # the filing names its total; nothing to infer
        raw = cells[amount_col]
        if not raw or not any(ch.isdigit() for ch in raw) or "年度" in raw:
            # No amount is claimed here, so there is nothing to fail to read.
            # A cell with no digit at all is a label -- 2897's header runs two
            # rows deep and its amount cell says 金額 -- and a bare year heads
            # a column rather than filling one.
            continue
        amount = _accounting_num(raw)
        if amount is None:
            # P15. An amount is printed here and cannot be read. Skipping it would
            # leave the sum short by an unknown quantity and let some other row
            # match it by accident, so the inference is abandoned instead.
            return None
        seen.append((i, amount, _to_num(cells[pct_col])
                     if pct_col < len(cells) else None))
    if len(seen) < 4:
        return None
    found = []
    for i, amount, pct in seen:
        rest = sum(a for j, a, _ in seen if j != i)
        if amount and abs(amount - rest) <= max(1.0, 0.005 * abs(amount)):
            if pct is None or 95 <= pct <= 105:
                found.append(i)
    return found[0] if len(found) == 1 else None


def _looks_like_mix(table) -> bool:
    """Would this table's rows read as a mix, whatever its columns turn out to be?

    A scope refusal is terminal, so it is raised only for a table that is a mix
    candidate: two or more rows passing _rows_from_table's own item filter and
    carrying a value. The table has already passed the region, context and axis gates."""
    label_col = _label_col(table)
    items = 0
    for idx, row in enumerate(table):
        if idx == 0:
            continue
        cells = [_clean(c) for c in row]
        if len(cells) <= label_col or not cells[label_col]:
            continue
        item = _strip_section_prefix(cells[label_col])
        if (not _is_label(item) or len(item) > 30 or _is_aggregate_row(item)
                or any(h in item for h in ("年度", "項目", "比重"))
                or _is_region(item) or _is_context_item(item)):
            continue
        if any(c and _to_num(c) is not None for c in cells[label_col + 1:]):
            items += 1
    return items >= 2


# R1-1 at header level: measures that are not a revenue share. 5452 heads 需求量 | 銷售量 |
# 市場佔有率; 5211 heads 內銷 量 | 內銷 值 | 外銷 量 | 外銷 值.
_NON_REVENUE_MEASURES = ("產量", "銷量", "需求量", "銷售量", "市占率", "市場占有率", "市場佔有率",
                         "預計數量", "複合年成長率",
                         # R2-4a / R2-2a, ruled 2026-09-15: 2105's 114年度生產, 4142's 數量 | 佔有率,
                         # 5522's ranking
                         "生產", "數量", "佔有率", "占有率", "名次", "排名")


def _non_revenue_table(table) -> bool:
    """Does the header name only non-revenue measures? Then the table is not a mix candidate.

    It disqualifies only where no revenue-share column remains: a product table that also
    prints each product's market share keeps its share column and stays a candidate."""
    head = [c for row in _scope_header_rows(table) for c in row if c]
    text = " ".join(head)
    # R2-4b, ruled 2026-09-15, narrow: rows named by periods and every share a region -- 2449's
    # 民國110年 … 民國114年 × 內銷比重 | 外銷比重 -- is a region axis over years. Item rows under
    # 內銷 | 外銷 columns (1307, 1708) are not this shape.
    body_labels = [_clean(r[0]) for r in table[len(_scope_header_rows(table)):] if r and _clean(r[0])]
    shares = [s for s in _column_scopes(table) if s.measure == "pct"]
    if (body_labels and sum(bool(_periods(x)) for x in body_labels) * 2 > len(body_labels)
            and shares and all("region" in s.entity for s in shares)):
        return True
    quantity_value = "量" in head and "值" in head
    if not quantity_value and not any(w in text for w in _NON_REVENUE_MEASURES):
        return False
    return not any(s.measure == "pct" and s.share_of == "revenue"
                   and not any(w in _clean(s.label) for w in _NON_REVENUE_MEASURES + ("量",))
                   for s in _column_scopes(table))


def _group_share_rows(table, label_col, amount_col, pct_col) -> set:
    """R2-9: rows whose printed share belongs to a group, not to the row's own amount.

    1711 永光 prints 電子化學品 | 光阻劑 | 530,036 | 24.0% and, on the next row, only
    其他 | 1,290,507 with no share of its own. Against the table's 合計 7,568,574 the row's
    amount alone is 7.0%; the row and the amount-only row below it are 24.0%. The share is
    the group's, and 530,036 is one sub-row of it -- a number that can be found is not the
    category's revenue.

    The filing's own arithmetic decides, and only when it can: a declared total, a share
    and an amount on the row, and one or more rows directly below carrying an amount and no
    share. The row enters this branch only when its own amount does not reproduce its share
    and the amount with the rows below does, within the share's printed rounding. A row
    whose amount fits its share keeps it (3708 航太複合材料, 4545 塑膠射出件), and a row
    that is its sub-rows' subtotal is not a group. Full scan: 1711 only.
    """
    if amount_col is None or pct_col is None:
        return set()
    rows = [[_clean(c) for c in r] for r in table]

    def cell(r, k):
        return r[k] if k < len(r) else ""

    total = None
    for r in rows:
        first = next((c for c in r if c), "")
        if first and _is_aggregate_row(first) and _to_num(cell(r, amount_col)):
            total = _to_num(cell(r, amount_col))
            break
    if not total:
        return set()
    out = set()
    for i in range(1, len(rows)):
        r = rows[i]
        label = cell(r, label_col)
        pct, amount = _to_num(cell(r, pct_col)), _to_num(cell(r, amount_col))
        if not label or _is_aggregate_row(label) or pct is None or amount is None:
            continue
        below = []
        for s in rows[i + 1:]:
            first = next((c for c in s if c), "")
            if (_to_num(cell(s, pct_col)) is not None or _CELL_PLACEHOLDER.match(cell(s, pct_col))
                    or _to_num(cell(s, amount_col)) is None or _is_aggregate_row(first)):
                break
            below.append(_to_num(cell(s, amount_col)))
        if not below:
            continue
        digits = re.search(r"\.(\d+)", cell(r, pct_col))
        tolerance = 0.5 * 10 ** -(len(digits.group(1)) if digits else 0) + 0.01
        subtotal = abs(amount - sum(below)) <= max(1.0, 0.005 * amount)
        own = abs(100.0 * amount / total - pct) <= tolerance
        group = abs(100.0 * (amount + sum(below)) / total - pct) <= tolerance
        if group and not own and not subtotal:
            out.add(i)
    return out


def _rows_from_table(table, fiscal_year) -> list[dict]:
    """Pull (item, revenue, pct) for one fiscal year out of a pdfplumber table."""
    if _non_revenue_table(table):
        return []
    amount_col, pct_col = _header_columns(table, fiscal_year)
    refusal = None
    if amount_col is None and pct_col is None:
        # No column: either the table has no shape a mix can take, or its scope could
        # not be established. Only the second is a refusal, and only once the gates
        # below have admitted the table as a mix -- a research-expense table on the
        # same page must not end the section's reading.
        refusal = _period_refusal(table, fiscal_year)
        if refusal is None:
            return []
    # A geography table (營業單位之營運地點 / 銷售地區) has the same year headers as
    # the product table, so reject it by looking at what the rows are named.
    labels = [_clean(row[0]) for row in table if row and _clean(row[0])]
    if labels and sum(_is_region(x) for x in labels) >= max(2, len(labels) // 3):
        return []
    # Same test one level up: dropping context rows one by one leaves a market
    # table looking like a thin product table (8401 白紗科 kept two of four rows
    # that way). If context is what the table is mostly about, none of it is a
    # revenue mix -- including the rows that happen to look clean.
    body = [x for x in labels[1:]] or labels
    if body and sum(_is_context_item(x) for x in body) >= max(1, len(body) // 2):
        return []
    # Gate 1, before any column is looked at: a table cut on an axis that is not
    # products is rejected whole. Column-level correctness cannot rescue it --
    # 2897 王道銀行's income statement has a correct header, a correctly located
    # percentage column and valid percentages, and is still an income statement.
    if _table_axis(table):
        return []
    if refusal is not None:
        if _looks_like_mix(table):
            raise ScopeUnresolved(*refusal)
        return []
    out = []
    label_col = _label_col(table)
    table = _join_split_rows(table, label_col, (amount_col, pct_col))
    own_total = _unnamed_total_row(table, label_col, amount_col, pct_col)
    group_shares = _group_share_rows(table, label_col, amount_col, pct_col)
    for idx, row in enumerate(table):
        if idx == 0 or idx == own_total:
            continue
        cells = [_clean(c) for c in row]
        if not cells or len(cells) <= label_col or not cells[label_col]:
            continue
        item = _strip_section_prefix(cells[label_col])
        if not _is_label(item):
            continue
        if (len(item) > 30 or _is_aggregate_row(item)
                or any(h in item for h in ("年度", "項目", "比重"))
                or _is_region(item) or _is_context_item(item)):
            continue
        pct = _to_num(cells[pct_col]) if pct_col is not None and pct_col < len(cells) else None
        amount = _to_num(cells[amount_col]) if amount_col is not None and amount_col < len(cells) else None
        if idx in group_shares:
            amount = None       # R2-9: the share is the group's; no sub-row stands for it
        if pct is None and amount is None:
            continue
        if pct is not None and not (0 <= pct <= 100):
            pct = None
        out.append({"item": item[:60], "revenue": amount, "pct": pct})
    if _only_residual(out):
        return []
    total = sum(r["pct"] for r in out if r["pct"])
    if out and total and not (50 <= total <= 150):
        return []
    # The table's own total is the only completeness evidence that does not need a
    # threshold invented for it. When the accepted rows fall short of what the
    # filing says they add up to, rows were dropped -- 6533 晶心科 81 of 100,
    # 6021 美好證 63.46 of 100, both because pdfplumber lost labels -- and an
    # incomplete read must not end the search. The tolerance absorbs rounding:
    # 2897's business lines are printed as whole numbers and sum to 101.
    declared = _declared_total(table, pct_col)
    if declared is None and own_total is not None and pct_col is not None:
        cells = [_clean(c) for c in table[own_total]]
        if pct_col < len(cells):
            value = _to_num(cells[pct_col])
            if value is not None and 50 <= value <= 150:
                declared = value
    if out and total and declared is not None:
        if abs(total - declared) > max(2.0, 0.5 * len(out)):
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
def _prose_window(text: str) -> str:
    """The text the prose reader reads: from 營業比重 to the next value-free section heading.

    R2-8, ruled 2026-09-15: the same boundary a line block has (L1). 4108's 「2.營業比重」 says only
    its total revenue; 「3.公司目前之商品(服務)項目」 below it describes 「含有 99.7%高純度…」, which
    is not a share of revenue."""
    if "營業比重" not in text:
        return text     # no window without 營業比重: the page is read as it was (3006, 8390)
    head = text[text.find("營業比重"):]
    lines = head.splitlines()
    cut = next((k for k, line in enumerate(lines) if k > 0 and _PROSE_SECTION_HEADING.match(line)
                and not _LINE_HEAVY.search(_PROSE_SECTION_HEADING.sub("", line, 1))), None)
    return "\n".join(lines[:cut]) if cut is not None else head


# A section boundary for the prose window: 「3.」「3、」「三、」「(三)」. A bracketed Arabic item 「(1)」 or a
# lettered item 「A.」 is a list inside the section, not its end (2524, 1308); 「3.2」 is a number.
_PROSE_SECTION_HEADING = re.compile(
    r"^\s*(\d+\s*[.．](?!\d)|\d+\s*、|[一二三四五六七八九十]+\s*、|[(（]\s*[一二三四五六七八九十]+\s*[)）])")


def _covers_many_companies(text: str) -> bool:
    return len(re.findall(r"所營事業比重", text)) > 1


def _strip_item_lead(item: str) -> str:
    """Remove what the sentence put in front of the product name.

    Four forms, all seen in filings and all fatal downstream because the noise
    they carry is exactly what PROSE_NOISE screens for:

      2204 中華   「營業比重製造及銷售整車及相關零組件」  -> section heading
      4401 東隆興 「營業比重(銷售量百分比)(1)布紗」        -> heading, gloss, marker
      3041 揚智   「本公司主要產品為數位機頂盒晶片」        -> sentence opening
      3006 晶豪科 「民國114年度積體電路(IC)」              -> year label

    Applied in a loop because they stack. Each pass must leave something that
    still reads as a product; when it does not, the item is returned as it was
    and the caller's filters decide.
    """
    for _ in range(4):
        before = item
        # The section regex is applied directly rather than through
        # _strip_section_prefix, whose guard refuses to strip when what follows
        # still looks like context -- correct when it stands alone, wrong here:
        # 4401's 「營業比重(銷售量百分比)(1)布紗」 keeps 銷售量 until two more layers
        # come off, and the guard would stop at the first one. The loop's own
        # exit and the caller's filters do that job at the end instead.
        item = _SECTION_PREFIX.sub("", unicodedata.normalize("NFKC", item))
        item = re.sub(r"^.*?(?:主要產品|主要商品|主要業務|產品|商品)(?:為|係|是)", "", item)
        item = re.sub(r"^(?:民國)?\d{2,4}年度?", "", item)
        # a bracketed gloss standing before the name, not around it
        item = re.sub(r"^[（(][^）)]{0,20}[）)]", "", item)
        # an enumeration marker: (1) / 1. / ①-style numbering
        item = re.sub(r"^[（(]?\d{1,2}[）).、]", "", item)
        item = item.strip(" 　,，、。:：")
        if item == before or len(item) < 2:
            return before if len(item) < 2 else item
    return item


def _trim_sales_suffix(item: str) -> str:
    """Drop the verb tail an inverted sentence leaves on the product name.

    「營業比重約100%是代理電子零組件暨周邊設備之銷售」 names the product and then says
    what was done with it. Keeping the tail pushes the name past the 14-character
    bar in _is_prose_noise, so a correctly read line would be discarded for being
    a sentence fragment -- which it no longer is once the tail is off.
    """
    item = item.strip(" 　,，、。")
    # Applied repeatedly: 「軟體服務產生的貢獻」 sheds 的貢獻 and then 產生.
    for _ in range(3):
        trimmed = re.sub(r"(?:所)?(?:產生|帶來|貢獻)?(?:之|的)?(?:銷售|貢獻|收入|營收)?$",
                         "", item).strip(" 　,，、。")
        if trimmed == item or len(trimmed) < 2:
            break
        item = trimmed
    return item


# A verb of change is not a product. 1417 嘉裕's 產業概況 reads 「出口總值為
# 62.23億美元,衰退8%;進口總值為37.47億美元,成長32%」 -- delimited, two pieces,
# two pairs: it passes the structural test on its own and is stopped here.
# PROSE_NOISE holds 成長率 but not the bare verb, which is what a delimited run
# leaves behind once the noun phrase before it has been cut off at the comma.
CHANGE_WORD = ("成長", "衰退", "增長", "增加", "減少", "下滑", "上升", "下降",
               "年增", "年減", "月增", "月減", "持平", "轉正", "轉負")


def _is_change_verb(item: str) -> bool:
    """Matched as a suffix, not only whole.

    1417 嘉裕's 產業概況 survives the bare-verb test because the delimiter keeps
    the noun attached: 「紗線衰退17%」, 「雜項紡織品衰退3%」, 「纖維衰退32%」.
    Read as a mix that says yarn is 17% of revenue; it says yarn fell 17%. The
    verb at the end is what makes it a rate of change rather than a share, so
    that is where the test has to look. Measured against the stored rows before
    writing: one item ends this way, 6158's 「本上升」, itself a fragment.
    """
    compact = re.sub(r"\s+", "", unicodedata.normalize("NFKC", item or ""))
    return compact in CHANGE_WORD or any(compact.endswith(w) for w in CHANGE_WORD)


# The vocabulary of the measurement, which in a real run is stated once before
# the run and never inside it. 2388 威盛 produced labels 「約占集團總」 and
# 「約占」 -- the connector cut in half and swallowed. 2527 宏璟 is a two-column
# page flattened into interleaved lines, so the delimiters joined a mix sentence
# in one column to the articles-of-association clause in the other: 「應付員工
# 酬勞及董事酬體營業額 100%」 beside 「分別按稅前淨利 4%」.
#
# Neither is a word that needs blacklisting as a product name. Both are evidence
# that the segmentation captured sentence machinery instead of a parallel list,
# and that is a property of the whole run -- so finding one rejects all of it,
# rather than dropping the offending pair and keeping a run that has just been
# shown to be mis-segmented.
MEASURE_TOKEN = ("占", "佔", "比重", "比率", "比例", "營業額", "營收",
                 "淨利", "酬勞", "稅前", "總額", "營業收入")


# 1109 信大: 「主要從事水泥及熟料之產銷,其營業比重為62.71%;次為混凝土產銷,
# 其營業比重為12.52%」. The ratio phrase follows the product and is joined with
# 為, which no reader above has: the 占/佔 reader needs 占, and the
# 「比重 N% 為 label」 reader needs the number first.
# 1109 信大: 「主要從事水泥及熟料之產銷,其營業比重為62.71%;次為混凝土產銷,
# 其營業比重為12.52%」. The ratio phrase follows the product and is joined with
# 為, which no reader above has: the 占/佔 reader needs 占, and the
# 「比重 N% 為 label」 reader needs the number first.
_P1_CONNECTOR = re.compile(
    r"[,，]?\s*(?:其|之)?\s*"
    r"(?:營業比重|營收比重|營業比率|銷售比重|事業比重|比重)\s*"
    r"(?:約為|為|約)\s*(?P<pct>\d{1,3}(?:\.\d+)?)\s*[%％]")
# Where the label starts: the sentence boundary, or the colon that introduced
# the section.
_P1_BOUND = re.compile(r"[。;；!?：:,，]")
# What the sentence put in front of the product.
_P1_LEAD = re.compile(
    r"^(?:[A-Za-z][.、]|[(（]\d{1,2}[)）]|\d{1,2}[.、]|其中|另|次為|再次為|以及|及|"
    r"一部分是|另一部分是|是針對|主要從事|主要為|本公司集團目前|本公司目前|本集團|"
    r"本公司|目前|係|為)+")
# 「之製造與銷售」, 「之產銷」, 「的銷售」 -- what was done with the product, not
# the product. Trimmed here rather than in _trim_sales_suffix because that one
# is shared with readers whose labels do not carry a 之/的 clause.
_P1_TAIL = re.compile(
    r"(?:之|的)?(?:製造|生產|加工)?(?:與|及|暨)?(?:製造|銷售|產銷|行銷|買賣|代工|"
    r"營運|業務|服務)+$")


def _rows_from_ratio_is(text: str) -> list[dict]:
    """The mix written as 「…其營業比重為 N%」, one clause per product.

    Every guard the other prose readers earned applies here too, because the
    same passages are in range: 1722 台肥's 研發費用占營業額比率, 1810 和成's
    內銷/外銷, 2338 光罩's 客戶行業別, 1417 嘉裕's 成長/衰退 rates. An explicit
    connector makes each pair independently anchored -- unlike a delimited run,
    where one bad label condemns the segmentation -- so a label that fails is
    dropped on its own.

    1413 宏洲 and 1452 宏益 write 「所營業務為聚酯粒及聚酯原絲之製造、行銷所佔
    營業比重為100%」. The connector there is 所佔…為, and what precedes it is a
    list of activities; the tail and the activity guard are what keep them out.
    """
    out = []
    prev_end = 0
    for m in _P1_CONNECTOR.finditer(text):
        seg = text[prev_end:m.start()]
        prev_end = m.end()
        pct = _to_num(m.group("pct"))
        if pct is None or not (0 < pct <= 100):
            continue
        # The label is the last clause of the segment, not all of it.
        pieces = _P1_BOUND.split(seg)
        item = pieces[-1] if pieces else ""
        for _ in range(3):
            stripped = _P1_LEAD.sub("", item.strip(" 　,，、"))
            if stripped == item:
                break
            item = stripped
        item = _P1_TAIL.sub("", item).strip(" 　,，、")
        item = _trim_sales_suffix(_strip_item_lead(item))
        if not item or not _is_label(item):
            continue
        if (_is_prose_noise(item) or _is_context_item(item) or _is_region(item)
                or _is_industry_name(item) or _is_activity_only(item)
                or _is_aggregate_row(item) or _is_change_verb(item)):
            continue
        if any(w in item for w in MEASURE_TOKEN):
            continue
        out.append({"item": item[:60], "revenue": None, "pct": pct})
    return out


_P6_SPLIT = re.compile(r"[、,，;；]")
_P6_PAIR = re.compile(r"([^%％]{2,24}?)(\d{1,3}(?:\.\d+)?)\s*[%％]")
_P6_LEAD = re.compile(r"^(?:及|與|和|其中|另|次為|以及)+")


def _rows_from_delimited_pairs(text: str) -> list[dict]:
    """The mix written as a delimited run of label+percent, with no connector.

    1466 聚隆: 「尼龍原絲16.16%、尼龍加工絲39.53%、聚酯加工絲10.27%、環保纖維
    22.94%、醫材類1.25%、其他9.85%」. 1618 合機 and 2031 新光鋼 write the same
    shape. Nothing joins the label to the number -- no 佔, no 為, no colon -- so
    none of the connector patterns above can see it, and it is the largest
    single unread shape in the no_section population.

    A rule that simply paired every percentage with the words in front of it
    would be the proximity heuristic this parser has been burned by: 產業概況
    growth rates, R&D as a share of revenue, domestic/export splits and customer
    industry breakdowns all read that way. What separates a mix from those is
    structure, not vocabulary: the pairs form a **delimited run**. So the text is
    split on its delimiters first, and pairs are only believed when they appear
    in at least two different pieces of that run.

    That requirement also settles the single-percentage case on purpose. 「某產
    品占營收 98.75%」 is one pair in one piece; it may well be true, but it is
    not this shape, and reading it here would mean guessing where a label ends
    with nothing to check the guess against.
    """
    # The first piece of the run still carries the heading that introduced it,
    # and the label cap then cuts a fragment out of the middle of it: 1466 聚隆's
    # 「營業比重:尼龍原絲16.16%」 lost its first product that way, and the run
    # came back 16.16 short. Cut at the last heading so the run starts clean.
    for _k in ("營業比重", "營業比率", "營收比重"):
        _last = text.rfind(_k)
        if _last >= 0:
            text = text[_last + len(_k):]
    text = text.lstrip("：: 　,，、")
    # 2317 鴻海's heading and its run are not the same phrase: 「2.營業比重方面」
    # then 「114 年集團各大產品類別營收比例:雲端與網路產品 40%、…」. Cutting at
    # the heading leaves the second introduction in front of the first product,
    # the label cap takes a fragment out of the middle of it, and the run comes
    # back 40 points short with its largest line missing. A colon just before
    # the run is what introduces it, so cut there too -- but only near the
    # start, where an introduction can be, not anywhere a colon appears.
    _colon = max(text.rfind("：", 0, 60), text.rfind(":", 0, 60))
    if _colon >= 0:
        text = text[_colon + 1:]
    pieces = _P6_SPLIT.split(text)
    out, pieces_used = [], 0
    for piece in pieces:
        found = []
        for m in _P6_PAIR.finditer(piece):
            item = _strip_item_lead(_P6_LEAD.sub("", m.group(1)).strip())
            item = _trim_sales_suffix(item)
            pct = _to_num(m.group(2))
            if pct is None or not (0 < pct <= 100):
                continue
            if not item or not _is_label(item):
                continue
            if (_is_prose_noise(item) or _is_context_item(item)
                    or _is_region(item) or _is_industry_name(item)
                    or _is_activity_only(item) or _is_aggregate_row(item)
                    or _is_change_verb(item)):
                continue
            found.append({"item": item[:60], "revenue": None, "pct": pct})
        if found:
            pieces_used += 1
            out.extend(found)
    # Two pairs inside one sentence are two percentages, not a run.
    if pieces_used < 2 or len(out) < 2:
        return []
    # The introduction belongs outside the run. Inside one, it means the pieces
    # are not parallel product names, and the run as a whole is not trustworthy.
    if any(w in r["item"] for r in out for w in MEASURE_TOKEN):
        return []
    return out


def _rows_from_prose(text: str) -> list[dict]:
    """Fallback for companies writing the mix as a sentence (上銀: 線性滑軌佔營收62%)."""
    # The PDF text layer wraps mid-sentence and every pattern below is written
    # across a clause rather than a line: 3006 晶豪科 breaks between the amount
    # and 仟元, 3041 揚智 between 約 and 85%, 6270 倍微 inside 「軟體服務產生」
    # itself. Collapsing is safe here and only here -- _rows_from_lines needs
    # the whitespace, because there it is the column separator.
    text = re.sub(r"\s+", "", text)
    # A parenthetical carrying a comma or a space is an English gloss or an
    # abbreviation list, not part of the product name: 3041's
    # 「數位機頂盒(Set-top Box, STB)晶片」 otherwise captures as 「STB)晶片」, which
    # is then discarded for its unbalanced bracket. 「積體電路(IC)」 keeps its gloss.
    text = re.sub(r"[（(][^）)]{0,40}?[ ,，][^）)]{0,40}?[）)]", "", text)
    out = []
    # 3041 揚智 reads 「數位機頂盒(STB)晶片,占整體營收約85%」: a comma sits between
    # the product and 占, and the ratio noun is 整體營收 rather than 營收, so the
    # old pattern missed both halves of the sentence and the surviving 15% fell
    # under the completeness bar -- the company parsed to nothing at all.
    # 1413 宏洲 writes 「之製造、行銷所佔營業比重為 100%」. Without 所佔 in the
    # connector the 所 is eaten by the product name, which then reads 「行銷所」.
    for match in re.finditer(r"([一-鿿A-Za-z0-9（）()／/\-]{2,20}?)[,，、]?\s*(?:所占|所佔|約占|約佔|占|佔)\s*(?:整體)?(?:營收|營業額|營業收入|營業比重|營業比率)?[^\d%]{0,4}(\d{1,3}(?:\.\d+)?)\s*%", text):
        item = re.sub(r"^(其中|另|及|與|和|，|、|：|:)+", "", match.group(1))
        item = _strip_item_lead(item)
        if (item and _is_label(item) and not _is_region(item) and not _is_prose_noise(item)
                and not _is_context_item(item)
                and not _is_industry_name(item)):
            out.append({"item": item[:60], "revenue": None, "pct": float(match.group(2))})
    if not out:
        # 「營業比重（民國114年度）：人機介面應用晶片100%。」 — the colon comes before
        # the product and the percentage is glued to it, with no 占/佔 to anchor on.
        for match in re.finditer(r"[：:]\s*([一-鿿A-Za-z][^\d%：:，,。]{1,20}?)(\d{1,3}(?:\.\d+)?)\s*%", text):
            item = _strip_item_lead(match.group(1).strip("（(）) 、"))
            if (item and _is_label(item) and not _is_region(item) and not _is_prose_noise(item)
                and not _is_context_item(item)
                and not _is_industry_name(item)):
                out.append({"item": item[:60], "revenue": None, "pct": float(match.group(2))})
    if not out:
        # The inverted sentence: the ratio comes first and the product follows it.
        # 3033 威健「營業比重約100%是代理電子零組件暨周邊設備之銷售」,
        # 6270 倍微「營業比重約當95%是電子產品的銷售代理,其他5%是軟體服務產生的貢獻」.
        # Nothing in the earlier patterns can match these: there is no 占 to anchor
        # on and the percentage precedes the thing it describes.
        for match in re.finditer(
                r"(?:營業比重|營收比重|營業比率|其他|另)\s*(?:約當|約|為|係|是)?\s*"
                r"(\d{1,3}(?:\.\d+)?)\s*%\s*(?:是|為|係)\s*([^,，。;；、]{2,30})", text):
            item = _trim_sales_suffix(_strip_item_lead(match.group(2)))
            if item and not _is_region(item) and not _is_prose_noise(item) \
                    and not _is_context_item(item) and not _is_industry_name(item):
                out.append({"item": item[:60], "revenue": None, "pct": float(match.group(1))})
    if not out:
        # 「積體電路(IC)銷售收入為 14,575,272 仟元,其占營業比率為 100%」 (3006 晶豪科):
        # the subject is separated from its ratio by the amount, so the ratio's
        # own sentence fragment ("其") is all the 占 patterns can see.
        for match in re.finditer(
                r"([一-鿿A-Za-z0-9（）()]{2,20}?)(?:銷售|營業)?收入為\s*([\d,]+)\s*仟元"
                r"[^%]{0,24}?(\d{1,3}(?:\.\d+)?)\s*%", text):
            item = _strip_item_lead(match.group(1))
            if item and not _is_region(item) and not _is_prose_noise(item) \
                    and not _is_context_item(item):
                out.append({"item": item[:60],
                            "revenue": _to_num(match.group(2)),
                            "pct": float(match.group(3))})
    if not out:
        # 「營業比重：工業用紙：100%」 — a colon list rather than a sentence (1906 寶隆).
        for match in re.finditer(r"([一-鿿A-Za-z0-9（）()／/\-]{2,20}?)\s*[：:]\s*(\d{1,3}(?:\.\d+)?)\s*%", text):
            item = _strip_item_lead(match.group(1))
            if (item and _is_label(item) and not _is_region(item) and not _is_prose_noise(item)
                and not _is_context_item(item)
                and not _is_industry_name(item)):
                out.append({"item": item[:60], "revenue": None, "pct": float(match.group(2))})
    if not out:
        # Before the delimited run, because this shape has an explicit connector
        # and the run has none. Placed after every reader above so it cannot
        # take a company away from one of them: 2007 燁興 writes 「占營收比重約為
        # 51%」 and is read by the 占 pattern, and must stay there.
        out = _rows_from_ratio_is(text)
    # Last, and only where the connector patterns did not produce a whole mix.
    # 1466 聚隆 writes 「營業比重:尼龍原絲16.16%、尼龍加工絲39.53%、...」, and the
    # colon pattern above takes the first pair and stops -- 16.16% alone, which
    # parse_pdf's 50..150 completeness bar then throws away, leaving the company
    # no_section. So the test is not "did anything match" but "is what matched a
    # mix": below 50 the earlier readers have found a fragment, and a delimited
    # run that accounts for more of the company is the better reading. A reader
    # that already produced a usable mix is never displaced.
    partial = sum(r["pct"] for r in out if r["pct"]) if out else 0
    if partial < 50:
        run = _rows_from_delimited_pairs(text)
        if run and sum(r["pct"] for r in run) > partial:
            out = run
    return [] if _only_residual(out) else out


def _line_periods(lines, fiscal_year):
    """(periods, slot, refusal) for a text-line table, by period identity.

    The header line decides which value belongs to the fiscal year, and every form a
    filing writes the year in counts: 2404 heads its lines 項 目 一一三 年度 一一四年度,
    which the old Arabic-only reading took for no year and so read slot 0 -- the 113
    column. periods is the ordered years the first header line naming two or more
    names; None when no line does. A header naming periods, none of them the fiscal
    year, is a refusal (P22-1).
    """
    target = fiscal_year - 1911
    for line in lines[:6]:
        periods = _periods(line)
        if len(periods) >= 2:
            if target in periods:
                return periods, periods.index(target), None
            return periods, None, ("no target period; the header names %s"
                                   % ", ".join(map(str, periods)))
    return None, 0, None


def _line_entity_slot(lines, width):
    """Which of `width` shares on a line is consolidated, from a header line naming entities.

    8112 heads its lines 合併 合併營收 個體 個體營收: repeated words collapse, giving the
    order [consolidated, parent]. The order has to account for every share, and exactly
    one entry must be consolidated or total; otherwise None. Only a line naming both a
    whole-company word and a strong part word counts, so 本公司 in narrative does not.
    """
    words = [(w, kind) for kind, ws in _ENTITY_WORDS if kind != "parent_weak" for w in ws]
    for line in lines[:6]:
        s = _clean(line)
        hits = sorted((m.start(), kind) for w, kind in words for m in re.finditer(re.escape(w), s))
        kinds = [k for _, k in hits]
        if not (set(kinds) & _WHOLE and set(kinds) & _PART):
            continue
        order = [k for i, k in enumerate(kinds) if i == 0 or k != kinds[i - 1]]
        if len(order) != width:
            return None
        whole = [i for i, k in enumerate(order) if k in _WHOLE]
        return whole[0] if len(whole) == 1 else None
    return None


# R1-3′, ruled 2026-09-14: a standalone dash among a line's values is a value position carrying
# NULL -- the period has no value (1806 其他 2,489 0.08 - - - -), or the share rounds to nothing
# beside a small amount (2641 客戶合約收入 31,604 1.91 30 -). Its measure, like every value's,
# comes from the header (L2), never from its form: R1-3 as first ruled took a value up to 100
# for a share and so made 2641's amount 30 a 30% row.
_PLACEHOLDER = r"(?<!\S)[-－—]%?(?!\S)"


def _line_values(region: str) -> list:
    """Value positions in order: (digits, mark) for a number, None for a placeholder."""
    return [None if m.group(1) is None else (m.group(1), m.group(2))
            for m in re.finditer(_PLACEHOLDER + r"|(\d[\d,]*(?:\.\d+)?)\s*(%?)", region)]


_LINE_LABEL = re.compile(r"^\s*([^\d%]{2,30}?)\s+(?=[\d(])")
_LINE_AMOUNT = (r"營業收入淨額|營收淨額|營業淨額|銷貨淨額|銷售淨額|收入淨額|銷售金額|營業收入|營業額"
                r"|銷售額|銷貨收入|金額|銷值")
_LINE_SHARE = r"營業比重|營收比重|佔營收比重|占營收比重|比重|百分比|比例|比率|占比|佔比"
_LINE_CHANGE = r"成長率|增\(減\)|增減|變動"
_LINE_MEASURE = re.compile("(%s)|(%s)|(%s)|(%%)" % (_LINE_CHANGE, _LINE_AMOUNT, _LINE_SHARE))
_LINE_LABEL_WORDS = re.compile(r"年度|項目|產品|名稱|營業|業務|內容|服務|商品|類別|主要|品項|部門|別|/|\(|\)|（|）|:|：")
_LINE_TOTAL = ("合計", "總計")
_LINE_HEADING = re.compile(r"^\s*([(（]\s*[\d一二三四五六七八九十]+\s*[)）]|[\d一二三四五六七八九十]+\s*[、]"
                           r"|\d+\s*[.．](?!\d)|[A-Za-z]\s*[.．、)）]|[壹貳參肆伍陸柒捌玖拾]+\s*、)")
_LINE_HEAVY = re.compile(r"\d+(?:\.\d+)?\s*%|\d{1,3}(?:,\d{3})+|\d{4,}")
_LINE_HUNDRED = re.compile(r"(?<![\d.,])\(?100(?:\.0+)?\)?\s*%?(?![\d.,])")


def _is_value_line(line) -> bool:
    """A labelled line carrying a number that is not a year, month or header word."""
    label = _LINE_LABEL.match(line)
    if not label:
        return False
    if _LINE_MEASURE.search(re.sub(r"\s", "", label.group(1))) and not _LINE_HEAVY.search(line):
        return False
    rest = line[label.end(1):]
    return any(not re.match(r"\s*(年|度|月)", rest[m.end():])
               for m in re.finditer(r"\d[\d,]*(?:\.\d+)?", rest))


_LINE_CHANGE_PHRASE = re.compile(r"(增\(減\)|增減|成長率|變動)(比率|比例|比)?(\(%\)|%)?")
_LINE_SHARE_WORD = re.compile(r"比重|比例|比率|百分比|占|佔|%")
_LINE_HEADER_WORDS = re.compile(r"營業比重|營收比重|比重|比例|比率|百分比|金額|營業額|營收|年度|產品名稱|產品|項目|名稱|%|\(|\)")
_LINE_COUNT_BEFORE = re.compile(r"(支|張|片|噸|公噸|個|台|套|條|箱|瓶|件|顆|桶)\s*$")


def _line_block_end(raw) -> int:
    """L1, ruled 2026-09-14: the index of the last line of the line evidence block.

    The block starts at the first value line and ends at the table's own total -- 合計 or 總計
    printing a 100 share -- or, without one, before the first section heading carrying no
    values. Past it the reader used to go on into prose (1325's 「98 年 5 月」), the next
    section's product table (3042) or an industry table (1522). A total that is not the table's
    own does not close it: 1522's next 合計 prints growth rates. A heading is not a heading when
    it carries values: 1316's items print 「C.保險經紀佣金收入 11.04%」, and 1905's
    table-of-contents sidebar interleaves 「二、市場及產銷概況 紙板 2,679,536 14.28」.
    """
    first = next((i for i, line in enumerate(raw) if i > 0 and _is_value_line(line)), None)
    if first is None:
        return len(raw) - 1
    for i in range(first + 1, len(raw)):
        head = re.sub(r"\s", "", re.split(r"\d", raw[i], 1)[0])
        if any(w in head for w in _LINE_TOTAL) and _LINE_HUNDRED.search(raw[i]):
            return i
    for i in range(first + 1, len(raw)):
        if _LINE_HEADING.match(raw[i]) and not _LINE_HEAVY.search(_LINE_HEADING.sub("", raw[i], 1)):
            return i - 1
    return len(raw) - 1


def _line_measures(compact) -> list:
    """Measures a compacted header line names, in order.

    A % right after a share word, or bracketed after it (比重(%), 營業比重%), belongs to that
    word; 佔/占 ... 比率/比重/百分比 is one share (6184 占本年累計營業收入百分比)."""
    compact = re.sub(r"[佔占][^%%]{0,14}?(比率|比重|百分比|比例)", "比重", compact)
    compact = re.sub(r"(%s)[(（]?%%[)）]?" % _LINE_SHARE, r"\1", compact)
    return [("change" if m.group(1) else "amount" if m.group(2) else "share")
            for m in _LINE_MEASURE.finditer(compact)]


class _MeasureHeader(NamedTuple):
    measures: list
    periods: list
    line: int


def _line_measure_header(raw, fiscal_year):
    """L2, ruled 2026-09-14: the measures a header names above the first value line, or None.

    A header line naming both an amount and a share gives every value position a measure,
    combined with the header's periods. The identity exists only if it accounts for at least
    one value line: 1582's header wraps over four lines and 4139's is garbled, so their lines
    are read as before rather than refused against a header that describes none of them.
    """
    first = next((i for i, line in enumerate(raw) if i > 0 and _is_value_line(line)), len(raw))
    measures = None
    for at, line in enumerate(raw[1:first], 1):
        compact = re.split(r"單位[:：]", re.sub(r"\s", "", line), 1)[0]
        found = _line_measures(compact)
        residual = _LINE_LABEL_WORDS.sub("", _LINE_MEASURE.sub("", _PERIOD_LABEL.sub("", compact)))
        residual = re.sub(r"[\d年度月日~\-]", "", residual)
        if "share" in found and "amount" in found and len(residual) <= 4:
            measures = found
            break
    if measures is None:
        return None
    periods = None
    for line in raw[1:first]:
        named = _periods(line)
        if len(named) >= 2:
            periods = list(named)
            break
    if periods is None:
        single = [p for line in raw[1:first] for p in _periods(line)]
        periods = single[:1] if len(set(single)) == 1 else []
    for line in raw[at + 1:]:
        label = _LINE_LABEL.match(line)
        if not label:
            continue
        lead = re.search(r"(?:\s+[-－—]%?)+$", label.group(1))
        region = line[label.start(1) + lead.start():] if lead else line[label.end(1):]
        n = len(_line_values(region))
        if n and _identify_positions(measures, periods, n) is not None:
            return _MeasureHeader(measures, periods, at)
    return None


def _identify_positions(measures, periods, n):
    """(period, measure) for each of n value positions, or None when the header does not
    account for n: a repeated unit (金額 % 金額 %), a measure block over a period row naming
    every position (6144 營業收入淨額 百分比 over 113 114 113 114), or one unit per period."""
    m, p = len(measures), len(periods)
    if m == n:
        if p == n:
            return list(zip(periods, measures))
        if p > 1 and n % p == 0 and measures == measures[:n // p] * p:
            u = n // p
            return [(periods[k // u], measures[k]) for k in range(n)]
        if p <= 1:
            return [(periods[0] if p else None, measures[k]) for k in range(n)]
        return None
    if p == n and n % m == 0:
        u = n // m
        return [(periods[k], measures[k // u]) for k in range(n)]
    if p and m * p == n:
        return [(periods[k // m], measures[k % m]) for k in range(n)]
    return None


def _read_by_identity(values, positions, target):
    """-> ("row", share, revenue) / ("skip",) / ("unresolved", why).

    The target share is the one (fiscal year, share) position and revenue the one (fiscal
    year, amount) position. A NULL share skips the row -- nothing is borrowed from another
    period. A value contradicting its position refuses: a % under an amount, an unmarked value
    over 100 under a share."""
    named = {per for per, _ in positions}
    if named == {None}:
        slots = [k for k, (_, mea) in enumerate(positions) if mea == "share"]
        amounts = [k for k, (_, mea) in enumerate(positions) if mea == "amount"]
    elif target not in named:
        return ("unresolved", "no target period in the measure header")
    else:
        slots = [k for k, (per, mea) in enumerate(positions) if per == target and mea == "share"]
        amounts = [k for k, (per, mea) in enumerate(positions) if per == target and mea == "amount"]
    if len(slots) != 1:
        return ("unresolved", "%d share positions for the target" % len(slots))
    for k, (_, mea) in enumerate(positions):
        v = values[k]
        if v is None:
            continue
        num = _to_num(v[0])
        if mea == "amount" and v[1]:
            return ("unresolved", "a % value under an amount")
        if mea == "share" and not v[1] and num is not None and num > 100:
            return ("unresolved", "a value over 100 under a share")
    share = values[slots[0]]
    if share is None:
        return ("skip",)
    revenue = None
    if len(amounts) == 1 and values[amounts[0]] is not None:
        revenue = _to_num(values[amounts[0]][0])
    return ("row", _to_num(share[0]), revenue)


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
    periods, slot, unresolved = _line_periods(raw, fiscal_year)
    # P22-1 on the opening line: 1467's 「2.一一三年主要產品佔營業比重」 names 113 before the window
    # begins; with no period on any header line, that heading is the only period the block has
    opening = text[text.rfind("\n", 0, start) + 1:start]
    opening_periods = _periods(opening)
    if periods is None and opening_periods and (fiscal_year - 1911) not in opening_periods:
        unresolved = unresolved or ("no target period; the heading names %s"
                                    % ", ".join(map(str, opening_periods)))
    out, mix_lines = [], 0
    # L1: lines past the evidence block are neither rows nor refusals
    end = _line_block_end(raw)
    # L2: where the header names the measures, they -- not a value's size -- say what it is
    header = _line_measure_header(raw, fiscal_year)
    first = next((k for k, line in enumerate(raw) if k > 0 and _is_value_line(line)), None)
    above = [re.sub(r"\s", "", line) for line in raw[1:first if first is not None else len(raw)]]
    above.insert(0, re.sub(r"\s", "", raw[0].split("營業比重", 1)[-1]))
    # R2-4c, ruled 2026-09-15: a header naming a change and no share -- 2929's 2024年度 2025年度
    # 增(減)比(%) -- makes the % values on its lines changes, not shares
    plain = [_LINE_CHANGE_PHRASE.sub("", re.split(r"單位[:：]", line, 1)[0]) for line in above]
    change_header = any(_LINE_CHANGE_PHRASE.search(line) for line in above)
    share_header = any(_LINE_SHARE_WORD.search(line) for line in plain)
    changes_only = change_header and not share_header
    acted = []      # lines that became a row or set the refusal, for R2-6b after the loop
    # R2-1, ruled 2026-09-15: under a header naming a quantity, a number beside a count unit is a
    # quantity -- 8411's 「支 1,066,874」 -- not a second amount
    quantity_header = any(re.search(r"數量|產量|銷量", line) for line in above)
    for i, line in enumerate(raw[1:end + 1], 1):
        state = (len(out), unresolved)
        try:
            if _is_region(line) or _is_aggregate_row(re.split(r"\d", line, 1)[0]):
                continue
            label = re.match(r"^\s*([^\d%]{2,30}?)\s+(?=[\d(])", line)
            if not label:
                continue
            if not _LINE_HEADER_WORDS.sub("", re.sub(r"\s", "", label.group(1))):
                continue    # R2-5: 3665's 「比重 比重 5」 is a header line with a page number, not values
            # R1-3′: a placeholder the label pattern swallowed (「新產品 - 1,234 5%」) is a value
            lead = re.search(r"(?:\s+[-－—]%?)+$", label.group(1))
            region = line[label.start(1) + lead.start():] if lead else line[label.end(1):]
            values = _line_values(region)
            if header is not None and i <= header.line:
                continue    # the header's own lines are not data
            if header is not None and values:
                positions = _identify_positions(header.measures, header.periods, len(values))
                got = (_read_by_identity(values, positions, fiscal_year - 1911) if positions is not None
                       else ("unresolved", "%d value positions do not fit the measure header" % len(values)))
                if got[0] == "skip":
                    continue
                if got[0] == "unresolved":
                    unresolved = unresolved or got[1]
                    # read only to judge mix-ness, as the unaligned lines below are
                    pick = ([v for v in values if v is not None and v[1]]
                            or [v for v in values if v is not None and not v[1]
                                and _to_num(v[0]) is not None and _to_num(v[0]) <= 100] or [None])[0]
                    if pick is None:
                        continue
                    chosen, revenue = pick, None
                else:
                    chosen, revenue = (str(got[1]), "%"), got[2]
                    if re.search(r"\d\s*[億萬]", line):
                        revenue = None    # 73.64億 is not in the table's unit (3388)
            else:
                tokens = re.findall(r"(\d[\d,]*(?:\.\d+)?)\s*(%?)", line)
                tokens = [(t[0], t[1]) for t in tokens if t[0]]
                if not tokens:
                    continue
                marked = [t for t in tokens if t[1]]
                if changes_only:
                    continue    # R2-4c: no share on this line
                if marked:
                    picks = marked
                else:
                    # 高端疫苗 prints the ratio column without a % sign, so fall back to the
                    # trailing 0-100 values on the line.
                    picks = [t for t in tokens if (_to_num(t[0]) or 999) <= 100][-2:]
                if not picks:
                    continue
                # P22-4: one column identity per value. The header declares the groups -- one per
                # period (2736, 6741) or per entity (8112) -- and which of them is the fiscal
                # year's consolidated one. Without a measure header (L2), each value's measure is
                # its own form: a % mark, or unmarked, a value up to 100 a share and a larger one
                # an amount. The
                # i-th share and the i-th amount belong to group i, so share and revenue are
                # taken from the SAME group. Revenue used to be the first amount on the line
                # whatever share was chosen, which paired 114's share with 113's revenue.
                amounts = [_to_num(m.group(1)) for m in re.finditer(r"(\d[\d,]*(?:\.\d+)?)\s*(%?)", line)
                           if m.group(1) and not m.group(2) and (_to_num(m.group(1)) or 0) > 100
                           and not (quantity_header and _LINE_COUNT_BEFORE.search(line[:m.start()]))]
                groups = index = None
                several_items = False
                if periods is not None:
                    groups, index = len(periods), slot
                elif len(picks) >= 2:
                    between = line.split(picks[0][0], 1)[-1].split(picks[1][0], 1)[0]
                    # an amount between them is not an item name: only words count
                    if len(re.sub(r"[\d\s%％、;；,，:：()（）.]", "", between)) >= 2:
                        several_items = True        # P23: two items on one line, not two groups
                    else:
                        # P22-2: a header line naming the entities in order decides (8112's 合併 |
                        # 個體); otherwise neither period nor entity is known, and the first is
                        # not current by position.
                        k = _line_entity_slot(raw, len(picks))
                        if k is None:
                            unresolved = unresolved or ("%d shares on a line and no period labelled"
                                                        % len(picks))
                        else:
                            groups, index = len(picks), k
                if groups is not None:
                    # every share on the line, not the trailing two: 3046 prints 「99,65」, which
                    # does not parse, and 113's 99.83 must not slide onto 114
                    shares = marked or [tk for tk in tokens if (_to_num(tk[0]) or 999) <= 100]
                    if index is None or len(shares) != groups:
                        if index is not None:
                            unresolved = unresolved or ("%d shares on a line do not align to the %d "
                                                        "groups named" % (len(shares), groups))
                        chosen, revenue = picks[0], None    # read only to judge mix-ness
                    else:
                        chosen = shares[index]
                        # the group's own amount, or none: an amount count that does not match
                        # the groups leaves revenue unestablished (D4), never borrowed
                        revenue = amounts[index] if len(amounts) == groups else None
                elif several_items:
                    chosen = picks[0]
                    revenue = amounts[0] if amounts else None   # P23's shape, left as it was
                else:
                    chosen = picks[0]
                    # one share, one group: its amount only when the line carries exactly one
                    revenue = amounts[0] if len(amounts) == 1 else None
            pct = _to_num(chosen[0])
            if pct is None or not (0 < pct <= 100):
                continue
            item = _strip_section_prefix((label.group(1)[:lead.start()] if lead else label.group(1))
                                         .strip("：:。、 "))
            if not _is_label(item) or _is_prose_noise(item) or _is_context_item(item):
                continue
            mix_lines += 1
            out.append({"item": item[:60], "revenue": revenue, "pct": pct})
        finally:
            if (len(out), unresolved) != state:
                acted.append(i)
    # R2-6b, ruled 2026-09-15: a block is read only if it holds share evidence -- a % value on a
    # line the reader acted on, or a share named in the header above the first such line (the
    # opening line counts only after 營業比重, as 1708's 「營業比重(%)」). 3042's 「3.2 x 2.5mm」 and
    # 6709's 「20、40、60 微克」 have neither, and neither become rows nor refuse.
    if acted:
        header_lines = [re.sub(r"\s", "", raw[0].split("營業比重", 1)[-1])]
        header_lines += [re.sub(r"\s", "", line) for line in raw[1:acted[0]]]
        header_share = any(_LINE_SHARE_WORD.search(_LINE_CHANGE_PHRASE.sub("", line))
                           for line in header_lines)
        pct_value = not changes_only and any(re.search(r"\d\s*%", raw[k]) for k in acted)
        if not (header_share or pct_value):
            return []
    if unresolved and mix_lines >= 2:
        raise ScopeUnresolved(unresolved, " / ".join(l.strip() for l in raw[:4])[:300])
    if _only_residual(out):
        return []
    total = sum(r["pct"] for r in out)
    if not out or not (50 <= total <= 150):
        return []
    return out


class _TableCandidate(NamedTuple):
    rows: list
    table: list
    caption: str


def _table_caption(page, bbox, boxes) -> str:
    """The text directly above a table, and nothing further (D3: table-local).

    From the table's top edge up to the bottom of the nearest table above it on the same
    page, or the page top, keeping the last three lines. Only tables that end above this
    one count: 6803's page is wrapped in a text frame pdfplumber returns as a table, and
    taking its bottom would swallow 「A. 崑鼎公司及子公司：合併營業比重」. Page text is never
    scope evidence -- a 合併 over another table must not assign scope to this one.
    """
    return _caption_from_lines(_caption_lines(page, bbox, boxes))


# R2-2b, ruled 2026-09-15: a footnote closes the table above it. 2884's 「註1：表列數字係玉山銀行
# 合併之資料。」 sat in the caption region of the next table and made it consolidated.
_FOOTNOTE_LINE = re.compile(r"^\s*[(（]?\s*註\s*\d*\s*[)）]?\s*[:：]?")


def _caption_from_lines(caption_lines) -> str:
    """The caption as table-level scope reads it: the last three lines, footnotes left out."""
    return " ".join([line for line in caption_lines if not _FOOTNOTE_LINE.match(line)][-3:])


# R2-2c′ and R2-4a, ruled 2026-09-15: what a table measures in is read from its unit note -- the
# text after the last 單位： -- never from narrative money in the caption. 4162's caption says
# 「美金 200 仟元(約新台幣 6,521 仟元)」 over 「單位: 新台幣仟元」.
_FOREIGN_CURRENCY = re.compile(r"日圓|日幣|美元|美金|港幣|港元|人民幣|歐元|越南盾|泰銖|USD|US\$|JPY|HKD|RMB|CNY|EUR")
_HOME_CURRENCY = re.compile(r"新台幣|新臺幣|台幣|臺幣|NTD|NT\$")
_COUNT_UNIT = re.compile(r"條|劑|噸|支|片|個|台|套|箱|瓶|件|張|顆|桶|公斤|千克|碼|批")


def _unit_note(caption_lines) -> str:
    joined = " ".join(caption_lines)
    parts = re.split(r"單位\s*[:：]", joined)
    return _clean(parts[-1]) if len(parts) > 1 else ""


def _non_revenue_unit(caption_lines) -> bool:
    """A foreign currency (and not the home one), or only count units and no money at all."""
    note = _unit_note(caption_lines)
    if not note:
        return False
    if _FOREIGN_CURRENCY.search(note) and not _HOME_CURRENCY.search(note):
        return True
    return bool(_COUNT_UNIT.search(note)) and not re.search(r"元|幣|NTD|\$", note)


def _caption_lines(page, bbox, boxes) -> list:
    """The lines between a table's top edge and the nearest table ending above it."""
    top = bbox[1]
    floor = max([b[3] for b in boxes if b[3] <= top] or [0])
    if top <= 0 or top - floor < 2:
        return []
    try:
        text = page.crop((0, max(0, floor), page.width, top)).extract_text() or ""
    except ValueError:
        return []
    return [line for line in text.splitlines() if line.strip()]


def _table_heading(page, bbox, boxes) -> str:
    """The heading a table sits under: the last caption line once a unit note is removed.

    「2.最近二年度主要銷貨客戶資料 單位：新台幣仟元；%」 heads with 主要銷貨客戶資料; a line
    holding only 「單位：仟元」 is not a heading. Narrative further up is not read, so a
    sentence mentioning 客戶 above a real mix table does not disqualify it.
    """
    return _heading_from_lines(_caption_lines(page, bbox, boxes))


def _heading_from_lines(caption_lines) -> str:
    """_table_heading on the caption lines themselves."""
    lines = [re.split(r"單位\s*[:：]", line, 1)[0].strip() for line in caption_lines]
    # a date or period alone before the unit note is not a heading: 8930 prints
    # 「(1)主要商品銷售區域：」 over 「114年12月31日 單位： 新台幣仟元」
    lines = [line for line in lines if re.sub(r"[\d\s年月日度民國⺠:：.]", "", line)]
    if not lines:
        return ""
    # A heading wrapped over lines is one heading: 7899's 「(2)最近二年度任一年度中曾占銷貨
    # 總額百分之十以上之客戶名稱及其銷貨金額與」 continues on 「比例，並說明其增減變動原因：」.
    # One continuation line at most, and none when the last line opens with a heading marker
    # or the line above ends a sentence or clause, so narrative above a mix heading is not
    # read (8088's market-share table sits under three unpunctuated lines of narrative).
    heading = [lines[-1]]
    for line in reversed(lines[:-1][-1:]):
        if _HEADING_MARKER.match(heading[0]) or re.search(r"[。：:；;]$", line):
            break
        heading.insert(0, line)
    return " ".join(heading)


_HEADING_MARKER = re.compile(r"^\s*([(（]?\s*[\d一二三四五六七八九十]+\s*[)）、.．]|[A-Za-z][.．、)）])")


# R1-1, ruled 2026-09-14: a table whose own heading names a non-revenue axis is not a
# revenue-composition candidate (R020 ①-C). Disqualification only -- a heading saying
# nothing is still a candidate, and no heading has to say 營業比重.
_NON_REVENUE_HEADING = re.compile(
    r"客戶|供應商|進貨|銷售.{0,6}地區|提供.{0,4}地區|銷售區域|地域別|分佈區域|占有率|佔有率"
    r"|研究發展|銷售對象|預計|流動性|現金流量"
    r"|排行|排名|名次")    # R2-2a, ruled 2026-09-15: 5522's 合併營業收入排行


def _non_revenue_heading(heading: str) -> bool:
    """No exception for a heading that also says 營業比重 or 營收比重: 5410's market-share
    table is headed 「市場占有率擬以本公司營收占同業之合計營收比重表示」, and 7610's region
    table 「主要商品（服務）之銷售（提供）地區」 splits 銷售地區 with a bracket."""
    return bool(_NON_REVENUE_HEADING.search(_clean(heading)))


# Entity words at table level. 合計 is absent on purpose: the caption region can pick up
# the previous table's 合計 row (3432), and a total is a column-level idea anyway. 個別 and
# 單一 are absent too: in running text 個別 reads "respectively" (6803's 個別說明經營概況).
# R1-2, ruled 2026-09-14: only 個體 and 母公司 mark a table part-scoped. 部門, 內銷 and 外銷
# name an axis or a market in captions -- 「以部門別為歸屬」 (8150), 「以內銷市場為主」 (2538)
# -- and remain entity words only over a value column, at column level.
_CAPTION_WHOLE = ("合併", "全球", "集團")
_CAPTION_PART = ("個體", "母公司")


def _table_scope(table, caption, fiscal_year):
    """(established, entity, other_period) for one mix table, from its own labels.

    established   its own header or caption names the fiscal year, or names it consolidated
    entity        "consolidated" · "part" · None
    other_period  its caption names periods and none is the fiscal year
    """
    target = fiscal_year - 1911
    period, _, scopes = _period_selection(table, fiscal_year)
    _, pct_col, _ = _select_columns(table, fiscal_year)
    chosen = next((s for s in scopes if s.col == pct_col), None)
    cap = _clean(caption)
    years = _periods(cap)
    if any(w in cap for w in _CAPTION_PART) and not any(w in cap for w in _CAPTION_WHOLE):
        entity = "part"
    elif any(w in cap for w in _CAPTION_WHOLE) or (chosen is not None and chosen.entity & _WHOLE):
        entity = "consolidated"
    else:
        entity = None
    other_period = period != "target" and bool(years) and target not in years
    established = (period == "target" or target in years or entity == "consolidated")
    return established and not other_period, entity, other_period


def _evidence_groups(cands):
    """Candidates grouped by evidence identity (D5): the same items carrying the same shares.

    Equal numbers alone are not identity; this is applied only to candidates already
    sharing the target scope. Where members of one group disagree on an item's revenue,
    that revenue is left NULL rather than taken from whichever table came first."""
    groups = {}
    for c in cands:
        key = frozenset((_clean(r["item"]), None if r["pct"] is None else round(r["pct"], 2))
                        for r in c.rows)
        groups.setdefault(key, []).append(c)
    out = []
    for members in groups.values():
        rows = [dict(r) for r in members[0].rows]
        for r in rows:
            seen = {m_r["revenue"] for m in members for m_r in m.rows
                    if _clean(m_r["item"]) == _clean(r["item"])}
            if len(seen) > 1:
                r["revenue"] = None
        out.append((members, rows))
    return out


def _own_section(cands, refusals, fiscal_year):
    """The rows of the one table that owns the section by scope, or None -- P22-3.

    Replaces "the first table that yields rows". Ruled 2026-09-14:
      - where any table is labelled consolidated, only consolidated tables compete;
      - otherwise, only tables whose own labels establish the fiscal year compete, and a
        table labelled parent-only, segment or region never does;
      - competitors must be ONE piece of evidence (D5); two different ones are refused,
        however they are ordered on the page;
      - a table that establishes nothing (rule 2: no period, one column) is accepted only
        when nothing else in the filing competes -- not beside a refused table;
      - only parent-only tables and nothing else → the target scope is unavailable (D2).
    Raises ScopeUnresolved when the section cannot be owned.
    """
    scored = [(c, _table_scope(c.table, c.caption, fiscal_year)) for c in cands]
    captions = " / ".join(c.caption for c in cands if c.caption)[:300]
    if any(entity == "consolidated" for _, (_, entity, other) in scored if not other):
        pool = [c for c, (_, entity, other) in scored if entity == "consolidated" and not other]
    else:
        pool = [c for c, (est, entity, _) in scored if est and entity != "part"]
    if pool:
        groups = _evidence_groups(pool)
        if len(groups) == 1:
            return groups[0][1]
        raise ScopeUnresolved("%d tables carry the target scope as different evidence"
                              % len(groups), captions)
    if refusals:
        raise refusals[0]
    loose = [c for c, (_, entity, other) in scored if not other and entity != "part"]
    if not loose:
        if any(entity == "part" for _, (_, entity, _) in scored):
            raise ScopeUnresolved("target consolidated scope unavailable", captions)
        return None
    groups = _evidence_groups(loose)
    if len(groups) == 1:
        return groups[0][1]
    raise ScopeUnresolved("%d unlabelled tables carry different evidence" % len(groups), captions)


def parse_pdf(content: bytes, fiscal_year: int) -> tuple[list[dict], str]:
    """Return (rows, method): 'table', 'prose', or '' when the section is absent."""
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        pages = []
        for page in pdf.pages:
            text = _page_text(page)
            # 3006 晶豪科 writes 「其占營業比率為100%」 and never uses 營業比重 or
            # 主要商品 anywhere, so no page was collected and every reader below
            # ran on nothing -- the prose patterns were never the blocker.
            if any(k in text for k in ("營業比重", "主要商品", "營業比率", "營收比重")):
                pages.append((page, text))
        amounts_only = None
        first_table_page = None
        refusal = None
        # P22-3: every mix table is collected before any is accepted; which one owns
        # the section is decided by scope (_own_section), not by coming first.
        candidates, refusals = [], []
        for page, _text in pages:
            found = page.find_tables()
            tables = [ft.extract() for ft in found]
            boxes = [ft.bbox for ft in found]
            if tables and first_table_page is None:
                first_table_page = page.page_number
            for ft, table in zip(found, tables):
                # R1-1 excludes a table without handing its place on the first page to
                # the next table: 5211's 內銷 量 | 值 table is left out, and the 員工人數 |
                # 平均年歲 table below it must not become the amounts-only mix. An empty
                # list closes the slot and is falsy at the end.
                excluded = (_non_revenue_table(table)
                            or _non_revenue_heading(_table_heading(page, ft.bbox, boxes))
                            or _non_revenue_unit(_caption_lines(page, ft.bbox, boxes)))
                if (excluded and amounts_only is None
                        and page.page_number == first_table_page):
                    amounts_only = []
                try:
                    rows = _rows_from_table(table, fiscal_year)
                except ScopeUnresolved as unresolved:
                    # R1-1: a table headed 主要銷貨客戶 or 銷售地區 is not a mix, so its
                    # unresolved scope is no refusal of the section.
                    if excluded:
                        continue
                    # Keep looking: another table in the filing may establish the
                    # fiscal year's scope on its own labels, and that is not a guess.
                    refusals.append(unresolved)
                    continue
                if rows and excluded:
                    # R1-1: not a candidate either -- it must not compete with the mix
                    continue
                # A table read yielding amounts but no percentage means the ratio
                # column was missed (慧洋: merged year header). Keep it aside and
                # let the text lines try first — amounts alone still carry the mix,
                # so it is the fallback rather than a discard.
                if rows and any(r["pct"] is not None for r in rows):
                    candidates.append(_TableCandidate(rows, table, _table_caption(page, ft.bbox, boxes)))
                    continue
                # The amounts-only read is a fallback for the mix table, not a
                # licence to take a table from a different chapter. 1338 廣華's
                # 被投資公司名稱 table sits on page 94 and its chapter also says
                # 營收比重, so a keyword gate does not separate them; what does
                # is that the mix table is the first table under the section.
                # Once page 64's table was rejected, five investee names became
                # the company's product mix.
                if (rows and amounts_only is None
                        and page.page_number == first_table_page):
                    amounts_only = rows
                transposed = _rows_from_transposed(table)
                if transposed:
                    candidates.append(_TableCandidate(transposed, table,
                                                      _table_caption(page, ft.bbox, boxes)))
        if candidates or refusals:
            # D1: _own_section raises when a mix table could not be resolved and no table
            # owns the section, so no weaker reader -- lines, prose or the amounts-only
            # fallback -- ever re-guesses it.
            owned = _own_section(candidates, refusals, fiscal_year)
            if owned:
                return owned, "table"
        for _page, text in pages:
            try:
                rows = _rows_from_lines(text, fiscal_year)
            except ScopeUnresolved as unresolved:
                refusal = refusal or unresolved
                continue
            if rows:
                return rows, "lines"
        if refusal is not None:
            raise refusal
        for _page, text in pages:
            head = _prose_window(text)
            rows = [] if _covers_many_companies(head) else _rows_from_prose(head[:800])
            # Same completeness bar as the other readers: a lone "15%" sentence is
            # a fragment of narrative, not the company's revenue mix (3041 揚智).
            if rows and 50 <= sum(r["pct"] for r in rows) <= 150:
                return rows, "prose"
        if amounts_only:
            return amounts_only, "table-amounts"
    return [], ""


# The methods parse_pdf can return, and therefore the only rows this module owns.
# tw.product_revenue is shared: annual_report_narrative_tables.py writes
# 'narrative_table' and annual_report_narrative_text.py writes 'narrative_text'.
# Reading a company does not make this module the owner of what is already
# stored against it -- that distinction is what the two statements in save()
# below are scoped by.
READER_METHODS = ("table", "lines", "prose", "table-amounts")


def _dedupe(rows: list[dict]) -> tuple[list[dict], int, list[str]]:
    """Split a parse into what can be stored, how many repeated, and what conflicts.

    tw.product_revenue is keyed on (stock_id, fiscal_year, item), so two parsed
    rows naming the same item cannot both be stored. Upserting them one by one
    let the second silently overwrite the first while the status row still
    reported the parsed count -- 2338 光罩 read "6 rows" next to one stored row.

    A repeat carrying the *same* numbers is harmless (a table read twice). A
    repeat carrying *different* numbers is not a duplicate at all: it means the
    reader did not separate the table's rows, and storing either value would be
    a guess. Those are returned as conflicts and stop the write.
    """
    seen: dict[str, dict] = {}
    unique, duplicates, conflicts = [], 0, []
    for row in rows:
        prior = seen.get(row["item"])
        if prior is None:
            seen[row["item"]] = row
            unique.append(row)
            continue
        duplicates += 1
        if (prior["revenue"], prior["pct"]) != (row["revenue"], row["pct"]):
            conflicts.append(row["item"])
    return unique, duplicates, conflicts


def save(stock_id: str, fiscal_year: int, rows: list[dict], method: str,
         source_file: str | None, status: str, detail: str | None = None):
    """Write a parse result. On success this REPLACES the company's rows.

    Upserting without deleting made the pipeline non-idempotent: rows an older
    parser produced and the current one no longer does stayed forever, so
    re-running a fixed parser left old output mixed in with new. Replacing makes
    a re-run converge on one canonical state.

    The delete only happens when the parse actually succeeded and survived
    validation. A failed download or an unreadable PDF must never be able to
    empty a company that already had a good read -- that would turn a transient
    fetch problem into data loss.

    **Both the delete and the retraction are scoped to READER_METHODS.** Reading
    a company correctly is not authority to overwrite what another producer
    stored against it, and it is not authority to destroy an audit trail: the
    delete also spares `retracted_at IS NOT NULL`, which is the whole point of
    retracting rather than deleting. Rows belonging to the narrative scrapers are
    left exactly as they are.

    That scoping is necessary and **not sufficient**. It stops the reader
    destroying another producer's evidence; it does not stop the reader
    *inserting alongside* it, which would leave one company holding two
    producers' mixes at once and double-count downstream. Nothing reaches that
    state today -- all 141 such companies sit at `ok` or `no_section`, and
    daily_sweep selects neither -- but any bulk apply must decide ownership
    before it writes, not only before it deletes. See db/PARSER_BACKLOG.md P11
    and P12.
    """
    parsed = len(rows)
    unique, duplicates, conflicts = _dedupe(rows)

    if conflicts and status == "ok":
        # Not a duplicate: the same item with two different values means the rows
        # were never separated. Keep whatever is stored and say why.
        status = "parse_conflict"
        detail = ("same item twice with different values: "
                  + ", ".join(sorted(set(conflicts))[:5]))
        unique = []

    replace = status == "ok" and bool(unique)
    # The document was opened and read, and it holds no mix this parser can use.
    # That is not the same as a download that failed: the rows this company
    # already had came out of this very file, so the read that produced them has
    # been contradicted by a read of the same source. Retract them -- they stay
    # in the table and stay auditable, but they stop being evidence.
    # 'no_report', 'download_failed' and 'parse_failed' are deliberately absent:
    # nothing was read, so nothing is disproved, and the standing rule that a
    # transient fetch problem must never empty a company still holds. That is
    # why 2302 麗正 keeps its rows: no filing was found this time, so there is
    # nothing to contradict them with.
    #
    # The match is on the fiscal year's filings rather than on this exact file:
    # a company may file two editions of the same annual report (2608 嘉里大榮
    # has F04 and F11 on the same date, and both read no_section), and the
    # second edition is the same evidence, not new evidence. A row sourced from
    # something that is not a filing for this year -- an investor deck, say --
    # does not match the pattern and survives, which is the distinction the rule
    # has to keep. Rows with no recorded source predate provenance and are
    # treated as the filing's, because that is what they were.
    retract = status in ("no_section", "parse_conflict") and source_file is not None

    with get_cursor() as cur:
        if retract:
            cur.execute(
                """
                UPDATE tw.product_revenue
                   SET retracted_at = NOW(),
                       retracted_reason = %s
                 WHERE stock_id = %s AND fiscal_year = %s
                   AND retracted_at IS NULL
                   AND method = ANY(%s)
                   AND (source_file IS NULL OR source_file LIKE %s)
                """,
                ("re-read of %s on %s yielded %s" % (source_file, date.today(), status),
                 stock_id, fiscal_year, list(READER_METHODS),
                 "%d_%s_%%" % (fiscal_year, stock_id)),
            )
        if replace:
            cur.execute("DELETE FROM tw.product_revenue "
                        "WHERE stock_id = %s AND fiscal_year = %s "
                        "  AND method = ANY(%s) AND retracted_at IS NULL",
                        (stock_id, fiscal_year, list(READER_METHODS)))
        for row in unique:
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
                (stock_id, fiscal_year, status, detail, source_file,
                 parsed_rows, persisted_rows, duplicate_rows)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (stock_id, fiscal_year) DO UPDATE SET
                status = EXCLUDED.status, detail = EXCLUDED.detail,
                parsed_rows = EXCLUDED.parsed_rows,
                persisted_rows = EXCLUDED.persisted_rows,
                duplicate_rows = EXCLUDED.duplicate_rows,
                source_file = EXCLUDED.source_file, updated_at = NOW()
            """,
            (stock_id, fiscal_year, status, detail, source_file,
             parsed, len(unique), duplicates),
        )
    # save() can downgrade 'ok' to 'parse_conflict', so the caller's tally and
    # the sweep's retry logic have to see what was actually written.
    return status


def process(stock_id: str, fiscal_year: int) -> str:
    filename, ambiguity = find_report(stock_id, fiscal_year)
    if not filename:
        save(stock_id, fiscal_year, [], "", None, "no_report", ambiguity)
        return "no_report"
    content = download_pdf(stock_id, filename)
    if not content:
        save(stock_id, fiscal_year, [], "", filename, "download_failed")
        return "download_failed"
    try:
        rows, method = parse_pdf(content, fiscal_year)
    except ScopeUnresolved as e:
        # A read that refused on scope, not a document that failed to open: nothing is
        # retracted and nothing is written but the status (save() retracts only on
        # no_section and parse_conflict).
        save(stock_id, fiscal_year, [], "", filename, "scope_unresolved", str(e)[:500])
        return "scope_unresolved"
    except Exception as e:  # pdfplumber raises a variety of errors on odd files
        save(stock_id, fiscal_year, [], "", filename, "parse_failed", f"{type(e).__name__}: {e}")
        return "parse_failed"
    if not rows:
        save(stock_id, fiscal_year, [], "", filename, "no_section")
        return "no_section"
    return save(stock_id, fiscal_year, rows, method, filename, "ok", ambiguity)


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
        elif filings[source_file].filed is None:
            outcome = "no_timestamp"
        else:
            with get_cursor() as cur:
                cur.execute(
                    "UPDATE tw.product_revenue_status SET filing_date = %s "
                    "WHERE stock_id = %s AND fiscal_year = %s",
                    (filings[source_file].filed, stock_id, fiscal_year))
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
