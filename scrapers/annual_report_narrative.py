"""
Business-description scraper (業務內容／業務範圍, 年報 PDF).

Companion to scrapers.annual_report_revenue, which reads the 營業比重 table out
of the same filing. That table is the better evidence when it exists, but the
company picks its own granularity and IC designers routinely book the whole
business as one line ("IC產品收入 98.94%"), which confirms nothing about which
product lines they actually run.

The 業務範圍 section under 營運概況 does list them by name. It cannot be turned
into percentages, so this stores the prose and leaves the judgement to a human.

Reuses find_report / download_pdf / _rate_limit from the revenue scraper — the
three-step doc.twse.com.tw dance and its encoding traps are the same, and the
PDF is now kept on disk so the next pass does not re-download it.

  python -m scrapers.annual_report_narrative --year 2025 --stocks 6415,3444
  python -m scrapers.annual_report_narrative --year 2025 --gap
  python -m scrapers.annual_report_narrative --year 2025 --all [--retry-failed]
"""

import argparse
import io
import os
import re
import sys
import time
import unicodedata

import pdfplumber

from db.connection import get_cursor
from scrapers.annual_report_revenue import find_report, download_pdf

ARCHIVE_DIR = r"D:\invest_data\annual_reports"

# Seconds between companies. The host throttles a tight loop, and a throttled
# listing is indistinguishable from a company that filed no report.
PACE = 4.0

# Where the description starts. 業務範圍 is the subsection that lists product
# lines; the wider headings are fallbacks for reports that number differently.
START_HINTS = ("業務範圍", "所營業務之主要內容", "主要產品之項目", "業務內容")

# Where it ends. The next sibling section under 營運概況 comes first in every
# layout seen so far; the 天干 headings catch a jump to the next chapter.
STOP_HINTS = ("市場及產銷概況", "從業員工", "環保支出", "勞資關係", "重要契約")
STOP_RE = re.compile(r"^\s*[陸柒捌玖拾]、")

MAX_PAGES = 8          # 業務範圍 runs long at conglomerates; past this it is noise
MAX_CHARS = 20000

# The table of contents lists every heading in the report, so it matches the
# start hints before the real section does and would be captured instead of it.
# Its giveaway is the dot leaders running to the page numbers.
# Dot leaders run to the page numbers. Some reports set them as hyphens
# instead of dots, which is why the hyphen run is matched too (longer,
# because short hyphen runs do appear inside real tables).
DOT_LEADER = re.compile(r"[.·‧．]{5,}|[-─—]{10,}")


def _is_toc(text: str) -> bool:
    return "目錄" in text.replace(" ", "") or len(DOT_LEADER.findall(text)) >= 5


def _norm(text: str) -> str:
    """NFKC-fold before matching.

    Annual reports embed CJK compatibility ideographs and full-width digits, so
    a plain substring test for 業務範圍 silently misses on some filings. Same
    trap the revenue parser hit (see its _clean).
    """
    return unicodedata.normalize("NFKC", text or "")


def extract_sections(content: bytes) -> tuple[list[dict], str]:
    """Return ([{section, body, pages}], detail) for one report PDF."""
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        pages = [_norm(p.extract_text() or "") for p in pdf.pages]

    # 業務範圍 on its own is not enough: the governance chapter uses the same
    # words ("董事於任期內就執行業務範圍", 薪資報酬委員會 tables), and six
    # companies were captured from there on the first pass. The chapter title
    # 營運概況 next to it is what pins the right section.
    start = None
    for i, text in enumerate(pages):
        if _is_toc(text):
            continue
        if "營運概況" in text and ("業務內容" in text or "業務範圍" in text):
            start = i
            break
    if start is None:
        for hint in START_HINTS:      # most specific heading first
            for i, text in enumerate(pages):
                if hint in text and not _is_toc(text):
                    start = i
                    break
            if start is not None:
                break
    if start is None:
        return [], "no start heading among %s" % (START_HINTS,)

    body, used = [], []
    for i in range(start, min(start + MAX_PAGES, len(pages))):
        text = pages[i]
        # Stop *before* appending: the sibling heading belongs to the next
        # section, and on the start page itself a stop word would be a
        # table-of-contents line rather than the real heading.
        if i > start and (any(h in text for h in STOP_HINTS)
                          or STOP_RE.search(text)):
            break
        body.append(text)
        used.append(i + 1)

    joined = re.sub(r"\n{3,}", "\n\n", "\n".join(body)).strip()[:MAX_CHARS]
    if len(joined) < 200:
        return [], f"section too short ({len(joined)} chars)"
    return [{"section": "業務內容", "body": joined,
             "pages": f"{used[0]}-{used[-1]}"}], f"{len(joined)} chars"


def _record_status(cur, stock_id, year, status, detail=None, source_file=None):
    cur.execute(
        """
        INSERT INTO tw.product_narrative_status
            (stock_id, fiscal_year, status, detail, source_file, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (stock_id, fiscal_year) DO UPDATE
        SET status = EXCLUDED.status, detail = EXCLUDED.detail,
            source_file = EXCLUDED.source_file, updated_at = NOW()
        """,
        (stock_id, year, status, detail, source_file))


def _known_filename(stock_id: str, year: int) -> str | None:
    """The filename the revenue scraper already resolved for this filing.

    Reusing it drops a third of the requests to doc.twse.com.tw and, more to
    the point, removes the step that fails first under rate limiting: a
    throttled listing looks exactly like "this company filed no report", and
    the first full run mislabelled 38 companies that way while their filenames
    were sitting in tw.product_revenue_status all along.
    """
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT source_file FROM tw.product_revenue_status "
            "WHERE stock_id = %s AND fiscal_year = %s AND source_file IS NOT NULL",
            (stock_id, year))
        row = cur.fetchone()
    return row["source_file"] if row else None


def scrape_one(stock_id: str, name: str, year: int) -> str:
    filename = _known_filename(stock_id, year) or find_report(stock_id, year)
    if not filename:
        with get_cursor() as cur:
            _record_status(cur, stock_id, year, "no_report")
        return "no_report"

    path = os.path.join(ARCHIVE_DIR, filename)
    content = None
    if os.path.exists(path):
        with open(path, "rb") as f:
            content = f.read()
    if not content:
        content = download_pdf(stock_id, filename)
        if content:
            os.makedirs(ARCHIVE_DIR, exist_ok=True)
            with open(path, "wb") as f:
                f.write(content)
    if not content:
        with get_cursor() as cur:
            _record_status(cur, stock_id, year, "download_failed", source_file=filename)
        return "download_failed"

    try:
        sections, detail = extract_sections(content)
    except Exception as exc:                      # a corrupt PDF must not stop the run
        with get_cursor() as cur:
            _record_status(cur, stock_id, year, "parse_failed", str(exc)[:200], filename)
        return "parse_failed"

    if not sections:
        with get_cursor() as cur:
            _record_status(cur, stock_id, year, "no_section", detail, filename)
        return "no_section"

    with get_cursor() as cur:
        for s in sections:
            cur.execute(
                """
                INSERT INTO tw.product_narrative
                    (stock_id, fiscal_year, section, body, pages, source_file, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (stock_id, fiscal_year, section) DO UPDATE
                SET body = EXCLUDED.body, pages = EXCLUDED.pages,
                    source_file = EXCLUDED.source_file, updated_at = NOW()
                """,
                (stock_id, year, s["section"], s["body"], s["pages"], filename))
        _record_status(cur, stock_id, year, "ok", detail, filename)
    return "ok"


def _targets(args) -> list[tuple[str, str]]:
    with get_cursor(commit=False) as cur:
        if args.stocks:
            cur.execute(
                "SELECT stock_id, name FROM tw.stocks WHERE stock_id = ANY(%s) "
                "ORDER BY stock_id", (args.stocks.split(","),))
        elif args.gap:
            # The memberships that have no revenue-table evidence: exactly the
            # ones this prose is meant to settle.
            cur.execute(
                """
                SELECT DISTINCT s.stock_id, s.name
                  FROM tw.stock_themes st JOIN tw.stocks s USING(stock_id)
                 WHERE st.evidence_kind IN ('filing_too_coarse', 'no_filing', 'pending')
                 ORDER BY 1
                """)
        else:
            cur.execute(
                "SELECT stock_id, name FROM tw.stocks "
                "WHERE is_active AND security_type = 'STOCK' ORDER BY stock_id")
        targets = [(r["stock_id"], r["name"]) for r in cur.fetchall()]

        if not args.retry_failed:
            cur.execute(
                "SELECT stock_id FROM tw.product_narrative_status "
                "WHERE fiscal_year = %s AND status IN ('ok', 'no_report')", (args.year,))
            done = {r["stock_id"] for r in cur.fetchall()}
            targets = [t for t in targets if t[0] not in done]
    return targets


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--stocks", help="comma-separated stock ids")
    ap.add_argument("--gap", action="store_true",
                    help="only stocks with theme memberships lacking evidence")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--retry-failed", action="store_true")
    args = ap.parse_args()
    if not (args.stocks or args.gap or args.all):
        sys.exit("pick one of --stocks / --gap / --all")

    targets = _targets(args)
    print(f"{len(targets)} companies to fetch for FY{args.year}", flush=True)
    counts = {}
    for i, (stock_id, name) in enumerate(targets, 1):
        if i > 1:
            time.sleep(PACE)
        status = scrape_one(stock_id, name, args.year)
        counts[status] = counts.get(status, 0) + 1
        print(f"[{i}/{len(targets)}] {stock_id} {name}: {status}", flush=True)
    print("done: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))


if __name__ == "__main__":
    main()
