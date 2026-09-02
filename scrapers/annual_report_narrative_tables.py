"""Read the product mix from the annual report's 業務範圍 tables.

The previous attempt regexed tw.product_narrative.body, which is that chapter
flattened to text. Flattening destroys exactly what the mix needs: which column
belongs to which year. Filings disagree on the order — 立誠 prints 114年度 first,
雍智 prints it last — so no positional rule works, and reconstructing the header
from flattened text cost more rows than it fixed.

pdfplumber keeps the grid. The header spans two rows, the year across the top
and 金額 / 比率(%) beneath it, so the target year's share column can be read
rather than guessed. The PDFs are already on disk from the narrative fetch and
tw.product_narrative.pages says which pages to look at, so this costs no
downloads.

Measured against 75 companies whose table parse is already trusted: 50 agree
exactly, 20 yield nothing, and all 5 disagreements turned out to be the stored
data being wrong — 雍智 held the prior year's shares, 單井 was missing its two
largest lines, 點序 recorded its smallest line as 100%.

Run: python -m scrapers.annual_report_narrative_tables --year 2025 [--stocks ...]
"""

import os
import re

import pdfplumber

# The same chapter carries a domestic/export split in the same shape as the
# product mix; 欣興's 內銷/外銷 table parsed cleanly and meant something else.
from scrapers.annual_report_revenue import REGION_WORDS, _is_label

PDF_DIR = r"D:\invest_data\annual_reports"
YEAR = re.compile(r"(\d{2,3})\s*年度")
SHARE_HEADER = re.compile(r"比率|比重|%|佔")
# 合計 rows are subtotals, and an empty name is a continuation line.
SKIP_NAME = re.compile(r"^\s*(合\s*計|總\s*計|小\s*計|)\s*$")
NUMBER = re.compile(r"^-?[\d,]+(?:\.\d+)?%?$")
# A share column alone is not enough to identify the mix: the same chapter has
# headcount-by-education and age tables with a 比率 column that also sums to 100.
# The first column has to be naming products.
PRODUCT_AXIS = re.compile(r"產品|商品|項目|品名|事業|部門|類別|營業|服務")
NON_PRODUCT_AXIS = re.compile(r"學歷|年齡|職稱|性別|年資|人數|地區|區域|國家")
# The header is not always honest about what the table counts. 展宇's headcount
# table parsed as a product mix because its first column is not labelled 學歷 —
# the rows say what it is.
NON_PRODUCT_ROW = re.compile(r"^(碩士|博士|學士|大專|大學|專科|高中|男|女|"
                             r"\d+\s*[-~]\s*\d+\s*歲|\d+\s*歲)")


def _cell(c) -> str:
    return re.sub(r"\s+", " ", (c or "").replace("\n", " ")).strip()


def _name(c) -> str:
    """A cell's text with the wrapping removed.

    A long product name is broken across lines inside its cell, and the join
    leaves a space in the middle of a word. Chinese does not space its words,
    so a space between two Han characters is always the wrap, never the name.
    """
    return re.sub(r"(?<=[一-鿿])\s+(?=[一-鿿])", "", _cell(c))


def _share_column(table, target_roc: int) -> int | None:
    """Index of the column holding the target year's share, or None.

    The year label sits on one row and spans several columns, leaving the cells
    to its right empty; the sub-header names them. Carrying the year forward
    across those blanks is what lets the two be matched up.
    """
    for top in range(min(2, len(table) - 1)):
        years, current = [], None
        for c in table[top]:
            m = YEAR.search(_cell(c))
            if m:
                current = int(m.group(1))
            years.append(current)
        if target_roc not in [y for y in years if y]:
            continue
        sub = [_cell(c) for c in table[top + 1]]
        for i, (y, s) in enumerate(zip(years, sub)):
            if y == target_roc and SHARE_HEADER.search(s):
                return i
        # Single-year tables sometimes name the share column on the year row.
        for i, (y, c) in enumerate(zip(years, table[top])):
            if y == target_roc and SHARE_HEADER.search(_cell(c)):
                return i

    # Most filings state one year and never label it — the report is for that
    # year by definition. 茂達 prints 「產 品 | 營業比重(%)」 and nothing else.
    head = [_name(c) for c in table[0]]
    if head and PRODUCT_AXIS.search(head[0]) and not NON_PRODUCT_AXIS.search(head[0]):
        shares = [i for i, h in enumerate(head[1:], 1) if SHARE_HEADER.search(h)]
        if len(shares) == 1:
            return shares[0]
    return None


def _is_skippable(name: str) -> bool:
    # _is_label rejects a cell that names nothing — 太欣 and 創惟 keep their
    # product names in a footnote and leave 「(1)」「(2)」 in the table. The
    # revenue scraper has always rejected those; this path had not been reusing it.
    return bool(SKIP_NAME.match(name) or NUMBER.match(name) or YEAR.search(name)
                or not _is_label(name))


def _num(cell: str) -> str:
    """Digits with the spacing the PDF put inside them removed.

    點序's shares extract as 「0 .35%」 and 「3 . 0 0 %」. Without this they fail
    the numeric test and vanish, leaving a single line that happens to fall
    inside the 90-110 total gate on its own.
    """
    return re.sub(r"\s+", "", cell)


def parse_table(table, target_roc: int = 114) -> list[tuple[str, float]]:
    col = _share_column(table, target_roc)
    if col is None:
        return []
    rows = []
    for r in table:
        name = _name(r[0] if r else "")
        if _is_skippable(name):
            continue
        if col >= len(r):
            continue
        raw = _num(_cell(r[col])).rstrip("%")
        if not raw or not NUMBER.match(raw + "%"):
            continue
        try:
            pct = float(raw.replace(",", ""))
        except ValueError:
            continue
        if 0 < pct <= 100:
            rows.append((name, pct))
    if any(w in n for n, _ in rows for w in REGION_WORDS):
        return []
    if any(NON_PRODUCT_ROW.match(n) for n, _ in rows):
        return []
    total = sum(p for _, p in rows)
    return rows if len(rows) >= 1 and 90 <= total <= 110 else []


def parse_transposed(table, target_roc: int = 114) -> list[tuple[str, float]]:
    """Products across the top, shares beneath — 聯發科's layout.

        ['產品項目', '晶片',   '其他(註)']
        ['營業比重', '98.22%', '1.78%']
    """
    if len(table) < 2:
        return []
    names = [_name(c) for c in table[0]]
    if not names or not PRODUCT_AXIS.search(names[0]) or NON_PRODUCT_AXIS.search(names[0]):
        return []
    for row in table[1:]:
        label = _name(row[0] if row else "")
        if not SHARE_HEADER.search(label):
            continue
        rows = []
        for name, cell in zip(names[1:], row[1:]):
            raw = _num(_cell(cell)).rstrip("%")
            if not name or not raw or not NUMBER.match(raw + "%"):
                continue
            try:
                pct = float(raw.replace(",", ""))
            except ValueError:
                continue
            if 0 < pct <= 100 and not _is_skippable(name):
                rows.append((name, pct))
        if rows and 90 <= sum(p for _, p in rows) <= 110:
            return rows
    return []


def parse_report(source_file: str, pages: str, target_roc: int = 114):
    path = os.path.join(PDF_DIR, source_file)
    if not os.path.exists(path):
        return []
    first, last = (int(x) for x in pages.split("-"))
    with pdfplumber.open(path) as pdf:
        for i in range(first - 1, min(last, len(pdf.pages))):
            for table in pdf.pages[i].extract_tables() or []:
                if len(table) < 3:
                    continue
                rows = parse_table(table, target_roc) or parse_transposed(table, target_roc)
                if rows:
                    return rows
    return []


def main():
    import argparse

    from db.connection import get_cursor

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--year", type=int, default=2025)
    ap.add_argument("--stocks", help="comma-separated stock ids")
    ap.add_argument("--gap", action="store_true",
                    help="only companies with no product mix at all")
    ap.add_argument("--replace", action="store_true",
                    help="clear the company's existing FY rows first; without it a "
                         "differently-named line is added alongside and the mix "
                         "sums past 100")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    roc = args.year - 1911

    with get_cursor() as cur:
        sql = """
            SELECT n.stock_id, s.name, n.pages, n.source_file
            FROM tw.product_narrative n
            JOIN tw.stocks s ON s.stock_id = n.stock_id
            WHERE n.fiscal_year = %s
        """
        params = [args.year]
        if args.stocks:
            sql += " AND n.stock_id = ANY(%s)"
            params.append([x.strip() for x in args.stocks.split(",") if x.strip()])
        if args.gap:
            # A share is not always printed: a quarter of filings give amounts
            # only, and tw.product_revenue_share derives the ratio from them.
            # Testing pct alone calls those companies empty when they are not —
            # 雍智 and 立誠 both have a full mix stored as amounts.
            sql += """ AND NOT EXISTS (SELECT 1 FROM tw.product_revenue_share r
                                       WHERE r.stock_id = n.stock_id
                                         AND r.fiscal_year = %s
                                         AND r.pct_effective IS NOT NULL)"""
            params.append(args.year)
        cur.execute(sql + " ORDER BY n.stock_id", params)
        targets = [dict(r) for r in cur.fetchall()]

    print(f"{len(targets)} narratives to read")
    written = solved = 0
    for t in targets:
        rows = parse_report(t["source_file"], t["pages"], roc)
        if not rows:
            continue
        solved += 1
        print(f"  {t['stock_id']} {t['name']}: " +
              ", ".join(f"{n} {p}%" for n, p in rows[:4]))
        if args.dry_run:
            continue
        with get_cursor() as cur:
            if args.replace:
                cur.execute("DELETE FROM tw.product_revenue "
                            "WHERE stock_id = %s AND fiscal_year = %s",
                            (t["stock_id"], args.year))
            for item, pct in rows:
                cur.execute("""
                    INSERT INTO tw.product_revenue (stock_id, fiscal_year, item, revenue,
                                                    pct, source_file, method, updated_at)
                    VALUES (%s, %s, %s, NULL, %s, %s, 'narrative_table', NOW())
                    ON CONFLICT (stock_id, fiscal_year, item) DO UPDATE
                       SET pct = EXCLUDED.pct, method = EXCLUDED.method,
                           source_file = EXCLUDED.source_file, updated_at = NOW()
                """, (t["stock_id"], args.year, item[:200], pct, t["source_file"]))
                written += 1
            cur.execute("""
                UPDATE tw.product_revenue_status
                   SET status = 'ok', detail = %s, updated_at = NOW()
                 WHERE stock_id = %s AND fiscal_year = %s
            """, (f"{len(rows)} rows from 業務範圍 narrative table",
                  t["stock_id"], args.year))
    print(f"solved {solved} of {len(targets)}; {written} rows written"
          + (" (dry run)" if args.dry_run else ""))


if __name__ == "__main__":
    main()
