"""Read the product mix out of the filing's own text, for the tables pdfplumber
cannot see.

scrapers.annual_report_narrative_tables reads the same tables through
pdfplumber's grid, which is the better tool when the grid survives the PDF. On
40 of the 81 flagged companies it does; on the six that prompted this it returns
nothing at all. Their tables are perfectly legible in the extracted text.

The safety net is that a product mix sums to 100. Every column is tried and the
result is accepted only if its shares total 95-105, so a misread column -- the
wrong year, a quantity taken for a percentage -- fails the test rather than
being written. Refusing is a fine outcome: the company stays flagged.

    python -m scrapers.annual_report_narrative_text --dry-run
    python -m scrapers.annual_report_narrative_text --apply

Targets are whatever db.check_industry's [mix] check currently flags, so a run
repairs what is known to be broken and leaves the rest alone.
"""
import argparse
import io
import re

from db.check_industry import check_mix
from db.connection import get_cursor
from scrapers.annual_report_revenue import _is_region

FISCAL_YEAR = 2025
ROC = FISCAL_YEAR - 1911  # 114
NUM = re.compile(r"-?[\d,]+(?:\.\d+)?")
# Rows that are not products. 合計 closes the table and its share is the 100.
SKIP = re.compile(r"^\s*(合\s*計|總\s*計|小\s*計|產品名稱|項\s*目|主要產品|"
                  r"所營業務之主要內容|產品項目|單位|金\s*額|年度)")
STOP = re.compile(r"^\s*(3\.|4\.|\(3\)|\(4\)|二、|２\.|計畫開發|目前之(主要)?商品|"
                  r"目前之商品|產業概況|公司目前)")


def _nums(line):
    out = []
    for m in NUM.finditer(line):
        t = m.group().replace(",", "")
        if t in ("", "-"):
            continue
        try:
            out.append(float(t))
        except ValueError:
            pass
    return out


# The unit column sits between the product and its figures, so stripping the
# numbers leaves it glued to the name: 南亞's 軟質膠布 came out 軟質膠布公噸.
# Only units that cannot be the tail of a product name. The first version also
# stripped 片/個/套/張/米/台, which turned 磊晶片 into 磊晶 and 積體電路晶片 into
# 積體電路晶 — and a weight_item that names a line nobody reported is worse than
# a name with a unit stuck on it.
UNIT = re.compile(r"(公噸|公斤|千碼|千張|千米|千平方英呎|平方英呎|仟元|千元|公升|公尺)$")
# Header fragments and other tables' rows read as products otherwise. 晶焱's
# "比重年度年度年第季" scored 1.00 and the total still landed inside tolerance.
NOT_A_PRODUCT = re.compile(r"年度|比重|第\s*季|合\s*計|單位|金\s*額|數\s*量|"
                           r"占營業額|銷售值|營業收入|內銷|外銷|民國")


def _regions(body):
    """Every slice that might hold the mix table, best candidate first.

    A chapter mentions 營業比重 in prose before printing it, and 京元電子 puts a
    five-year export-ratio table in between; parsing from the first mention
    swallows those rows and no column sums to 100. Each occurrence is offered
    and the caller keeps the first that parses.
    """
    out = []
    start = 0
    while True:
        i = body.find("營業比重", start)
        if i < 0:
            break
        out.append(_slice(body, i))
        start = i + 1
    return [r for r in out if r]


def _slice(body, i):
    lines = body[i:i + 3000].splitlines()
    kept = []
    for ln in lines[1:]:
        if STOP.match(ln) and len(kept) > 2:
            break
        kept.append(ln)
    return kept


# Both calendars appear. IET-KY heads its columns 2024年度 / 2025年度 and the
# next filing along uses 113年度 / 114年度 for the same two years.
YEAR_TOKEN = re.compile(r"(\d{2,4})\s*年")


def _year_columns(lines):
    """Which column of the header is the fiscal year, and how many there are.

    Reading the header is the whole point. Taking the last column instead looks
    right until a filing appends a stub period: IET-KY prints 2024年度, 2025年度
    and 2026年截至3月31日, and that first-quarter column sums to 100 exactly like
    a full year does, so no arithmetic check can catch it. Returns (index,
    count), or None when the header does not name the fiscal year.
    """
    for ln in lines[:10]:
        years = []
        for tok in YEAR_TOKEN.findall(ln):
            v = int(tok)
            v = v if v > 1911 else v + 1911
            if 2000 <= v <= FISCAL_YEAR + 2 and v not in years:
                years.append(v)
        if len(years) >= 2 and FISCAL_YEAR in years:
            return years.index(FISCAL_YEAR), len(years)
    return None


def parse(body):
    regions = _regions(body)
    if not regions:
        return None, "no 營業比重 heading"
    last = "no column sums to 100"
    for lines in regions:
        vals, why = _parse_one(lines)
        if vals:
            return vals, why
        last = why
    return None, last


def _parse_one(lines):
    ycol = _year_columns(lines)

    # A name and its numbers are not always on the same line: 6552 puts the
    # product above its figures, 6122 wraps a long name around them. Carry the
    # nearest preceding text-only line as the pending name.
    rows, pending = [], None
    for ln in lines:
        if SKIP.match(ln):
            continue
        ns = _nums(ln)
        name = NUM.sub("", ln).strip(" 　:：%()（）")
        name = re.sub(r"\s+", "", name)
        if not ns:
            if name and len(name) >= 2 and not NOT_A_PRODUCT.search(name):
                pending = name[:60]
            continue
        label = name if len(name) >= 2 else pending
        if not label or NOT_A_PRODUCT.search(label):
            pending = None
            continue
        label = UNIT.sub("", label) or label
        rows.append((label[:60], ns))
        pending = None
    if not rows:
        return None, "no numeric rows"

    # With a multi-year header, the share's position is known rather than
    # guessed: n years of (amount, share) pairs put the fiscal year's share at
    # 2i+1, and a share-only table puts it at i.
    if ycol:
        i, n = ycol
        picks = [(2 * i + 1, 2 * n), (i, n)]
    else:
        picks = [(-1, None), (-2, None), (-3, None)]

    for pos, width in picks:
        vals = []
        for label, ns in rows:
            if width is not None and len(ns) != width:
                vals = None
                break
            if width is None and len(ns) < abs(pos):
                vals = None
                break
            v = ns[pos]
            if not (0 <= v <= 100):
                vals = None
                break
            vals.append((label, v))
        if not vals:
            continue
        total = sum(v for _, v in vals)
        # A geography table sums to 100 exactly as well as a product mix does,
        # so the total cannot tell them apart — 嘉聯益 parsed cleanly as
        # 美國/中國大陸/台灣/其他 and would have been written as products.
        if sum(_is_region(lbl) for lbl, _ in vals) >= max(1, len(vals) // 2):
            return None, "geography table, not a product mix"
        # 驊訊 reports 銷貨/勞務/權利金收入: revenue recognition categories, which
        # name no product and cannot be mapped to a node.
        if all(re.search(r"收入$|收益$", lbl) for lbl, _ in vals):
            return None, "revenue categories, not products"
        if 95 <= total <= 105:
            if len(vals) < 2:
                continue  # a single row summing to 100 proves nothing
            return vals, "%s col %d, sums %.2f" % (
                "header" if ycol else "rightmost", pos, total)
    return None, "no column sums to 100"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    with get_cursor(commit=False) as cur:
        flagged = {r["stock_id"]: r for r in check_mix(cur)}
        cur.execute("""SELECT stock_id, body FROM tw.product_narrative
                       WHERE fiscal_year = %s AND stock_id = ANY(%s)""",
                    (FISCAL_YEAR, list(flagged)))
        prose = {r["stock_id"]: r["body"] or "" for r in cur.fetchall()}

    solved, refused, report = {}, [], []
    for sid in sorted(flagged):
        vals, why = parse(prose.get(sid, ""))
        kind = flagged[sid]["kind"]
        if vals:
            solved[sid] = vals
            report.append("OK   %s %-10s %-18s %s" % (sid, flagged[sid]["name"], kind, why))
            report.append("       " + " / ".join("%s %.2f" % v for v in vals[:6]))
        else:
            refused.append(sid)
            report.append("SKIP %s %-10s %-18s %s" % (sid, flagged[sid]["name"], kind, why))
    io.open("tmp/mix_text_report.txt", "w", encoding="utf-8").write("\n".join(report))
    print("solved %d, refused %d, of %d flagged" % (len(solved), len(refused), len(flagged)))

    if not args.apply:
        print("dry run: nothing written; see tmp/mix_text_report.txt")
        return

    with get_cursor() as cur:
        for sid, vals in solved.items():
            cur.execute("DELETE FROM tw.product_revenue WHERE stock_id=%s AND fiscal_year=%s",
                        (sid, FISCAL_YEAR))
            for item, pct in vals:
                cur.execute("""
                    INSERT INTO tw.product_revenue (stock_id, fiscal_year, item, revenue,
                                                    pct, source_file, method, updated_at)
                    VALUES (%s, %s, %s, NULL, %s, NULL, 'narrative_text', NOW())
                    ON CONFLICT (stock_id, fiscal_year, item) DO UPDATE
                       SET pct = EXCLUDED.pct, method = EXCLUDED.method, updated_at = NOW()
                """, (sid, FISCAL_YEAR, item, pct))
            cur.execute("""UPDATE tw.product_revenue_status SET status='ok',
                                  detail=%s, updated_at=NOW()
                            WHERE stock_id=%s AND fiscal_year=%s""",
                        ("%d rows re-read from the narrative text" % len(vals), sid, FISCAL_YEAR))
    print("applied to %d companies" % len(solved))


if __name__ == "__main__":
    main()
