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
SKIP = re.compile(r"^\s*(合\s*計|總\s*計|小\s*計|TOTAL|Total|產品名稱|項\s*目|主要產品|"
                  r"所營業務之主要內容|產品項目|單位|金\s*額|年度|營業淨額|營業收入淨額|"
                  r"減\s*[:：]|銷貨退回)")
# 3、 is as common as 3. for a numbered heading, and a table that does not stop
# drags the next section's stray digits in as one-number rows, which makes every
# column the wrong width. 昶昕 failed on exactly that.
# (3) and (4) are section numbers only when a section title follows them. Bare,
# they are the third item of any enumeration -- 擎亞 lists its registered
# businesses "(3)國際貿易業。" and 神盾 its product families "(3) 無線音頻控制
# IC" -- and cutting there ended the slice before the table it was looking for.
# 區域別 is a second table under the same heading and sums to 100 exactly like
# the product one: 信立 prints 內銷/外銷/亞洲 straight after its products, and
# the geography guard then rejected the pair.
STOP = re.compile(r"^[\s•●■◆]*([34][\.、．]|\((3|4)\)\s*(公司)?(目前|計[畫劃]|產業)|"
                  r"二、|三、|２\.|計畫開發|目前(之)?(主要)?商品|產業概況|公司目前|"
                  r"[(（]?\d?[)）]?\s*各種?區域|區域別|地區別)")

# A cell left blank as a dash carries no number, so the row comes out narrower
# than its neighbours and the whole column is rejected for inconsistent width —
# 天擎's 工控SoC reads "- - 2,253 0.80" for a line it did not sell last year.
# Only a dash standing alone is a cell; the one in -KY is part of a name.
DASH_CELL = re.compile(r"(?<![\w\d])[-–—](?![\w\d])")


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
        # 通泰 heads it 主要產品佔營業收入淨額比重, which contains neither
        # 營業比重 nor 營業收入比重 as a substring.
        i = min([p for p in (body.find("營業比重", start),
                             body.find("收入淨額比重", start)) if p >= 0], default=-1)
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


# Some filings print the table the other way round: the products run across one
# line and the metrics down the side. 瑞昱 heads it "114年度 積體電路產品 其他
# 合計" with 營業比重(%) underneath, 凱鈺 the same with 百分比. The row-wise
# reader cannot see it at all — every row it finds is a metric, not a product.
TRANSPOSED_HEAD = re.compile(r"合\s*計\s*$")
SHARE_ROW = re.compile(r"^\s*(營業比重|營收比重|百分比|比\s*重)")


def _parse_transposed(lines):
    for k, ln in enumerate(lines):
        if not TRANSPOSED_HEAD.search(ln):
            continue
        names = [w for w in re.split(r"\s+", NUM.sub(" ", ln)).__iter__() if w.strip()]
        names = [re.sub(r"[()（）%]", "", w) for w in names]
        names = [w for w in names if w and not NOT_A_PRODUCT.search(w)]
        if names and re.match(r"^合\s*計$", names[-1]):
            names = names[:-1]
        if len(names) < 2:
            continue
        for nxt in lines[k + 1:k + 7]:
            if not SHARE_ROW.match(nxt):
                continue
            ns = _nums(nxt)
            # one figure per product plus the 100 that closes the row
            if len(ns) != len(names) + 1:
                continue
            vals = list(zip(names, ns[:len(names)]))
            if sum(_is_region(l) for l, _ in vals) >= max(1, len(vals) // 2):
                return None, "geography table, not a product mix"
            total = sum(v for _, v in vals)
            if 95 <= total <= 105:
                return vals, "transposed, sums %.2f" % total
    return None, None


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
    for lines in regions:
        vals, why = _parse_transposed(lines)
        if vals:
            return vals, why
        if why:
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
            # 合計 closes a table. 通泰 prints two of them one after the other,
            # by form and then by series, and both sum to 100 — read as one
            # block they sum to 200 and neither is found.
            if rows and re.match(r"^\s*(合\s*計|總\s*計|主要產品)", ln):
                rows.append(None)
            continue
        ns = _nums(DASH_CELL.sub(" 0 ", ln))
        name = NUM.sub("", DASH_CELL.sub(" ", ln)).strip(" 　:：%％$＄()（）")
        name = re.sub(r"\s+", "", name)
        if not ns:
            if name and len(name) >= 2 and not NOT_A_PRODUCT.search(name):
                pending = name[:60]
            continue
        label = name if len(name) >= 2 else pending
        # 其他營業收入 is a real line of the table, not a header, and dropping it
        # left 南電 with one row where the check needs two to believe a column.
        residual = label and re.match(r"^(其他|其它|Other)", label)
        if not label or (NOT_A_PRODUCT.search(label) and not residual):
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

    # Rows of a different width are not part of the same table: 神盾 and 擎亞 both
    # print a numbered list of registered businesses above theirs, and each entry
    # carries exactly one number — its own bullet. Split into runs of equal width
    # and judge each run, rather than letting one stray row void the column.
    runs, cur_run = [], []
    for r in rows + [None]:
        if r is None or (cur_run and len(r[1]) != len(cur_run[0][1])):
            if len(cur_run) >= 2:
                runs.append(cur_run)
            cur_run = []
        if r is not None:
            cur_run.append(r)
    if not runs:
        return None, "no numeric rows"

    for pos, width in picks:
      for run in runs:
        vals = []
        for label, ns in run:
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
        if 95 <= total <= 105 and len(vals) >= 2:
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
