# -*- coding: utf-8 -*-
"""A total row read as a product discards the read that was already correct.

1256 鮮活果汁-KY is the case this file exists for. The line reader had already
lifted 果汁 55%, 果粒 43% and 果粉 2% off the page. It then read the next line,
「銷貨淨額 4,119,789 100%」, as a fourth product -- 銷貨淨額 was not in the
aggregate vocabulary, which held only 合計/總計/小計 and was tested with
startswith. The four percentages summed to 200, the 50..150 completeness bar
rejected the lot, and the company was recorded no_section: the parser looked as
though it had found nothing, when in fact it had found everything and thrown it
away.

That is why these are rejection tests, not coverage tests. The failure they
guard is a false negative produced by validation logic, and it is invisible from
downstream -- a discarded read and an absent section leave the same row.

The text below is transcribed verbatim from the filings, through the same page
filter the parser uses (tmp/rejudge/a4_textlayer.txt). Hand-written one-liners
passed an earlier round of prose patterns that then failed on real filings,
because real ones wrap.
"""
from scrapers.annual_report_revenue import _is_aggregate_row, _rows_from_lines

WRAP = chr(10)


# --------------------------------------------------------------------------
# the caption test itself
# --------------------------------------------------------------------------

def test_the_wordings_that_were_missing():
    for caption in ("銷貨淨額", "銷售淨額", "營業收入淨額", "合併營收淨額", "Total"):
        assert _is_aggregate_row(caption), caption


def test_the_wordings_that_already_worked_still_do():
    # 「合 計」 is typeset with padding; a startswith test on the first
    # whitespace token sees only 「合」.
    for caption in ("合計", "合 計", "總 計", "小計", "營業收入合計"):
        assert _is_aggregate_row(caption), caption


def test_a_product_is_not_an_aggregate():
    # Total Station is a surveying instrument, so the English word is matched
    # whole rather than as a substring.
    for caption in ("果汁", "果粒", "Total Station", "3C產品銷售收入", "其他",
                    "尼龍原絲", "DC散熱風扇"):
        assert not _is_aggregate_row(caption), caption


# --------------------------------------------------------------------------
# the read that used to be discarded
# --------------------------------------------------------------------------

# 1256 鮮活果汁-KY, 2025 annual report, page 71.
SIANHUO = WRAP.join([
    "(2)營業比重",
    "本公司產品 2025年度營業收入及其營業比重如下：",
    "單位：新台幣仟元",
    "2025年度",
    "項目",
    "營收淨額 營業比重（%）",
    "果汁 2,240,003 55%",
    "果粒 1,783,457 43%",
    "果粉 76,461 2%",
    "其他 19,868 0%",
    "銷貨淨額 4,119,789 100%",
    "- 66 -",
])


def test_the_total_row_no_longer_costs_the_whole_read():
    rows = _rows_from_lines(SIANHUO, 2025)
    assert [r["item"] for r in rows] == ["果汁", "果粒", "果粉"]
    assert [r["pct"] for r in rows] == [55.0, 43.0, 2.0]
    assert rows[0]["revenue"] == 2240003


def test_the_total_row_is_not_among_the_products():
    assert all("淨額" not in r["item"] for r in _rows_from_lines(SIANHUO, 2025))


# 1441 大東, 2025 annual report: the same shape with 「合 計」 and no % sign.
DADONG = WRAP.join([
    "(2) 營業比重:",
    "單位:新台幣仟元",
    "113 年 度 114年 度",
    "年 度",
    "營業收入總額 營業收入總額",
    "項 目 營業比重(%) 營業比重(%)",
    "(仟元) (仟元)",
    "紗 518,419 31.88 410,869 27.12",
    "布 1,107,560 68.12 1,104,136 72.88",
    "合 計 1,625,979 100.00 1,515,005 100.00",
])


def test_a_spaced_total_caption_is_rejected():
    assert _is_aggregate_row("合 計 1,625,979 100.00".split("1")[0])


def test_dadong_is_still_unread_and_this_records_why():
    """Not a passing case: 大東 belongs to the shape this batch does not touch.

    Its ratio column carries no % sign (「紗 518,419 31.88 410,869 27.12」), so
    the reader falls back to the trailing 0..100 values on the line and lands on
    the wrong year's column. Asserting the empty result keeps the claim honest --
    the aggregate fix alone does not rescue this company, and a test that merely
    said "no total row among the products" would have passed on nothing at all.
    """
    assert _rows_from_lines(DADONG, 2025) == []


# --------------------------------------------------------------------------
# 1338 廣華-KY: two wrongs that summed to a right
# --------------------------------------------------------------------------

# The mix table nests: 汽車零部件 is a parent whose four sub-rows carry the
# products, and the parent's label cell is filled only on the first of them.
# The reader takes column 0, so it read the parent's first sub-row and then
# skipped three rows whose label cell is empty -- 22.54 + 37.34 + 5.58 lost.
# It also kept 銷貨淨額 100.00 as a product. 30.29 + 4.25 + 100.00 = 134.54
# landed inside the 50..150 completeness bar, so a read that was wrong in two
# directions passed validation because the two errors cancelled.
#
# Removing the total row alone leaves 34.54, which the bar now rejects: the
# company fails visibly instead of storing a mix that was never its mix. That
# is the intended outcome, and this fixture exists so a later change cannot
# quietly restore the coincidence.
GUANGHUA = [
    ["項目", "", "114年度", ""],
    ["", "", "營收淨額", "營業比重"],
    ["汽車零部件(註1)", "表面塗裝", "1,584,803", "30.29%"],
    ["", "射出成型", "1,179,347", "22.54%"],
    ["", "轉印件", "1,953,352", "37.34%"],
    ["", "其他(註2)", "291,947", "5.58%"],
    ["模具", "", "222,217", "4.25%"],
    ["銷貨淨額", "", "5,231,666", "100.00%"],
]


def test_the_total_row_is_not_read_as_a_product():
    from scrapers.annual_report_revenue import _rows_from_table
    assert all(not _is_aggregate_row(r["item"])
               for r in _rows_from_table(GUANGHUA, 2025))


def test_a_merged_parent_table_now_fails_visibly_instead_of_by_luck():
    """The read must not be rescued by including the total row.

    Asserting empty rather than asserting the correct four products: reading a
    merged-cell hierarchy is a different shape and is not implemented. What must
    hold today is that the incomplete read is rejected, not stored.
    """
    from scrapers.annual_report_revenue import _rows_from_table
    rows = _rows_from_table(GUANGHUA, 2025)
    assert rows == [], rows


def test_the_products_that_the_reader_cannot_yet_see():
    """Names the loss, so the backlog item is not just a memory.

    Column 1 holds 表面塗裝 / 射出成型 / 轉印件 / 其他, and with 模具 they sum
    to 100.00. Any future merged-cell reader has to produce these five.
    """
    subrows = [r[1] for r in GUANGHUA[2:] if r[1]]
    assert subrows == ["表面塗裝", "射出成型", "轉印件", "其他(註2)"]
    pcts = [float(r[3].rstrip("%")) for r in GUANGHUA[2:-1]]
    assert round(sum(pcts), 2) == 100.00
