# -*- coding: utf-8 -*-
"""The mix written as 「…其營業比重為 N%」, with the ratio phrase after the product.

1109 信大: 「主要從事水泥及熟料之產銷,其營業比重為62.71%;次為混凝土產銷,其營業
比重為12.52%」. No reader had this shape -- the 占/佔 reader needs 占, and the
「比重 N% 為 label」 reader needs the number first -- so the companies writing it
were recorded no_section while their mix sat in the captured text.

Three of the assertions below are on a total rather than on an item list,
because the failures this reader produced while it was being built were all of
one kind: it read something, the something summed into range, and what it had
lost was invisible. 1236 宏亞 writes 約 for its first product and 為 for the
next two; requiring 為 kept 18.68 and 9.69 and dropped 71.63, and 28.37 passes
the 50..150 completeness bar as comfortably as 100.00 does.

All source text is transcribed from the filings.
"""
from scrapers.annual_report_revenue import _rows_from_prose, _rows_from_ratio_is


def _mix(text):
    return [(r["item"], r["pct"]) for r in _rows_from_prose(text)]


def _total(text):
    return round(sum(p for _, p in _mix(text)), 2)


# --------------------------------------------------------------------------
# the shape
# --------------------------------------------------------------------------

XINDA = ("營業比重:本公司集團目前主要從事水泥及熟料之產銷,其營業比重為62.71%;"
         "次為混凝土產銷,其營業比重為12.52%。")
HONGYA = ("營業比重:A.『77巧克力』系列產品之製造與銷售,營業比重約71.63%。"
          "B.『禮坊囍餅』及法式烘焙類產品之製造與銷售,營業比重為18.68%。"
          "C.各式蛋糕、麵包、烘焙類產品代工,營業比重為9.69%")
LITEON = ("營業比重本公司業務主要分兩部分,一部分是偏光板的銷售比重為97%。"
          "另一部分,是針對太陽眼鏡、觸控及相關光學材料的銷售,營收比重為3%。")


def test_the_ratio_phrase_after_the_product_is_read():
    assert _mix(XINDA) == [("水泥及熟料", 62.71), ("混凝土", 12.52)]


def test_the_label_does_not_swallow_the_heading():
    """The first label used to grow leftwards until it found a connector.

    Matching label-and-connector with one regex made 1109's first label
    「營業比重:本公司集團目前主要從事水泥及熟料之產銷」, which the screens then
    correctly rejected for containing 比重 -- losing 62.71 with it. Each label
    now starts where the previous match ended.
    """
    assert all("比重" not in item for item, _ in _mix(XINDA))
    assert ("水泥及熟料", 62.71) in _mix(XINDA)


def test_an_enumerated_mix_is_read_whole():
    """1236 宏亞, asserted on the total.

    Its first product is joined with 約 and the other two with 為. A reader that
    knows only 為 returns 18.68 + 9.69 = 28.37 -- inside the completeness bar,
    and missing the largest line.
    """
    assert _total(HONGYA) == 100.00
    assert ("『77巧克力』系列產品", 71.63) in _mix(HONGYA)


def test_a_clause_boundary_can_be_a_comma():
    """3051 力特 introduces each half of its business with a comma clause."""
    assert _total(LITEON) == 100.00
    assert ("偏光板", 97.0) in _mix(LITEON)


def test_the_sum_never_exceeds_what_was_disclosed():
    for text in (XINDA, HONGYA, LITEON):
        assert _total(text) <= 100.00


# --------------------------------------------------------------------------
# what must not be read
# --------------------------------------------------------------------------

HONGZHOU = "營業比重本公司所營業務為聚酯粒及聚酯原絲之製造、行銷所佔營業比重為100%。"
HONGYI = "營業比重:本公司所營業務為聚酯加工絲之製造、行銷所佔營業比重為100%。"


def test_an_activity_is_not_a_product_line():
    """1413 宏洲 and 1452 宏益 must stay unread, by earlier ruling.

    「之製造、行銷所佔營業比重為100%」 describes what the company does to its
    products, and the product itself is the noun phrase before 之. Reading the
    verb would put 「行銷 100%」 into the revenue table, where it becomes a
    weight_item and then Core evidence.
    """
    assert _rows_from_prose(HONGZHOU) == []
    assert _rows_from_prose(HONGYI) == []


def test_a_ratio_of_something_else_is_not_a_product_mix():
    """The four passages that have produced bad rows in this parser before,
    rewritten into this grammar so the guards are exercised in it."""
    cases = {
        "rnd": "研究發展支出占營業額比率為0.33%,前一年度比率為0.29%",
        "region": "內銷比率為78%,外銷比率為22%",
        "customer": "半導體業之營業比重為95.63%,醫療器材業之營業比重為0.98%",
        "growth": "紗線衰退比率為17%,纖維衰退比率為32%",
    }
    for name, text in cases.items():
        assert _rows_from_ratio_is(text) == [], (name, _rows_from_ratio_is(text))


def test_a_market_share_is_not_a_revenue_share():
    assert _rows_from_ratio_is("本公司市場占有率為23%,次要對手市場占有率為11%") == []


# --------------------------------------------------------------------------
# precedence
# --------------------------------------------------------------------------

YEHXING = ("營業比重:本公司主要產品為不銹鋼線材、碳鋼線材等,114年度營收,"
           "不銹鋼系列占營收比重約為51%,非不銹鋼系列及其他占營收比重約為49%。")


def test_a_reader_with_its_own_connector_keeps_its_company():
    """2007 燁興 writes 占…比重約為 and is read by the 占 pattern.

    This reader runs only on an empty result, so it cannot take a company away
    from one that matched. Asserted through _rows_from_prose, which is where the
    precedence lives, rather than by calling this reader directly.
    """
    got = _mix(YEHXING)
    assert ("不銹鋼系列", 51.0) in got
    assert round(sum(p for _, p in got), 2) == 100.00
