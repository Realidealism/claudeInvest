# -*- coding: utf-8 -*-
"""P6: the mix written as a delimited run of label+percent, with no connector.

1466 聚隆 writes 「尼龍原絲16.16%、尼龍加工絲39.53%、聚酯加工絲10.27%、環保纖維
22.94%、醫材類1.25%、其他9.85%」. Nothing joins the label to the number, so none
of the connector readers (占/佔, 比重為, a colon, an enumeration marker, a
parenthesised percent) can see it. It is the largest single unread shape in the
no_section population.

The danger this file exists to hold down is that the obvious implementation --
pair every percentage with the words in front of it -- is the proximity
heuristic that has produced every bad row in this parser's history: 產業概況
growth rates (1417 嘉裕), R&D as a share of revenue (1722 台肥), domestic/export
splits (1810 和成), customer industry breakdowns (2338 光罩), anonymised
counterparties (3709 鑫聯大投控).

What separates a mix from those is structure, not vocabulary. The pairs form a
delimited run, so the text is split on its delimiters and pairs are believed
only when they appear in at least two different pieces. Every negative case
below fails on that structural test, on the label screens, or on both -- and the
tests say which, because a guard that happens to work for the wrong reason is a
guard that will stop working.

All source text is transcribed from real filings.
"""
from scrapers.annual_report_revenue import (
    _rows_from_delimited_pairs, _rows_from_prose,
)

WRAP = chr(10)


def _mix(text):
    return [(r["item"], r["pct"]) for r in _rows_from_prose(text)]


# --------------------------------------------------------------------------
# the shape itself
# --------------------------------------------------------------------------

JULONG = ("2、營業比重本公司民國114 年營業比重:尼龍原絲16.16%、尼龍加工絲39.53%、"
          "聚酯加工絲 10.27%、環保纖維 22.94%、醫材類1.25%、 其他 9.85%。")
HEJI = ("營業比重:以114年度營收分析,交連PE電力電纜65.71%、塑膠電線電纜8.68%、"
        "裸鋁線5.36%及其他20.25%。")
XINGUANGGANG = ("營業比重合併公司114年度之營業比重,其主要產品營業額佔總銷售額之比率"
                "分別為,鋼板 45.7%、特殊鋼板 7.2%、熱軋及鍍鋅鋼板 30.3%、不鏽鋼 9.4%、"
                "型鋼1.6%、加工及其他5.8%。")


def test_julong_reads_the_whole_run():
    assert _mix(JULONG) == [("尼龍原絲", 16.16), ("尼龍加工絲", 39.53),
                            ("聚酯加工絲", 10.27), ("環保纖維", 22.94),
                            ("醫材類", 1.25), ("其他", 9.85)]


def test_heji_splits_on_the_trailing_conjunction():
    # 「裸鋁線5.36%及其他20.25%」 is two pairs inside one delimiter piece.
    assert _mix(HEJI) == [("交連PE電力電纜", 65.71), ("塑膠電線電纜", 8.68),
                          ("裸鋁線", 5.36), ("其他", 20.25)]


def test_a_conjunction_inside_a_label_is_not_a_delimiter():
    # 「熱軋及鍍鋅鋼板」 and 「加工及其他」 must survive whole.
    items = [i for i, _ in _mix(XINGUANGGANG)]
    assert "熱軋及鍍鋅鋼板" in items and "加工及其他" in items


def test_the_runs_account_for_the_whole_company():
    for text in (JULONG, HEJI, XINGUANGGANG):
        assert round(sum(p for _, p in _mix(text)), 2) == 100.00


# --------------------------------------------------------------------------
# what must not be read as a mix
# --------------------------------------------------------------------------

def test_two_percentages_in_one_sentence_are_not_a_run():
    """The structural test, stated on its own.

    1417 嘉裕's 產業概況: two percentages, one clause, no delimited structure.
    Even if the labels passed every vocabulary screen, this must not read.
    """
    growth = "一一四年我國紡織品出口總值為62.23億美元,衰退8%;進口總值為37.47億美元,成長32%"
    assert _rows_from_delimited_pairs(growth) == []


def test_a_domestic_export_split_is_not_a_product_mix():
    # 1810 和成. Delimited, but the labels are places.
    assert _rows_from_delimited_pairs("其中內銷比率為78%,外銷比率為 22%") == []


def test_an_industry_breakdown_is_not_a_product_mix():
    # 2302 麗正 names industries, not products.
    text = "半導體業佔95.63%、醫療器材業佔0.98%、其他佔3.39%"
    assert all(not i.endswith("業") for i, _ in
               [(r["item"], r["pct"]) for r in _rows_from_delimited_pairs(text)])


def test_an_anonymised_counterparty_run_is_not_a_product_mix():
    # 3709 鑫聯大投控's major-customer table, flattened.
    assert _rows_from_delimited_pairs("甲公司12.94%、乙公司9.10%、其他77.96%") == []


def test_an_activity_run_is_not_a_product_mix():
    assert _rows_from_delimited_pairs("製造45%、行銷30%、銷售25%") == []


# --------------------------------------------------------------------------
# the single-percentage case is deliberately not this shape
# --------------------------------------------------------------------------

def test_one_pair_is_not_a_run():
    """Two pieces are required, so a lone percentage is never P6.

    3709's real mix is 「(1)電腦、零組件及週邊設備 98.75%」 -- true, but one
    pair. Reading it here would mean guessing where the label starts with
    nothing to check the guess against; the enumeration reader owns that shape.
    """
    assert _rows_from_delimited_pairs("聚酯粒及聚酯原絲98.75%") == []


def test_two_pairs_in_the_same_piece_are_not_enough():
    """pieces_used < 2 rejects even when two pairs are present."""
    assert _rows_from_delimited_pairs("甲產品60%乙產品40%") == []


# --------------------------------------------------------------------------
# precedence: a reader with a connector to rely on is never displaced
# --------------------------------------------------------------------------

def test_a_complete_connector_read_is_not_displaced():
    """1445 大宇 is read by the 佔 pattern and must stay that way."""
    dayu = ("2.營業比重:依114年度營業額計算,聚酯纖維布佔 67.1%,聚酯加工絲"
            "佔 24.6%,房地租賃及太陽能躉售電力8.3%。")
    got = _mix(dayu)
    assert ("聚酯纖維布", 67.1) in got and ("聚酯加工絲", 24.6) in got


def test_the_shape_p6_does_not_cover_is_covered_by_its_own_reader():
    """1109 信大 was recorded here as unread, and it is not unread any more.

    The note this replaces was correct when P6 landed: 「主要從事水泥及熟料之
    產銷,其營業比重為62.71%」 puts the ratio noun after the product and joins it
    with 為, which no reader then had. It is still not a delimited run -- cutting
    at the last heading leaves one usable pair, and one pair is not a run -- so
    what changed is not P6 but that the shape got a reader of its own.

    Kept rather than deleted, because the claim worth holding is the boundary
    between the two readers, not the coverage number of either.
    """
    xinda = ("主要從事水泥及熟料之產銷,其營業比重為62.71%;次為混凝土產銷,"
             "其營業比重為 12.52%")
    assert _rows_from_delimited_pairs(xinda) == []
    got = _mix(xinda)
    assert ("水泥及熟料", 62.71) in got and ("混凝土", 12.52) in got


# --------------------------------------------------------------------------
# boundary evidence: what a mis-segmented run looks like
# --------------------------------------------------------------------------

def test_the_connector_inside_a_label_condemns_the_whole_run():
    """2388 威盛. The introduction belongs outside the run.

    Labels came back as 「約占集團總」 and 「約占」 -- the connector cut in half
    and swallowed. That is not a product name that needs blacklisting; it is
    proof that the segmentation did not find parallel names, so the run as a
    whole is rejected rather than the offending pair dropped.
    """
    weisheng = "本集團北美地區營收約占集團總 56%、亞洲地區約占 16%"
    assert _rows_from_delimited_pairs(weisheng) == []


def test_two_columns_flattened_into_one_stream_are_not_one_run():
    """2527 宏璟. A two-column page interleaves unrelated topics.

    The real mix is 「投資興建國民住宅…佔個/體營業額100%」, split across the
    column break; beside it sits the articles-of-association clause on employee
    and director compensation, 「分別按稅前淨利之 4%」. Delimiters cannot see a
    column boundary, so the run joined them. The measurement vocabulary inside
    the labels is what gives it away.
    """
    hongjing = ("2.營業比重:投資興建國民住宅、商業大樓及智慧型工業廠房大樓出租、"
                "出售業務,佔個,體營業額100%。,分別按稅前淨利之 4%,及 2%計算")
    assert _rows_from_delimited_pairs(hongjing) == []


HONHAI = ("2.營業比重方面" + WRAP
          + "114 年集團各大產品類別營收比例:雲端與網路產品 40%、消費智"
          + WRAP + "能產品 38%、電腦終端產品 15%、元件及其他產品 7%。")


def test_a_second_introduction_does_not_cost_the_first_product():
    """2317 鴻海's heading and its run are different phrases.

    「2.營業比重方面」 introduces the section; 「營收比例:」 introduces the run.
    Cutting only at the heading left the second introduction in front of the
    first product, the label cap took a fragment out of the middle of it, the
    label screens then correctly rejected that fragment -- and the run came
    back 40 points short with its largest line gone. A read that loses the
    biggest product and still sums into range is the worst possible outcome,
    so this is asserted on the total, not just on the item list.
    """
    got = _mix(HONHAI)
    assert ("雲端與網路產品", 40.0) in got
    assert round(sum(p for _, p in got), 2) == 100.00


def test_the_five_true_runs_are_untouched_by_that_rule():
    """The same rule, checked against every run this reader is meant to read."""
    for text in (JULONG, HEJI, XINGUANGGANG, HONHAI):
        assert round(sum(p for _, p in _mix(text)), 2) == 100.00
