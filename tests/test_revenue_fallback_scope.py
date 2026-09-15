# -*- coding: utf-8 -*-
"""Three guards that exist because a fix made the parser confidently wrong.

Causes 1 and 2 (aggregate captions, source normalisation) each rescued real
companies, and each also produced a row that was worse than the gap it filled:

  1338 廣華-KY  once its own mix table was correctly rejected, the amounts-only
                fallback walked on to page 94 and stored 星禧科技, 廣佳投資,
                廣泰投資, 智景控股 and HIROTAI墨西哥 -- the 被投資公司名稱
                table -- as the company's product mix, with status ok.
  1413 宏洲     normalising the page let a prose pattern fire on 「之製造、行銷
  1452 宏益     所佔營業比重為 100%」 and store 「行銷所」: the connector 所佔
                was not in the vocabulary, so its 所 was eaten by the label.

Both were written to the database before the run was stopped. They are kept as
tests because they share a property that makes them dangerous: a wrong row and a
correct row are indistinguishable downstream. no_section is visible; 「行銷所
100%」 becomes a weight_item and then Core evidence.
"""
import re

from scrapers.annual_report_revenue import _is_activity_only, _rows_from_prose

WRAP = chr(10)

# 1413 宏洲 and 1452 宏益, 2025 annual reports, verbatim.
HONGZHOU = ("2、所營業務之主要內容及營業比重" + WRAP
            + "本公司所營業務為聚酯粒及聚酯原絲之製造、行銷所佔營業比重為 100%。")


def test_the_connector_is_not_part_of_the_product_name():
    # With 所佔 absent from the connector set the label read 「行銷所」.
    assert "行銷所" not in str(_rows_from_prose(HONGZHOU))


def test_an_activity_is_not_a_product():
    for caption in ("行銷", "製造", "製造、行銷", "製造及銷售", "產銷", "經銷"):
        assert _is_activity_only(caption), caption


def test_a_service_sold_is_revenue_and_must_survive():
    """The guard's first draft deleted real revenue lines.

    It listed 租賃, 服務, 維修, 批發, 零售, 生產 and 加工 as activities, which
    would have rejected eight rows already stored -- 1110 東泥 租賃 6.92%,
    3147 大綜 服務 3.93%, 6843 維修 5.41%, 5301 批發及零售 94.35% among them.
    Selling a service is revenue. Only what a company does *to* something else
    belongs in the vocabulary.
    """
    for caption in ("租賃", "服務", "維修", "批發及零售", "生產", "加工",
                    "行銷服務", "IC設計", "3C產品銷售收入", "聚酯纖維布",
                    "服務業務", "其他", "設計服務"):
        assert not _is_activity_only(caption), caption


def test_hongzhou_reads_nothing_rather_than_reading_the_verb():
    """The right answer here is the gap, not a rescue.

    The product is the noun phrase before 之 -- 聚酯粒及聚酯原絲 -- and reading
    that is a shape this parser does not have. Asserting empty records that the
    company stays no_section on purpose, instead of holding 「行銷 100%」.
    """
    assert _rows_from_prose(HONGZHOU) == []


def test_the_fallback_is_scoped_to_the_first_table_bearing_page():
    """The gate is stated in the source, so state what it must say.

    A keyword gate cannot separate 1338's two chapters: page 64 (the mix) and
    page 94 (被投資公司名稱) both contain 營收比重. What separates them is that
    the mix table is the first table under the section.
    """
    src = re.search(r"amounts_only is None.*?amounts_only = rows",
                    open("scrapers/annual_report_revenue.py",
                         encoding="utf-8").read(), re.S).group(0)
    assert "page.page_number == first_table_page" in src


# --------------------------------------------------------------------------
# 3709 鑫聯大投控: a customer table reads exactly like a product mix
# --------------------------------------------------------------------------

def test_an_anonymised_counterparty_is_not_a_product():
    from scrapers.annual_report_revenue import _is_context_item
    for caption in ("甲公司", "乙公司", "丙 公 司", "丁客戶", "戊廠商", "己供應商"):
        assert _is_context_item(caption), caption


def test_a_real_company_name_is_still_read():
    """The guard matches the whole label, so it cannot reach a product.

    Deliberately narrow: filings anonymise counterparties with exactly the ten
    heavenly stems, and screening every label containing 公司 would delete real
    lines. Measured before writing: this rejects one stored row, the wrong one.
    """
    from scrapers.annual_report_revenue import _is_context_item
    for caption in ("甲醇", "甲基丙烯酸酯", "乙二醇", "丙烯酸", "甲公司股份",
                    "台積電", "光電子設備"):
        assert not _is_context_item(caption), caption


def test_a_mix_made_only_of_the_residual_category_is_not_a_mix():
    from scrapers.annual_report_revenue import _only_residual
    assert _only_residual([{"item": "其他", "pct": 87.06}])
    assert _only_residual([{"item": "其 它", "pct": 100.0}])
    assert _only_residual([{"item": "Other", "pct": 99.0}])
    assert _only_residual([{"item": "其他(註)", "pct": 90.0}])


def test_a_residual_alongside_products_is_kept():
    from scrapers.annual_report_revenue import _only_residual
    assert not _only_residual([{"item": "果汁", "pct": 55.0},
                               {"item": "其他", "pct": 45.0}])
    assert not _only_residual([])
