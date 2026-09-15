# -*- coding: utf-8 -*-
"""The parser must ask what a passage is before it asks what is in it.

Every case here is a row this parser actually stored in tw.product_revenue as a
product line. They are kept as tests rather than as a note because the failure
mode they share is invisible downstream: a market-statistics row parses cleanly,
sums plausibly, and becomes a weight_item like any other.

6141 柏承 is the reason this file exists. 「全球PCB銷售產值」 -- the global PCB
industry's sales value, lifted out of the 產業概況 chapter -- was stored as its
only revenue line, became the weight_item of its B2-01 exposure, and the Core
evidence sentence quoted it back: "年報營業比重表：全球PCB銷售產值 100.00%".
A parser artefact was supporting a Core ruling, and no check between the two
layers could see it, because everything downstream takes the revenue table as
given. The chain is: parse -> item -> weight_item -> evidence -> Core.
"""
from scrapers.annual_report_revenue import (
    _is_context_item, _is_industry_name, _rows_from_table, _rows_from_prose,
)


# --------------------------------------------------------------------------
# What the market is doing is not what the company sold
# --------------------------------------------------------------------------

def test_the_case_that_reached_a_core_weight():
    """6141 柏承: an industry statistic that became a 100% Core weight."""
    assert _is_context_item("全球PCB銷售產值")


def test_market_share_rows():
    """8401 白紗科 and 2206 三陽工業 stored their market-position tables."""
    for item in ("市場佔有率(%)", "市場佔有增(減)率(%)", "國內市場佔有率",
                 "整體印刷業銷售總值(仟元)", "白紗公司營收淨額(仟元)",
                 "成長率", "銷售量(出廠)"):
        assert _is_context_item(item), item


def test_income_statement_captions():
    """2883 凱基金 stored an equity-method share of NT$33bn as a product."""
    assert _is_context_item("採用權益法認列子公司、關聯企業及合資損益之份額")


def test_section_heading_glued_to_the_first_product():
    """1905 華紙: 「ㄧ、業務內容 紙張」 -- the chapter title came along."""
    assert _is_context_item("ㄧ、業務內容 紙張")


# --------------------------------------------------------------------------
# An industry name is not a product line
# --------------------------------------------------------------------------

def test_industry_name_is_not_a_product():
    """1718 中纖, a fibre maker, stored 「銀行業 100%」 from a subsidiary's text.

    Written as a shape, not a word list: a 銀行 blacklist would have let the
    next 鋼鐵業 through.
    """
    assert _is_industry_name("銀行業")
    assert _is_industry_name("鋼鐵業")
    assert _is_industry_name("印刷業")


def test_a_segment_named_事業_is_still_a_real_line():
    """1402 遠東新's own segments end in 事業 and must survive the shape test."""
    for item in ("生產事業", "生 產 事 業", "不動產開發事業", "投資及其他事業"):
        assert not _is_industry_name(item), item


# --------------------------------------------------------------------------
# The gate must not eat real product lines
# --------------------------------------------------------------------------

def test_real_product_lines_pass():
    for item in ("紙張", "紙漿", "螺絲", "盤元", "印刷電路板", "工業用紙",
                 "主餐類", "資訊服務類", "眼 鏡",
                 "食品飲料廠整廠生產線工程及套裝系統",
                 "晶圓,半成品,記憶體模組等工業包的B2B通路",
                 "高速傳輸光模組(含矽光子產品)", "光學面板"):
        assert not _is_context_item(item), item


# --------------------------------------------------------------------------
# Table level: a market table is not a thin product table
# --------------------------------------------------------------------------

def test_a_market_table_is_rejected_whole():
    """8401 白紗科's table. Dropping context rows one at a time would have left
    two clean-looking rows and a mix that sums to nothing."""
    table = [
        ["項目", "113年度", "114年度"],
        ["整體印刷業銷售總值(仟元)", "80,000,000", "83,350,000"],
        ["白紗公司營收淨額(仟元)", "1,500,000", "1,543,839"],
        ["市場佔有率(%)", "1.9", "1.85"],
        ["市場佔有增(減)率(%)", "0.1", "-0.05"],
    ]
    assert _rows_from_table(table, 2025) == []


def test_a_real_product_table_still_reads():
    table = [
        ["主要產品", "114年度金額", "114年度比重"],
        ["磨床", "461,582", "45.78"],
        ["CNC磨床", "212,624", "21.09"],
        ["其他(註)", "142,998", "14.18"],
    ]
    rows = _rows_from_table(table, 2025)
    assert [r["item"] for r in rows] == ["磨床", "CNC磨床", "其他(註)"]
    assert rows[0]["pct"] == 45.78


# --------------------------------------------------------------------------
# Prose level
# --------------------------------------------------------------------------

def test_prose_reader_refuses_an_industry_ratio():
    """The 中纖 shape: a ratio belonging to a subsidiary in another industry."""
    assert _rows_from_prose("2、台中銀行 目前所營事業比重:銀行業100%") == []


def test_prose_reader_still_reads_a_real_sentence():
    rows = _rows_from_prose("線性滑軌佔營收62%,滾珠螺桿佔營收20%")
    assert {r["item"] for r in rows} == {"線性滑軌", "滾珠螺桿"}


# --------------------------------------------------------------------------
# 1905 華紙: two failure modes in one table, and they need different fixes
#
# Its text layer reads
#     ㄧ、業務內容 紙張   12,855,367  68.50
#     紙漿                2,111,420  11.25
#     率達到                          7.37
#     紙膠帶                700,644    3.73
#     其他                  323,105    1.72
#     林木                   96,919    0.52
#
# The first row is a real product wearing the chapter title above it; the third
# is a sentence fragment from the surrounding commentary. Treating both as "bad
# rows to drop" cost the company its largest line, took the rest under the 50%
# completeness bar, and failed the whole filing -- so the two are separated here.
# --------------------------------------------------------------------------
from scrapers.annual_report_revenue import _strip_section_prefix, _is_prose_noise


def test_failure_mode_1_section_heading_glued_to_a_product():
    assert _strip_section_prefix("ㄧ、業務內容 紙張") == "紙張"
    assert _strip_section_prefix("一、業務內容 紙漿") == "紙漿"
    assert _strip_section_prefix("業務範圍：印刷電路板") == "印刷電路板"
    assert _strip_section_prefix("2.主要產品 磨床") == "磨床"


def test_stripping_is_conditional_not_a_rename():
    """Nothing survives the heading -> leave it alone and let the gate judge."""
    assert _strip_section_prefix("業務內容") == "業務內容"
    assert _is_context_item("業務內容")
    # what follows the heading is context of its own: also left alone
    assert _strip_section_prefix("產業概況 市場佔有率") == "產業概況 市場佔有率"


def test_a_product_name_is_never_touched():
    for item in ("紙張", "紙漿", "林木", "高速傳輸光模組(含矽光子產品)"):
        assert _strip_section_prefix(item) == item


def test_failure_mode_2_statistical_sentence_fragment():
    """A different defect from the same table: 「率達到」 is commentary, not a line."""
    assert _is_prose_noise("率達到")
    assert _is_prose_noise("增長率為")
    assert _is_prose_noise("售企業零售額年增率微幅轉正至")
    # and it must not be rescued by the prefix stripper
    assert _strip_section_prefix("率達到") == "率達到"


# --------------------------------------------------------------------------
# Prose shapes the reader could not see until 2026-09-09
#
# Each of these companies had status no_section and a weight_status of
# Not_Extracted, meaning "the filing says it and we did not get it out". They
# are kept together because they are four different sentence shapes, and the
# fix for each is a different pattern rather than a wider version of one.
# --------------------------------------------------------------------------

def test_shape_ratio_after_the_amount():
    """3006 晶豪科: the subject is separated from its ratio by the amount.

    Every 占-anchored pattern sees only 「其」 as the subject here.
    """
    rows = _rows_from_prose("積體電路(IC)銷售收入為 14,575,272 仟元,其占營業比率為 100%")
    assert rows == [{"item": "積體電路(IC)", "revenue": 14575272.0, "pct": 100.0}]


def test_shape_inverted_ratio_first():
    """3033 威健 and 6270 倍微 put the percentage before the product."""
    assert _rows_from_prose("營業比重約100%是代理電子零組件暨周邊設備之銷售") == [
        {"item": "代理電子零組件暨周邊設備", "revenue": None, "pct": 100.0}]
    rows = _rows_from_prose(
        "營業比重約當95%是電子產品的銷售代理,其他5%是軟體服務產生的貢獻")
    assert [(r["item"], r["pct"]) for r in rows] == [
        ("電子產品的銷售代理", 95.0), ("軟體服務", 5.0)]


def test_the_patterns_work_across_the_pdf_line_wrap():
    """The text layer wraps mid-sentence, and the synthetic one-liners hid it.

    These three strings are the filings' real text, wrap included. The first
    round of patterns passed on hand-written versions of the same sentences and
    then failed on all three of these, which is why the break is kept where the
    PDF puts it. WRAP is built from chr(10) rather than written as an escape:
    an escape sequence passing through a generator is how a real NUL byte once
    ended up in a source file here.
    """
    WRAP = chr(10)
    # 3006 晶豪科 -- the break falls between the amount and 仟元
    rows = _rows_from_prose(
        "民國114年度積體電路(IC)銷售收入為14,575,272" + WRAP + "仟元,其占營業比率為100%。")
    assert rows == [{"item": "積體電路(IC)", "revenue": 14575272.0, "pct": 100.0}]

    # 3041 揚智 -- the break falls between 約 and 85%, and the product carries an
    # English gloss with a comma inside its brackets
    rows = _rows_from_prose(
        "本公司主要產品為數位機頂盒(Set-top Box, STB)晶片,占整體營收約" + WRAP +
        "85%,網路相關設備約占15%。")
    assert [(r["item"], r["pct"]) for r in rows] == [
        ("數位機頂盒晶片", 85.0), ("網路相關設備", 15.0)]

    # 6270 倍微 -- the break falls inside the product name itself
    rows = _rows_from_prose(
        "營業比重約當95%是電子產品的銷售代理,其他5%是軟體服務產" + WRAP + "生的貢獻。")
    assert [(r["item"], r["pct"]) for r in rows] == [
        ("電子產品的銷售代理", 95.0), ("軟體服務", 5.0)]


def test_a_year_label_is_not_part_of_the_product_name():
    """「民國114年度積體電路(IC)」 -- 年度 is in PROSE_NOISE, so leaving the label on
    would discard a correct read."""
    rows = _rows_from_prose("民國114年度積體電路(IC)銷售收入為14,575,272仟元,其占營業比率為100%")
    assert rows[0]["item"] == "積體電路(IC)"


def test_a_gloss_without_a_comma_is_kept():
    """Only comma- or space-bearing parentheticals are dropped; (IC) stays."""
    rows = _rows_from_prose("積體電路(IC)佔營收62%")
    assert rows[0]["item"] == "積體電路(IC)"


def test_shape_comma_between_product_and_占():
    """3041 揚智: 「…晶片,占整體營收約85%」.

    Two things broke at once — the comma before 占 and the 整體 before 營收 — so
    the 85% half never matched and the surviving 15% fell under the completeness
    bar. The company parsed to nothing at all.
    """
    rows = _rows_from_prose(
        "本公司主要產品為數位機頂盒(STB)晶片,占整體營收約85%,網路相關設備約占15%")
    assert [(r["item"], r["pct"]) for r in rows] == [
        ("數位機頂盒(STB)晶片", 85.0), ("網路相關設備", 15.0)]


def test_the_sentence_opening_is_not_part_of_the_product_name():
    """The capture reaches back to the start of the clause, so 「本公司主要產品為」
    arrives attached to the product — and _is_prose_noise then throws the row
    away for containing 本公司."""
    rows = _rows_from_prose("本公司主要產品為數位機頂盒(STB)晶片,占整體營收約85%")
    assert rows[0]["item"] == "數位機頂盒(STB)晶片"


def test_the_old_shapes_still_read():
    """2049 上銀's sentence and 1906 寶隆's colon list are the regression guard."""
    rows = _rows_from_prose("2025年營業額:線性滑軌佔營收62%,滾珠螺桿佔營收20%,機器人佔營收10%,其他8%")
    assert [(r["item"], r["pct"]) for r in rows] == [
        ("線性滑軌", 62.0), ("滾珠螺桿", 20.0), ("機器人", 10.0)]
    assert _rows_from_prose("營業比重：工業用紙：100%") == [
        {"item": "工業用紙", "revenue": None, "pct": 100.0}]


def test_the_tail_of_an_amount_is_not_a_product():
    """2701 萬企 stored 「357仟元 87%」 and 「646仟元 13%」 as its product mix.

    The item character class allows digits -- it has to, for names like 5G計算平台
    -- so a capture can start inside an amount and end on its unit. _is_label
    passes it, because 仟元 is two perfectly good non-digit characters.
    """
    assert _rows_from_prose("娛樂設施收入12,345,357仟元,占營業比重87%") == []
    # and the digits a real name needs are untouched
    assert _rows_from_prose("5G計算平台佔營收30%")[0]["item"] == "5G計算平台"
