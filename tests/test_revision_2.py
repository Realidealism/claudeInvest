"""P22 repair, revision 2 (db/P22_REPAIR_REVISION_2.md), ruled 2026-09-15.

The goal is that output matches the printed evidence, not that the rejected companies produce output
again. Expected values are read off the printed tables and lines quoted in each test. Tables come
from tests/fixtures/mix_sections.json (caption lines and headings captured with the parser's own
functions), pages from tests/fixtures/mix_lines.json. L3 wrapped rows, digits in a label and 5609's
unrecognised 集團營業額 total are outside this revision: nothing here asserts what they read.
"""
import io
import json
import os
import re

import pytest

from scrapers.annual_report_revenue import (
    ScopeUnresolved, _TableCandidate, _caption_from_lines, _column_scopes, _heading_from_lines,
    _non_revenue_heading, _non_revenue_table, _non_revenue_unit, _own_section, _prose_window,
    _rows_from_lines, _rows_from_prose, _rows_from_table, _select_columns, _table_scope,
)

HERE = os.path.join(os.path.dirname(__file__), "fixtures")
with io.open(os.path.join(HERE, "mix_sections.json"), encoding="utf-8") as _fh:
    SECTIONS = json.load(_fh)
with io.open(os.path.join(HERE, "mix_lines.json"), encoding="utf-8") as _fh:
    LINES = json.load(_fh)


def table_rows(key):
    return {re.sub(r"\s", "", r["item"]): (r["pct"], r["revenue"])
            for r in _rows_from_table(SECTIONS[key]["table"], 2025)}


def line_rows(key):
    return {re.sub(r"\s", "", r["item"]): (r["pct"], r["revenue"])
            for r in _rows_from_lines(LINES[key]["text"], 2025)}


def line_refusal(key):
    try:
        _rows_from_lines(LINES[key]["text"], 2025)
    except ScopeUnresolved as refused:
        return refused.reason
    return None


def cand(key):
    case = SECTIONS[key]
    return _TableCandidate(_rows_from_table(case["table"], 2025), case["table"],
                           _caption_from_lines(case["caption_lines"]))


def test_every_revision_2_fixture_records_where_it_came_from():
    for source in (SECTIONS, LINES):
        for key, case in source.items():
            if key.startswith("r2_"):
                assert case["source_file"].endswith(".pdf") and case["page"] >= 1, key


# --------------------------------------------------------------------------------
# R2-1 revenue preservation
# --------------------------------------------------------------------------------

@pytest.mark.parametrize("key,item,expected", [
    # 「114年度 | 金額 | 佔合併營收比重」: the amount leaves its entity unstated
    ("r2_1795_amount_without_entity", "商品銷貨收入", (98.0, 20174421.0)),
    ("r2_4741_amount_without_entity", "溶劑性墨水", (65.17, 316006.0)),
    # no period on the columns: 「114年度」 over the label column, or in the heading
    ("r2_2374_year_over_label_column", "光電產品", (100.0, 9908160.0)),
    ("r2_9928_year_over_label_column", "廣告收入", (45.2, 397762.0)),
    ("r2_3055_year_in_label_header", "半導體設備", (93.77, 430621.0)),
    ("r2_4534_year_in_heading", "電動工具", (48.95, 401946.0)),
    ("r2_1737_amount_under_share_word", "鹽產品", (48.0, 1574359.0)),
])
def test_the_amount_printed_beside_the_share_is_kept(key, item, expected):
    assert table_rows(key)[item] == expected


def test_unknown_is_not_any_period():
    """4142 prints 「113年度 營業收入 營業比重(%) | 114年度 營業收入 營業比重(%)」: 流感疫苗系列 113
    1,282,982 82.40% and 114 1,088,585 77.67%. The 114 share pairs with 114's amount only."""
    assert table_rows("r2_4142_mix")["流感疫苗系列"] == (77.67, 1088585.0)


def test_a_number_beside_a_count_unit_is_a_quantity_not_revenue():
    """8411 「馬 口 鐵 罐 2,694,259 29.33% 支 1,066,874」 under 合併營收金額 營收比重 and 數量 銷售數量."""
    rows = line_rows("r2_8411_quantity_beside_amount")
    assert rows["馬口鐵罐"] == (29.33, 2694259.0)
    assert rows["鋁罐"] == (54.6, 5015932.0)


# --------------------------------------------------------------------------------
# R2-2 owner selection
# --------------------------------------------------------------------------------

def test_a_ranking_table_is_not_a_candidate():
    """5522's page 90 is headed …合併營業收入排行…本公司114年度位列第2名; its % column is the rank.
    Its 合併 would make it consolidated and outrank page 87's real mix."""
    ranking = SECTIONS["r2_5522_ranking"]
    assert _table_scope(ranking["table"], _caption_from_lines(ranking["caption_lines"]), 2025)[1] == "consolidated"
    assert _non_revenue_heading(_heading_from_lines(ranking["caption_lines"]))
    owned = {re.sub(r"\s", "", r["item"]): r["pct"] for r in _own_section([cand("r2_5522_mix")], [], 2025)}
    assert owned["一般住宅開發、興建與出售業務"] == 78.0


def test_a_footnote_above_belongs_to_the_table_above():
    """2884: 「註1：表列數字係玉山銀行合併之資料。 B. 營業比重」 made the next table consolidated."""
    case = SECTIONS["r2_2884_footnote_caption"]
    assert "合併" in " ".join(case["caption_lines"])
    assert _table_scope(case["table"], _caption_from_lines(case["caption_lines"]), 2025)[1] != "consolidated"


@pytest.mark.parametrize("key", ["r2_3705_yen_subsidiary", "r2_6023_usd_subsidiary"])
def test_a_foreign_currency_unit_note_disqualifies_a_table(key):
    assert _non_revenue_unit(SECTIONS[key]["caption_lines"])


@pytest.mark.parametrize("key", ["r2_3705_company_mix", "r2_6023_own_mix"])
def test_the_company_own_table_is_not_disqualified(key):
    assert not _non_revenue_unit(SECTIONS[key]["caption_lines"])
    assert cand(key).rows


def test_narrative_dollars_in_a_caption_are_not_a_unit():
    """4162's caption says 「美金 200 仟元(約新台幣 6,521 仟元)」 over 「單位: 新台幣仟元」; this table
    is its only mix."""
    case = SECTIONS["r2_4162_narrative_dollars_ntd_unit"]
    assert "美金" in " ".join(case["caption_lines"])
    assert not _non_revenue_unit(case["caption_lines"])
    assert table_rows("r2_4162_narrative_dollars_ntd_unit")["銷售權利金收入"][0] == 68.88


# --------------------------------------------------------------------------------
# R2-3 column identity over the header's columns
# --------------------------------------------------------------------------------

@pytest.mark.parametrize("key", ["r2_6827_target_all_placeholders", "r2_6876_target_all_placeholders"])
def test_a_target_column_of_placeholders_gives_no_rows_and_borrows_nothing(key):
    """「113年度 | | 114年度 |」 with 114 printed 「- | -」: 113's 100% is not 114's."""
    assert _rows_from_table(SECTIONS[key]["table"], 2025) == []


def test_amounts_written_in_words_are_still_columns():
    """5609 prints 「188億0仟4佰萬元 64.1% 170億2仟8佰萬元 60.9%」 under 114年度 | 113年度: the 114 share
    column is identified as 114 (it took 113 when the text amounts were not counted)."""
    table = SECTIONS["r2_5609_text_amounts"]["table"]
    assert _select_columns(table, 2025)[1] == 2
    assert next(s for s in _column_scopes(table) if s.col == 2).periods == (114,)


# --------------------------------------------------------------------------------
# R2-4 non-revenue competition
# --------------------------------------------------------------------------------

@pytest.mark.parametrize("key", ["r2_2105_production_sales", "r2_4142_doses_market_share",
                                 "r2_2449_region_trend"])
def test_a_non_revenue_table_does_not_compete(key):
    assert _non_revenue_table(SECTIONS[key]["table"])


@pytest.mark.parametrize("key", ["r2_2105_mix", "r2_4142_mix"])
def test_the_mix_beside_it_is_not_disqualified(key):
    assert not _non_revenue_table(SECTIONS[key]["table"])


@pytest.mark.parametrize("key", ["r2_1307_region_items", "r2_1708_region_items"])
def test_item_rows_under_region_columns_still_refuse(key):
    assert not _non_revenue_table(SECTIONS[key]["table"])
    with pytest.raises(ScopeUnresolved):
        _rows_from_table(SECTIONS[key]["table"], 2025)


def test_a_change_rate_is_not_a_share():
    """2929 「童裝 2,065,430 1,668,125 -19.24%」 under 2024年度 2025年度 增(減)比(%)."""
    assert line_refusal("r2_2929_change_rate") is None
    assert line_rows("r2_2929_change_rate") == {}


# --------------------------------------------------------------------------------
# R2-5, R2-6b, R2-7, R2-8
# --------------------------------------------------------------------------------

def test_a_header_line_with_a_page_number_is_not_values():
    """3665's 「比重 比重 5」; the table under it gives 114 資訊科技與數據傳播 42.4%."""
    assert line_refusal("r2_3665_header_line_with_page_number") is None
    assert line_rows("r2_3665_header_line_with_page_number")["資訊科技與數據傳播"] == (42.4, None)


@pytest.mark.parametrize("key", ["r2_3042_dimension_table", "r2_6709_no_revenue"])
def test_a_block_without_share_evidence_neither_reads_nor_refuses(key):
    """3042's 「3.2 x 2.5mm」, 6709's 「20、40、60 微 / 克」."""
    assert line_refusal(key) is None
    assert line_rows(key) == {}


def test_a_share_label_contradicted_by_its_data_is_not_a_share():
    """1467 「產品名稱 | 營業比重 | 百分比(%)」: 營業比重 holds 604,445 …; 百分比 holds 11.74 …. The
    contradicted column is not made the amount (ruled: revenue NULL, P26)."""
    amount_col, pct_col, refusal = _select_columns(SECTIONS["r2_1467_share_label_over_amounts"]["table"], 2025)
    assert (amount_col, pct_col, refusal) == (None, 2, None)


def test_a_heading_naming_another_year_is_no_target_period():
    """1467's page heads the block 「2.一一三年主要產品佔營業比重」; the window starts at 營業比重 and
    no header line names a period."""
    assert line_refusal("r2_1467_heading_names_113") == "no target period; the heading names 113"


def test_the_prose_window_ends_at_the_next_section():
    """4108: 「2.營業比重」 gives only total revenue; 「含有 99.7%高純度」 is under 「3.公司目前之商品(服務)項目」."""
    window = _prose_window(LINES["r2_4108_prose_after_section"]["text"])
    assert "含有" not in window
    assert _rows_from_prose(window[:800]) == []

# --------------------------------------------------------------------------------
# R2-8 scope (ruled 2026-09-15 after the third run: the implementation had overreached)
# --------------------------------------------------------------------------------

def prose_rows(key):
    window = _prose_window(LINES[key]["text"])
    return {re.sub(r"\s", "", r["item"]): r["pct"] for r in _rows_from_prose(window[:800])}


@pytest.mark.parametrize("key", ["r2_3006_prose_without_section_word", "r2_8390_prose_without_section_word"])
def test_a_page_without_the_section_word_gets_no_window(key):
    """3006 writes 「民國114年度積體電路(IC)銷售收入為14,575,272…」 and 8390 「金及含金混合貴金屬佔整體營收比重為
    39%」, neither under 營業比重: cutting the whole page at its first heading dropped them."""
    text = LINES[key]["text"]
    assert "營業比重" not in text
    assert _prose_window(text) == text


def test_the_prose_of_a_page_without_the_section_word_is_still_read():
    assert prose_rows("r2_3006_prose_without_section_word")["積體電路(IC)"] == 100.0
    assert prose_rows("r2_8390_prose_without_section_word")["金及含金混合貴金屬"] == 39.0


def test_a_numbered_item_is_not_a_section_end():
    """2524: 「營業比重 (1)委託營造廠… (2)一般旅館業。 (3)本公司合併財報營業收入類別以 114 年度而言,住宅大樓出售占
    90.41%,觀光旅館事業占 7.76%、其他及租金收入占 1.83%。 2.目前之商品(服務)項目」."""
    window = _prose_window(LINES["r2_2524_prose_numbered_items"]["text"])
    assert "住宅大樓出售" in window and "2.目前之商品" not in window
    rows = prose_rows("r2_2524_prose_numbered_items")
    assert (rows["住宅大樓出售"], rows["觀光旅館事業"], rows["其他及租金收入"]) == (90.41, 7.76, 1.83)


def test_a_list_of_items_runs_on_inside_the_section():
    """1308 lists 「(1)低密度聚乙烯… (5)乙烯醋酸乙烯酯共聚合樹脂之製造… (7)機械批發業」 under 營業比重."""
    window = _prose_window(LINES["r2_1308_prose_numbered_items"]["text"])
    assert "(5)乙烯醋酸乙烯酯共聚合樹脂" in window.replace(" ", "")
