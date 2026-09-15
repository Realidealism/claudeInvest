"""Entity selection -- step 2 of the P22 repair (P22-2, db/P22_REPAIR_DESIGN.md).

Under one period, several columns can differ only by entity: parent-only against
consolidated, segments against their total. A revenue mix describes the whole company
(P20), so the consolidated or total column is selected -- by its label, never by being
first. 本公司 is weak: beside a consolidated column it is the parent-only side; alone it
is the company itself. The amount follows the share's entity, and without an amount of
that identity revenue stays NULL (D4).

Not covered by a fixture yet: a filing that prints only parent-only columns (D2, refused
as "target consolidated scope unavailable"). No such table was captured from a cached
filing, and a fixture is never typed. The differential run will surface real cases.

Two tables on one page, one per entity (2013), are table-level scope and belong to P22-3.
"""
import io
import json
import os

from scrapers.annual_report_revenue import (
    _entity_select, _column_scopes, _header_columns, _rows_from_lines, _rows_from_table,
)

HERE = os.path.join(os.path.dirname(__file__), "fixtures")
with io.open(os.path.join(HERE, "mix_tables.json"), encoding="utf-8") as _fh:
    TABLES = json.load(_fh)
with io.open(os.path.join(HERE, "mix_lines.json"), encoding="utf-8") as _fh:
    LINES = json.load(_fh)


def first_row(key):
    r = _rows_from_table(TABLES[key]["table"], 2025)[0]
    return r["item"], r["revenue"], r["pct"]


# --------------------------------------------------------------------------------
# Consolidated over parent-only under the fiscal year
# --------------------------------------------------------------------------------

def test_benshi_beside_consolidated_is_the_parent_only_side():
    # 2332: 本公司 | 合併公司, both 114; the reader took 本公司 (50.73)
    assert _header_columns(TABLES["2332_scope_columns"]["table"], 2025) == (3, 4)
    assert first_row("2332_scope_columns") == ("交換機產品", 5256313.0, 39.32)


def test_consolidated_is_chosen_inside_the_fiscal_year_block():
    # 8472: 113 and 114, each split 個體 | 合併; the reader took 114 個體 (60.14)
    assert _header_columns(TABLES["p22_8472_parent_and_consolidated_per_year"]["table"], 2025) == (7, 8)
    assert first_row("p22_8472_parent_and_consolidated_per_year") == ("商品銷售收入", 333552.0, 69.77)


def test_a_header_one_column_right_of_its_values_does_not_borrow_another_entitys_column():
    # 9930: 占銷售額% one column right of the 114 consolidated values heads 113's
    # parent-only values, and must not be taken for the selected column's header
    assert _header_columns(TABLES["p22_9930_parent_and_consolidated_statements"]["table"], 2025) == (None, 2)
    assert first_row("p22_9930_parent_and_consolidated_statements") == ("高爐石粉", None, 40.3)


# --------------------------------------------------------------------------------
# No period labelled: the entity words alone single the column out
# --------------------------------------------------------------------------------

def test_global_beats_parent_individual():
    assert _header_columns(TABLES["p22_2492_parent_and_global"]["table"], 2025) == (None, 2)
    assert first_row("p22_2492_parent_and_global") == ("積層陶瓷晶片電容", None, 43.5)


def test_consolidated_beats_single():
    assert _header_columns(TABLES["p22_4722_single_and_consolidated"]["table"], 2025) == (None, 2)
    assert first_row("p22_4722_single_and_consolidated") == ("UV光固化材料", None, 49.0)


def test_scope_printed_over_the_amount_selects_the_share_beside_it():
    # 8996: 合併財報 names no measure, so its amount is not taken (D4) -- the share is
    assert _header_columns(TABLES["p22_8996_scope_in_amount_header"]["table"], 2025) == (None, 4)
    assert first_row("p22_8996_scope_in_amount_header") == ("板式熱交換器", None, 26.28)


def test_the_total_over_segments_is_the_companys_mix():
    # 4131: 生技部門 | 醫療器材部門 | 合計; the reader took one segment (99 / 1)
    assert _header_columns(TABLES["p22_4131_segments_and_total"]["table"], 2025) == (5, 6)
    assert first_row("p22_4131_segments_and_total") == ("商品銷貨", 184446.0, 76.0)


# --------------------------------------------------------------------------------
# 本公司 alone, and what entity selection must leave alone
# --------------------------------------------------------------------------------

def test_benshi_alone_is_the_company_itself():
    # 5487: 佔本公司營收比重, with no consolidated column beside it -- not a D2 refusal
    assert _header_columns(TABLES["p22_5487_company_share_and_industry_share"]["table"], 2025) == (None, 1)


def test_benshi_is_weak_in_the_selector_itself():
    s = {x.col: x for x in _column_scopes(TABLES["p22_5487_company_share_and_industry_share"]["table"])}
    chosen, why = _entity_select([s[1]])
    assert (chosen.col, why) == (1, None)


def test_tables_without_entity_words_are_unchanged():
    assert _header_columns(TABLES["p22_6462_arabic_two_years"]["table"], 2025) == (3, 4)
    assert _header_columns(TABLES["p22_2727_cn_numeral_years"]["table"], 2025) == (3, 4)
    assert _header_columns(TABLES["p22_1305_single_share_column"]["table"], 2025) == (None, 1)


def test_two_tables_one_per_entity_are_not_decided_here():
    # P22-3: each 2013 table carries no entity word of its own
    assert first_row("p22_2013_parent_table")[2] == 91.0
    assert first_row("p22_2013_consolidated_table")[2] == 93.0


# --------------------------------------------------------------------------------
# Line reader
# --------------------------------------------------------------------------------

def test_a_line_header_naming_entities_selects_the_consolidated_share():
    # 8112: 年度 114年度 / 合併 合併營收 個體 個體營收; two shares on each line
    rows = _rows_from_lines(LINES["p22_8112_consolidated_and_parent_on_a_line"]["text"], 2025)
    assert [(r["item"], r["pct"]) for r in rows] == [("記 憶 體 元 件", 72.24), ("非記憶體元件", 27.76)]
