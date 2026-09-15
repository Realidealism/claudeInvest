"""Period selection -- step 1 of the P22 repair (P22-1, db/P22_REPAIR_DESIGN.md).

Which column or line value belongs to the fiscal year is decided by period identity --
every form a filing writes the year in -- and never by position. Where periods are named
and none is the fiscal year, or where several candidates remain with no period to tell
them apart, the reader refuses (ScopeUnresolved) instead of taking the first.

Entity selection (P22-2) is tested in tests/test_scope_entity_selection.py, and the
pairing of share and amount from one identity (P22-4) in tests/test_scope_value_identity.py.
The terminal rule in parse_pdf -- a refusal is not handed to a weaker reader -- needs a
real PDF and is verified by the whole-population differential run, not here.

Tables come from tests/fixtures/mix_tables.json and page text from
tests/fixtures/mix_lines.json, both captured from cached filings.
"""
import inspect
import io
import json
import os

import pytest

from scrapers.annual_report_revenue import (
    ScopeUnresolved, _header_columns, _rows_from_lines, _rows_from_table, save,
)

HERE = os.path.join(os.path.dirname(__file__), "fixtures")
with io.open(os.path.join(HERE, "mix_tables.json"), encoding="utf-8") as _fh:
    TABLES = json.load(_fh)
with io.open(os.path.join(HERE, "mix_lines.json"), encoding="utf-8") as _fh:
    LINES = json.load(_fh)


def table_rows(key):
    return [(r["item"], r["revenue"], r["pct"]) for r in _rows_from_table(TABLES[key]["table"], 2025)]


def line_shares(key):
    return [r["pct"] for r in _rows_from_lines(LINES[key]["text"], 2025)]


def test_every_line_fixture_records_where_it_came_from():
    for key, case in LINES.items():
        assert case["source_file"].endswith(".pdf"), key
        assert case["page"] >= 1, key
        assert case["text"], key


# --------------------------------------------------------------------------------
# Table reader
# --------------------------------------------------------------------------------

def test_a_chinese_numeral_header_reads_the_fiscal_year_not_the_first_column():
    # P22-1: the reader took 民國一一三年度 (94.44 / 5.56)
    assert _header_columns(TABLES["p22_2727_cn_numeral_years"]["table"], 2025) == (3, 4)
    assert [p for _, _, p in table_rows("p22_2727_cn_numeral_years")] == [94.68, 5.32]


def test_three_chinese_numeral_years_read_the_fiscal_year():
    # P22-1: the reader took 一一二年度 (71.50 ...)
    assert _header_columns(TABLES["p22_7742_cn_numeral_three_years"]["table"], 2025) == (5, 6)
    assert table_rows("p22_7742_cn_numeral_three_years")[0] == ("新能源材料", 926166.0, 79.12)


def test_a_header_without_the_fiscal_year_is_refused():
    # 7907 prints 112 | 113 only; the reader took 112
    with pytest.raises(ScopeUnresolved) as refused:
        _rows_from_table(TABLES["p22_7907_no_target_year"]["table"], 2025)
    assert "no target period" in refused.value.reason
    assert "112" in refused.value.reason and "113" in refused.value.reason


def test_target_first_tables_are_unchanged():
    assert table_rows("p22_4995_cn_target_first")[0] == ("高亮度液晶面板模組", 545456.0, 42.29)
    assert table_rows("p22_1470_cn_three_years_target_first")[0] == ("染整代工", 131384.0, 16.34)


def test_a_year_written_du_selects_its_column():
    assert _header_columns(TABLES["p22_2465_year_written_du"]["table"], 2025) == (None, 2)
    assert table_rows("p22_2465_year_written_du")[0] == ("電腦產品", None, 99.66)


def test_an_amount_is_taken_from_the_fiscal_year_not_its_comparison():
    # 5310's amount was 113年同期 (1,367); the fiscal year's is 795
    assert _header_columns(TABLES["p22_5310_header_offset_amount_period"]["table"], 2025) == (1, 4)
    assert table_rows("p22_5310_header_offset_amount_period")[0] == ("系統整合及維修收入", 795.0, 97.0)


def test_equal_candidates_under_the_fiscal_year_are_refused_not_taken_in_order():
    # 2739: four hotels, every one summing to 100 under 114
    with pytest.raises(ScopeUnresolved) as refused:
        _rows_from_table(TABLES["p22_2739_per_hotel"]["table"], 2025)
    assert "under the target period" in refused.value.reason


def test_a_single_unlabelled_share_column_is_still_read():
    assert _header_columns(TABLES["p22_1305_single_share_column"]["table"], 2025) == (None, 1)


def test_a_share_of_industry_output_is_not_a_candidate():
    # 5487: without that exclusion the company share would stand beside it and refuse
    assert _header_columns(TABLES["p22_5487_company_share_and_industry_share"]["table"], 2025) == (None, 1)


def test_arabic_and_spacer_tables_are_unchanged():
    assert _header_columns(TABLES["p22_6462_arabic_two_years"]["table"], 2025) == (3, 4)
    assert _header_columns(TABLES["p22_2637_spacer_columns"]["table"], 2025) == (3, 6)
    assert _header_columns(TABLES["p22_7883_three_years_deep_header"]["table"], 2025) == (5, 6)


# --------------------------------------------------------------------------------
# Line reader
# --------------------------------------------------------------------------------

def test_a_chinese_numeral_line_header_reads_the_fiscal_year():
    # P22-1 on the line reader: 2404 was read as 99.46 / 0.07 / 0.47, the 113 column
    assert line_shares("p22_2404_cn_numeral_years") == [99.54, 0.04, 0.42]


def test_marked_shares_align_to_the_periods_named():
    assert line_shares("p22_2736_two_years_marked") == [83.76, 45.16, 7.15]


def test_unmarked_shares_align_to_three_periods():
    assert line_shares("p22_6741_three_years_unmarked") == [72.45, 19.78, 7.77]


def test_a_share_that_does_not_parse_does_not_slide_onto_another_period():
    # 3046 prints 「99,65」 for 114; 113's 99.83 was being taken as 114
    with pytest.raises(ScopeUnresolved) as refused:
        _rows_from_lines(LINES["p22_3046_unparseable_share"]["text"], 2025)
    assert "do not align" in refused.value.reason


def test_line_tables_right_today_are_unchanged():
    assert line_shares("p22_1268_quarter_beside_year") == [94.75, 2.98, 2.27]
    assert line_shares("p22_8923_cn_numeral_target_first") == [75.85, 24.15]


def test_two_shares_and_no_period_on_a_line_are_refused():
    with pytest.raises(ScopeUnresolved) as refused:
        _rows_from_lines(LINES["p22_4116_two_shares_no_period"]["text"], 2025)
    assert "no period labelled" in refused.value.reason


def test_two_items_on_one_line_are_not_two_periods():
    # P23, out of scope: must not become a refusal
    assert [(r["item"], r["pct"]) for r in _rows_from_lines(LINES["p22_1762_several_items_one_line"]["text"], 2025)] \
        == [("生物科技產品", 71.0)]


# --------------------------------------------------------------------------------
# The refusal is its own outcome
# --------------------------------------------------------------------------------

def test_a_scope_refusal_is_never_retracted_on():
    # save() retracts stored rows only on no_section and parse_conflict
    src = inspect.getsource(save)
    assert 'retract = status in ("no_section", "parse_conflict")' in src
    assert "scope_unresolved" not in src
