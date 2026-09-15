"""Scope identity -- step 0 of the P22 repair (db/P22_REPAIR_DESIGN.md).

A value column may be selected only once its scope is known. These tests pin what the
filing's own labels establish about each column -- period, entity, measure -- on tables
captured from cached filings (tests/fixtures/mix_tables.json, added by
tmp/rejudge/m5/add_p22_fixtures.py). Nothing here selects a column; the helpers are not
yet on a read path, so the reader's behaviour is unchanged by this step.
"""
import io
import json
import os

from scrapers.annual_report_revenue import _column_scopes, _periods

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures", "mix_tables.json")
with io.open(FIXTURES, encoding="utf-8") as _fh:
    CASES = json.load(_fh)


def cell(key, row, col):
    return CASES[key]["table"][row][col]


def scopes(key):
    return {s.col: s for s in _column_scopes(CASES[key]["table"])}


# --------------------------------------------------------------------------------
# Period: every form a filing writes the year in, and nothing that is not a year
# --------------------------------------------------------------------------------

def test_a_chinese_numeral_roc_year_is_a_period():
    # P22-1: the form the reader could not read, so it defaulted to the first column
    assert _periods(cell("p22_2727_cn_numeral_years", 0, 1)) == (113,)
    assert _periods(cell("p22_2727_cn_numeral_years", 0, 3)) == (114,)
    assert _periods(cell("p22_7742_cn_numeral_three_years", 0, 5)) == (114,)


def test_a_year_written_du_is_a_period():
    assert _periods(cell("p22_2465_year_written_du", 0, 2)) == (114,)


def test_an_ad_year_is_the_same_period_as_its_roc_year():
    assert _periods(cell("p22_2637_spacer_columns", 0, 3)) == (114,)
    assert _periods(cell("p22_2637_spacer_columns", 0, 9)) == (113,)


def test_a_prior_period_comparison_names_its_own_year():
    assert _periods(cell("p22_5310_header_offset_amount_period", 0, 8)) == (113,)


def test_an_amount_is_never_a_period():
    assert _periods(cell("p22_6462_arabic_two_years", 2, 3)) == ()      # 1,931,730
    assert _periods(cell("p22_7907_no_target_year", 2, 1)) == ()        # 951,718


def test_a_label_with_no_year_names_no_period():
    # unknown is not current: an empty tuple, never the fiscal year by default
    assert _periods(cell("p22_1305_single_share_column", 0, 1)) == ()


# --------------------------------------------------------------------------------
# Column scope: what each column's labels establish
# --------------------------------------------------------------------------------

def test_every_column_under_a_chinese_numeral_header_gets_its_year():
    s = scopes("p22_7742_cn_numeral_three_years")
    assert [(c, s[c].measure, s[c].periods) for c in sorted(s)] == [
        (1, "amount", (112,)), (2, "pct", (112,)), (3, "amount", (113,)),
        (4, "pct", (113,)), (5, "amount", (114,)), (6, "pct", (114,))]


def test_a_header_with_no_target_year_carries_only_other_years():
    s = scopes("p22_7907_no_target_year")
    assert {p for c in s for p in s[c].periods} == {112, 113}


def test_parent_and_consolidated_are_told_apart_under_one_year():
    # P22-2: both carry 114; position was the only thing deciding between them
    s = scopes("p22_8472_parent_and_consolidated_per_year")
    assert (s[6].measure, s[6].periods, s[6].entity) == ("pct", (114,), frozenset({"parent"}))
    assert (s[8].measure, s[8].periods, s[8].entity) == ("pct", (114,), frozenset({"consolidated"}))


def test_scope_words_under_a_shared_measure_header_reach_each_column():
    s = scopes("p22_9930_parent_and_consolidated_statements")
    assert (s[1].periods, s[1].entity) == ((114,), frozenset({"parent"}))
    assert (s[2].periods, s[2].entity) == ((114,), frozenset({"consolidated"}))


def test_a_share_takes_the_scope_printed_over_its_amount():
    # 8996 prints 個體財報 / 合併財報 over the amounts and 營業比重(%) beside each
    s = scopes("p22_8996_scope_in_amount_header")
    assert (s[2].measure, s[2].entity) == ("pct", frozenset({"parent"}))
    assert (s[4].measure, s[4].entity) == ("pct", frozenset({"consolidated"}))


def test_a_share_takes_the_year_printed_only_over_its_amount():
    s = scopes("2332_scope_columns")
    assert (s[2].measure, s[2].periods) == ("pct", (114,))
    assert (s[4].measure, s[4].periods, s[4].entity) == ("pct", (114,), frozenset({"consolidated"}))


def test_benshi_alone_is_recorded_weak_not_parent():
    # 5487's 佔本公司營收比重 is the company's own share; only selection, beside a
    # consolidated column, may read 本公司 as parent-only
    s = scopes("p22_5487_company_share_and_industry_share")
    assert s[1].entity == frozenset({"parent_weak"})


def test_a_share_of_something_other_than_revenue_is_marked():
    s = scopes("p22_5487_company_share_and_industry_share")
    assert (s[1].share_of, s[2].share_of) == ("revenue", "other")


def test_two_shares_side_by_side_do_not_lend_each_other_scope():
    s = scopes("p22_5487_company_share_and_industry_share")
    assert s[2].entity == frozenset()


def test_segments_and_their_total_are_told_apart():
    s = scopes("p22_4131_segments_and_total")
    assert s[2].entity == frozenset({"segment"})
    assert s[4].entity == frozenset({"segment"})
    assert s[6].entity == frozenset({"total"})


def test_unnamed_business_scopes_stay_distinct():
    # 2739 heads one column pair per hotel with no scope word at all
    s = scopes("p22_2739_per_hotel")
    target = [c for c in s if 114 in s[c].periods]
    assert len({s[c].residual for c in target}) == len(target) == 4


def test_a_header_printed_off_its_values_is_aligned_to_them():
    # 5310: headers at 2/5/8/11, values at 1/4/7/10
    s = scopes("p22_5310_header_offset_amount_period")
    assert sorted(s) == [1, 4, 7, 10]
    assert (s[1].measure, s[1].periods) == ("amount", (114,))
    assert (s[4].measure, s[4].periods) == ("pct", (114,))
    assert (s[7].measure, s[7].periods) == ("amount", (113,))


def test_spacer_columns_do_not_shift_a_label():
    s = scopes("p22_2637_spacer_columns")
    assert [(c, s[c].measure, s[c].periods) for c in sorted(s)] == [
        (3, "amount", (114,)), (6, "pct", (114,)), (9, "amount", (113,)), (12, "pct", (113,))]


def test_an_askew_measure_word_does_not_relabel_an_amount():
    # 7883 prints 比重(%) one row too high, where it spans onto the 114 amount column
    s = scopes("p22_7883_three_years_deep_header")
    assert (s[5].measure, s[5].periods) == ("amount", (114,))
    assert (s[6].measure, s[6].periods) == ("pct", (114,))


def test_an_amount_word_gate_2_lacks_still_names_the_measure():
    s = scopes("p22_4995_cn_target_first")
    assert (s[1].measure, s[1].periods) == ("amount", (114,))


def test_a_single_unlabelled_share_column_is_known_to_be_unlabelled():
    s = scopes("p22_1305_single_share_column")
    assert list(s) == [1]
    assert (s[1].measure, s[1].periods, s[1].entity) == ("pct", (), frozenset())


def test_two_tables_on_one_page_are_identical_at_column_level():
    # P22-3: nothing inside either 2013 table tells them apart; that has to come from
    # table-level scope, which is why column scope alone cannot fix it
    a = [(s.measure, s.periods, s.entity) for s in _column_scopes(CASES["p22_2013_parent_table"]["table"])]
    b = [(s.measure, s.periods, s.entity) for s in _column_scopes(CASES["p22_2013_consolidated_table"]["table"])]
    assert a == b == [("amount", (114,), frozenset()), ("pct", (114,), frozenset())]
