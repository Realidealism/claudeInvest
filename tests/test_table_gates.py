# -*- coding: utf-8 -*-
"""Three gates read a mix table, in order, and a later one may not undo an earlier.

    Gate 1  is this table a revenue mix at all?   table semantics
    Gate 2  what does each column mean?           header semantics
    Gate 3  where in the data does it sit?        data evidence -- veto only

Design note: db/PARSER_TABLE_GATES.md.

**Every table here comes out of a filing.** The first version of this file typed
its tables by hand from a reading of the page; 2897's had three data rows where
the filing has nine, and the three happened to be the ones carrying the word
損益, so the fixture tripped Gate 1, the test asserted that it did, and the suite
was green while the real filing did the opposite. Two further fixtures were
fiction in the same way. Fixtures now come from tests/fixtures/mix_tables.json,
extracted by tmp/rejudge/make_fixtures.py from cached filings, each carrying the
page and table index it was taken from.

One test below asserts a result that is **wrong and known to be wrong**: Gate 1
reads column 0 where the reader reads the label column, so a supplier table can
pass it. It is named test_defect_* and says why it is recorded rather than
patched. It is not a specification.
"""
import io
import json
import os

import pytest

from scrapers.annual_report_revenue import (
    _accounting_num, _clean, _declared_total, _header_columns, _header_kind,
    _is_aggregate_row, _label_col, _rows_from_table, _table_axis, _to_num,
    _unnamed_total_row,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures", "mix_tables.json")
with io.open(FIXTURES, encoding="utf-8") as _fh:
    CASES = json.load(_fh)


def table(key):
    return CASES[key]["table"]


def rows(key):
    return [(r["item"], r["pct"]) for r in _rows_from_table(table(key), 2025)]


def test_every_fixture_records_where_it_came_from():
    """A fixture without provenance is indistinguishable from one I invented."""
    for key, case in CASES.items():
        assert case["source_file"].endswith(".pdf"), key
        assert case["page"] >= 1, key
        assert case["table"], key


# --------------------------------------------------------------------------
# Gate 1 -- the table is rejected whole, or not at all
# --------------------------------------------------------------------------

def test_a_region_table_is_rejected_whole():
    """2851 中再保 page 94. The issuer settles the axis itself: the same
    國內/國外 labels appear on page 102 under a header that reads 區域."""
    assert _table_axis(table("2851_geography")) == "geography"
    assert _header_columns(table("2851_geography"), 2025) == (1, 2)  # columns fine
    assert rows("2851_geography") == []                              # table is not


def test_rejection_frees_the_real_mix_on_the_same_page():
    """2851's insurance lines sit one table further down the same page."""
    assert _table_axis(table("2851_insurance_lines")) is None
    assert rows("2851_insurance_lines")[:4] == [
        ("任意汽車險", 21.61), ("強制汽車險", 7.73),
        ("強制機車險", 5.92), ("火險", 19.06)]


def test_a_variance_table_is_rejected():
    """3017 奇鋐 page 4 -- 營業收入 / 營業毛利 / 稅後淨利 down the label column."""
    assert _table_axis(table("3017_variance")) == "income_statement"
    assert rows("3017_variance") == []


def test_the_product_mix_behind_it_reads():
    assert rows("3017_products") == [("3C電子產品", 100.0)]


# --------------------------------------------------------------------------
# Gate 2 -- a rate of change is not a share
# --------------------------------------------------------------------------

def test_change_beats_pct_on_the_same_cell():
    """變動比率 contains 比率, so CHANGE has to win on the cell, not merely exist
    beside PCT. No numeric range can make this call: a growth rate of 94.59 sits
    inside 0..100 as comfortably as any share."""
    assert _header_kind("變動比率") == "change"
    assert _header_kind("增減比率") == "change"
    assert _header_kind("比重") == "pct"
    assert _header_kind("營收比重") == "pct"
    assert _header_kind("金額") == "amount"


# --------------------------------------------------------------------------
# Gate 3 -- data evidence is veto evidence, not mandatory evidence
# --------------------------------------------------------------------------

def test_a_table_of_percentages_only_is_read():
    """1432 大魯閣 page 80. Nothing in it exceeds 100, which used to make the
    header scan swallow every row and leave no data to confirm the header with."""
    assert rows("1432_pct_only") == [("百貨商場", 41.0), ("運動休閒", 59.0)]


def test_one_product_is_still_a_mix():
    """One value, where the column test used to demand two."""
    assert rows("3526_one_line") == [("連接器", 100.0)]
    assert rows("3588_one_line") == [("電源管理IC", 100.0)]


def test_the_wanted_year_is_picked_out_of_a_two_year_header():
    assert _header_columns(table("2897_business_lines"), 2025) == (1, 2)
    assert rows("2897_business_lines")[0] == ("企金授信業務", 66.0)


def test_a_region_table_short_of_its_own_total_is_refused():
    """6451 訊芯-KY page 80. Gate 1 does not catch this one: only 美洲 and 其他
    reach its body -- 亞洲's row carries no value in the wanted year -- and one of
    two is not a majority. The rows are refused one level further down instead,
    because they add to 52.91 against the table's own 合計 of 100.

    Recorded as the Gate 1 hole it is: the completeness check covers the symptom,
    not the cause.
    """
    assert _table_axis(table("6451_geography")) is None, "Gate 1 still misses it"
    assert _declared_total(table("6451_geography"), 4) == 100.0
    assert rows("6451_geography") == []


def test_a_row_split_across_two_physical_rows_is_rejoined():
    """6533 晶心科 page 56. pdfplumber returns 權利金收入 as a label carrying only
    113年度's figures, then a nameless row carrying 114年度's 282,337 / 19. Each
    half fails the reader alone, and the line was lost -- 65 + 16 = 81 against a
    declared 100.
    """
    tb = table("6533_ip_licensing")
    assert [c for c in tb[3]][0] == "權利金收入" and not "".join(tb[3][3:])
    assert not tb[4][0] and tb[4][3]          # the nameless half carries the values
    got = rows("6533_ip_licensing")
    assert got == [("矽智財授權收入", 65.0), ("權利金收入", 19.0),
                   ("矽智財維護服務收入", 16.0), ("其他收入", None)]
    assert sum(p for _, p in got if p is not None) == 100.0


def test_a_table_that_contradicts_its_own_total_is_refused():
    """6021 美好證 page 99. Three of the five departments lost both their label and
    their percentage in extraction, leaving 62.44 + 1.02 against a declared 100.

    Refusing it is what lets parse_pdf reach the line reader, which has all five.
    """
    tb = table("6021_departments")
    assert _declared_total(tb, 8) == 100.0
    assert rows("6021_departments") == []


def test_rounding_does_not_count_as_contradiction():
    """2897's business lines are printed as whole numbers and sum to 101 against a
    declared 100. A completeness check that cannot tolerate rounding would throw
    away correctly-read tables."""
    got = rows("2897_business_lines")
    assert sum(p for _, p in got if p is not None) == 101.0
    assert _declared_total(table("2897_business_lines"), 2) == 100.0
    assert got[0] == ("企金授信業務", 66.0)


def test_a_table_with_no_total_is_not_required_to_have_one():
    """Absence of a declared total is not evidence against the rows. 3526 凡甲 and
    3588 通嘉 print no 合計 at all."""
    for key in ("3526_one_line", "3588_one_line"):
        pct_col = _header_columns(table(key), 2025)[1]
        assert _declared_total(table(key), pct_col) is None
        assert rows(key)


def test_a_table_that_counts_its_own_total_as_a_part_is_refused():
    """2897 王道銀行 page 132 table 0 -- the income-statement table that used to win.

    Its last row, 淨收益 6,690,492, is the sum of the eight above it and no word in
    it says 合計, so the reader emitted the total as a ninth product at 100%. Four
    real rows were separately dropped by _is_context_item, which left the accepted
    rows at 125 -- inside the 50..150 window, so nothing caught it -- and with no
    declared total to check against, because the declared total was the product.

    Gate 1 is deliberately unchanged: the axis is still None, and no vocabulary
    was added. TAXONOMY_GAPS G20 forbids rejecting a table for being income types,
    because 玉山's 利息收入/手續費收入/其他淨收益 is all 玉山 discloses. What is
    wrong here is arithmetic, not subject matter.
    """
    tb = table("2897_income_types")
    assert _table_axis(tb) is None, "Gate 1 was not widened to catch this"
    assert _declared_total(tb, 2) is None, "the filing never writes 合計"
    amount_col, pct_col = _header_columns(tb, 2025)
    assert _unnamed_total_row(tb, 0, amount_col, pct_col) == 10
    assert [c for c in tb[10]][:2] == ["淨收益", "6,690,492"]
    assert rows("2897_income_types") == []


def test_an_income_type_table_no_one_else_breaks_down_is_still_read():
    """2884 玉山金 page 63. The counterpart 2897's fix must not touch.

    玉山 discloses no business line at all -- G20 records it as one of three such
    issuers -- so its income types are the only composition its filing offers. The
    rows add to their own 合計, which is named, so nothing here is inferred.
    """
    got = rows("2884_income_types")
    assert got == [("利息收入", 65.68), ("手續費收入", 22.57), ("其他淨收益", 11.75)]
    assert sum(p for _, p in got) == 100.0
    assert _declared_total(table("2884_income_types"), 2) == 100.0


def test_a_named_total_is_never_second_guessed():
    """When the filing says 合計, the arithmetic is not consulted at all.

    2897's own business-line table ends on 淨收益合計, and its rows are printed as
    whole numbers summing to 101 -- an arithmetic test with any tolerance tighter
    than that would otherwise start hunting for a total among the parts.
    """
    tb = table("2897_business_lines")
    amount_col, pct_col = _header_columns(tb, 2025)
    assert _unnamed_total_row(tb, 0, amount_col, pct_col) is None
    assert _declared_total(tb, 2) == 100.0
    assert rows("2897_business_lines")[0] == ("企金授信業務", 66.0)


def test_defect_gate_1_does_not_consume_the_label_column():
    """2069 運錩 page 78 table 1 -- a purchase concentration table Gate 1 lets past.

    Column 0 holds 1/2/3/4 and the supplier names sit in column 1. _label_col says
    so and _rows_from_table uses it, but _table_axis takes each row's first
    non-empty cell, so its body is 其他 and 進貨淨額 -- two entries, neither an
    anonymised party -- and the counterparty test never sees A公司.

    Gate 1 says it looks at labels and is in fact looking at column 0. The fix is
    a column contract decided once by the structure parser and consumed by Gate 1,
    not a second positional rule bolted on, so it is recorded rather than patched.

    The table became readable in this round: with 進貨淨額 100% still counted as
    one of its rows the sum was 200 and the 50..150 window refused it by accident.
    No reader outcome changes today -- parse_pdf never reaches page 78 for 2069 --
    which is what makes this latent rather than a regression.
    """
    tb = table("2069_suppliers")
    assert _label_col(tb) == 1, "the reader knows where the names are"
    assert [_clean(c) for c in tb[2]][:2] == ["1", "A公司"]
    assert _table_axis(tb) is None, "Gate 1 does not, and this is the defect"
    got = rows("2069_suppliers")
    assert [i for i, _ in got] == ["A公司", "B公司", "C公司", "D公司", "其他"]
    assert sum(p for _, p in got) == 100.0, "and it looks exactly like a clean mix"


def test_the_reader_does_not_reach_the_table_gate_1_missed():
    """What keeps the defect above latent: 2069's winner is decided earlier.

    Asserted so that a change in page order or candidate selection cannot make
    the defect observable without a test going red.
    """
    import io as _io
    import os as _os
    from scrapers.annual_report_revenue import parse_pdf
    cache = "tmp/rejudge/pdfcache"
    hit = [f for f in _os.listdir(cache)] if _os.path.isdir(cache) else []
    hit = [f for f in hit if f.startswith("2069_")]
    if not hit:
        pytest.skip("2069's filing is not in the local cache")
    got, _ = parse_pdf(_io.open(_os.path.join(cache, hit[0]), "rb").read(), 2025)
    assert not any(i.endswith("公司") and len(i) == 3 for i in (r["item"] for r in got))


# --------------------------------------------------------------------------
# the order itself
# --------------------------------------------------------------------------

def test_gate_1_cannot_be_overridden_by_a_clean_gate_2_and_3():
    """The invariant that makes this a pipeline rather than a score.

    2851's region table passes Gate 2 (header correct) and Gate 3 (column found,
    values valid). If the gates were weighed against each other instead of
    ordered, two clean results out of three would carry it.
    """
    tb = table("2851_geography")
    assert _header_columns(tb, 2025) == (1, 2)
    assert _table_axis(tb) is not None
    assert _rows_from_table(tb, 2025) == []


@pytest.mark.parametrize("key", ["2882_by_subsidiary", "2882_life_mix"])
def test_no_financial_axis_is_asserted_yet(key):
    """The financial boundary is open (TAXONOMY_GAPS G20).

    Whichever way it is settled, real tests will replace this one. Until then the
    suite must not imply that current behaviour on these tables is correct.
    """
    assert CASES[key]["table"]


def test_a_percentage_only_table_keeps_its_percentages():
    """3054 立萬利 page 70. One year column, and it is the ratio column:

        ['產品別', '主要用途', '114年度營業比重(%)']
        ['電子產品', 'DRAMIC、3C產品', '64.93%']
        ['生技產品', '主要係營養保健食品等生技類產品。', '35.07%']
        ['合計', '', '100.00%']

    The header-detection loop stops at a value above 100 or at two numbers in one
    row. This table offers neither -- every row carries one value, none above 100
    -- so it used to consume all four rows. Gate 3 was then handed an empty body,
    judged the named pct column contradicted by its own absent data, and the
    merged-header fallback handed that same column over as the amount. The mix
    was stored with NULL percentages although the header says 營業比重 in words.

    2301 光寶科 and 2426 鼎元 were lost the same way and are restored to exactly
    what was stored for them before. 4545 銘鈺 is **not** this defect -- its table
    is merged-cell and sparse, which is P9's family.
    """
    tb = table("3054_pct_only")
    assert len(tb) == 4, "the fixture is the whole table, header included"

    # the header must not be able to eat the data
    assert _header_columns(tb, 2025) == (None, 2), \
        "the ratio column must not end up in the amount slot"

    got = rows("3054_pct_only")
    assert got == [("電子產品", 64.93), ("生技產品", 35.07)]
    assert sum(p for _, p in got) == 100.0
    assert _declared_total(tb, 2) == 100.0, "the 合計 row is still its own total"


def test_a_two_column_ratio_table_was_never_broken():
    """The control that localises P19: give the same shape a second year column,
    so a data row carries two numbers, and the old loop stopped correctly. This
    is why the defect only ever showed on single-year percentage tables."""
    tb = [["產品別", "113年度營業比重", "114年度營業比重"],
          ["電子產品", "60.00%", "64.93%"],
          ["生技產品", "40.00%", "35.07%"]]
    assert _header_columns(tb, 2025) == (None, 2)
    assert [(r["item"], r["pct"]) for r in _rows_from_table(tb, 2025)] == \
        [("電子產品", 64.93), ("生技產品", 35.07)]


def test_an_accounting_number_reads_what_a_filing_prints():
    """Local to the completeness arithmetic. The shared _to_num stays strict --
    widening it was measured over 90 filings and made two companies worse."""
    assert _accounting_num("1,485,732") == 1485732.0
    assert _accounting_num("(1,485,732)") == -1485732.0
    assert _accounting_num("\u25b31,485") == -1485.0
    assert _accounting_num("\u25b21,485") == -1485.0
    assert _accounting_num("\u22121,485") == -1485.0
    assert _accounting_num("abc") is None
    assert _accounting_num("") is None
    # and the strict reader it defers to is unchanged
    assert _to_num("(1,485,732)") is None


def test_an_accounting_negative_does_not_change_a_rows_identity():
    """P15. 2897's own-total row is found by arithmetic, so a row it cannot add
    up used to take the whole inference down with it -- and the 淨收益 total came
    back as a product at 100%, which is the regression this detector exists for.

    The table here is the real 2897 fixture with one row printed the way a bank
    prints a loss, and its total moved to match. That is what the same filing
    would look like in a year with a negative line; the arithmetic still holds,
    so the identity of the total row must still hold.
    """
    clean = table("2897_income_types")
    amount_col, pct_col = _header_columns(clean, 2025)
    total = _unnamed_total_row(clean, 0, amount_col, pct_col)
    assert total == 10

    m = [list(r) for r in clean]
    victim = next(i for i in range(1, len(m))
                  if i != total
                  and any(ch.isdigit() for ch in _clean(m[i][amount_col]))
                  and _to_num(_clean(m[i][amount_col])) is not None
                  and not _is_aggregate_row(_clean(m[i][0])))
    v = _to_num(_clean(m[victim][amount_col]))
    was = _to_num(_clean(m[total][amount_col]))
    m[victim][amount_col] = "(%s)" % _clean(m[victim][amount_col])
    m[total][amount_col] = "{:,.0f}".format(was - 2 * v)

    assert _unnamed_total_row(m, 0, amount_col, pct_col) == total, \
        "a minus sign must not change which row is the total"
    assert not any(item == "淨收益" for item, _ in
                   [(r["item"], r["pct"]) for r in _rows_from_table(m, 2025)])


def test_a_header_cell_is_not_an_unreadable_amount():
    """Found by breaking it. Refusing to infer from an amount that cannot be read
    is right, but 2897's header runs two rows deep and its amount cell says 金額 --
    a label, not a number that failed to parse. Treating it as unreadable removed
    the detector and let the total through as a product again.
    """
    tb = table("2897_income_types")
    amount_col, _ = _header_columns(tb, 2025)
    assert _clean(tb[1][amount_col]) == "金額", "the fixture's second header row"
    assert _unnamed_total_row(tb, 0, *_header_columns(tb, 2025)) == 10


def test_the_own_total_is_not_inferred_without_percentages():
    """P16. The percentage guard is what separates 'this row is the whole' from
    'this row happens to equal the rest'. With no percentage column every pct is
    None and the guard passes for free, so 甲 -- a real product holding exactly
    half the table -- was deleted, and with no percentages left the completeness
    check could not notice either.
    """
    tb = [["項目", "金額"], ["甲", "500"], ["乙", "200"], ["丙", "200"], ["丁", "100"]]
    assert _unnamed_total_row(tb, 0, 1, None) is None
    # and with percentages present the detector still works
    tb2 = [["項目", "金額", "比重"], ["甲", "500", "100"], ["乙", "200", "40"],
           ["丙", "200", "40"], ["丁", "100", "20"]]
    assert _unnamed_total_row(tb2, 0, 1, 2) == 1


def test_a_consolidated_share_is_read_where_a_parent_only_one_is_also_printed():
    """P20, fixed by P22-2 -- 2332 友訊 page 114. One year printed twice, once per
    reporting scope:

        產品項目    本公司                         合併公司
                    114年度銷售金額  占全年度營收比重(%)  114年度銷售金額  占全年度營收比重(%)
        交換機產品  2,590,003       50.73               5,256,313       39.32

    An industry mix describes the group, so the consolidated column is the evidence.
    The reader used to take the first share column for the wanted year -- the parent
    company's alone, 50.73 -- and this test asserted that wrong behaviour by name.
    Both columns sum to 100 and carry the same items, so no arithmetic check can see
    it. Entity selection now takes the consolidated column, and the amount with it.
    """
    tb = table("2332_scope_columns")
    assert any("本公司" in _clean(c) for c in tb[0])
    assert any("合併公司" in _clean(c) for c in tb[0])
    assert _header_columns(tb, 2025) == (3, 4), "the consolidated amount and share"
    got = dict(rows("2332_scope_columns"))
    assert got["交換機產品"] == 39.32, "consolidated share, not the parent-only 50.73"
    assert got["其他產品"] == 37.87, "consolidated share, not the parent-only 3.98"
