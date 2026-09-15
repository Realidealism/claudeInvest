"""Table-level scope ownership -- step 4 of the P22 repair (P22-3, db/P22_REPAIR_DESIGN.md).

parse_pdf used to return the first table that yielded rows. Now every mix table in the
filing is collected, each is scoped from its own header and its table-local caption, and
the section belongs to the one table that is uniquely the target scope as evidence:

  - where any table is labelled consolidated, only consolidated tables compete;
  - otherwise only tables whose own labels establish the fiscal year compete, and a
    parent-only, segment or region table never does;
  - competitors must be one piece of evidence (D5), whatever their order on the page;
  - a table establishing nothing (rule 2) is accepted only when nothing else competes --
    never beside a refused table;
  - only parent-only tables → the target scope is unavailable (D2).

Tables and captions come from tests/fixtures/mix_sections.json, captured with the
parser's own _table_caption. Two tests combine real tables from two filings, because no
cached filing carries that shape on its own; the tables are captured, only the pairing
is test logic, and each such test says so.
"""
import io
import json
import os

import pytest

from scrapers.annual_report_revenue import (
    ScopeUnresolved, _TableCandidate, _own_section, _rows_from_table,
)

HERE = os.path.join(os.path.dirname(__file__), "fixtures")
with io.open(os.path.join(HERE, "mix_sections.json"), encoding="utf-8") as _fh:
    SECTIONS = json.load(_fh)
with io.open(os.path.join(HERE, "mix_tables.json"), encoding="utf-8") as _fh:
    TABLES = json.load(_fh)


def cand(key, source=SECTIONS):
    case = source[key]
    return _TableCandidate(_rows_from_table(case["table"], 2025), case["table"], case.get("caption", ""))


def refusal(key):
    try:
        _rows_from_table(TABLES[key]["table"], 2025)
    except ScopeUnresolved as refused:
        return refused
    raise AssertionError("%s was expected to refuse" % key)


def shares(rows):
    return [(r["item"], r["pct"]) for r in rows]


def test_every_section_fixture_records_where_it_came_from():
    for key, case in SECTIONS.items():
        assert case["source_file"].endswith(".pdf"), key
        assert case["page"] >= 1 and case["table"], key


# --------------------------------------------------------------------------------
# Consolidated owns the section; order does not
# --------------------------------------------------------------------------------

def test_the_consolidated_table_owns_the_section_although_printed_second():
    # 2013: 個體 first (工程收入 91), 合併 second (93); the reader took the first
    owned = _own_section([cand("p22_2013_parent_table"), cand("p22_2013_consolidated_table")], [], 2025)
    assert shares(owned)[0] == ("工程收入", 93.0)


def test_page_order_does_not_change_the_owner():
    forward = _own_section([cand("p22_2013_parent_table"), cand("p22_2013_consolidated_table")], [], 2025)
    reverse = _own_section([cand("p22_2013_consolidated_table"), cand("p22_2013_parent_table")], [], 2025)
    assert forward == reverse


def test_a_parent_only_table_alone_does_not_provide_the_target_scope():
    # D2 at table level
    with pytest.raises(ScopeUnresolved) as refused:
        _own_section([cand("p22_2013_parent_table")], [], 2025)
    assert refused.value.reason == "target consolidated scope unavailable"


def test_the_same_evidence_printed_twice_is_one_owner():
    # 3432: the business report's 合併報表 and 營運概況's (合併報表) carry the same items and
    # shares; the 個體報表 beside them is excluded. Repetition is not a refusal (D5).
    owned = _own_section([cand("p22_3432_p4_consolidated"), cand("p22_3432_p4_parent"),
                          cand("p22_3432_p41_mix")], [], 2025)
    assert shares(owned) == shares(cand("p22_3432_p41_mix").rows)
    assert shares(owned)[0] == ("連接器類", 11.0)


def test_a_subsidiarys_own_mix_does_not_compete_with_the_groups():
    # 6803: A. 崑鼎公司及子公司：合併營業比重, then C. 信鼎公司's own table
    owned = _own_section([cand("p22_6803_group_mix"), cand("p22_6803_subsidiary_mix")], [], 2025)
    assert shares(owned)[0] == ("廢棄物處理收入", 28.43)


def test_two_different_consolidated_evidences_are_refused_not_ordered():
    # Pairing of real tables from two filings (2013, 3432): both captions say 合併, both
    # name 114, and they are different evidence. Neither may win by coming first.
    with pytest.raises(ScopeUnresolved) as refused:
        _own_section([cand("p22_2013_consolidated_table"), cand("p22_3432_p4_consolidated")], [], 2025)
    assert "different evidence" in refused.value.reason


# --------------------------------------------------------------------------------
# Rule 2 does not survive a refusal
# --------------------------------------------------------------------------------

def test_an_unlabelled_single_column_table_alone_is_still_accepted():
    owned = _own_section([cand("p22_1305_single_share_column", TABLES)], [], 2025)
    assert shares(owned)[0] == ("氯乙烯單體", 11.0)


def test_an_unlabelled_single_column_table_is_not_accepted_beside_a_refused_one():
    # Pairing of real tables from two filings (7907, 1305). The refused table tells the
    # reader the section has scope ambiguity, so "one column, no period" is not a default.
    with pytest.raises(ScopeUnresolved) as refused:
        _own_section([cand("p22_1305_single_share_column", TABLES)], [refusal("p22_7907_no_target_year")], 2025)
    assert "no target period" in refused.value.reason


def test_a_table_establishing_the_fiscal_year_is_accepted_beside_a_refused_one():
    # the P22-1 ruling: a later table that completes its own scope identity is not a guess
    owned = _own_section([cand("p22_2013_consolidated_table")], [refusal("p22_7907_no_target_year")], 2025)
    assert shares(owned)[0] == ("工程收入", 93.0)
