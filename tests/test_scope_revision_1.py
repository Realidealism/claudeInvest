"""P22 repair, revision 1 (db/P22_REPAIR_REVISION_1.md), ruled 2026-09-14.

The differential run stopped on CONTRACT_TOO_STRICT: the uniqueness contract was given tables
that are not a revenue composition, and two presentation details were read as scope facts.

  R1-1  a table whose own heading names a non-revenue axis, or whose header names only
        non-revenue measures, is not a candidate and not a refusal -- disqualification only;
  R1-2  at table level only 個體 and 母公司 mark part scope; 部門 / 內銷 / 外銷 in a caption
        name an axis or a market;
  R1-3  replaced by R1-3′ (a placeholder is a NULL value position, measured by the header);
        its tests are in tests/test_line_contracts.py, over real lines.

The genuine choices stay refused: 6496's two product axes, a financial holding's one mix per
subsidiary, 2601's parent-only table. Tables, captions and headings come from
tests/fixtures/mix_sections.json, captured with the parser's own _table_caption and
_table_heading.
"""
import io
import json
import os
import re

import pytest

from scrapers.annual_report_revenue import (
    ScopeUnresolved, _TableCandidate, _heading_from_lines, _non_revenue_heading,
    _non_revenue_table, _own_section, _rows_from_table,
)

HERE = os.path.join(os.path.dirname(__file__), "fixtures")
with io.open(os.path.join(HERE, "mix_sections.json"), encoding="utf-8") as _fh:
    SECTIONS = json.load(_fh)


def cand(key):
    case = SECTIONS[key]
    return _TableCandidate(_rows_from_table(case["table"], 2025), case["table"], case["caption"])


def shares(rows):
    return [(r["item"], r["pct"]) for r in rows]


# --------------------------------------------------------------------------------
# R1-1 by heading
# --------------------------------------------------------------------------------

@pytest.mark.parametrize("key", ["r1_2352_customers", "r1_7610_regions", "r1_5410_market_share"])
def test_a_table_headed_by_a_non_revenue_axis_is_disqualified(key):
    assert _non_revenue_heading(SECTIONS[key]["heading"]), SECTIONS[key]["heading"]


def test_the_customer_table_would_otherwise_have_competed_with_the_mix():
    """Why the heading matters: 2352's 主要銷貨客戶 table yields rows of its own, so under
    P22-3 it was a second piece of evidence beside 一、營業比重."""
    assert cand("r1_2352_customers").rows


def test_a_bracket_inside_the_axis_name_does_not_hide_it():
    """7610 prints 主要商品（服務）之銷售（提供）地區: 銷售地區 never appears as one word."""
    assert "銷售地區" not in SECTIONS["r1_7610_regions"]["heading"]
    assert _non_revenue_heading(SECTIONS["r1_7610_regions"]["heading"])


def test_saying_revenue_share_does_not_rescue_a_market_share_table():
    """5410's market-share table is headed 市場占有率擬以本公司營收占同業之合計營收比重表示:
    營收比重 there is the company's share of its industry, not a revenue composition."""
    heading = SECTIONS["r1_5410_market_share"]["heading"]
    assert "營收" in heading and "比重" in heading
    assert _non_revenue_heading(heading)


@pytest.mark.parametrize("key", ["r1_2352_mix", "r1_7610_mix", "r1_5410_mix",
                                 "r1_8150_department_caption", "r1_1709_department_caption",
                                 "r1_6496_by_drug", "r1_6496_by_specialty", "r1_2891_bank"])
def test_a_mix_heading_is_not_disqualified(key):
    assert not _non_revenue_heading(SECTIONS[key]["heading"]), SECTIONS[key]["heading"]


def test_a_table_without_a_heading_is_not_disqualified_by_it():
    """Disqualification only: no heading is required to say 營業比重."""
    assert SECTIONS["r1_5211_capacity"]["heading"] == ""
    assert not _non_revenue_heading("")


@pytest.mark.parametrize("key,word", [
    ("r1_7899_customers", "客戶"),
    ("r1_7921_suppliers", "供應商"),
    ("r1_8088_market_share", "佔有率"),
])
def test_a_heading_wrapped_over_two_lines_is_read_whole(key, word):
    """The axis word sits on the first line of the heading; the last line alone says only
    「比例，並說明其增減變動原因」 or 「司營收佔同業之合計營收比重表示。」."""
    lines = SECTIONS[key]["caption_lines"]
    last = re.split(r"單位\s*[:：]", lines[-1], 1)[0]
    assert word not in last
    assert _non_revenue_heading(_heading_from_lines(lines))


def test_a_date_before_the_unit_note_is_not_the_heading():
    """8930 prints 「114年12月31日 單位： 新台幣仟元」 under 「(1)主要商品銷售區域：」."""
    lines = SECTIONS["r1_8930_regions"]["caption_lines"]
    assert lines[-1].startswith("114年12月31日")
    assert _heading_from_lines(lines) == "(1)主要商品銷售區域："
    assert _non_revenue_heading(_heading_from_lines(lines))


def test_narrative_above_a_heading_is_not_read_into_it():
    """8088's market-share heading sits under more unpunctuated narrative than the one line
    it continues from; the heading takes one continuation line, not the paragraph."""
    lines = SECTIONS["r1_8088_market_share"]["caption_lines"]
    assert len(lines) >= 3
    assert _heading_from_lines(lines).count(" ") == 1


@pytest.mark.parametrize("key", ["r1_8088_mix", "r1_8930_mix"])
def test_a_mix_heading_read_from_its_lines_is_not_disqualified(key):
    assert not _non_revenue_heading(_heading_from_lines(SECTIONS[key]["caption_lines"]))


# --------------------------------------------------------------------------------
# R1-1 by header
# --------------------------------------------------------------------------------

@pytest.mark.parametrize("key", ["r1_5211_capacity", "r1_5211_sales_volume_value",
                                 "r1_5452_market_share"])
def test_a_header_of_quantities_values_or_market_share_is_not_a_mix(key):
    table = SECTIONS[key]["table"]
    assert _non_revenue_table(table)
    assert _rows_from_table(table, 2025) == []


@pytest.mark.parametrize("key", ["r1_2352_mix", "r1_7610_mix", "r1_5410_mix",
                                 "r1_8150_department_caption", "r1_1709_department_caption"])
def test_a_revenue_mix_header_is_not_disqualified(key):
    assert not _non_revenue_table(SECTIONS[key]["table"])
    assert cand(key).rows


def test_an_excluded_table_does_not_hand_its_fallback_slot_to_the_next_table():
    """5211: once the 銷售量值 table is left out, the 員工人數 | 平均年歲 table below it on the
    same page used to become the amounts-only mix (員工人數 10, 平均年歲 44.69). The exclusion
    closes the slot instead; the table itself is not in the fixture, so the gate is stated."""
    src = io.open("scrapers/annual_report_revenue.py", encoding="utf-8").read()
    gate = re.search(r"excluded = \(_non_revenue_table\(table\).*?amounts_only = \[\]", src, re.S)
    assert gate and "page.page_number == first_table_page" in gate.group(0)


# --------------------------------------------------------------------------------
# R1-2
# --------------------------------------------------------------------------------

@pytest.mark.parametrize("key,first", [
    ("r1_8150_department_caption", ("封裝", 28.53)),
    ("r1_1709_department_caption", ("烷化部門(註1)", 80.88)),
])
def test_a_department_axis_in_the_caption_is_not_a_part_scope(key, first):
    assert "部門" in SECTIONS[key]["caption"]
    assert shares(_own_section([cand(key)], [], 2025))[0] == first


def test_a_parent_only_caption_still_refuses():
    with pytest.raises(ScopeUnresolved) as refused:
        _own_section([cand("r1_2601_parent_caption")], [], 2025)
    assert refused.value.reason == "target consolidated scope unavailable"


# --------------------------------------------------------------------------------
# what revision 1 must not dissolve
# --------------------------------------------------------------------------------

def test_two_real_product_axes_still_refuse():
    with pytest.raises(ScopeUnresolved) as refused:
        _own_section([cand("r1_6496_by_drug"), cand("r1_6496_by_specialty")], [], 2025)
    assert "different evidence" in refused.value.reason


def test_a_financial_holdings_subsidiary_mixes_still_refuse():
    with pytest.raises(ScopeUnresolved) as refused:
        _own_section([cand("r1_2891_bank"), cand("r1_2891_life")], [], 2025)
    assert "different evidence" in refused.value.reason
