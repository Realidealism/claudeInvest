"""Share and amount from one identity -- step 3 of the P22 repair (P22-4).

The line reader used to take the share at the fiscal year's position and the revenue
from the first amount on the line, as two independent choices: 114's share was paired
with 113's revenue (2736), and 2025's with 2023's (6741). Now the header declares the
groups -- one per period, or one per entity -- and the i-th share and the i-th amount
both belong to group i, so share and revenue come from the same group. When the amounts
on a line do not match the groups, revenue is left NULL (D4) rather than borrowed.

Every assertion here pairs revenue with share, so a row whose share is right and whose
revenue is another period's cannot pass.

The table path reaches the same result through the same contract: once period and
entity restrict the columns (P22-1, P22-2), the amount can only be found among the
fiscal year's own columns (5310).

Not covered by a fixture yet: a line whose shares align to the groups while its amounts
do not (revenue NULL by D4). The differential run will surface real cases; fixtures are
never typed.
"""
import io
import json
import os

import pytest

from scrapers.annual_report_revenue import ScopeUnresolved, _rows_from_lines, _rows_from_table

HERE = os.path.join(os.path.dirname(__file__), "fixtures")
with io.open(os.path.join(HERE, "mix_tables.json"), encoding="utf-8") as _fh:
    TABLES = json.load(_fh)
with io.open(os.path.join(HERE, "mix_lines.json"), encoding="utf-8") as _fh:
    LINES = json.load(_fh)


def line_pairs(key):
    return [(r["item"], r["revenue"], r["pct"]) for r in _rows_from_lines(LINES[key]["text"], 2025)]


def test_two_periods_pair_the_fiscal_years_share_with_its_own_revenue():
    # 2736: 113 revenue 251,023 was stored beside 114's share 83.76
    assert line_pairs("p22_2736_two_years_marked") == [
        ("客房收入", 322647.0, 83.76), ("餐飲收入", 173966.0, 45.16), ("其他收入", 27555.0, 7.15)]


def test_three_periods_pair_the_fiscal_years_share_with_its_own_revenue():
    # 6741: 2023 revenue 1,095,443 was stored beside 2025's share 72.45
    assert line_pairs("p22_6741_three_years_unmarked") == [
        ("經常性 軟體收入", 1308874.0, 72.45), ("經常性 服務收入", 357294.0, 19.78),
        ("非經常性收入", 140417.0, 7.77)]


def test_two_entities_pair_the_consolidated_share_with_the_consolidated_revenue():
    assert line_pairs("p22_8112_consolidated_and_parent_on_a_line") == [
        ("記 憶 體 元 件", 163642484.0, 72.24), ("非記憶體元件", 62885529.0, 27.76)]


def test_a_line_that_does_not_align_is_refused_whole_not_half_read():
    # 3046: neither the share nor the revenue can be placed in the fiscal year's group
    with pytest.raises(ScopeUnresolved) as refused:
        _rows_from_lines(LINES["p22_3046_unparseable_share"]["text"], 2025)
    assert "do not align" in refused.value.reason


def test_the_fiscal_year_first_pairs_are_unchanged():
    assert line_pairs("p22_1268_quarter_beside_year") == [
        ("餐飲收入", 5900162.0, 94.75), ("商品銷貨收入", 185318.0, 2.98), ("其他收入", 141486.0, 2.27)]


def test_shares_without_amounts_carry_no_revenue():
    assert line_pairs("p22_2404_cn_numeral_years") == [
        ("系統整合", None, 99.54), ("維護服務", None, 0.04), ("勞務業務、產品銷售", None, 0.42)]


def test_the_table_path_takes_the_amount_from_the_fiscal_years_own_columns():
    # 5310: header one column off its values; the amount had been 113年同期 (1,367)
    rows = _rows_from_table(TABLES["p22_5310_header_offset_amount_period"]["table"], 2025)
    assert [(r["item"], r["revenue"], r["pct"]) for r in rows] == [
        ("系統整合及維修收入", 795.0, 97.0), ("線上遊戲收入", 24.0, 3.0)]
