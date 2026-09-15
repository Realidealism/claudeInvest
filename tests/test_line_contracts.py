"""Line-reader contracts R1-3′, L1 and L2, ruled 2026-09-14 (db/P22_LINE_CONTRACTS_DRAFT.md).

  R1-3′ a standalone dash among a line's values is a value position carrying NULL; its measure
        comes from the header, never from its form. R1-3 as first ruled took a value up to 100
        for a share and made 2641's amount 30 a 30% row.
  L1    the line evidence block ends at the table's own total (a 100 share), or before the first
        section heading carrying no values; lines past it are neither rows nor refusals.
  L2    where a header names both an amount and a share, and fits a value line, every value
        position takes its measure from the header -- not ≤100 = share, >100 = amount.

Expected values are read off the printed lines quoted in each test, not taken from the reader.
Page texts come from tests/fixtures/mix_lines.json (keys lc_*), captured with the reader's own
_page_text. Wrapped rows (L3) and digits inside a label are backlog: no test here asserts what
the reader makes of them.
"""
import io
import json
import os
import re

import pytest

from scrapers.annual_report_revenue import ScopeUnresolved, _rows_from_lines

HERE = os.path.join(os.path.dirname(__file__), "fixtures")
with io.open(os.path.join(HERE, "mix_lines.json"), encoding="utf-8") as _fh:
    LINES = json.load(_fh)


def read(key):
    return {re.sub(r"\s", "", r["item"]): (r["pct"], r["revenue"])
            for r in _rows_from_lines(LINES[key]["text"], 2025)}


def refusal(key):
    try:
        _rows_from_lines(LINES[key]["text"], 2025)
    except ScopeUnresolved as refused:
        return refused.reason
    return None


def test_every_line_contract_fixture_records_where_it_came_from():
    for key, case in LINES.items():
        if key.startswith("lc_"):
            assert case["source_file"].endswith(".pdf") and case["page"] >= 1, key


# --------------------------------------------------------------------------------
# R1-3′: a placeholder is a NULL position, measured by the header
# --------------------------------------------------------------------------------

def test_a_dash_beside_a_small_amount_is_no_share_and_no_row():
    """2641 prints 「客戶合約收入 31,604 1.91 30 -」 under 營業額 營業比重(%) × 113 | 114:
    in 114 the amount is 30 and the share is 「-」. There is no 30% row."""
    rows = read("lc_2641_dash_is_a_share_beside_a_small_amount")
    assert "客戶合約收入" not in rows
    assert rows["營業租賃收入"] == (89.68, 2123427.0)
    assert rows["房地銷售收入"] == (6.22, 147246.0)


def test_a_dash_beside_an_amount_of_two_is_no_share():
    """6648 「其他收入 - - 2 -」: the 114 amount is 2, its share 「-」 -- no 2% row."""
    rows = read("lc_6648_small_amount_and_dash")
    assert "其他收入" not in rows
    assert rows["智慧電表(MIU)"] == (23.85, 77236.0)


def test_placeholders_under_the_other_periods_leave_the_target_read():
    """1806 「其他 2,489 0.08 - - - -」 under 114 | 113 | 112."""
    rows = read("lc_1806_placeholder_three_years")
    assert rows["其他"] == (0.08, 2489.0)
    assert rows["磁磚"] == (91.2, 2970577.0)


def test_a_placeholder_under_the_target_skips_the_row_and_borrows_nothing():
    """5301 「食材批發 - - 110,204 30.01」 under 114 | 113: no 114 share, so no row --
    113's 30.01 is not taken."""
    rows = read("lc_5301_placeholder_other_year")
    assert "食材批發" not in rows
    assert rows["批發及零售"] == (94.35, 223114.0)


def test_an_em_dash_is_a_placeholder_too():
    """6467 「勞務收入 — — 8,328 99.02」 under 113 | 114."""
    rows = read("lc_6467_em_dash_placeholders")
    assert rows["勞務收入"] == (99.02, 8328.0)
    assert rows["權利金收入"] == (0.98, 82.0)


# --------------------------------------------------------------------------------
# L2: measure identity from the header
# --------------------------------------------------------------------------------

def test_a_small_amount_beside_a_share_is_an_amount():
    """6856 「其 他 10 0.00% - 0.00%」: 10 is 113's amount, not a second share, so the
    section is read, not refused."""
    assert refusal("lc_6856_small_amount_beside_a_share") is None
    rows = read("lc_6856_small_amount_beside_a_share")
    assert rows["廣告業務"] == (26.11, 199388.0)
    assert rows["節目製作"] == (13.07, 99785.0)


def test_an_amount_of_59_with_no_114_value_is_not_a_refusal():
    """7834 「其他營業收入 59 0.24 - -」 under 營收淨額 營業比重 × 113 | 114."""
    assert refusal("lc_7834_small_amount_other_year") is None
    rows = read("lc_7834_small_amount_other_year")
    assert "其他營業收入" not in rows
    assert rows["軟體授權認證收入"] == (73.67, 37604.0)


def test_zeros_are_values_in_their_positions():
    """3689 「電源及LED照明 0 0 143,229 2.31」: 113 printed zero amount and zero share."""
    assert read("lc_3689_zero_zero")["電源及LED照明"] == (2.31, 143229.0)


def test_a_measure_block_over_a_period_row_is_read_by_position():
    """6144 heads 營業收入淨額 百分比 over 113年度 114年度 113年度 114年度:
    「影 音 產 品 92,407 88,408 49.63% 51.19%」 -- 114 is the second amount and second share."""
    rows = read("lc_6144_measure_first_header")
    assert rows["影音產品"] == (51.19, 88408.0)
    assert rows["文創餐飲"] == (48.81, 84314.0)


def test_a_measure_block_with_the_target_first():
    """9950 heads 營業淨額 比 重(%) over 114 年度 113 年度 114 年度 113 年度."""
    rows = read("lc_9950_measure_first_header")
    assert rows["行李箱"] == (94.17, 2010547.0)
    assert rows["其他"] == (5.83, 124376.0)


def test_a_header_fitting_no_line_is_no_identity():
    """1582's measure header wraps over four lines (營業比 / 113年度 114年度 營業比重 /
    主要產品 重 / 營業淨額 營業淨額 (%)); it describes none of the four-value lines, so they
    are read as before instead of refused against it."""
    assert refusal("lc_1582_header_wrapped_over_lines") is None
    assert read("lc_1582_header_wrapped_over_lines")["底座產品"] == (99.66, 8650088.0)


def test_share_of_revenue_is_one_share():
    """6184 heads 占本年累計營業收入百分比: one share column, not an amount and a share."""
    assert refusal("lc_6184_share_of_revenue_phrase") is None
    assert read("lc_6184_share_of_revenue_phrase")["視訊收入"] == (47.0, None)


def test_an_amount_in_another_unit_is_not_the_revenue():
    """3388 prints 「73.64億 100%」 under 營 業 額 比 重: the share is read, the amount in
    億 is not a revenue in the table's unit."""
    rows = read("lc_3388_amount_in_another_unit")
    assert [v for v in rows.values()] == [(100.0, None)]


# --------------------------------------------------------------------------------
# L1: the evidence block ends
# --------------------------------------------------------------------------------

def test_the_block_ends_before_the_next_section_when_there_is_no_total():
    """1522 has no 合計; 「3.計劃開發之新商品(服務)」 ends the block, and the industry
    statistics table further down -- 汽車業 912 -16.26% 2,077 ... -- no longer refuses it."""
    assert refusal("lc_1522_block_runs_into_industry_table") is None
    rows = read("lc_1522_block_runs_into_industry_table")
    assert rows == {"汽車類": (93.0, None), "機車類": (2.0, None), "其他": (5.0, None)}


def test_an_interleaved_sidebar_heading_does_not_cut_the_block():
    """1905's table-of-contents sidebar prints 「三、最近二年度…從業員工資料」 between 紙膠帶
    and 其他; the block runs to 合計 18,766,991 100.00."""
    rows = read("lc_1905_sidebar_interleaved")
    assert rows["紙膠帶"] == (3.73, 700644.0)
    assert rows["其他"] == (1.72, 323105.0)


def test_items_with_letter_markers_are_rows_and_the_next_section_is_not():
    """1316's items print 「A.保健食品 34.89%」 ... 「F. 房地收入 5.18%」; below
    「(3)目前之商品(服務)項目」, 「樹脂 1.聚胺酯樹脂等」 was read as a 1% row."""
    rows = read("lc_1316_items_with_letter_markers")
    assert len(rows) == 6
    assert "樹脂" not in rows and "PU合成皮" not in rows


def test_prose_past_the_total_no_longer_refuses():
    """2072: after 「合計 9,664,454 100.00% 10,276,165 100.00%」 comes 「3. 公司目前之商品(服務)
    項目」 and prose (「截至民國 114 年底,國內已有 8 個離岸風場…」). That prose refused the
    section. The rows the block holds are wrapped (L3, backlog): not asserted here."""
    assert refusal("lc_2072_prose_past_the_total") is None


# --------------------------------------------------------------------------------
# refusals the contracts keep
# --------------------------------------------------------------------------------

def test_a_header_naming_the_other_year_over_the_share_still_refuses():
    """6194 heads 114年度營業收入 | 113年度營業比重: there is no 114 share position."""
    assert refusal("lc_6194_header_names_the_other_year") is not None


def test_a_number_split_by_extraction_refuses_and_is_not_joined():
    """Ruled: 6910 「授權收入 73 ,092 19%」 -- the amount cannot be established as one value,
    the line fits no position, and nothing joins 「73 ,092」 back."""
    assert refusal("lc_6910_number_split_by_extraction") == "3 value positions do not fit the measure header"
