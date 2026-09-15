"""P22 repair, R2-9 and R2-10 (ruled 2026-09-15, after the fourth differential run's source review).

R2-9  a share row whose own amount does not reproduce its printed share against the table's total,
      while the amount with the amount-only rows directly below does, keeps its share and gets no
      revenue -- a sub-row is not the category's revenue (1711). Rows whose amount is their own
      share keep it (3708, 4545).
R2-10 a leading $ directly followed by a digit is read as an amount, at the lowest numeric layer, so
      column identity and value reading agree (3057, 4113, 6261). Nothing wider: a parenthesised
      ($705,357) stays unread, and a region table with $ amounts is still no candidate.

Expected values are read off the printed tables in tests/fixtures/mix_sections.json. 2371's 部門收入
column is not asserted here: made readable by R2-10 it meets P22-2's segment-column refusal, which
is awaiting a ruling.
"""
import io
import json
import os
import re

from scrapers.annual_report_revenue import _non_revenue_heading, _rows_from_table, _to_num

HERE = os.path.join(os.path.dirname(__file__), "fixtures")
with io.open(os.path.join(HERE, "mix_sections.json"), encoding="utf-8") as _fh:
    SECTIONS = json.load(_fh)


def table_rows(key):
    return {re.sub(r"\s", "", r["item"]): (r["pct"], r["revenue"])
            for r in _rows_from_table(SECTIONS[key]["table"], 2025)}


def cell(key, text):
    found = [c for row in SECTIONS[key]["table"] for c in row if c and text in c]
    assert len(found) == 1, (key, text, found)
    return found[0]


def test_every_r2_9_10_fixture_records_where_it_came_from():
    keys = [k for k in SECTIONS if k.startswith(("r2_9_", "r2_10_"))]
    assert len(keys) == 9
    for key in keys:
        assert SECTIONS[key]["source_file"].endswith(".pdf") and SECTIONS[key]["page"] >= 1, key


# ---- R2-9 ------------------------------------------------------------------------------------

def test_a_share_that_belongs_to_a_group_gets_no_sub_row_revenue():
    # 電子化學品 | 光阻劑 | 530,036 | 24.0%  /  其他 | 1,290,507  ...  合計 7,568,574 | 100.0%
    # 醫藥化學品 | 前列腺素 | 163,312 | 2.7%  /  其他原料藥 | 40,807
    rows = table_rows("r2_9_1711_group_share")
    assert rows["電子化學品"] == (24.0, None)
    assert rows["醫藥化學品"] == (2.7, None)
    assert all(revenue not in (530036.0, 163312.0) for _, revenue in rows.values())


def test_rows_outside_a_group_keep_their_amounts():
    rows = table_rows("r2_9_1711_group_share")
    assert rows["色料化學品"] == (36.4, 2756093.0)
    assert rows["特用化學品"] == (24.0, 1813821.0)
    assert rows["碳粉產品"] == (12.9, 973998.0)


def test_a_row_whose_amount_is_its_own_share_keeps_it():
    # 3708: 航太複合材料 | 40,315 | 0.58, then 其他(註) | 1,243,028 | 17.79100.00, 合計 6,985,352
    assert table_rows("r2_9_3708_own_share")["航太複合材料"] == (0.58, 40315.0)
    # 4545: 塑膠射出件 | 523,033 | 22%, then 自動化機台 172,685 and 其他 176,874, 合計 2,387,660
    assert table_rows("r2_9_4545_own_share")["塑膠射出件"] == (22.0, 523033.0)


# ---- R2-10 -----------------------------------------------------------------------------------

def test_a_dollar_sign_before_an_amount_is_read():
    # 3057: 系統 | $551 | 94.35 / 其他 | 33 | 5.65 / 合計 | $584 | 100.0
    assert table_rows("r2_10_3057_dollar_amount") == {"系統": (94.35, 551.0), "其他": (5.65, 33.0)}
    # 4113: 房地收入 | $609,477 | 99.89% / 租賃收入 | 698 | 0.11% / 合計 | $610,175
    assert table_rows("r2_10_4113_dollar_amount") == {"房地收入": (99.89, 609477.0), "租賃收入": (0.11, 698.0)}


def test_a_dollar_amount_beside_a_parenthesised_negative():
    # 6261: 測試代工收入 $2,138,456 51.03% ... 減:銷貨退回及折讓 (33,925) (0.81%) / 營業淨額 $4,190,701
    rows = table_rows("r2_10_6261_dollar_and_negative")
    assert rows == {"測試代工收入": (51.03, 2138456.0), "切割挑檢代工收入": (10.52, 440764.0),
                    "機器組裝等產品銷售及其他": (39.26, 1645406.0)}
    assert _to_num(cell("r2_10_6261_dollar_and_negative", "33,925")) is None


def test_only_the_ruled_dollar_shape_is_read():
    key = "r2_10_6901_dollar_parenthesised"
    assert _to_num(cell(key, "705,357")) is None             # ($705,357)
    assert _to_num(cell(key, "1,120,448")) is None           # ($1,120,448)


def test_a_region_table_with_dollar_amounts_is_still_no_candidate():
    key = "r2_10_1536_dollar_region"
    assert _non_revenue_heading(SECTIONS[key]["heading"])
    assert _rows_from_table(SECTIONS[key]["table"], 2025) == []
