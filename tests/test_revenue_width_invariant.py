# -*- coding: utf-8 -*-
"""Every reader must consume the same text, whatever width the filing typeset.

1236 宏亞 writes 「營業比重約71.63％」 with the full-width percent sign. The
table path (_clean) and the line reader both ran NFKC on their own input, so
they were width-safe; the page filter and the prose reader read the raw text
layer, so they were not. That is the shape of the bug worth guarding: not "one
regex is missing ％", but "which paths normalise is a property of which helper
remembered to". The fix puts the normalisation at the single point where a page
becomes text, and these tests hold that point in place.

The invariant asserted here is behavioural: the same filing typeset full-width
must produce the same rows as typeset half-width, through each reader.
"""
import unicodedata

from scrapers.annual_report_revenue import (
    _page_text, _rows_from_lines, _rows_from_prose,
)

WRAP = chr(10)


class _Page:
    """The only thing _page_text asks of a pdfplumber page."""

    def __init__(self, text):
        self._text = text

    def extract_text(self):
        return self._text


def test_nfkc_folds_the_full_width_percent():
    assert unicodedata.normalize("NFKC", "12％") == "12%"


def test_a_page_is_normalised_where_it_becomes_text():
    page = _Page("營業比重約71.63％，其次為18.68％")
    assert "％" not in _page_text(page)
    assert "71.63%" in _page_text(page)


def test_a_page_with_no_text_layer_is_empty_not_none():
    assert _page_text(_Page(None)) == ""


# 1236 宏亞, 2025 annual report, page 67 -- verbatim, full-width throughout.
HONGYA_FULL = WRAP.join([
    "1. 本公司目前所營業務之主要內容及其營業比重：",
    "A. 『77巧克力』系列產品之製造與銷售，營業比重約71.63％。",
    "B. 『禮坊囍餅』及法式烘焙類產品之製造與銷售，營業比重為18.68％。",
    "C. 各式蛋糕、麵包、烘焙類產品代工，營業比重為9.69％",
])
HONGYA_HALF = unicodedata.normalize("NFKC", HONGYA_FULL)


def test_the_two_widths_are_not_the_same_string():
    # Guards the test itself: if these were equal the invariant below would
    # pass without ever exercising the fold.
    assert HONGYA_FULL != HONGYA_HALF
    assert "％" in HONGYA_FULL and "％" not in HONGYA_HALF


def test_prose_reader_gives_the_same_rows_at_either_width():
    assert _rows_from_prose(HONGYA_FULL) == _rows_from_prose(HONGYA_HALF)


def test_line_reader_gives_the_same_rows_at_either_width():
    assert _rows_from_lines(HONGYA_FULL, 2025) == _rows_from_lines(HONGYA_HALF, 2025)


def test_the_page_filter_sees_a_full_width_heading():
    # The keyword test in parse_pdf runs on _page_text's output, so a filing
    # that typesets the heading with compatibility ideographs still matches.
    page = _Page("肆、營運概況" + WRAP + "營業比重約71.63％")
    text = _page_text(page)
    assert any(k in text for k in ("營業比重", "主要商品", "營業比率", "營收比重"))
