# -*- coding: utf-8 -*-
"""The parse -> storage contract for tw.product_revenue.

tw.product_revenue is keyed on (stock_id, fiscal_year, item), and save() used to
upsert row by row with no dedupe: two parsed rows naming the same item collapsed
into one while the status row still reported the parsed count. 2338 光罩 read
"6 rows" beside a single stored row for two weeks with nothing able to see it.

These tests cover _dedupe only -- no database. The write path is exercised by
running the scraper; what needs pinning here is the decision _dedupe makes,
because that is where a silent collapse turns into a counted one.
"""
from scrapers.annual_report_revenue import _dedupe


def _row(item, revenue=None, pct=None):
    return {"item": item, "revenue": revenue, "pct": pct}


def test_distinct_items_pass_through_untouched():
    rows = [_row("磨床", pct=45.78), _row("CNC磨床", pct=21.09)]
    unique, duplicates, conflicts = _dedupe(rows)
    assert unique == rows
    assert duplicates == 0
    assert conflicts == []


def test_identical_repeat_is_counted_not_silently_dropped():
    """A table read twice is harmless -- but the loss must still be visible."""
    rows = [_row("LED", pct=5.0), _row("LED", pct=5.0)]
    unique, duplicates, conflicts = _dedupe(rows)
    assert len(unique) == 1
    assert duplicates == 1, "the second row is gone; the count is what says so"
    assert conflicts == []


def test_same_item_with_different_values_is_a_conflict():
    """Two values for one item means the reader never separated the rows.

    Storing either one would be a guess, so this stops the write instead of
    picking a winner.
    """
    rows = [_row("紙張", revenue=100, pct=68.5), _row("紙張", revenue=200, pct=11.2)]
    unique, duplicates, conflicts = _dedupe(rows)
    assert conflicts == ["紙張"]
    assert duplicates == 1


def test_a_conflict_does_not_hide_the_rows_that_were_fine():
    rows = [_row("A", pct=10.0), _row("B", pct=20.0),
            _row("A", pct=30.0)]
    unique, duplicates, conflicts = _dedupe(rows)
    assert [r["item"] for r in unique] == ["A", "B"]
    assert conflicts == ["A"]


def test_first_occurrence_wins_so_a_rerun_is_deterministic():
    rows = [_row("X", pct=1.0), _row("X", pct=1.0), _row("X", pct=1.0)]
    unique, duplicates, _ = _dedupe(rows)
    assert unique[0]["pct"] == 1.0
    assert duplicates == 2


def test_empty_parse_is_not_an_error():
    assert _dedupe([]) == ([], 0, [])
