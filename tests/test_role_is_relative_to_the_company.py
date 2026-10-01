# -*- coding: utf-8 -*-
"""R026's direction: role measures the business against the company, not the
company against the industry.

1303 南亞 is the case that makes the difference visible. It is one of Taiwan's
largest producers of glass fabric, copper foil and epoxy resin — and each of
those is a single-digit share of 南亞's own revenue. The two quantities point in
opposite directions, and Exposure Role records the second one.

The rows sat at Major while their weight was NULL, which is correct: R026 makes
Major the default for a Direct row with no figure, because Minor is the label
that needs evidence. When the filing's own numbers were finally written into
weight_item on 2026-09-09 they came out at 1.82 / 5.35 / 8.29, and the numeric
default took over.

This is asserted against the database rather than against a helper, because the
thing being protected is the stored ruling: someone who knows the industry may
later look at 「玻纖布 1.82% → Minor」 and 'correct' it upward.
"""
import psycopg2
import psycopg2.extras
import pytest

import db.connection as dbc


@pytest.fixture(scope="module")
def cur():
    conn = psycopg2.connect(**dbc.DB_CONFIG)
    yield conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    conn.close()


def _row(cur, node):
    cur.execute("""
        SELECT role::text, weight_pct, weight_item
          FROM tw.company_industry_exposure
         WHERE stock_id = '1303' AND node_code = %s AND effective_to IS NULL
    """, (node,))
    return cur.fetchone()


@pytest.mark.parametrize("node,item,pct", [
    ("B1-02", "玻纖布", 1.82),
    ("B1-05", "銅箔", 5.35),
    ("B1-03", "環氧樹脂", 8.29),
])
def test_a_leading_producer_is_still_minor_on_its_own_books(cur, node, item, pct):
    row = _row(cur, node)
    assert row is not None, node
    assert row["weight_item"] == item
    assert float(row["weight_pct"]) == pytest.approx(pct, abs=0.005)
    assert row["role"] == "Minor", (
        "%s is %.2f%% of 南亞's revenue. Market standing in the %s industry is a "
        "different quantity and does not lift the role (R026)." % (item, pct, item))


def test_the_node_that_is_actually_large_keeps_its_own_status(cur):
    """銅箔基板 10.91% is the same company's Core — role is not a size ranking."""
    row = _row(cur, "B1-01")
    assert row["weight_item"] == "銅箔基板"
    assert row["role"] == "Core"


def test_glass_yarn_was_not_added_to_glass_fabric(cur):
    """玻纖絲 0.64% is upstream of 玻纖布, and merging them is an ontology call.

    Adding the two to reach 2.46% would be accounting aggregation standing in
    for a decision about what B1-02 collects. The filing lists them separately;
    so does this.
    """
    row = _row(cur, "B1-02")
    assert "玻纖絲" not in (row["weight_item"] or "")
    assert float(row["weight_pct"]) < 2.0
