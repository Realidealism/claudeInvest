# -*- coding: utf-8 -*-
"""Normalising the text also renames the thing the mapping points at.

_page_text put NFKC at the one place a page becomes text, which fixed three
companies whose filings use the full-width percent sign. It also changed what
tw.product_revenue stores: 1341 富林-KY's line went from 「PU／TPU」 to
「PU/TPU」. db/industry_data.py names revenue lines by their text -- that string
IS the join key -- so the exposure's weight_item stopped matching and
db.fill_weight fell from 524 resolved to 523, with the failure surfacing three
steps downstream as one line of output nobody was looking for.

The lesson is not "remember to fix industry_data.py after normalising". It is
that extraction and mapping share an identity, and a change to one is a change
to both. These tests assert that shared identity directly, so the next
normalisation change fails here rather than in a weight count.

Note what is NOT asserted: that any particular string is stored. That would
break every time a filing is re-read. What must hold is that the two sides
agree, and that neither side carries a form the other cannot produce.
"""
import unicodedata

import psycopg2
import psycopg2.extras

import db.connection as dbc
from scrapers.annual_report_revenue import _page_text


class _Page:
    def __init__(self, text):
        self._text = text

    def extract_text(self):
        return self._text


def _cursor():
    return psycopg2.connect(**dbc.DB_CONFIG).cursor(
        cursor_factory=psycopg2.extras.RealDictCursor)


def test_extraction_emits_only_normalised_text():
    """Whatever the filing typesets, what leaves the extractor is canonical."""
    raw = "乳膠皮 PU／TPU 柔軟皮（膠布） 一二３％"
    out = _page_text(_Page(raw))
    assert out == unicodedata.normalize("NFKC", out)
    assert "／" not in out and "（" not in out and "３" not in out


def test_no_stored_revenue_item_carries_a_form_the_extractor_cannot_emit():
    """A stored item that NFKC would change came from before the invariant.

    Such a row can never again be produced by this parser, so any weight_item
    pointing at it is a dangling reference waiting to happen.
    """
    c = _cursor()
    c.execute("SELECT stock_id, item FROM tw.product_revenue "
              "WHERE fiscal_year = 2025 AND retracted_at IS NULL")
    stale = [(r["stock_id"], r["item"]) for r in c.fetchall()
             if unicodedata.normalize("NFKC", r["item"]) != r["item"]]
    assert stale == [], stale


def test_every_weight_item_component_is_normalised():
    """The mapping side of the same identity.

    weight_item may name several revenue lines joined by ' + ', and each
    component is matched against a stored item, so each must be canonical.
    """
    c = _cursor()
    c.execute("SELECT stock_id, node_code, weight_item "
              "FROM tw.company_industry_exposure "
              "WHERE effective_to IS NULL AND weight_item IS NOT NULL")
    bad = []
    for r in c.fetchall():
        for part in r["weight_item"].split(" + "):
            if unicodedata.normalize("NFKC", part) != part:
                bad.append((r["stock_id"], r["node_code"], part))
    assert bad == [], bad
