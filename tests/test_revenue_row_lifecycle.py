# -*- coding: utf-8 -*-
"""A row stops being evidence when the document that produced it says otherwise.

save() replaces a company's rows only on a successful parse, so that a failed
download cannot empty a company that already had a good read. That rule is
right and stays. What it did not cover is a parse that opens the document and
finds no mix: there the old rows are not merely old, they have been contradicted
by a re-read of their own source. 23 companies sat in that state holding what
earlier parsers produced before the context gate existed.

Retraction is not deletion, and these tests hold that line from both sides: the
row and its provenance stay in the base table so a past claim stays auditable,
and it disappears from the view every consumer reads -- including from the
denominator, so a retracted line cannot go on halving the shares of the lines
that remain.

The distinction that has to survive is between contradicted and unconfirmed.
2302 麗正's filing could not be found this time; nothing read it, so nothing
disproved it, and its rows stand.
"""
import ast
import io

import psycopg2
import psycopg2.extras

import db.connection as dbc
from scrapers.annual_report_revenue import READER_METHODS

YEAR = 2025
READ_STATUSES = ("no_section", "parse_conflict")
UNREAD_STATUSES = ("no_report", "download_failed", "parse_failed")


def _cursor():
    return psycopg2.connect(**dbc.DB_CONFIG).cursor(
        cursor_factory=psycopg2.extras.RealDictCursor)


def test_the_retraction_actually_happened():
    """Guards every other test here: on an empty set they would all pass."""
    c = _cursor()
    c.execute("SELECT count(*) AS n FROM tw.product_revenue "
              "WHERE fiscal_year = %s AND retracted_at IS NOT NULL", (YEAR,))
    assert c.fetchone()["n"] > 0


def test_a_retracted_row_has_a_reason():
    c = _cursor()
    c.execute("SELECT count(*) AS n FROM tw.product_revenue "
              "WHERE retracted_at IS NOT NULL "
              "AND btrim(COALESCE(retracted_reason, '')) = ''")
    assert c.fetchone()["n"] == 0


def test_a_retracted_row_is_not_evidence():
    """It must be gone from the view every consumer reads."""
    c = _cursor()
    c.execute("""
        SELECT count(*) AS n
          FROM tw.product_revenue_share v
          JOIN tw.product_revenue p
            ON p.stock_id = v.stock_id AND p.fiscal_year = v.fiscal_year
           AND p.item = v.item
         WHERE p.retracted_at IS NOT NULL
    """)
    assert c.fetchone()["n"] == 0


def test_a_retracted_row_is_still_there_to_be_audited():
    """Retraction is not deletion: provenance survives."""
    c = _cursor()
    c.execute("SELECT count(*) AS n FROM tw.product_revenue "
              "WHERE fiscal_year = %s AND retracted_at IS NOT NULL "
              "AND source_file IS NOT NULL", (YEAR,))
    assert c.fetchone()["n"] > 0


def test_a_contradicted_company_holds_no_live_rows():
    """The document was read and holds no mix, so its rows are not current."""
    c = _cursor()
    c.execute("""
        SELECT st.stock_id, count(p.item) AS live
          FROM tw.product_revenue_status st
          LEFT JOIN tw.product_revenue p
                 ON p.stock_id = st.stock_id AND p.fiscal_year = st.fiscal_year
                AND p.retracted_at IS NULL
         WHERE st.fiscal_year = %s AND st.status = ANY(%s)
         GROUP BY st.stock_id
        HAVING count(p.item) > 0
    """, (YEAR, list(READ_STATUSES)))
    assert c.fetchall() == []


def test_an_unread_company_keeps_what_it_had():
    """Nothing read it, so nothing disproved it.

    Asserted as a rule rather than as a company: whichever companies are in
    this state, none of their rows may carry a retraction.
    """
    c = _cursor()
    c.execute("""
        SELECT st.stock_id, count(*) AS retracted
          FROM tw.product_revenue_status st
          JOIN tw.product_revenue p
            ON p.stock_id = st.stock_id AND p.fiscal_year = st.fiscal_year
         WHERE st.fiscal_year = %s AND st.status = ANY(%s)
           AND p.retracted_at IS NOT NULL
           AND p.retracted_reason LIKE 're-read of%%'
         GROUP BY st.stock_id
    """, (YEAR, list(UNREAD_STATUSES)))
    assert c.fetchall() == []


def test_a_retracted_row_is_out_of_the_denominator():
    """Otherwise it would go on distorting the lines that remain.

    6166 凌華 is the precedent: one missing row left an 88% denominator and
    every share was quietly wrong. A retracted row left inside sum(revenue)
    would do the same in the other direction.
    """
    c = _cursor()
    c.execute("""
        SELECT v.stock_id, round(sum(v.pct_effective), 2) AS total
          FROM tw.product_revenue_share v
         WHERE v.fiscal_year = %s
           AND v.stock_id IN (SELECT stock_id FROM tw.product_revenue
                               WHERE fiscal_year = %s AND retracted_at IS NOT NULL)
         GROUP BY v.stock_id
        HAVING sum(v.pct_effective) > 150
    """, (YEAR, YEAR))
    assert c.fetchall() == []


# ---------------------------------------------------------------------------
# Ownership: tw.product_revenue has three writers, and reading a company is not
# authority over what another writer stored against it. Two statements in save()
# used to decide what to destroy by asking what a row looked like -- the DELETE
# asked nothing at all, the retraction asked about source_file shape. Neither
# asked who wrote it. db/PARSER_BACKLOG.md P11 and P12.
#
# The scoping is asserted against the source text rather than by exercising
# save(). Proving a DELETE spares rows requires rows it might have taken, and
# this suite runs against production: a test that writes to find out would be
# the very accident it is guarding against (see 2026-09-03 in
# .claude/rules/02_judgment.md, where a "finally: rollback" control test
# committed 30 production rows).
# ---------------------------------------------------------------------------

NARRATIVE_METHODS = ("narrative_table", "narrative_text")


def _source():
    import scrapers.annual_report_revenue as ar
    return io.open(ar.__file__, encoding="utf-8").read()


def _save_body():
    src = _source()
    tree = ast.parse(src)
    fn = next(n for n in tree.body
              if isinstance(n, ast.FunctionDef) and n.name == "save")
    return ast.get_source_segment(src, fn)


def test_the_reader_claims_exactly_the_methods_it_can_produce():
    """READER_METHODS is the ownership boundary, so it must not drift from the
    methods parse_pdf actually returns -- in either direction."""
    import scrapers.annual_report_revenue as ar
    src = _source()
    tree = ast.parse(src)
    fn = next(n for n in tree.body
              if isinstance(n, ast.FunctionDef) and n.name == "parse_pdf")
    produced = set()
    for node in ast.walk(fn):
        if isinstance(node, ast.Return) and isinstance(node.value, ast.Tuple):
            for el in node.value.elts:
                if isinstance(el, ast.Constant) and isinstance(el.value, str) and el.value:
                    produced.add(el.value)
    assert produced == set(ar.READER_METHODS), (
        "parse_pdf returns %s but READER_METHODS is %s"
        % (sorted(produced), sorted(ar.READER_METHODS)))


def test_another_producers_method_is_never_the_readers():
    import scrapers.annual_report_revenue as ar
    for m in NARRATIVE_METHODS:
        assert m not in ar.READER_METHODS


def test_the_delete_cannot_reach_another_producer_or_the_audit_trail():
    """P12: it used to be `WHERE stock_id AND fiscal_year`, nothing else."""
    body = _save_body()
    start = body.index("DELETE FROM tw.product_revenue")
    stmt = body[start:start + 400]
    assert "method = ANY(%s)" in stmt, "the DELETE is not scoped to a producer"
    assert "retracted_at IS NULL" in stmt, "the DELETE can destroy the audit trail"


def test_the_retraction_cannot_reach_another_producers_rows():
    """P11: source_file shape answers 'does this look like mine', not 'is it'."""
    body = _save_body()
    start = body.index("SET retracted_at = NOW()")
    stmt = body[start:start + 500]
    assert "method = ANY(%s)" in stmt, "the retraction is not scoped to a producer"


def test_no_company_holds_live_rows_from_two_producers():
    """The state the scoping exists to keep out of the table. It is also the
    precondition the release measurement's stratum split assumes."""
    c = _cursor()
    c.execute("""
        SELECT stock_id, count(DISTINCT CASE
                   WHEN method = ANY(%s) THEN 'reader' ELSE 'other' END) AS producers
          FROM tw.product_revenue
         WHERE fiscal_year = %s AND retracted_at IS NULL
         GROUP BY stock_id HAVING count(DISTINCT CASE
                   WHEN method = ANY(%s) THEN 'reader' ELSE 'other' END) > 1
    """, (list(READER_METHODS), YEAR, list(READER_METHODS)))
    mixed = [r["stock_id"] for r in c.fetchall()]
    assert not mixed, "companies with two producers' live rows: %s" % mixed[:10]


def test_the_other_producers_population_is_real():
    """Guards the tests above: on an empty set they would pass for free."""
    c = _cursor()
    c.execute("""SELECT count(DISTINCT stock_id) AS n FROM tw.product_revenue
                  WHERE fiscal_year = %s AND retracted_at IS NULL
                    AND method = ANY(%s)""", (YEAR, list(NARRATIVE_METHODS)))
    assert c.fetchone()["n"] > 100
