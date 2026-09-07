"""Regression tests for the definition-quality checks in db/check_industry.py.

Every case below is a real defect the 2026-09-07 taxonomy pass found by reading,
after [definition] -- which only asked whether a leaf with members had an empty
main_products -- had reported 3 and missed all of them. The point of these tests
is not that the numbers stay where they are; it is that the *detector* keeps
detecting the shape, so the next definition written like one of these gets
caught by a query instead of by somebody happening to look.

Each test states the historical case in its docstring and then asserts on a
synthetic tree passed to the same SQL. Synthetic rather than live, because the
live rows are being fixed as this work proceeds and a test that asserts "A6-02
still names 旺矽" would fail the moment the data is right -- which is backwards.

Marked needs_db: the checks are SQL, so they need a real PostgreSQL to run
against. Everything is created inside a transaction that is never committed.
"""
import psycopg2
import psycopg2.extras
import pytest

import db.connection as dbc
from db.check_industry import (
    check_definition_axis_undeclared,
    check_definition_boundary,
    check_definition_boundary_overlap,
    check_definition_entity_reference,
    check_definition_placeholder,
)

pytestmark = pytest.mark.needs_db


@pytest.fixture
def cur():
    """A cursor on a transaction that is rolled back, with the tree emptied.

    The checks read tw.industry_tree, tw.company_industry_exposure and
    tw.stocks. Deleting inside an uncommitted transaction gives each test the
    whole tree to itself without touching what is stored.
    """
    try:
        conn = psycopg2.connect(**dbc.DB_CONFIG)
    except Exception as exc:                                # pragma: no cover
        pytest.skip(f"database unavailable: {type(exc).__name__}: {exc}")
    conn.autocommit = False
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    # order matters: both tables carry a foreign key onto industry_tree
    c.execute("DELETE FROM tw.company_industry_exposure")
    c.execute("DELETE FROM tw.core_reassessment")
    c.execute("DELETE FROM tw.industry_tree")
    try:
        yield c
    finally:
        conn.rollback()
        conn.close()


def node(cur, code, parent, level, name, products=None):
    cur.execute(
        "INSERT INTO tw.industry_tree (node_code, parent_code, level, name, main_products)"
        " VALUES (%s, %s, %s, %s, %s)", (code, parent, level, name, products))


def a_listed_name(cur):
    """Any active ordinary share's name, for the entity-reference case."""
    cur.execute("SELECT name FROM tw.stocks WHERE is_active AND security_type = 'STOCK'"
                " AND length(name) >= 2 ORDER BY stock_id LIMIT 1")
    return cur.fetchone()["name"]


def codes(findings, key):
    return {f[key] for f in findings}


def test_entity_reference_catches_a_minority_of_company_names(cur):
    """A6-02 read 'Probe、Probe Card、Probe Interface（旺矽、雍智）'.

    Two names among five terms. [definition_completeness]'s own name test wants
    company names to be at least half the terms -- it was written for V1, whose
    definition was three distributor names and nothing else -- so this shape went
    straight through it for as long as it existed. And 雍智 was not even on the
    node: it sits on A6-03, so the definition cited a mapping that was wrong as
    well as circular.
    """
    listed = a_listed_name(cur)
    node(cur, "T", None, 1, "root")
    node(cur, "T1", "T", 2, "with a name",
         "Probe、Probe Card、Probe Interface（%s）" % listed)
    node(cur, "T2", "T", 2, "clean", "Probe、Probe Card、Probe Interface")

    found = codes(check_definition_entity_reference(cur), "node_code")
    assert "T1" in found, "a definition naming a listed company must be reported"
    assert "T2" not in found, "a definition of pure product terms must not be"


def test_placeholder_catches_a_definition_that_is_only_a_pointer(cur):
    """A5-04 HBM Packaging's whole definition was '（HBM 本身為 Theme T07）'.

    Not empty, so completeness passed it; not missing, so boundary passed it;
    and useless, because a cross-reference says where else to look and never says
    what belongs here. The test is what survives once the brackets come off.
    """
    node(cur, "T", None, 1, "root")
    node(cur, "T1", "T", 2, "pointer", "（HBM 本身為 Theme T07）")
    node(cur, "T2", "T", 2, "deferred", "（待裁）")
    node(cur, "T3", "T", 2, "real boundary with a reference",
         "碳粉、碳粉匣、列印耗材、電子成像用色料（碳黑原料見 T1）")

    found = codes(check_definition_placeholder(cur), "node_code")
    assert {"T1", "T2"} <= found, "a definition that is only a pointer must be reported"
    assert "T3" not in found, (
        "a real boundary that happens to carry a cross-reference must not be -- "
        "J16 was flagged by an earlier length-only version of this test")


def test_axis_undeclared_wants_the_parent_to_say_what_its_children_divide_on(cur):
    """B2 held five axes at once and nothing in the tree said what its axis was.

    Whether an axis is violated needs a person; whether one was ever declared
    does not. A parent with two or more children and members underneath owes the
    declaration, because it is what a person checks a violation against. A parent
    with one child owes a scope instead and is out of scope here.
    """
    # two companies, because one may hold only one current Core
    cur.execute("SELECT stock_id FROM tw.stocks WHERE is_active"
                " AND security_type = 'STOCK' ORDER BY stock_id LIMIT 2")
    sids = [r["stock_id"] for r in cur.fetchall()]

    node(cur, "T", None, 1, "root")
    node(cur, "T1", "T", 2, "no axis stated", "一般硬板、多層板、HDI")
    node(cur, "T1-01", "T1", 3, "leaf a", "甲")
    node(cur, "T1-02", "T1", 3, "leaf b", "乙")
    node(cur, "T2", "T", 2, "axis stated",
         "本組的分類軸是板材結構：整片皆硬、整片皆軟，兩者互斥。")
    node(cur, "T2-01", "T2", 3, "leaf c", "丙")
    node(cur, "T2-02", "T2", 3, "leaf d", "丁")
    for sid, n in zip(sids, ("T1-01", "T2-01")):
        cur.execute(
            "INSERT INTO tw.company_industry_exposure"
            " (stock_id, node_code, role, evidence, evidence_kind, exposure_type,"
            "  effective_from) VALUES (%s, %s, 'Core', 'x', 'company_site',"
            " 'Direct', DATE '2025-12-31')", (sid, n))

    found = codes(check_definition_axis_undeclared(cur), "node_code")
    assert "T1" in found, "a branch in use that never states its axis must be reported"
    assert "T2" not in found, "a branch that states one must not be"


def test_boundary_population_counts_the_subtree_not_direct_members(cur):
    """A3 晶圓製造, A1 半導體材料 and A7 半導體廠務 sit in a set holding 361
    companies and were never reported, because every one of those companies is
    attached to an L3 leaf and the query asked whether the L2 sibling itself
    carried an exposure.

    A scope node is not supposed to carry members, so asking that question of it
    answers a different one. The set is in use when companies are being sorted
    by it anywhere underneath.
    """
    cur.execute("SELECT stock_id FROM tw.stocks WHERE is_active"
                " AND security_type = 'STOCK' ORDER BY stock_id LIMIT 1")
    sid = cur.fetchone()["stock_id"]

    node(cur, "T", None, 1, "root")
    node(cur, "T1", "T", 2, "undefined scope node")
    node(cur, "T1-01", "T1", 3, "leaf a", "甲")
    node(cur, "T2", "T", 2, "defined scope node", "本組的分類軸是製程段")
    node(cur, "T2-01", "T2", 3, "leaf b", "乙")
    # a second set, identical in shape, with nobody in it
    node(cur, "U", None, 1, "empty root")
    node(cur, "U1", "U", 2, "undefined and unused")
    node(cur, "U1-01", "U1", 3, "leaf c", "丙")
    node(cur, "U2", "U", 2, "defined and unused", "本組的分類軸是材質")
    cur.execute(
        "INSERT INTO tw.company_industry_exposure"
        " (stock_id, node_code, role, evidence, evidence_kind, exposure_type,"
        "  effective_from) VALUES (%s, 'T2-01', 'Core', 'x', 'company_site',"
        " 'Direct', DATE '2025-12-31')", (sid,))

    found = codes(check_definition_boundary(cur), "node_code")
    assert "T1" in found, (
        "the set is in use through an L3 leaf, so its undefined sibling must be"
        " reported")
    assert "U1" not in found, "a set with no population anywhere must not be"


def test_boundary_population_counts_companies_not_exposure_rows(cur):
    """One company holding three exposures under one set is one company.

    The predicate asks whether real companies are being sorted by this set. Rows
    would answer how much work has been recorded, which is a different thing and
    would let a single heavily-split company make a set look populated.
    """
    cur.execute("SELECT stock_id FROM tw.stocks WHERE is_active"
                " AND security_type = 'STOCK' ORDER BY stock_id LIMIT 1")
    sid = cur.fetchone()["stock_id"]

    node(cur, "T", None, 1, "root")
    node(cur, "T1", "T", 2, "undefined scope node")
    node(cur, "T2", "T", 2, "defined scope node", "本組的分類軸是製程段")
    node(cur, "T2-01", "T2", 3, "leaf a", "甲")
    node(cur, "T2-02", "T2", 3, "leaf b", "乙")
    for leaf, role in (("T2-01", "Core"), ("T2-02", "Major")):
        cur.execute(
            "INSERT INTO tw.company_industry_exposure"
            " (stock_id, node_code, role, evidence, evidence_kind, exposure_type,"
            "  effective_from) VALUES (%s, %s, %s, 'x', 'company_site',"
            " 'Direct', DATE '2025-12-31')", (sid, leaf, role))

    rows = [f for f in check_definition_boundary(cur) if f["node_code"] == "T1"]
    assert rows, "the set is in use, so its undefined sibling must be reported"
    assert rows[0]["subtree_members"] == 1, (
        "two exposures held by one company are one company, not two")


def test_boundary_overlap_catches_a_definition_claiming_a_siblings_name(cur):
    """B2-01 listed HDI while B2-05 was named HDI / SLP; K2 claimed 射出 while K4
    was 塑膠射出.

    In both the empty sibling was the symptom and the defined one was the cause,
    and neither existing check could say so: one asks whether a node has a
    definition, the other whether a set has gaps, and here the gap was made by a
    definition that had already taken the ground.

    The two suppressions matter as much as the finding. Naming the sibling's code
    means handing the ground over, which is what a boundary sentence does; and a
    word that names more than one sibling is the domain rather than a claim,
    which is why PCB under B and 半導體 under A are not findings.
    """
    node(cur, "T", None, 1, "root")
    node(cur, "T1", "T", 2, "硬板 / 多層板 / HDI", "一般硬板、多層板、HDI、高層數板")
    node(cur, "T2", "T", 2, "HDI / SLP", None)
    node(cur, "T3", "T", 2, "塑膠加工", "薄膜、片材（壓延、押出、射出、貼合）")
    node(cur, "T4", "T", 2, "塑膠射出", None)
    node(cur, "T5", "T", 2, "清洗設備", None)
    node(cur, "T6", "T", 2, "封裝設備", "固晶、打線；★服務封裝製程的清洗機屬本節點而非 T5")

    pairs = {(f["claiming_node"], f["claimed_node"])
             for f in check_definition_boundary_overlap(cur)}
    assert ("T1", "T2") in pairs, "listing HDI while a sibling is named HDI must be reported"
    assert ("T3", "T4") in pairs, "claiming 射出 while a sibling is 塑膠射出 must be reported"
    assert ("T6", "T5") not in pairs, (
        "naming the sibling's code is a boundary sentence, not a claim")


def test_boundary_overlap_ignores_a_word_that_names_several_siblings(cur):
    """PCB under B names B1 PCB材料, B2 PCB製造 and B5 PCB設備.

    A term that appears in more than one sibling's name is the branch's subject,
    not one node reaching into another's ground. Without this the check reported
    74 rows and most of them were definitions doing their job.
    """
    node(cur, "T", None, 1, "root")
    node(cur, "T1", "T", 2, "PCB材料", "銅箔基板、玻纖布")
    node(cur, "T2", "T", 2, "PCB製造", "PCB 之製造：硬板、軟板")
    node(cur, "T3", "T", 2, "PCB設備", "鑽孔、曝光")

    pairs = {(f["claiming_node"], f["claimed_node"])
             for f in check_definition_boundary_overlap(cur)}
    assert ("T2", "T1") not in pairs and ("T2", "T3") not in pairs, (
        "the branch's own subject word is not a claim on a sibling")
