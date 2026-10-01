# -*- coding: utf-8 -*-
"""A new weight_status must not be able to enter the system as "fillable by default".

The seventh value, Granularity_Insufficient, exists because a line can be disclosed along a product axis, be too
coarse for the node, and still have no namable second node (R2, ruled 2026-09-17). Its whole purpose is to say
"this number cannot be derived". If db.fill_weight does not know about it, fill_weight resolves the row's
weight_item and writes the line's number onto the node -- producing exactly the figure the status says does not
exist, and doing it silently.

Before this test, both consumers decided by comparing against the literal 'Too_Coarse'. These cases pin the
behaviour to the shared table instead, and fail if either consumer starts deciding on its own again.
"""
import io
import re

import pytest

from db import weight_status as ws


def test_every_value_has_an_autofill_decision():
    # the module raises at import if a value is added to ALL without one; this states the invariant it enforces
    decided = ws.AUTOFILL_BLOCKED | {"Not_Disclosed", "Not_Extracted", "Disclosure_Invalid"}
    assert ws.ALL - decided == set()


def test_both_coarse_statuses_block_autofill():
    assert ws.blocks_autofill("Too_Coarse")
    assert ws.blocks_autofill("Granularity_Insufficient")


def test_not_applicable_blocks_autofill():
    # Ruled 2026-09-29, shipped in 64ec862a. A Not_Applicable row is not an empty row: it names a real reported line
    # whose number exists, and says that number is not this node's weight -- 3374's 測試服務 is 47.86% of revenue and
    # is counted inside A5-05's total instead. Let fill_weight resolve it and the adjudication is silently undone.
    # Nothing else here pins which side of the decision it falls on: the import-time check and
    # test_every_value_has_an_autofill_decision only require that every value has a decision, not which one.
    assert ws.blocks_autofill("Not_Applicable")


@pytest.mark.parametrize("status", ["Not_Disclosed", "Not_Extracted", "Disclosure_Invalid", None])
def test_other_statuses_do_not_block_autofill(status):
    # these rows name no line that fill_weight could mistake for the node's total
    assert not ws.blocks_autofill(status)


def test_seed_accepts_granularity_insufficient_after_migration_113():
    assert ws.PENDING_MIGRATION == set()
    assert ws.usable_in_seed("Granularity_Insufficient") == (True, None)


def test_seed_refuses_a_value_while_it_is_pending(monkeypatch):
    # the gate outlives the value it was built for: the next ruled status must wait for its own migration
    monkeypatch.setattr(ws, "ALL", ws.ALL | {"Next_Ruled_Status"})
    monkeypatch.setattr(ws, "PENDING_MIGRATION", {"Next_Ruled_Status"})
    ok, why = ws.usable_in_seed("Next_Ruled_Status")
    assert not ok and "not yet in the database enum" in why


def test_seed_accepts_the_migrated_values_and_refuses_unknown():
    for status in sorted(ws.ALL - ws.PENDING_MIGRATION) + [None]:
        assert ws.usable_in_seed(status)[0], status
    ok, why = ws.usable_in_seed("Too_Granular")
    assert not ok and "unknown weight_status" in why


@pytest.mark.parametrize("path", ["db/fill_weight.py", "db/seed_industry.py"])
def test_consumers_do_not_compare_weight_status_to_a_literal(path):
    """The failure this guards against is a second copy of the value set, not a wrong value."""
    source = io.open(path, encoding="utf-8").read()
    code = "\n".join(line.split("#")[0] for line in source.splitlines())
    assert not re.search(r"weight_status\"?\]?\s*[!=]=\s*[\"']", code), \
        f"{path} decides on a weight_status literal again instead of db.weight_status"


def test_granularity_insufficient_is_coarse_and_reread():
    # ruled 2026-09-18: a company all of whose rows carry it is coarse_filing, not missing; and it is re-read, because
    # convergence (b) -- the source adds a usable split -- is only observable by reading the filing again
    assert "Granularity_Insufficient" in ws.COARSE
    assert "Granularity_Insufficient" in ws.REREAD
    assert ws.COARSE <= ws.ALL and ws.REREAD <= ws.ALL


# A set-membership decision written as SQL: a negation or an IN-list against a quoted status. Equality with ONE value
# stays legal -- a check about that value's own lifecycle (Not_Extracted, Not_Applicable) is not a copy of a set.
SQL_SET_LITERAL = re.compile(
    r"weight_status(?:::text)?\s*(?:<>|!=|IS\s+DISTINCT\s+FROM|(?:NOT\s+)?IN)\s*\(?\s*'", re.I)
# Before its migration, comparing the enum column with a value is an "invalid input value" error, not zero rows.
# Built from an explicit set: with PENDING_MIGRATION empty, an empty alternation would only match ''.
def enum_pending(values):
    return re.compile(r"weight_status\s*=\s*'(?:%s)'" % "|".join(sorted(values))) if values else None


SQL_ENUM_PENDING = enum_pending({"Granularity_Insufficient"})   # the value the guard was proven on


@pytest.mark.parametrize("bad", [
    "AND e.weight_status IS DISTINCT FROM 'Too_Coarse')",           # check_industry coverage, before 2026-09-18
    "AND e.weight_status <> 'Too_Coarse')",                         # check_industry consistency (c), same
    "AND e.weight_status IN ('Too_Coarse', 'Not_Disclosed',",       # annual_report_narrative --gap, same
])
def test_sql_guard_fires_on_the_three_literals_it_replaced(bad):
    assert SQL_SET_LITERAL.search(bad)


@pytest.mark.parametrize("good", [
    "AND e.weight_status = 'Not_Extracted'",
    "OR NOT e.weight_status::text = ANY(%s)))",
    "AND e.weight_status::text = %s",
])
def test_sql_guard_leaves_single_value_and_parameterised_checks_alone(good):
    assert not SQL_SET_LITERAL.search(good) and not SQL_ENUM_PENDING.search(good)


def test_sql_guard_fires_on_an_enum_comparison_with_the_unmigrated_value():
    assert SQL_ENUM_PENDING.search("WHERE e.weight_status = 'Granularity_Insufficient'")


@pytest.mark.parametrize("path", ["db/fill_weight.py", "db/seed_industry.py", "db/check_industry.py",
                                  "scrapers/annual_report_narrative.py"])
def test_sql_does_not_decide_on_a_weight_status_set_literal(path):
    source = io.open(path, encoding="utf-8").read()
    assert not SQL_SET_LITERAL.search(source), f"{path} spells a weight_status set out in SQL instead of db.weight_status"
    pending = enum_pending(ws.PENDING_MIGRATION)
    assert not (pending and pending.search(source)), f"{path} compares the enum with a value it does not carry yet"
