# -*- coding: utf-8 -*-
"""The weight_status value set, in one place, with the consequence attached to each value.

Two consumers used to carry their own copy of this knowledge: db.seed_industry held a literal tuple of the accepted
values, and db.fill_weight decided what to skip by comparing against the single string 'Too_Coarse'. Both copies were
written when Too_Coarse was the only status that meant "a share exists in the filing but not for this node". Adding a
seventh value to the enum without touching them would have left fill_weight resolving the row's weight_item and
writing a number the rules say cannot be derived -- the exact failure the new value exists to prevent.

So the table below is not a list of names. Each value declares whether a row carrying it may be auto-filled, and
EVERY value must appear in exactly one bucket: a value added to ALL without a decision raises at import time rather
than defaulting to fillable.

Namespaces (R020(1)-S): AVAILABILITY answers "what did the source give us", COUNTING answers "what does the counting
framework allow". They are not interchangeable.
"""

# One count, two frames, and they differ by one. The rule text R020(1)-S describes WEIGHTED as a status; the database
# enum does not have it, because a row that has a weight has nothing to explain and leaves weight_status NULL. So the
# rule text goes from six values to seven with this change, while the enum goes from five to six. Anyone writing the
# migration should count the enum, not the prose.

# What the source gave us.
AVAILABILITY = {
    "Too_Coarse",                # disclosed along a product axis, coarser than the tree, and the OTHER node is namable
    "Granularity_Insufficient",  # same, except the other node cannot be named (R2, ruled 2026-09-17)
    "Not_Disclosed",             # no product axis at all -- terminal
    "Not_Extracted",             # the source exists and we failed to read it -- a work queue
    "Disclosure_Invalid",        # a figure is given and it is not usable
}

# What the counting framework allows.
COUNTING = {
    "Not_Applicable",            # the revenue is already counted on another node
}

ALL = AVAILABILITY | COUNTING

# Rows whose weight_item names a real reported line that is NOT this node's total. fill_weight would resolve the item
# and overwrite the reason with the line's number, so it must leave them alone.
# Not_Applicable joined on 2026-09-29. It belongs here for the same reason and by a different route: the line is real
# and it is not this node's total because ANOTHER node already counts it. 3374 精材 is the case that showed it --
# A6-01 carries weight_item 測試服務 with the share removed, and that share sits in tw.product_revenue, so the next
# fill_weight run would blank the table and write 47.86 straight back, with weight_status = NULL on top of it.
# The adjudication would have been undone by a routine re-run and nothing would have said so.
AUTOFILL_BLOCKED = {"Too_Coarse", "Granularity_Insufficient", "Not_Applicable"}

# Disclosed along a product axis but coarser than the tree. A company whose every current exposure carries one of these
# is a closed disclosure case (db.check_industry coverage_status 'coarse_filing'), not a mapping someone forgot.
# Ruled 2026-09-18 for Granularity_Insufficient. This classifies disclosure granularity only: it does not say a weight
# can be filled, that the exposure is right, that a gap is resolved or that Core is settled. It used to coincide with
# AUTOFILL_BLOCKED and stopped on 2026-09-29, when Not_Applicable joined that set and not this one -- which is the
# clearest statement of why the two are kept apart: this one is about how coarse the disclosure is, that one is
# about whether fill_weight may resolve the item.
COARSE = {"Too_Coarse", "Granularity_Insufficient"}

# Rows whose filing should be re-read for a revenue split that was not there before (scrapers.annual_report_narrative
# --gap). Granularity_Insufficient ruled in 2026-09-18: convergence condition (b) -- the source adds a usable split --
# can only be observed by re-reading. Being in this set is a reason to look again, not a finding that the row is wrong.
REREAD = {"Too_Coarse", "Granularity_Insufficient", "Not_Disclosed", "Not_Extracted"}

# In the enum in code but not yet in the database type. Empty since migration 113 (applied 2026-09-18) added
# Granularity_Insufficient. The gate stays: the next ruled value goes here until its own migration is applied, and
# only leaves after pg_enum is checked. While a value is here, SQL must compare weight_status::text against it --
# an enum comparison with a value the type does not carry is an "invalid input value" error, not an empty result.
PENDING_MIGRATION = set()

_undecided = ALL - AUTOFILL_BLOCKED - {"Not_Disclosed", "Not_Extracted", "Disclosure_Invalid"}
if _undecided:
    raise RuntimeError(f"weight_status values with no auto-fill decision: {sorted(_undecided)}")
if AUTOFILL_BLOCKED - ALL or COARSE - ALL or REREAD - ALL or PENDING_MIGRATION - ALL:
    raise RuntimeError("AUTOFILL_BLOCKED / COARSE / REREAD / PENDING_MIGRATION name a status that is not in ALL")


def blocks_autofill(status):
    """True when fill_weight must leave this row's weight_pct alone."""
    return status in AUTOFILL_BLOCKED


def usable_in_seed(status):
    """(ok, reason) -- whether db.seed_industry may accept this status today."""
    if status is None or status in ALL - PENDING_MIGRATION:
        return True, None
    if status in PENDING_MIGRATION:
        return False, f"{status} is ruled but not yet in the database enum; the migration is not authorized yet"
    return False, f"unknown weight_status {status!r}"
