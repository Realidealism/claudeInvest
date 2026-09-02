"""The Core Candidate Set of R019, as code rather than as prose.

R019 defines the set with four predicates and a grouping. Written out they are
the whole of it — no judgement is involved in *forming* the set, only in
choosing from it (R023/R024). So the set can be computed, and where it holds
exactly one member the Core follows with no judgement at all: it is an identity.

That split is what this module exists for. It computes the set, hands back the
companies whose Core is an identity, and hands back the ones where a person
actually decided. The first group can be verified mechanically against what is
stored; the second cannot, and pretending otherwise is what R019 warns about.

The eligibility predicate deliberately does not read role = 'Core'/'Major'/
'Minor'. Those are outputs of the rule; feeding them back makes the Core
self-reinforcing (R019). The only role value consulted is 'Emerging', which
records a fact about the business rather than a ranking.

    python -m db.core_candidates             # verify every company
    python -m db.core_candidates --as-of 2025-06-30
"""

import argparse
import datetime

from db.connection import get_cursor

# R019: as-of valid, Direct, leaf, business status not Emerging. weight_pct is
# read but never filtered on — 超豐 is a Core without one.
CANDIDATES = """
    SELECT e.stock_id, s.name, e.node_code, t.name AS node_name,
           e.role::text, e.weight_pct, e.evidence
    FROM tw.company_industry_exposure e
    JOIN tw.stocks s ON s.stock_id = e.stock_id
    JOIN tw.industry_tree t ON t.node_code = e.node_code
    WHERE e.effective_from <= %(as_of)s
      AND (e.effective_to IS NULL OR %(as_of)s < e.effective_to)
      AND e.exposure_type = 'Direct'
      AND e.role IS DISTINCT FROM 'Emerging'
      AND NOT EXISTS (SELECT 1 FROM tw.industry_tree c
                      WHERE c.parent_code = t.node_code)
    ORDER BY e.stock_id, e.weight_pct DESC NULLS LAST, e.node_code
"""

STORED_CORE = """
    SELECT e.stock_id, e.node_code, e.exposure_type::text, e.evidence,
           NOT EXISTS (SELECT 1 FROM tw.industry_tree c
                       WHERE c.parent_code = e.node_code) AS on_leaf
    FROM tw.company_industry_exposure e
    WHERE e.role = 'Core'
      AND e.effective_from <= %(as_of)s
      AND (e.effective_to IS NULL OR %(as_of)s < e.effective_to)
"""


def candidate_sets(cur, as_of):
    """{stock_id: [candidate, ...]}, deduplicated on (stock_id, node_code).

    The unique index already allows only one row per (stock_id, node_code) in a
    period, so the dedup never fires today. It is here because R019 requires the
    set to be keyed that way, not because the table currently needs it.
    """
    cur.execute(CANDIDATES, {"as_of": as_of})
    sets = {}
    for r in cur.fetchall():
        per_node = sets.setdefault(r["stock_id"], {})
        per_node.setdefault(r["node_code"], dict(r))
    return {sid: list(nodes.values()) for sid, nodes in sets.items()}


def verify(cur, as_of):
    """Compare every company's stored Core against what R019 can settle."""
    sets = candidate_sets(cur, as_of)
    cur.execute(STORED_CORE, {"as_of": as_of})
    stored = {r["stock_id"]: dict(r) for r in cur.fetchall()}

    identity, adjudicated, findings = [], [], []
    for stock_id, cands in sets.items():
        core = stored.get(stock_id)
        if core is None:
            findings.append(("no_core", stock_id, cands[0]["name"],
                             f"{len(cands)} candidates, no Core row (R019 pending)"))
            continue
        if len(cands) == 1:
            identity.append(stock_id)
            if core["node_code"] != cands[0]["node_code"]:
                findings.append(("identity_mismatch", stock_id, cands[0]["name"],
                                 f"only candidate is {cands[0]['node_code']}, "
                                 f"stored Core is {core['node_code']}"))
        else:
            adjudicated.append(stock_id)
            if core["node_code"] not in {c["node_code"] for c in cands}:
                findings.append(("core_outside_set", stock_id, cands[0]["name"],
                                 f"Core {core['node_code']} is not a candidate "
                                 f"({', '.join(c['node_code'] for c in cands)})"))
            elif "Core 理由" not in (core["evidence"] or ""):
                findings.append(("no_reason", stock_id, cands[0]["name"],
                                 f"{len(cands)} candidates, adjudication not recorded"))

    # A Core that never reaches the candidate set at all: Indirect, or on a
    # branch node. Neither is caught above, because such a company may have no
    # candidates and so no entry in `sets`.
    for stock_id, core in stored.items():
        if core["exposure_type"] != "Direct":
            findings.append(("core_indirect", stock_id, "",
                             f"Core {core['node_code']} is {core['exposure_type']}"))
        if not core["on_leaf"]:
            findings.append(("core_on_branch", stock_id, "",
                             f"Core {core['node_code']} is not a leaf"))
        if stock_id not in sets:
            findings.append(("core_no_candidates", stock_id, "",
                             f"Core {core['node_code']} but the candidate set is empty"))
    return sets, identity, adjudicated, findings


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--as-of", default=str(datetime.date.today()))
    ap.add_argument("--limit", type=int, default=20)
    args = ap.parse_args()

    with get_cursor() as cur:
        sets, identity, adjudicated, findings = verify(cur, args.as_of)

    print(f"as-of {args.as_of}: {len(sets)} companies with candidates  "
          f"identity {len(identity)}  adjudicated {len(adjudicated)}")
    by_kind = {}
    for kind, stock_id, name, why in findings:
        by_kind.setdefault(kind, []).append((stock_id, name, why))
    if not findings:
        print("no findings")
    for kind, rows in sorted(by_kind.items()):
        print(f"\n[{kind}] {len(rows)}")
        for stock_id, name, why in rows[:args.limit]:
            print(f"    {stock_id} {name} — {why}")
        if len(rows) > args.limit:
            print(f"    ... and {len(rows) - args.limit} more")


if __name__ == "__main__":
    main()
