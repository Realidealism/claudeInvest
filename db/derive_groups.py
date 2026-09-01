"""Derive listed group membership from real shareholding.

R014 separates two things the spreadsheet had mixed together: a group is
ownership, an ecosystem is commerce. Ownership we already hold as hard data —
tw.company_affiliations carries 31k MOPS 轉投資 rows — so membership here is
computed from it and only the group's name and anchor are authored, in
db/group_data.py.

Membership is a directed descent from the anchor: the companies the anchor
controls, then what those control. That matches what a group means. Treating the
graph as undirected was tried and rejected — it merges unrelated groups through
any shared holding (日月光 grew from 7 members to 54).

Two kinds of group derive nothing, and both are correct rather than broken:

  * financial holdings (中信、富邦、國泰、中租) own banks and insurers that are
    not separately listed. 富邦金 has 45 investments and none is a listed
    company; the group genuinely has no other listed member to find.
  * 台塑集團 is held sideways. 台化 owns 19% of 南亞 and 台塑化 owns 29%, but
    the anchor 台塑 itself holds no listed peer, so nothing descends from it.
    Its members would have to be authored by hand.

Only 2025Q4 is read. Q1-Q3 filings list movements rather than the full holding,
so a subsidiary missing from a Q2 filing has not been sold — it simply was not
restated, and treating that as a disposal would close memberships that never
ended.

    python -m db.derive_groups --check
    python -m db.derive_groups
"""

import argparse
import collections
from datetime import date

from db.connection import get_cursor
from db.group_data import GROUPS

PERIOD = "2025Q4"
MIN_HOLDING = 10.0    # below this the holding is an investment, not control


def _strength(pct):
    if pct >= 50:
        return 5
    if pct >= 30:
        return 4
    if pct >= 20:
        return 3
    return 2


def _edges(cur):
    cur.execute("""
        SELECT parent_id, investee_id, holding_pct
        FROM tw.company_affiliations
        WHERE period = %s AND investee_id IS NOT NULL AND holding_pct >= %s
    """, (PERIOD, MIN_HOLDING))
    edges = collections.defaultdict(list)
    for r in cur.fetchall():
        edges[r["parent_id"]].append((r["investee_id"], float(r["holding_pct"])))
    return edges


def derive(cur):
    cur.execute("SELECT stock_id, name FROM tw.stocks WHERE is_active AND security_type = 'STOCK'")
    index = {}
    for r in cur.fetchall():
        index.setdefault(r["name"], r["stock_id"])
    edges = _edges(cur)

    groups, problems = {}, []
    for code, name, kind, anchor_name, note in GROUPS:
        anchor = index.get(anchor_name)
        if anchor is None:
            problems.append(f"{code} {name}: anchor {anchor_name!r} is not an active listed company")
            continue
        members, frontier = {}, [(anchor, None)]
        while frontier:
            node, _ = frontier.pop()
            for investee, pct in edges.get(node, ()):
                if investee == anchor or investee in members:
                    continue
                members[investee] = (pct, node)
                frontier.append((investee, node))
        groups[code] = {"name": name, "kind": kind, "anchor": anchor,
                        "note": note, "members": members}
    return groups, problems


def sync(cur, groups, effective_from):
    for code, g in groups.items():
        cur.execute("""
            INSERT INTO tw.business_group (group_code, group_name, group_type, anchor_stock_id, note)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (group_code) DO UPDATE SET
                group_name = EXCLUDED.group_name,
                group_type = EXCLUDED.group_type,
                anchor_stock_id = EXCLUDED.anchor_stock_id,
                note = EXCLUDED.note
        """, (code, g["name"], g["kind"], g["anchor"], g["note"]))

    cur.execute("""
        SELECT id, stock_id, group_code FROM tw.company_group WHERE effective_to IS NULL
    """)
    live = {(r["stock_id"], r["group_code"]): r["id"] for r in cur.fetchall()}

    want = {}
    for code, g in groups.items():
        for stock_id, (pct, via) in g["members"].items():
            want[(stock_id, code)] = (pct, via)

    added = closed = 0
    for key in live:
        if key not in want:
            cur.execute("UPDATE tw.company_group SET effective_to = %s WHERE id = %s",
                        (effective_from, live[key]))
            closed += 1
    for (stock_id, code), (pct, via) in want.items():
        if (stock_id, code) in live:
            continue
        relation = "母子公司" if pct >= 50 else "關係企業"
        cur.execute("""
            INSERT INTO tw.company_group
                   (stock_id, group_code, relation_type, linkage_strength,
                    derived_from, effective_from)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (stock_id, code, relation, _strength(pct),
              f"tw.company_affiliations {PERIOD}: {via} 持股 {pct:.2f}%", effective_from))
        added += 1
    return added, closed


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--effective-from", default=date.today().isoformat())
    args = ap.parse_args()

    with get_cursor() as cur:
        groups, problems = derive(cur)
        if problems:
            print(f"REFUSING TO WRITE — {len(problems)} problem(s):")
            for p in problems:
                print("  -", p)
            raise SystemExit(1)

        empty = [f"{c} {g['name']}" for c, g in groups.items() if not g["members"]]
        total = sum(len(g["members"]) for g in groups.values())
        print(f"{len(groups)} groups, {total} derived memberships")
        if empty:
            print(f"no listed member derivable for {len(empty)}: " + ", ".join(empty))
        if args.check:
            for code, g in sorted(groups.items()):
                print(f"  {code} {g['name']:12s} {len(g['members']):2d}")
            print("--check: nothing written")
            return

        added, closed = sync(cur, groups, args.effective_from)

    print(f"added {added}, closed {closed}")


if __name__ == "__main__":
    main()
