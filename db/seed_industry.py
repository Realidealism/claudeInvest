"""Sync db/industry_data.py into tw.company_industry_exposure.

The file describes the present: every membership that holds right now. The
table describes the present AND the past, so the two are reconciled rather than
mirrored — a membership that disappears from the file is closed with an
effective_to, never deleted. Backtests read the table as of a date and would
silently change their answers if history were rewritten under them.

    python -m db.seed_industry --check          # report, write nothing
    python -m db.seed_industry                  # sync, dated today
    python -m db.seed_industry --effective-from 2025-12-31

Company names, not ids: an active listed name resolves to one company, and a
mistyped id resolves quietly to the wrong one. Any name that fails to resolve
aborts the whole run before a single row is written.

known_date comes from tw.product_revenue_status.filing_date — the day the
annual report behind the evidence was filed with MOPS. It stays NULL when the
company filed no report; that is honest, and a backtest filtering on
known_date will simply not see the row.

weight_pct is not set here. db.fill_weight reads weight_item against
tw.product_revenue and fills it, so re-run that after any sync that opened rows.
"""

import argparse
from datetime import date

from db.connection import get_cursor
from db.industry_data import MEMBERS

FIELDS = ("role", "weight_item", "evidence_kind", "evidence", "exposure_type")
FISCAL_YEAR = 2025


def _name_index(cur):
    """Active ordinary shares by name. Names are unique within that set."""
    cur.execute("""
        SELECT stock_id, name FROM tw.stocks
        WHERE is_active AND security_type = 'STOCK'
    """)
    index, dupes = {}, set()
    for r in cur.fetchall():
        if r["name"] in index:
            dupes.add(r["name"])
        index[r["name"]] = r["stock_id"]
    for d in dupes:
        del index[d]
    return index, dupes


def resolve(cur):
    """File contents keyed by (stock_id, node_code); raises on any bad row."""
    index, dupes = _name_index(cur)
    cur.execute("SELECT node_code FROM tw.industry_tree")
    nodes = {r["node_code"] for r in cur.fetchall()}

    resolved, problems = {}, []
    for node_code, members in MEMBERS.items():
        if node_code not in nodes:
            problems.append(f"{node_code}: not a node in tw.industry_tree")
            continue
        for name, role, weight_item, evidence_kind, evidence, exposure_type in members:
            stock_id = index.get(name)
            if stock_id is None:
                why = "ambiguous name" if name in dupes else "not an active listed company"
                problems.append(f"{node_code} / {name}: {why}")
                continue
            if not (evidence or "").strip():
                problems.append(f"{node_code} / {name}: empty evidence")
                continue
            key = (stock_id, node_code)
            if key in resolved:
                problems.append(f"{node_code} / {name}: listed twice under the same node")
                continue
            resolved[key] = {
                "role": role, "weight_item": weight_item,
                "evidence_kind": evidence_kind, "evidence": evidence,
                "exposure_type": exposure_type, "name": name,
            }

    cores = {}
    for (stock_id, node_code), v in resolved.items():
        if v["role"] == "Core":
            if stock_id in cores:
                problems.append(f"{v['name']}: Core on both {cores[stock_id]} and {node_code}")
            cores[stock_id] = node_code
    for (stock_id, _), v in resolved.items():
        if stock_id not in cores:
            problems.append(f"{v['name']}: no Core exposure")
            cores[stock_id] = None

    return resolved, problems


def current_layer(cur):
    cur.execute("""
        SELECT id, effective_from, stock_id, node_code, role::text, weight_item, evidence_kind,
               evidence, exposure_type::text
        FROM tw.company_industry_exposure
        WHERE effective_to IS NULL
    """)
    return {(r["stock_id"], r["node_code"]): dict(r) for r in cur.fetchall()}


def _known_dates(cur):
    cur.execute("""
        SELECT stock_id, filing_date FROM tw.product_revenue_status
        WHERE fiscal_year = %s AND filing_date IS NOT NULL
    """, (FISCAL_YEAR,))
    return {r["stock_id"]: r["filing_date"] for r in cur.fetchall()}


def sync(cur, resolved, effective_from):
    live = current_layer(cur)
    known = _known_dates(cur)

    added, changed, closed = [], [], []
    for key, want in resolved.items():
        have = live.get(key)
        if have is None:
            added.append(key)
        elif any(have[f] != want[f] for f in FIELDS):
            changed.append(key)
    for key in live:
        if key not in resolved:
            closed.append(key)

    # A change dated to the day the row already starts is a correction, not a new
    # version: closing it would leave a zero-length row and collide with the
    # (stock, node, effective_from) uniqueness. Only a later date makes history.
    corrected = [k for k in changed if str(live[k]["effective_from"]) == str(effective_from)]
    superseded = [k for k in changed if k not in corrected]

    # A membership removed on the very day it started never held for any window.
    # Closing it would leave a zero-length row that no as-of query can ever see,
    # so it is deleted instead — the same reasoning as the correction path above.
    retracted = [k for k in closed if str(live[k]["effective_from"]) == str(effective_from)]
    for key in retracted:
        cur.execute("DELETE FROM tw.company_industry_exposure WHERE id = %s", (live[key]["id"],))

    for key in [k for k in closed if k not in retracted] + superseded:
        cur.execute(
            "UPDATE tw.company_industry_exposure SET effective_to = %s WHERE id = %s",
            (effective_from, live[key]["id"]))

    # Demotions before promotions: only one row per company may be the current
    # Core, and the index enforcing that is not deferrable, so moving a Core from
    # one node to another has to give it up before claiming it.
    corrected.sort(key=lambda k: resolved[k]["role"] == "Core")

    for key in corrected:
        w = resolved[key]
        cur.execute("""
            UPDATE tw.company_industry_exposure
               SET role = %s, weight_item = %s, evidence_kind = %s,
                   evidence = %s, exposure_type = %s
             WHERE id = %s
        """, (w["role"], w["weight_item"], w["evidence_kind"], w["evidence"],
              w["exposure_type"], live[key]["id"]))

    for key in added + superseded:
        stock_id, node_code = key
        w = resolved[key]
        cur.execute("""
            INSERT INTO tw.company_industry_exposure
                   (stock_id, node_code, role, weight_item, evidence, evidence_kind,
                    exposure_type, effective_from, known_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (stock_id, node_code, w["role"], w["weight_item"], w["evidence"],
              w["evidence_kind"], w["exposure_type"], effective_from,
              known.get(stock_id)))

    return len(added), len(superseded), len(corrected), len(closed), len(retracted)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true", help="report and write nothing")
    ap.add_argument("--effective-from", default=date.today().isoformat(),
                    help="the day the current layer takes effect (default today)")
    args = ap.parse_args()

    with get_cursor() as cur:
        resolved, problems = resolve(cur)
        if problems:
            print(f"REFUSING TO WRITE — {len(problems)} problem(s):")
            for p in problems[:40]:
                print("  -", p)
            raise SystemExit(1)

        companies = len({s for s, _ in resolved})
        print(f"file: {len(resolved)} memberships over {companies} companies, "
              f"{len(MEMBERS)} nodes")
        if args.check:
            live = current_layer(cur)
            print(f"db:   {len(live)} current memberships")
            print(f"would add {len(set(resolved) - set(live))}, "
                  f"close {len(set(live) - set(resolved))}")
            print("--check: nothing written")
            return

        added, superseded, corrected, closed, retracted = sync(cur, resolved, args.effective_from)

    print(f"added {added}, superseded {superseded}, corrected {corrected}, "
          f"closed {closed - retracted}, retracted {retracted}")
    print("now run: python -m db.fill_weight")


if __name__ == "__main__":
    main()
