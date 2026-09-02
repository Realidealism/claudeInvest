"""Sync db/industry_data.py into tw.company_industry_exposure.

The file describes the present: every membership that holds right now. The
table describes the present AND the past, so the two are reconciled rather than
mirrored — a membership that disappears from the file is closed with an
effective_to, never deleted. Backtests read the table as of a date and would
silently change their answers if history were rewritten under them.

    python -m db.seed_industry --check          # report, write nothing
    python -m db.seed_industry                  # sync into the FY2025 layer
    python -m db.seed_industry --effective-from 2026-12-31   # open the FY2026 layer

Editing what we believe about a period is a correction and replaces the row.
History opens only when the period does: a superseded row should describe a
window during which the classification really was different, never a window
during which we happened to be wrong.

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
# The annual report evidences the year it reports on, not the day it was filed.
EVIDENCE_PERIOD_END = "2025-12-31"
DATED_EVIDENCE = ("annual_report_table", "annual_report_prose", "filing_too_coarse")


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
    # A company with no Core is allowed (R019): where the exposures cannot be
    # ranked into one, saying so beats inventing a winner. It is reported by
    # db.check_industry rather than refused here.
    coreless = sorted({v["name"] for (sid, _), v in resolved.items() if sid not in cores})
    if coreless:
        print(f"note: {len(coreless)} company(ies) with no Core exposure: "
              + "、".join(coreless[:8]) + ("…" if len(coreless) > 8 else ""))

    return resolved, problems


def current_layer(cur):
    cur.execute("""
        SELECT id, effective_from, stock_id, node_code, role::text, weight_item, evidence_kind,
               evidence, exposure_type::text,
               confidence::text, strategic_priority::text, strategic_evidence,
               supply_chain_position
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


# confidence, strategic_priority, strategic_evidence and supply_chain_position
# are judgements, so they are not in the mirror file — nothing in industry_data
# could restore them. A supersede rebuilds the row from the file, so without
# this they would be silently emptied the first time an unrelated field moved.
CARRIED = ("confidence", "strategic_priority", "strategic_evidence",
           "supply_chain_position")


def _carried(previous):
    """The judgement columns of the row being superseded, or NULLs if new."""
    if previous is None:
        return (None,) * len(CARRIED)
    return tuple(previous[f] for f in CARRIED)


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

    # A change within the same evidence period is a correction of our reading, not
    # a change in the world, and it replaces the row rather than closing it. The
    # alternative writes history that was never true: 精材 booked 44.98% because we
    # had missed its second line, and superseding would have left a period during
    # which the database says that was the figure. A new layer opens when the
    # evidence period does — when the FY2026 reports land, not when we edit.
    corrected = [k for k in changed if str(live[k]["effective_from"]) == str(effective_from)]
    superseded = [k for k in changed if k not in corrected]

    # A membership removed within the same evidence period was never true either,
    # so it is deleted rather than closed — same reasoning as the correction path.
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
                    exposure_type, effective_from, known_date, evidence_period_end,
                    confidence, strategic_priority, strategic_evidence,
                    supply_chain_position)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (stock_id, node_code, w["role"], w["weight_item"], w["evidence"],
              w["evidence_kind"], w["exposure_type"], effective_from,
              known.get(stock_id),
              EVIDENCE_PERIOD_END if w["evidence_kind"] in DATED_EVIDENCE else None,
              *_carried(live.get(key))))

    # known_date is derived from the filing date, which often arrives after the
    # membership does — a company classified from its company site before the
    # annual report is parsed opens with NULL. Fill it in as it becomes
    # available rather than leaving the row invisible to a backtest forever.
    cur.execute("""
        UPDATE tw.company_industry_exposure e
           SET known_date = st.filing_date
          FROM tw.product_revenue_status st
         WHERE st.stock_id = e.stock_id AND st.fiscal_year = %s
           AND st.filing_date IS NOT NULL
           AND e.effective_to IS NULL AND e.known_date IS NULL
    """, (FISCAL_YEAR,))

    return len(added), len(superseded), len(corrected), len(closed), len(retracted)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true", help="report and write nothing")
    ap.add_argument("--effective-from", default=EVIDENCE_PERIOD_END,
                    help="the evidence period this layer belongs to; pass a later "
                         "date only when the facts themselves changed, which is "
                         "when a new filing year arrives — editing our own reading "
                         "is a correction and must keep the same date")
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
