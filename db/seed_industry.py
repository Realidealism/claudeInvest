"""Sync db/industry_data.py into tw.company_industry_exposure.

The file describes the present: every membership that holds right now. The
table describes the present AND the past, so the two are reconciled rather than
mirrored — a membership that disappears from the file is closed with an
effective_to, never deleted. Backtests read the table as of a date and would
silently change their answers if history were rewritten under them.

    python -m db.seed_industry --check          # report, write nothing
    python -m db.seed_industry                  # sync into the FY2025 layer
    python -m db.seed_industry --with-tree      # sync the tree in the same transaction
    python -m db.seed_industry --effective-from 2026-12-31   # open the FY2026 layer

--with-tree is what makes splitting a leaf that has members possible. The
exposures may only move onto the new children, and the leaf guard refuses
children while the parent still has members, so the two edits have to commit
together or neither is reachable. Use it whenever db/industry_tree_data.py and
db/industry_data.py change in the same breath.

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

An 'investor_conf' row is dated by its deck instead, because that is what it is
evidence of: evidence_period_end is the newest period the deck reports and
known_date is the conference date. Taking the annual report's dates for it would
say a 2026/Q2 figure was known in May and describes FY2025, and both halves of
that are false.

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


def _deck_dates(cur):
    """(evidence_period_end, known_date) per company, from its newest deck period.

    Matches db.fill_weight, which takes the most recent period a deck gives; the
    date has to describe the same figure the weight came from, or the row says
    one period and means another. The usability test has to match fill_weight's
    too — it is stated twice and the two have already disagreed once, when this
    one still required a printed pct after the data file moved to amounts.

    Only reached for rows whose evidence_kind is 'investor_conf' AND whose
    company has deck lines at all. 台光電, 台燿 and 聯茂 carry deck evidence but
    read their weights off annual-report lines, and have no rows here, so they
    keep the report's dates — which is what their weights describe.
    """
    cur.execute("""
        SELECT DISTINCT ON (stock_id) stock_id, period_end, conf_date
        FROM tw.conference_product_mix
        WHERE axis IN ('product', 'business_unit')
          AND (pct IS NOT NULL OR revenue IS NOT NULL)
        ORDER BY stock_id, period_end DESC
    """)
    return {r["stock_id"]: (r["period_end"], r["conf_date"]) for r in cur.fetchall()}


def _dates_for(w, stock_id, known, decks):
    """(known_date, evidence_period_end) for one row, by what evidences it."""
    if w["evidence_kind"] == "investor_conf" and stock_id in decks:
        period_end, conf_date = decks[stock_id]
        return conf_date, period_end
    return (known.get(stock_id),
            EVIDENCE_PERIOD_END if w["evidence_kind"] in DATED_EVIDENCE else None)


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
    decks = _deck_dates(cur)

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

    # The dates move with the correction. A row re-read from a deck rather than
    # the annual report changes what period it evidences and when that became
    # knowable, and leaving those at the old values is how a row comes to claim
    # a date its evidence never had.
    for key in corrected:
        w = resolved[key]
        known_date, period_end = _dates_for(w, key[0], known, decks)
        # weight_pct belongs to db.fill_weight, with one exception: a row being
        # corrected to filing_too_coarse may not keep the share it used to
        # claim. The constraint from migration 100 rejects the pair outright, so
        # leaving it made "this line turned out to span two nodes" -- 6877
        # 鏵友益 42.78% on 2026-09-07 -- an edit the seeder could not express.
        cur.execute("""
            UPDATE tw.company_industry_exposure
               SET role = %s, weight_item = %s, evidence_kind = %s,
                   evidence = %s, exposure_type = %s,
                   known_date = %s, evidence_period_end = %s,
                   weight_pct = CASE WHEN %s = 'filing_too_coarse'
                                     THEN NULL ELSE weight_pct END
             WHERE id = %s
        """, (w["role"], w["weight_item"], w["evidence_kind"], w["evidence"],
              w["exposure_type"], known_date, period_end, w["evidence_kind"],
              live[key]["id"]))

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
              *_dates_for(w, stock_id, known, decks),
              *_carried(live.get(key))))

    # Deck-dated rows are reconciled every run, not only when one of the mirrored
    # fields moved. The dates are derived from tw.conference_product_mix rather
    # than written in db/industry_data.py, so a newer deck period changes them
    # while the membership file stays byte-identical — and the correction branch,
    # which only fires when a mirrored field differs, would never see it.
    cur.execute("""
        UPDATE tw.company_industry_exposure e
           SET known_date = d.conf_date, evidence_period_end = d.period_end
          FROM (SELECT DISTINCT ON (stock_id) stock_id, period_end, conf_date
                  FROM tw.conference_product_mix
                 WHERE axis IN ('product', 'business_unit')
                   AND (pct IS NOT NULL OR revenue IS NOT NULL)
                 ORDER BY stock_id, period_end DESC) d
         WHERE d.stock_id = e.stock_id AND e.effective_to IS NULL
           AND e.evidence_kind = 'investor_conf'
           AND (e.known_date IS DISTINCT FROM d.conf_date
             OR e.evidence_period_end IS DISTINCT FROM d.period_end)
    """)

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
    ap.add_argument("--with-tree", action="store_true",
                    help="sync tw.industry_tree from db/industry_tree_data.py in "
                         "the same transaction; required when a node is split")
    ap.add_argument("--effective-from", default=EVIDENCE_PERIOD_END,
                    help="the evidence period this layer belongs to; pass a later "
                         "date only when the facts themselves changed, which is "
                         "when a new filing year arrives — editing our own reading "
                         "is a correction and must keep the same date")
    args = ap.parse_args()

    with get_cursor() as cur:
        # Before resolve(), which validates node codes against the table: a node
        # added to the file this round has to exist by the time it is checked.
        if args.with_tree and not args.check:
            from db.seed_skeleton import sync_tree, validate
            bad = validate()
            if bad:
                print(f"REFUSING TO WRITE — {len(bad)} problem(s) in the tree:")
                for b in bad[:20]:
                    print("  -", b)
                raise SystemExit(1)
            n_nodes, n_stale = sync_tree(cur)
            print(f"tree: {n_nodes} nodes upserted, {n_stale} removed")

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
