"""Sync db/theme_data.py into tw.company_theme_mapping.

Same contract as db.seed_industry: the file states what holds now, the table
keeps what held before. A membership dropped from the file is closed with an
effective_to rather than deleted, so a backtest reading the table as of a past
date keeps getting the same answer.

    python -m db.seed_themes --check            # report, write nothing
    python -m db.seed_themes                    # sync, dated today
    python -m db.seed_themes --effective-from 2025-12-31

Themes carry no revenue test — that is what separates them from the industry
tree, and why evidence is required on every row instead. The loader refuses the
whole run if any row lacks one, or if any company name fails to resolve.
"""

import argparse
from datetime import date

from db.connection import get_cursor
from db.theme_data import MEMBERS

FIELDS = ("exposure_score", "evidence_kind", "evidence")
FISCAL_YEAR = 2025


def _name_index(cur):
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
    index, dupes = _name_index(cur)
    cur.execute("SELECT theme_code FROM tw.theme_master")
    codes = {r["theme_code"] for r in cur.fetchall()}

    resolved, problems = {}, []
    for (theme_code, sub_theme), members in MEMBERS.items():
        if theme_code not in codes:
            problems.append(f"{theme_code}: not in tw.theme_master")
            continue
        for name, score, evidence_kind, evidence in members:
            stock_id = index.get(name)
            if stock_id is None:
                why = "ambiguous name" if name in dupes else "not an active listed company"
                problems.append(f"{theme_code}/{sub_theme} / {name}: {why}")
                continue
            if not (evidence or "").strip():
                problems.append(f"{theme_code}/{sub_theme} / {name}: empty evidence")
                continue
            key = (stock_id, theme_code, sub_theme)
            if key in resolved:
                problems.append(f"{theme_code}/{sub_theme} / {name}: listed twice")
                continue
            resolved[key] = {"exposure_score": score, "evidence_kind": evidence_kind,
                             "evidence": evidence, "name": name}
    return resolved, problems


def current_layer(cur):
    cur.execute("""
        SELECT id, effective_from, stock_id, theme_code, sub_theme, exposure_score,
               evidence_kind, evidence
        FROM tw.company_theme_mapping
        WHERE effective_to IS NULL
    """)
    return {(r["stock_id"], r["theme_code"], r["sub_theme"]): dict(r)
            for r in cur.fetchall()}


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

    # Same rule as db.seed_industry: a change dated to the row's own start date
    # is a correction and is applied in place, not turned into history.
    corrected = [k for k in changed if str(live[k]["effective_from"]) == str(effective_from)]
    superseded = [k for k in changed if k not in corrected]

    # A membership removed on the very day it started never held for any window.
    # Closing it would leave a zero-length row that no as-of query can ever see,
    # so it is deleted instead — the same reasoning as the correction path above.
    retracted = [k for k in closed if str(live[k]["effective_from"]) == str(effective_from)]
    for key in retracted:
        cur.execute("DELETE FROM tw.company_theme_mapping WHERE id = %s", (live[key]["id"],))

    for key in [k for k in closed if k not in retracted] + superseded:
        cur.execute(
            "UPDATE tw.company_theme_mapping SET effective_to = %s WHERE id = %s",
            (effective_from, live[key]["id"]))

    for key in corrected:
        w = resolved[key]
        cur.execute("""
            UPDATE tw.company_theme_mapping
               SET exposure_score = %s, evidence_kind = %s, evidence = %s
             WHERE id = %s
        """, (w["exposure_score"], w["evidence_kind"], w["evidence"], live[key]["id"]))

    for key in added + superseded:
        stock_id, theme_code, sub_theme = key
        w = resolved[key]
        cur.execute("""
            INSERT INTO tw.company_theme_mapping
                   (stock_id, theme_code, sub_theme, exposure_score,
                    evidence, evidence_kind, effective_from, known_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (stock_id, theme_code, sub_theme, w["exposure_score"], w["evidence"],
              w["evidence_kind"], effective_from, known.get(stock_id)))

    return len(added), len(superseded), len(corrected), len(closed), len(retracted)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true", help="report and write nothing")
    ap.add_argument("--effective-from", default=date.today().isoformat())
    args = ap.parse_args()

    with get_cursor() as cur:
        resolved, problems = resolve(cur)
        if problems:
            print(f"REFUSING TO WRITE — {len(problems)} problem(s):")
            for p in problems[:40]:
                print("  -", p)
            raise SystemExit(1)

        companies = len({s for s, _, _ in resolved})
        print(f"file: {len(resolved)} memberships over {companies} companies, "
              f"{len(MEMBERS)} themes")
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


if __name__ == "__main__":
    main()
