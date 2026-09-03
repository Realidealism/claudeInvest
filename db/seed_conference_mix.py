"""Load db/conference_mix_data.py into tw.conference_product_mix.

Small and idempotent: every stock named in the file has its rows replaced, so
re-running an unchanged file writes the same table back. Nothing is superseded
or historised — a deck figure is a reading of one slide, and a correction to it
is a correction rather than a new period.

Refuses to write anything if a single row is bad, on the same principle as
db.seed_industry: a partly applied mix is harder to notice than none at all.

The check that matters is the total. A period's lines are compared against
tw.monthly_revenue for the same period and rejected at more than 1% apart,
because that is the failure this whole layer exists to avoid repeating: 1314
中石化's stored mix was a table of tonnes totalling 2.1% of its revenue, and
every downstream percentage derived from it looked entirely well-formed. A
period we hold no filings for is reported as unverified rather than passed.

    python -m db.seed_conference_mix --dry-run
    python -m db.seed_conference_mix
"""

import argparse
import datetime

from db.conference_mix_data import MIXES
from db.connection import get_cursor

TOLERANCE = 0.01


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


def _reported_revenue(cur, stock_id, period_end):
    """Revenue for the period a deck names, or None if we do not hold it all.

    Monthly filings rather than quarterly statements, for two reasons. They go
    up to last month where tw.income_statements stops at the last closed
    quarter, which is the difference between checking 辛耘's 1H2026 and shrugging
    at it — that table holds no 2026 Q2 for any company. And they agree with the
    deck more closely: 辛耘's FY2025 lines total 11,371,557, which is the monthly
    filings to the thousand and 189 above the quarterly statements.

    A period_end alone can only express a period that runs from January, which
    is how Taiwanese decks label cumulative periods and how 辛耘's "2026/Q2"
    turned out to read. A deck meaning a single quarter would fail this check
    loudly rather than pass wrongly, which is the right way round.
    """
    start = "%d-01" % period_end.year
    end = "%d-%02d" % (period_end.year, period_end.month)
    cur.execute("""
        SELECT sum(revenue) AS r, count(*) AS n FROM tw.monthly_revenue
        WHERE stock_id = %s AND year_month BETWEEN %s AND %s
    """, (stock_id, start, end))
    row = cur.fetchone()
    return row["r"] if row["n"] == period_end.month else None


def resolve(cur):
    """File contents as flat rows, plus every problem and every unchecked total."""
    index, dupes = _name_index(cur)
    rows, problems, unverified = [], [], []

    for name, m in MIXES.items():
        stock_id = index.get(name)
        if stock_id is None:
            why = "ambiguous name" if name in dupes else "not an active listed company"
            problems.append(f"{name}: {why}")
            continue
        # The deck has to be one we hold, or the page numbers in the data file
        # point at nothing and the claim cannot be re-checked.
        cur.execute("""
            SELECT 1 FROM tw.investor_conferences
            WHERE stock_id = %s AND conf_date = %s AND file_zh = %s
        """, (stock_id, m["conf_date"], m["source_file"]))
        if cur.fetchone() is None:
            problems.append(f"{name}: no {m['source_file']} on {m['conf_date']} "
                            "in tw.investor_conferences")
            continue

        for label, period_end, items in m["periods"]:
            if period_end > m["conf_date"]:
                problems.append(f"{name} {label}: period ends after the conference")
                continue
            total = sum(rev for _, rev, _ in items)
            reported = _reported_revenue(
                cur, stock_id, datetime.date.fromisoformat(period_end))
            if reported is None:
                unverified.append(f"{name} {label}: no financials held for this "
                                  f"period; lines total {total:,}")
            elif abs(total - float(reported)) / float(reported) > TOLERANCE:
                problems.append(f"{name} {label}: lines total {total:,} against "
                                f"reported revenue {reported:,} "
                                f"({(total - float(reported)) / float(reported) * 100:+.2f}%)")
                continue
            for item, rev, page in items:
                rows.append((stock_id, m["conf_date"], label, period_end, item,
                             rev, m["axis"], page, m["source_file"]))
    return rows, problems, unverified


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="report without writing")
    args = ap.parse_args()

    with get_cursor() as cur:
        rows, problems, unverified = resolve(cur)
        for u in unverified:
            print("unverified —", u)
        if problems:
            print(f"refusing to write — {len(problems)} problem(s):")
            for p in problems:
                print("   ", p)
            return
        stocks = sorted({r[0] for r in rows})
        print(f"{len(rows)} rows across {len(stocks)} company(ies)")
        if args.dry_run:
            print("--dry-run: nothing written")
            return

        cur.execute("DELETE FROM tw.conference_product_mix WHERE stock_id = ANY(%s)",
                    (stocks,))
        cur.executemany("""
            INSERT INTO tw.conference_product_mix
                   (stock_id, conf_date, period_label, period_end, item, revenue,
                    axis, deck_page, source_file)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, rows)
    print(f"wrote {len(rows)} rows")


if __name__ == "__main__":
    main()
