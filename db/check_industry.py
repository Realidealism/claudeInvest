"""Read-only quality checks over the industry classification.

Nothing here writes. Membership edits belong in db/industry_data.py followed by
db.seed_industry; shares belong to db.fill_weight.

Checks are deliberately limited to what CAN go wrong. The database already
refuses a second current Core, an unknown node code, an empty evidence string
and an effective_to before its effective_from, so re-testing those would only
produce a wall of green that teaches nothing. What is left is the drift the
schema cannot see:

    core        a company with exposures but no Core at all
    listing     a member that has been delisted, suspended, or is not an
                ordinary share any more
    stale       a component of weight_item no longer matches any line in the
                current annual report — usually the company renamed its
                segments. weight_item may name several lines joined by " + "
                when one node owns more than one of them, and each is checked
    below       the share behind an exposure is small, which may be correct for
                a niche node and is a question either way
    double      the same revenue line carrying a share on two nodes (R004)
    depth       an exposure hanging on a branch node rather than a leaf
    unknown     evidence recorded as pending, i.e. an admitted gap
    known       exposures with no known_date, invisible to a backtest that
                filters on it
    official    a listed company with no official-classification row. Emerging
                board (ESB) companies are excluded: the exchange assigns no
                industry category before a full listing, so their blank is the
                truth rather than a gap

A finding is a question, not a verdict.

    python -m db.check_industry
    python -m db.check_industry --check below --below-threshold 10
"""

import argparse

from db.connection import get_cursor

CHECKS = ("core", "listing", "stale", "below", "double", "depth",
          "unknown", "known", "official")
FISCAL_YEAR = 2025


def _rows(cur, sql, params=()):
    cur.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


def check_core(cur):
    return _rows(cur, """
        SELECT e.stock_id, s.name, count(*) AS exposures
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        WHERE e.effective_to IS NULL
        GROUP BY e.stock_id, s.name
        HAVING count(*) FILTER (WHERE e.role = 'Core') = 0
        ORDER BY e.stock_id
    """)


def check_listing(cur):
    return _rows(cur, """
        SELECT e.stock_id, s.name, e.node_code, s.is_active,
               s.security_type, s.delisted_date
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        WHERE e.effective_to IS NULL
          AND (NOT s.is_active OR s.security_type <> 'STOCK'
               OR s.delisted_date IS NOT NULL)
        ORDER BY e.stock_id
    """)


def check_stale(cur):
    return _rows(cur, """
        WITH part AS (
            SELECT e.stock_id, e.node_code, s.name,
                   btrim(unnest(string_to_array(e.weight_item, ' + '))) AS item
            FROM tw.company_industry_exposure e
            JOIN tw.stocks s ON s.stock_id = e.stock_id
            WHERE e.effective_to IS NULL AND e.weight_item IS NOT NULL
        )
        SELECT p.stock_id, p.name, p.node_code, p.item AS missing_item
        FROM part p
        WHERE NOT EXISTS (
            SELECT 1 FROM tw.product_revenue_share r
            WHERE r.stock_id = p.stock_id AND r.fiscal_year = %s
              AND r.item = p.item)
        ORDER BY p.stock_id
    """, (FISCAL_YEAR,))


def check_below(cur, threshold):
    return _rows(cur, """
        SELECT e.stock_id, s.name, e.node_code, t.name AS node_name,
               e.weight_pct, e.weight_item, e.role::text
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        JOIN tw.industry_tree t ON t.node_code = e.node_code
        WHERE e.effective_to IS NULL AND e.weight_pct IS NOT NULL
          AND e.weight_pct < %s
        ORDER BY e.weight_pct
    """, (threshold,))


def check_double(cur):
    return _rows(cur, """
        SELECT e.stock_id, s.name, e.weight_item,
               array_agg(e.node_code ORDER BY e.node_code) AS nodes
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        WHERE e.effective_to IS NULL AND e.weight_pct IS NOT NULL
        GROUP BY e.stock_id, s.name, e.weight_item
        HAVING count(*) > 1
        ORDER BY e.stock_id
    """)


def check_depth(cur):
    return _rows(cur, """
        SELECT e.stock_id, s.name, e.node_code, t.name AS node_name, t.level
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        JOIN tw.industry_tree t ON t.node_code = e.node_code
        WHERE e.effective_to IS NULL
          AND EXISTS (SELECT 1 FROM tw.industry_tree c WHERE c.parent_code = t.node_code)
        ORDER BY e.stock_id
    """)


def check_unknown(cur):
    return _rows(cur, """
        SELECT e.stock_id, s.name, e.node_code, e.evidence_kind
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        WHERE e.effective_to IS NULL AND e.evidence_kind = 'pending'
        ORDER BY e.stock_id
    """)


def check_known(cur):
    return _rows(cur, """
        SELECT e.stock_id, s.name, count(*) AS exposures
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        WHERE e.effective_to IS NULL AND e.known_date IS NULL
        GROUP BY e.stock_id, s.name
        ORDER BY e.stock_id
    """)


def check_official(cur):
    return _rows(cur, """
        SELECT DISTINCT e.stock_id, s.name
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        WHERE e.effective_to IS NULL AND s.market <> 'ESB'
          AND NOT EXISTS (
              SELECT 1 FROM tw.company_official_classification o
              WHERE o.stock_id = e.stock_id AND o.effective_to IS NULL)
        ORDER BY e.stock_id
    """)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", choices=CHECKS, action="append",
                    help="run only these checks (repeatable)")
    ap.add_argument("--below-threshold", type=float, default=20.0)
    ap.add_argument("--limit", type=int, default=15, help="rows printed per check")
    args = ap.parse_args()

    wanted = args.check or list(CHECKS)
    with get_cursor() as cur:
        cur.execute("SELECT count(*) c FROM tw.company_industry_exposure WHERE effective_to IS NULL")
        live = cur.fetchone()["c"]
        print(f"{live} current exposures\n")

        for name in wanted:
            findings = (check_below(cur, args.below_threshold) if name == "below"
                        else globals()[f"check_{name}"](cur))
            print(f"[{name}] {len(findings)}")
            for f in findings[:args.limit]:
                print("   ", {k: v for k, v in f.items() if v is not None})
            if len(findings) > args.limit:
                print(f"    ... and {len(findings) - args.limit} more")
            print()


if __name__ == "__main__":
    main()
