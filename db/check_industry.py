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

CHECKS = ("core", "core_overlap", "core_reason", "listing", "stale", "below", "role", "coverage",
          "temporal",
          "double", "depth",
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


def check_core_overlap(cur):
    """Companies with two Core rows whose validity periods overlap (R029).

    The unique index only covers the current layer, so nothing stops a history
    row from being written across another one — and then "which node was the
    Core in 2024" has two answers. Intervals are [effective_from, effective_to),
    so touching endpoints are fine and only real overlap is reported.
    """
    return _rows(cur, """
        SELECT a.stock_id, s.name,
               a.node_code AS node_a, a.effective_from AS from_a, a.effective_to AS to_a,
               b.node_code AS node_b, b.effective_from AS from_b, b.effective_to AS to_b
        FROM tw.company_industry_exposure a
        JOIN tw.company_industry_exposure b
          ON b.stock_id = a.stock_id AND b.id > a.id
        JOIN tw.stocks s ON s.stock_id = a.stock_id
        WHERE a.role = 'Core' AND b.role = 'Core'
          AND a.effective_from < coalesce(b.effective_to, 'infinity'::date)
          AND b.effective_from < coalesce(a.effective_to, 'infinity'::date)
        ORDER BY a.stock_id
    """)


def check_core_reason(cur):
    """Cores that were adjudicated but did not say why (R019).

    Where a company has one candidate the Core is an identity and needs no
    argument. Where it has several, R023 was actually applied — someone weighed
    capability against value creation against chain role — and that reasoning is
    the only thing separating the result from a preference. It goes in the Core
    row's evidence, marked "Core 理由：".

    383 companies are the identity case and never appear here; the list is the
    64 where a real decision was made.
    """
    return _rows(cur, """
        WITH candidates AS (
            SELECT e.stock_id, count(*) AS n
            FROM tw.company_industry_exposure e
            JOIN tw.industry_tree t ON t.node_code = e.node_code
            WHERE e.effective_to IS NULL AND e.exposure_type = 'Direct'
              AND e.role IS DISTINCT FROM 'Emerging'
              AND NOT EXISTS (SELECT 1 FROM tw.industry_tree c
                              WHERE c.parent_code = t.node_code)
            GROUP BY e.stock_id
        )
        SELECT e.stock_id, s.name, e.node_code, c.n AS candidates
        FROM candidates c
        JOIN tw.company_industry_exposure e
          ON e.stock_id = c.stock_id AND e.effective_to IS NULL AND e.role = 'Core'
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        WHERE c.n > 1 AND e.evidence NOT LIKE '%%Core 理由%%'
        ORDER BY c.n DESC, e.stock_id
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


def check_role(cur):
    """Non-Core rows whose role disagrees with R026's 15% default.

    The default is only a default — a named reason in the evidence makes any
    label legitimate — so this cannot decide who is wrong. It lists the rows a
    reader has to look at, which is what stops the column drifting back to the
    state R026 was written to end (Major at 6.90%, Minor at 19.26%).

    Emerging and Indirect rows are outside the numeric default and excluded.
    """
    return _rows(cur, """
        SELECT e.stock_id, s.name, e.node_code, e.role::text, e.weight_pct
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        WHERE e.effective_to IS NULL AND e.role NOT IN ('Core', 'Emerging')
          AND e.exposure_type = 'Direct' AND e.weight_pct IS NOT NULL
          AND ((e.role = 'Major' AND e.weight_pct < 15)
            OR (e.role = 'Minor' AND e.weight_pct >= 15))
        ORDER BY e.weight_pct
    """)


def check_coverage(cur, threshold):
    """Companies whose exposures account for little of their reported revenue.

    A node that was never built is indistinguishable from a business the company
    does not have (R027), so this is the check that keeps the difference visible.
    The denominator excludes the lines that name no product — 其他 and friends —
    because they could not be classified even in principle.

    Companies with no assigned weight at all are the point of the LEFT JOIN: they
    are the worst cases and an inner join would drop every one of them.

    The denominator wants mutually exclusive leaf lines. Totals and contra rows
    are excluded by name, but the parser still emits the occasional fragment of
    running text as a product line, so a handful of companies sum past 100 and
    their coverage is unreliable rather than a measurement. Not a lower bound
    either: an inflated denominator understates, a missed line overstates, and
    nothing here says the error runs one way.

    coverage_status says which of those a low number is, because 58% means very
    different things depending on the cause and a caller cannot tell them apart
    from the number:

        reliable            at or above the threshold
        parser_anomaly      the denominator itself is wrong (sums past 105)
        coarse_filing       every exposure is filing_too_coarse; nothing to
                            assign, and this is a closed case not a backlog
        incomplete_mapping  the filing splits further than the exposures do

    The threshold decides who is listed, not how they are labelled, so a
    caller wanting the whole table with its statuses raises the threshold past
    100 (--coverage-threshold 101).

    Not a failure: a distributor legitimately books most of its revenue under one
    node, and a company whose filing gives one coarse line has nothing to assign.
    It is a work list, not an error list.
    """
    return _rows(cur, """
        WITH classified AS (
            SELECT DISTINCT stock_id FROM tw.company_industry_exposure
            WHERE effective_to IS NULL
        ), assigned AS (
            SELECT stock_id, sum(weight_pct) AS w
            FROM tw.company_industry_exposure
            WHERE effective_to IS NULL AND exposure_type = 'Direct'
              AND weight_pct IS NOT NULL AND weight_type = 'Revenue'
            GROUP BY stock_id
        ), reported AS (
            SELECT stock_id, sum(pct_effective) AS t
            FROM tw.product_revenue_share
            WHERE fiscal_year = %s AND pct_effective IS NOT NULL
              AND item NOT IN ('其他', '其它', '合計', '其他營業收入',
                               '租賃收入', '勞務收入', '銷貨收入',
                               '銷貨淨額', '營業收入', '營業收入淨額',
                               '銷貨收入淨額', '總計')
              AND item NOT LIKE '減%%'
            GROUP BY stock_id
        )
        SELECT c.stock_id, s.name,
               round(100 * coalesce(a.w, 0) / r.t, 1) AS coverage_pct,
               round(r.t - coalesce(a.w, 0), 1) AS unassigned_pp,
               CASE
                   WHEN 100 * coalesce(a.w, 0) / r.t >= %s THEN 'reliable'
                   WHEN r.t > 105 THEN 'parser_anomaly'
                   WHEN NOT EXISTS (
                       SELECT 1 FROM tw.company_industry_exposure e
                       WHERE e.stock_id = c.stock_id AND e.effective_to IS NULL
                         AND e.evidence_kind <> 'filing_too_coarse')
                       THEN 'coarse_filing'
                   ELSE 'incomplete_mapping'
               END AS coverage_status
        FROM classified c
        JOIN reported r USING (stock_id)
        JOIN tw.stocks s USING (stock_id)
        LEFT JOIN assigned a USING (stock_id)
        WHERE r.t > 0 AND 100 * coalesce(a.w, 0) / r.t < %s
        ORDER BY 100 * coalesce(a.w, 0) / r.t
    """, (FISCAL_YEAR, threshold, threshold))


def check_temporal(cur):
    """Rows whose three time axes disagree with each other (R030).

    Two of the four are CHECK constraints (migration 095); the two that compare
    against the as-of date cannot be, because "as of" is the moment the query
    runs and no constraint can see it. All four are checked here so the rule has
    one place to point at, and so a dropped constraint shows up rather than
    passing silently.

    known_date after effective_to is NOT a violation and is not checked: a
    business that ran through 2020 and was disclosed in the FY2020 report filed
    in 2021 is exactly what known_date is for (R030).
    """
    return _rows(cur, """
        SELECT e.stock_id, s.name, e.node_code,
               e.effective_from, e.effective_to, e.known_date, e.evidence_period_end,
               CASE
                   WHEN e.known_date > current_date THEN 'known_date in the future'
                   WHEN e.effective_to IS NOT NULL AND e.effective_from > e.effective_to
                       THEN 'effective_from after effective_to'
                   WHEN e.evidence_period_end > e.known_date
                       THEN 'evidence_period_end after known_date'
                   WHEN e.evidence_period_end > current_date
                       THEN 'evidence_period_end in the future'
               END AS problem
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        WHERE e.known_date > current_date
           OR (e.effective_to IS NOT NULL AND e.effective_from > e.effective_to)
           OR e.evidence_period_end > e.known_date
           OR e.evidence_period_end > current_date
        ORDER BY e.stock_id
    """)


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
    ap.add_argument("--below-threshold", type=float, default=15.0)
    ap.add_argument("--coverage-threshold", type=float, default=70.0)
    ap.add_argument("--limit", type=int, default=15, help="rows printed per check")
    args = ap.parse_args()

    wanted = args.check or list(CHECKS)
    with get_cursor() as cur:
        cur.execute("SELECT count(*) c FROM tw.company_industry_exposure WHERE effective_to IS NULL")
        live = cur.fetchone()["c"]
        print(f"{live} current exposures\n")

        for name in wanted:
            if name == "below":
                findings = check_below(cur, args.below_threshold)
            elif name == "coverage":
                findings = check_coverage(cur, args.coverage_threshold)
            else:
                findings = globals()[f"check_{name}"](cur)
            print(f"[{name}] {len(findings)}")
            for f in findings[:args.limit]:
                print("   ", {k: v for k, v in f.items() if v is not None})
            if len(findings) > args.limit:
                print(f"    ... and {len(findings) - args.limit} more")
            print()


if __name__ == "__main__":
    main()
