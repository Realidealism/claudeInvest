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

And a rising count is not always a falling quality. Discovery expansion can
increase DQ findings without reducing data quality: every check here is scoped
to the classified population, so a company's existing defects become visible on
the day it is classified, not on the day they were introduced. This has now
happened three times in one taxonomy round -- [definition_boundary] jumped when
a subtree walk replaced a direct-member test, [scale] and [mix] each gained a
company whose percentages had always been stored in the amount column, and
[mix] gained three more whose filings disclose a revenue table the database
never parsed. Read a jump against what entered the population, before reading
it as damage.

    python -m db.check_industry
    python -m db.check_industry --check below --below-threshold 10
"""

import argparse
import datetime
import re

from db.connection import get_cursor
from db.weight_status import AUTOFILL_BLOCKED, COARSE

CHECKS = ("core", "core_overlap", "core_reason", "core_pending", "listing", "stale",
          "below", "role_numeric_consistency", "role_indirect_rationale_missing",
          "role_direct_null_minor_unsupported", "coverage",
          "temporal", "mix", "scale", "definition_completeness",
          "definition_boundary", "definition_entity_reference",
          "definition_placeholder", "definition_axis_undeclared",
          "definition_boundary_overlap",
          "double", "depth",
          "unknown", "known_missing", "known_unavailable", "weight_reason",
          "weight_status_consistency", "weight_status_lifecycle", "granularity_queue",
          "revenue_rows", "provenance", "official", "rule_node_reference")
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

    Delegates to db.core_candidates so the candidate set has one implementation.
    Where a company has one candidate the Core is an identity and needs no
    argument; where it has several, R023 was actually applied and that reasoning
    is the only thing separating the result from a preference. It goes in the
    Core row's evidence, marked "Core 理由：".

    The same pass also catches a Core that is not in its own candidate set —
    Indirect, on a branch node, or absent — which no other check looks for.
    """
    from db.core_candidates import verify

    _sets, _identity, _adjudicated, findings = verify(cur, datetime.date.today())
    return [{"kind": kind, "stock_id": stock_id, "name": name, "why": why}
            for kind, stock_id, name, why in findings]


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
    """weight_item naming a line no source has.

    Both sources count, because db.fill_weight tries both: the annual report
    first, then a conference deck. 辛耘's 代理 is a business its report never
    separates — that is the whole reason the exposure reads off a deck — and
    testing only the report would flag the one row that is right. Testing only
    the deck for anything marked investor_conf is the mirror mistake: 台光電,
    台燿 and 聯茂 carry deck evidence and annual-report weight lines.
    """
    return _rows(cur, """
        WITH part AS (
            SELECT e.stock_id, e.node_code, s.name, e.evidence_kind,
                   btrim(unnest(string_to_array(e.weight_item, ' + '))) AS item
            FROM tw.company_industry_exposure e
            JOIN tw.stocks s ON s.stock_id = e.stock_id
            WHERE e.effective_to IS NULL AND e.weight_item IS NOT NULL
        )
        SELECT p.stock_id, p.name, p.node_code, p.item AS missing_item,
               p.evidence_kind
        FROM part p
        WHERE NOT EXISTS (
                  SELECT 1 FROM tw.product_revenue_share r
                  WHERE r.stock_id = p.stock_id AND r.fiscal_year = %s
                    AND r.item = p.item)
          AND NOT EXISTS (
                  SELECT 1 FROM tw.conference_product_mix c
                  WHERE c.stock_id = p.stock_id AND c.item = p.item
                    AND c.axis IN ('product', 'business_unit'))
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


def check_role_numeric_consistency(cur):
    """Non-Core rows whose role disagrees with R026's 15% default.

    The default is only a default — a named reason in the evidence makes any
    label legitimate — so this cannot decide who is wrong. It lists the rows a
    reader has to look at, which is what stops the column drifting back to the
    state R026 was written to end (Major at 6.90%, Minor at 19.26%).

    Scope, and why the name says "numeric": this can only speak about rows the
    15% default actually governs — Direct, with a weight. R026 has a second half
    for every row it does not govern, and those split two ways rather than one:
    [role_indirect_rationale_missing] for Indirect and Emerging, where role comes
    from evidence of importance, and [role_direct_null_minor_unsupported] for the
    Direct rows with no weight, where Major is the default and Minor is a claim.
    Splitting them is 2026-09-07: this check reported 0 while 9 of 12 Indirect
    rows carried an unjustified Major/Minor and 14 of 22 Direct rows with no
    weight were Minor with nothing supporting it. "[role] 0" was read as "the
    column is auditable" when it only ever meant "no Direct row with a number
    contradicts the threshold".

    A row whose evidence carries "role 理由：" is excluded. R026 says a deviation
    must be named, not that it must not happen, so a named one is the rule
    working rather than a finding — the same contract [core_reason] uses.
    Without that, the check can never reach zero and stops meaning anything.
    """
    return _rows(cur, """
        SELECT e.stock_id, s.name, e.node_code, e.role::text, e.weight_pct
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        WHERE e.effective_to IS NULL AND e.role NOT IN ('Core', 'Emerging')
          AND e.exposure_type = 'Direct' AND e.weight_pct IS NOT NULL
          AND e.evidence NOT LIKE '%%role 理由%%'
          AND ((e.role = 'Major' AND e.weight_pct < 15)
            OR (e.role = 'Minor' AND e.weight_pct >= 15))
        ORDER BY e.weight_pct
    """)


def check_role_indirect_rationale_missing(cur):
    """Indirect / Emerging rows ranked Major or Minor with no stated reason.

    R026 takes the numeric default away from these rows on purpose — an agency
    line or a subsidiary's line has weight NULL by R020③, and defaulting it to
    Major would call somebody else's business the company's important one — and
    puts something else in its place: role is judged from the evidence of actual
    economic or strategic importance, "並於該列 evidence 記載判定理由".

    Nothing checked the replacement. 2493 揚博 is what that looks like: three
    Indirect rows off one agency-supply table in one filing, no weight on any of
    them, labelled Major / Major / Minor, and not a word anywhere about what
    separates the third from the other two. 2481 強茂 is worse than undocumented
    — its Indirect row is Minor and the only number in sight is the subsidiary's
    8.05%, which is exactly the yardstick R026 forbids here.

    Level 1 only: this asks whether a reason exists, not whether it is any good.
    "role 理由：因為很重要" passes the lint and fails a reading, so the string is
    a floor and the substance stays an evidence review — the same division
    [core_reason] lives with.

    of_rows carries the population so the report reads "9 of 12" rather than a
    bare 9: a count of failures alone cannot say whether the rule is nearly kept
    or barely known.

    Not to be confused with [role_direct_null_minor_unsupported]. Both ask for a
    written reason and both accept the same "role 理由：" marker, but they are
    asking different questions: here, why this borrowed line is important (or
    not) to the parent; there, why a line the company runs itself is ancillary.
    A Direct row with no weight defaults to Major and owes nothing.
    """
    return _rows(cur, """
        SELECT e.stock_id, s.name, e.node_code, t.name AS node_name,
               e.role::text, e.exposure_type, e.weight_item,
               (SELECT count(*) FROM tw.company_industry_exposure x
                 WHERE x.effective_to IS NULL
                   AND (x.exposure_type = 'Indirect' OR x.role = 'Emerging')
                   AND x.role IN ('Major', 'Minor')) AS of_rows
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        JOIN tw.industry_tree t ON t.node_code = e.node_code
        WHERE e.effective_to IS NULL
          AND (e.exposure_type = 'Indirect' OR e.role = 'Emerging')
          AND e.role IN ('Major', 'Minor')
          AND e.evidence NOT LIKE '%%role 理由%%'
        ORDER BY e.stock_id, e.node_code
    """)


def check_role_direct_null_minor_unsupported(cur):
    """Direct rows with no weight labelled Minor and nothing saying why.

    R026 makes this branch asymmetric on purpose. A Direct row whose weight is
    NULL asks one question -- is there evidence this business is ancillary? --
    and the two answers are not equally free: Major is the default and owes
    nothing, Minor is a claim about the company and owes evidence. "「附屬業務」
    須有 evidence 支持，不得僅因 weight 為 NULL 或營收未揭露而認定" is the whole
    point; without it Minor becomes the place unquantifiable lines quietly go.

    Nothing checked it, and the clause went from unexercised to heavily used
    while nobody was looking: R026's own 現況 line said 0 such rows as of
    2026-09-02 and there are now 22, 14 of them with no stated support. The eight
    that do have it show what the bar looks like -- 5351 鈺創 "系統晶片為記憶體以外
    之次要產品線"; 3581 博磊 "年報自述主力產品為測試產品、切割機及植球機，清洗機不在
    其列"; 6129 普誠 "MCU 僅商品表一列". Each names what the main business is and
    where this line sits relative to it.

    Level 1 only, and the marker is shared with
    [role_indirect_rationale_missing] because the eight compliant rows already
    write it that way; one marker, two required contents. What the lint cannot
    see is a row like 1722 台肥 J8, whose evidence ends "年報無營業比重表故不計
    比重" -- that explains the NULL, not the Minor, and it would pass the moment
    someone appended the marker to it. R026's definition of 附屬業務 exists so the
    reader can fail that on a reading.
    """
    return _rows(cur, """
        SELECT e.stock_id, s.name, e.node_code, t.name AS node_name,
               e.role::text, e.weight_item, e.evidence_kind,
               (SELECT count(*) FROM tw.company_industry_exposure x
                 WHERE x.effective_to IS NULL AND x.exposure_type = 'Direct'
                   AND x.weight_pct IS NULL AND x.role = 'Minor') AS of_rows
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        JOIN tw.industry_tree t ON t.node_code = e.node_code
        WHERE e.effective_to IS NULL AND e.exposure_type = 'Direct'
          AND e.weight_pct IS NULL AND e.role = 'Minor'
          AND e.evidence NOT LIKE '%%role 理由%%'
        ORDER BY e.stock_id, e.node_code
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
        coarse_filing       every exposure carries a coarse weight_status
                            (db.weight_status.COARSE: Too_Coarse or
                            Granularity_Insufficient); nothing to assign, and
                            this is a closed case not a backlog. It classifies
                            disclosure granularity only -- not that a weight can
                            be filled, the exposure is right, a gap is resolved
                            or Core is settled
        off_period          the weights describe a period the denominator does
                            not. A deck-sourced weight is whatever quarter the
                            deck gave, and dividing a 2026/Q2 share by the FY2025
                            report is not a coverage figure — it is two numbers
                            about different years. Counted as unassigned, and
                            said out loud rather than passed off as a gap in the
                            mapping. An undated weight is not counted as
                            off-period — NULL says nothing about the period, and
                            8 rows predate the column being filled in
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
              AND (evidence_period_end IS NULL
                   OR evidence_period_end = make_date(%s, 12, 31))
            GROUP BY stock_id
        ), off_period AS (
            SELECT DISTINCT stock_id FROM tw.company_industry_exposure
            WHERE effective_to IS NULL AND weight_pct IS NOT NULL
              AND evidence_period_end IS NOT NULL
              AND evidence_period_end <> make_date(%s, 12, 31)
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
                         AND (e.weight_status IS NULL
                              OR NOT e.weight_status::text = ANY(%s)))
                       THEN 'coarse_filing'
                   WHEN EXISTS (SELECT 1 FROM off_period o
                                WHERE o.stock_id = c.stock_id)
                       THEN 'off_period'
                   ELSE 'incomplete_mapping'
               END AS coverage_status
        FROM classified c
        JOIN reported r USING (stock_id)
        JOIN tw.stocks s USING (stock_id)
        LEFT JOIN assigned a USING (stock_id)
        WHERE r.t > 0 AND 100 * coalesce(a.w, 0) / r.t < %s
        ORDER BY 100 * coalesce(a.w, 0) / r.t
    """, (FISCAL_YEAR, FISCAL_YEAR, FISCAL_YEAR, threshold, sorted(COARSE), threshold))


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

    A fifth condition, added 2026-09-07: filing-dated evidence with no
    evidence_period_end at all. The other four compare the axes against each
    other, and a NULL makes every one of those comparisons UNKNOWN — so the row
    that has lost an axis is exactly the row this check cannot see. Three were
    sitting there (6261 久元, 6789 采鈺, 3663 鑫科), untouched since the initial
    load because _dates_for only fires on insert or correction.
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
                   WHEN e.evidence_period_end IS NULL
                       THEN 'filing-dated evidence with no evidence period'
               END AS problem
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        WHERE e.known_date > current_date
           OR (e.effective_to IS NOT NULL AND e.effective_from > e.effective_to)
           OR e.evidence_period_end > e.known_date
           OR e.evidence_period_end > current_date
           OR (e.evidence_period_end IS NULL
               AND e.evidence_kind IN ('annual_report_table', 'annual_report_prose'))
        ORDER BY e.stock_id
    """)



# The parse guards are string comparisons against a PDF's text, and the text
# arrives with spaces wedged between Han characters — 嘉聯益's region table said
# "美 國", which is not the "美國" the scraper's REGION_WORDS looks for. Anything
# comparing names here strips whitespace first, or it repeats the bug it is
# trying to find.
_SPACE = re.compile(r"\s+")
REGION_WORDS = ("美國", "日本", "中國", "大陸", "台灣", "臺灣", "歐洲", "亞洲", "德國",
                "韓國", "新加坡", "外銷", "內銷", "營運地點", "其他地區", "地區別")
# The heading a filing puts above its product mix. Its presence in the prose
# means a table exists, whatever the revenue scraper concluded.
MIX_HEADING = ("營業比重", "營收比重", "營業收入比重")


# 營業比重 appears in prose as often as it heads a table — 威健 says "營業比重約
# 100%是代理電子零組件暨周邊設備之銷售" and stores nothing, correctly, because
# there is nothing to store. A table is rows of figures, so that is what is
# looked for: two or more lines carrying at least two numbers each, somewhere in
# the 25 lines after the heading.
_ROWISH = re.compile(r"\d[\d,]*(?:\.\d+)?")


def _looks_tabular(body):
    for m in re.finditer(r"比\s*重", body):
        window = body[m.end():m.end() + 1800].splitlines()[:25]
        rows = [ln for ln in window if len(_ROWISH.findall(ln)) >= 2]
        if len(rows) >= 3:
            return True
    return False


def check_mix(cur):
    """Stored product mixes that the filing itself contradicts (R020).

    weight_pct is read off tw.product_revenue, and three ways of getting that
    wrong all survive every existing check because none of them raises anything:

        no_stored_table   the filing discloses a mix and we stored nothing.
                          status says 'no_section', which reads as "the company
                          did not disclose" and is a different claim entirely.
                          Disclosure in prose counts: 揚智 writes "占整體營收約
                          85%…網路相關設備約占15%" and prints no table, which is
                          a figure the database should hold and does not
        region_as_product the rows are a geography breakdown, not products. The
                          shares are real percentages of real revenue, so they
                          look perfectly well-formed
        partial_parse     rows went missing and the share was derived from the
                          survivors, so the remainder silently inflates — 擎邦's
                          55.50% is stored as 96.55% because the 42.51% line is
                          not there. 京元電子 is the same failure at scale: 103
                          億 stored against the filing's 349 億

    partial_parse asks whether the stored sum is any of the totals printed on
    the table's 合計 line. A mix table prints one per period and the flattened
    text cannot say which column is FY2025, so matching any of them is the most
    that can be claimed — and a parse that dropped a row matches none.

    Only companies whose business-scope prose is on hand can be checked; the
    prose is the filing quoting itself, and it is the only second opinion
    available without re-reading the PDF.
    """
    rows = _rows(cur, """
        SELECT n.stock_id, s.name, n.body,
               coalesce(st.status, '(none)') AS status,
               (SELECT count(*) FROM tw.product_revenue p
                 WHERE p.stock_id = n.stock_id AND p.fiscal_year = %s) AS stored,
               (SELECT array_agg(p.item) FROM tw.product_revenue p
                 WHERE p.stock_id = n.stock_id AND p.fiscal_year = %s) AS items,
               (SELECT sum(p.revenue) FROM tw.product_revenue p
                 WHERE p.stock_id = n.stock_id AND p.fiscal_year = %s) AS stored_sum,
               (SELECT sum(p.pct) FROM tw.product_revenue p
                 WHERE p.stock_id = n.stock_id AND p.fiscal_year = %s) AS stored_pct,
               (SELECT count(p.revenue) FROM tw.product_revenue p
                 WHERE p.stock_id = n.stock_id AND p.fiscal_year = %s) AS stored_amounts
        FROM tw.product_narrative n
        JOIN tw.stocks s ON s.stock_id = n.stock_id
        LEFT JOIN tw.product_revenue_status st
               ON st.stock_id = n.stock_id AND st.fiscal_year = n.fiscal_year
        WHERE n.fiscal_year = %s
          AND EXISTS (SELECT 1 FROM tw.company_industry_exposure e
                      WHERE e.stock_id = n.stock_id AND e.effective_to IS NULL)
    """, (FISCAL_YEAR,) * 6)

    out = []
    for r in rows:
        body = _SPACE.sub("", r["body"] or "")
        has_table = any(h in body for h in MIX_HEADING)
        items = [_SPACE.sub("", i) for i in (r["items"] or [])]

        if has_table and r["stored"] == 0 and _looks_tabular(r["body"] or ""):
            out.append({"kind": "no_stored_table", "stock_id": r["stock_id"],
                        "name": r["name"], "status": r["status"],
                        "why": "年報揭露了營業比重，庫內 0 列"})
            continue
        hits = [i for i in items if any(w in i for w in REGION_WORDS)]
        if hits and len(hits) >= max(1, len(items) // 2):
            out.append({"kind": "region_as_product", "stock_id": r["stock_id"],
                        "name": r["name"], "status": r["status"],
                        "why": "存進來的是地區別：" + "、".join(hits[:4])})
            continue
        totals = _filing_totals(r["body"] or "")
        if totals and r["stored_sum"]:
            stored = float(r["stored_sum"])
            gap, nearest = min((abs(stored - t) / t, t) for t in totals)
            if gap > 0.02:
                out.append({"kind": "partial_parse", "stock_id": r["stock_id"],
                            "name": r["name"], "status": r["status"],
                            "why": "庫內合計 %d，年報各期合計皆不符（最近者 %d，差 %.0f%%）"
                                   % (r["stored_sum"], nearest, gap * 100)})
            continue
        # Roughly a quarter of filings print a ratio and no amount, and the test
        # above cannot see any of them: stored_sum is NULL, so `and r["stored_sum"]`
        # is false and the row leaves without being checked. 華夏 sat at 85% —
        # PVC 建材 7% and 塑膠布 8% both missing — and passed every check there was.
        # The mix table sums to 100 by construction, so that is the total to
        # compare against, and no 合計 line is needed to find it.
        if r["stored_amounts"] == 0 and r["stored_pct"] is not None:
            pct = float(r["stored_pct"])
            if abs(pct - 100) > 2:
                out.append({"kind": "partial_pct", "stock_id": r["stock_id"],
                            "name": r["name"], "status": r["status"],
                            "why": "庫內比重合計 %.2f%%，不是 100%%（缺列或多列）" % pct})
    return out


# The whole run of figures after 合計, not just the first one. Read off the RAW
# prose, spaces and all: strip the whitespace first and "合 計 259,911,744
# 100.00" closes up into 259911744100.00, so the total swallows the percentage
# and every company looks like a partial parse.
#
# A mix table almost always prints more than one total on that line, because it
# shows more than one period: 鴻勁 ends on "合計 13,992,344 100.00 30,270,625
# 100.00" and 宏碩系統 on three columns (115Q1 / 114 / 113). Which of them is
# FY2025 cannot be told from text this flat — the column headers are several
# lines away and the rows have already been reflowed. Taking the first made
# every company whose revenue grew look like a partial parse, and taking the
# max made the ones that shrank look like one instead.
#
# So the question asked is not "does the stored sum equal THE total" but "is it
# any of the totals the table prints". A parse that dropped a row matches none
# of them, which is the failure this check exists to find. The cost is a miss
# when a parser stores the wrong year in full — that lands on the prior year's
# total and passes. [stale] catches the renamed-line version of that, and
# nothing catches the rest; it is a real hole, and a much smaller one than
# burying 40 real findings under 35 false ones.
_TOTAL_LINE = re.compile(r"合\s*計((?:\s*[\d,]+(?:\.\d+)?%?)+)")
_AMOUNT = re.compile(r"[\d,]{4,}")


def _filing_totals(body):
    """Every figure printed on a 合計 line, as a set of integers."""
    out = set()
    for run in _TOTAL_LINE.findall(body):
        for m in _AMOUNT.findall(run):
            try:
                v = int(m.replace(",", ""))
            except ValueError:
                continue
            if v >= 1000:
                out.add(v)
    return out


def check_scale(cur):
    """Stored amounts that are not the company's revenue at all.

    [mix] compares the stored lines against the filing's own text, which catches
    a dropped row but not a whole table read off the wrong place. This compares
    them against a number from somewhere else entirely — tw.income_statements —
    and that is what makes it a different check rather than a stricter one.

    1314 中石化 is the case it was written for. Its three stored lines are
    196,161 / 198,983 / 1,580, which come to 2.1% of the 19,217,032 it earned.
    They are TONNES: the filing's own prose puts AN capacity at 230,000 tonnes a
    year and CPL at 200,000, and the stored figures sit just under both. Every
    weight derived from them renormalised against a base of 396,724, so J9 came
    out at 50.157% and J1-01 at 49.445% when the filing's 營業比重 table says
    46.81% and 40.14%. Nothing looked wrong: the shares summed to 100 and the
    ranking between them even survived.

    Three shapes come out of this, and the ratio tells them apart:

        ~0%     percentages written into the amount column — the stored total is
                exactly 100, which is not a coincidence
        1-5%    tonnes, litres or units in the amount column
        10-50%  a real amount table with rows missing, or the parent company's
                figures against consolidated revenue

    Only companies that stored amounts are checked. A filing that prints ratios
    and no amounts has nothing to compare, and [mix]'s partial_pct covers those.
    """
    return _rows(cur, """
        WITH stored AS (
            SELECT stock_id, sum(revenue) AS amount, count(*) AS lines
            FROM tw.product_revenue
            WHERE fiscal_year = %s AND revenue IS NOT NULL
            GROUP BY stock_id
        ), reported AS (
            SELECT stock_id, sum(revenue) AS r, count(*) AS q
            FROM tw.income_statements
            WHERE year = %s AND period_type = 'Q'
            GROUP BY stock_id
        )
        SELECT st.stock_id, s.name, st.lines, st.amount AS stored_amount,
               rp.r AS reported_revenue,
               round(st.amount::numeric / rp.r * 100, 2) AS ratio_pct,
               CASE WHEN st.amount BETWEEN 99 AND 101 THEN 'pct in the amount column'
                    WHEN st.amount::numeric / rp.r < 0.05 THEN 'units, not money'
                    ELSE 'rows missing, or parent vs consolidated'
               END AS looks_like
        FROM stored st
        JOIN reported rp USING (stock_id)
        JOIN tw.stocks s USING (stock_id)
        WHERE rp.q = 4 AND rp.r > 0
          AND EXISTS (SELECT 1 FROM tw.company_industry_exposure e
                      WHERE e.stock_id = st.stock_id AND e.effective_to IS NULL)
          AND (st.amount::numeric / rp.r < 0.5 OR st.amount::numeric / rp.r > 2)
        ORDER BY st.amount::numeric / rp.r
    """, (FISCAL_YEAR, FISCAL_YEAR))


def check_definition_completeness(cur):
    """Nodes with no product boundary of their own.

    R025 and R031 say the definition comes first and the mapping second: a
    company may only be hung on a node whose product boundary is already
    written. Most of that needs a person -- whether two prose definitions
    overlap is a judgement, and no query decides it. This is the half a query
    can decide about one node; [definition_boundary] is the half about a set.

    A leaf with members and an empty main_products has had the sequence run
    backwards, and it is not hypothetical: V1 held 32 companies against a
    definition field reading the names of three distributors, a mapping result
    written where the definition belongs. That kind reads as a definition,
    which is worse than empty, so it is tested by asking whether the field is
    mostly names of listed companies.

    Branch nodes joined the population on 2026-09-07. Until then the query
    opened with NOT EXISTS(children) and 95 of the tree's nodes at the time were
    structurally invisible, 88 of them undefined. A branch's definition may be
    far more abstract than a leaf's -- it states the scope its children divide,
    not a product list -- but it cannot be absent once the subtree is in use,
    because then the thing the children are supposed to partition was never
    said. E is the case: E1 through E6 are all undefined at L2 while their L3
    children carry definitions and members, and E3's own deferral note reads
    "every sibling in E is undefined too, so there is nothing to be exclusive
    against" -- a judgement a person had to make because nothing reported it.

    In use means the subtree has members or a child already carries a
    definition. A branch whose whole subtree is empty is a gap in the taxonomy
    rather than a rule being broken, the same allowance empty leaves get.
    """
    return _rows(cur, """
        WITH node AS (
            SELECT t.node_code, t.name AS node_name, t.parent_code, t.main_products,
                   EXISTS (SELECT 1 FROM tw.industry_tree c
                            WHERE c.parent_code = t.node_code) AS is_branch,
                   (SELECT count(*) FROM tw.company_industry_exposure e
                     WHERE e.node_code = t.node_code AND e.effective_to IS NULL) AS members,
                   (SELECT count(*) FROM tw.company_industry_exposure e
                      JOIN tw.industry_tree d ON d.node_code = e.node_code
                     WHERE e.effective_to IS NULL
                       AND (d.node_code = t.node_code
                         OR d.parent_code = t.node_code
                         OR d.parent_code IN (SELECT g.node_code FROM tw.industry_tree g
                                               WHERE g.parent_code = t.node_code))) AS subtree_members,
                   EXISTS (SELECT 1 FROM tw.industry_tree c
                            WHERE c.parent_code = t.node_code
                              AND c.main_products IS NOT NULL) AS child_defined
            FROM tw.industry_tree t
        ), tok AS (
            SELECT n.node_code, btrim(x) AS token
            FROM node n,
                 regexp_split_to_table(coalesce(n.main_products, ''), '[、,，/／(（]') AS x
            WHERE btrim(x) <> ''
        ), namey AS (
            SELECT t.node_code, count(*) AS tokens,
                   count(*) FILTER (WHERE EXISTS (
                       SELECT 1 FROM tw.stocks s
                       WHERE s.is_active AND s.security_type = 'STOCK'
                         AND s.name = t.token AND length(s.name) >= 2)) AS names
            FROM tok t GROUP BY t.node_code
        )
        SELECT n.node_code, n.node_name, n.parent_code, n.members, n.subtree_members,
               CASE WHEN n.main_products IS NOT NULL THEN 'definition is company names'
                    WHEN n.is_branch THEN 'branch in use with no scope definition'
                    ELSE 'leaf with members and no definition' END AS kind
        FROM node n
        LEFT JOIN namey y USING (node_code)
        WHERE (n.main_products IS NULL
                 AND ((NOT n.is_branch AND n.members > 0)
                   OR (n.is_branch AND (n.subtree_members > 0 OR n.child_defined))))
           OR (n.main_products IS NOT NULL AND n.members > 0
                 AND y.names > 0 AND y.names * 2 >= y.tokens)
        ORDER BY n.subtree_members DESC, n.node_code
    """)


def check_definition_boundary(cur):
    """Sibling sets whose exclusivity cannot be verified, because a sibling has
    no boundary written.

    This is what R031 actually asks for. "Sibling nodes under one parent must
    have mutually exclusive product definitions" is a claim about a set, and a
    set containing an undefined member cannot support it -- there is nothing to
    compare the others against. [definition_completeness] cannot see this,
    because it asks each node about itself and an undefined node with no members
    has no obligation of its own.

    B2-05 is the case it was written for. It is named HDI / SLP and has no
    definition; B2-01's definition lists HDI in so many words and carries 28
    companies. Whether any of those 28 belongs in B2-05 instead is unanswerable
    and always has been, and the old [definition] said nothing, because B2-05 is
    empty. Empty is precisely the state that makes the question unanswerable.

    A6-04 Burn-in is the other half of the same problem and the reason R031
    gained its 2026-09-07 enforcement clause: that day 3581 博磊's burn-in test
    socket was reasoned into A6-03 partly because A6-04 had no definition and no
    members. An empty node is not evidence of exclusion, and this check is what
    should have been consulted instead.

    Scoped to sets that are in use, and to sets with at least two siblings,
    since a lone child has nothing to be exclusive against and owes only its own
    definition.

    In use is a property of the sibling *set*, not of any one node in it.
    Until 2026-09-07 this query asked whether a sibling carried an exposure on
    its own node_code, which is the wrong question wherever exposures legally
    land on leaves below it: a scope node is not supposed to carry members. So
    every L2 set whose companies all sat at L3 was invisible -- A3 晶圓製造,
    A1 半導體材料 and A7 半導體廠務 share a set holding 361 companies and were
    never once reported, while F, G and Y were reported only because some
    sibling of theirs happened to hold a direct member. The correction took
    findings from 26 to 66; the 40 are false negatives being uncovered, not new
    violations, and none of them are ordered by this check.

    Population is therefore counted over each node's whole subtree, by DISTINCT
    company: the question is whether real companies are being sorted by this set,
    not how many exposure rows exist. Written as a descendant walk rather than a
    parent/child join so that it keeps answering the same question if the tree
    ever grows a fourth level (R016 defines a leaf as a node without children,
    not as level 3).
    """
    return _rows(cur, """
        WITH RECURSIVE descendant AS (
            SELECT node_code AS root, node_code FROM tw.industry_tree
            UNION ALL
            SELECT d.root, t.node_code
              FROM descendant d
              JOIN tw.industry_tree t ON t.parent_code = d.node_code
        ), sib AS (
            SELECT t.parent_code, t.node_code, t.name AS node_name,
                   t.main_products IS NOT NULL AS defined,
                   (SELECT count(DISTINCT e.stock_id)
                      FROM descendant d
                      JOIN tw.company_industry_exposure e ON e.node_code = d.node_code
                     WHERE d.root = t.node_code AND e.effective_to IS NULL) AS members
            FROM tw.industry_tree t
            WHERE t.parent_code IS NOT NULL
        ), grp AS (
            SELECT s.parent_code, count(*) AS siblings,
                   count(*) FILTER (WHERE s.defined) AS defined_siblings,
                   (SELECT count(DISTINCT e.stock_id)
                      FROM tw.industry_tree sub
                      JOIN descendant d ON d.root = sub.node_code
                      JOIN tw.company_industry_exposure e ON e.node_code = d.node_code
                     WHERE sub.parent_code = s.parent_code
                       AND e.effective_to IS NULL) AS subtree_members
            FROM sib s GROUP BY s.parent_code
        )
        SELECT s.parent_code, s.node_code, s.node_name, s.members,
               g.siblings, g.defined_siblings, g.subtree_members
        FROM sib s JOIN grp g USING (parent_code)
        WHERE NOT s.defined AND g.siblings >= 2 AND g.subtree_members > 0
        ORDER BY g.subtree_members DESC, s.parent_code, s.node_code
    """)


def _def_tokens(col):
    """The SQL fragment that splits a definition into its listed terms."""
    return ("regexp_split_to_table(coalesce(%s, ''), "
            "'[、,，/／()（）:：;；。★\\n]') " % col)


def check_definition_entity_reference(cur):
    """Definitions that name a listed company.

    A boundary says what a product is. The moment it says who is currently on
    the node, the reasoning closes a loop -- the company is here, so the
    definition names it, so the definition proves it belongs here -- and the
    node can no longer be applied to anyone else. A6-02 read Probe、Probe Card、
    Probe Interface（旺矽、雍智）, and 雍智 was not even on it: it sits on A6-03 with
    98.04% of test load boards, so the definition cited a mapping that was wrong
    as well as circular.

    [definition_completeness] already tests for this and missed it, because that
    test asks whether the field is *mostly* company names -- it was written for
    V1, whose definition was three distributor names and nothing else. Two names
    among five terms is the more common shape and slips under it. This asks
    whether there is any at all.

    Names of two characters or more, matched as whole terms against active
    ordinary shares, so a definition may still use a word that happens to be part
    of a company name without tripping.
    """
    return _rows(cur, """
        WITH tok AS (
            SELECT t.node_code, t.name AS node_name,
                   btrim(x) AS token
            FROM tw.industry_tree t,
                 %s AS x
            WHERE btrim(x) <> ''
        )
        SELECT k.node_code, k.node_name,
               string_agg(DISTINCT k.token, '、') AS company_names,
               (SELECT count(*) FROM tw.company_industry_exposure e
                 WHERE e.node_code = k.node_code AND e.effective_to IS NULL) AS members
        FROM tok k
        WHERE EXISTS (SELECT 1 FROM tw.stocks s
                       WHERE s.is_active AND s.security_type = 'STOCK'
                         AND s.name = k.token AND length(s.name) >= 2)
        GROUP BY k.node_code, k.node_name
        ORDER BY k.node_code
    """ % _def_tokens('t.main_products'))


def check_definition_placeholder(cur):
    """Definitions that are a pointer, an annotation or a promise.

    A5-04 HBM Packaging carried, as its entire definition, （HBM 本身為 Theme
    T07）. It is not empty, so [definition_completeness] passed it, and it has a
    definition, so [definition_boundary] passed it too -- while nobody could have
    used it to classify anything. A cross-reference tells a reader where else to
    look; it does not say what belongs here.

    The test is what is left once the parentheses come off. A definition whose
    whole content is a bracketed reference has nothing outside them; J16 reads
    碳粉、碳粉匣、列印耗材、電子成像用色料（碳黑原料見 J15） and keeps a full boundary
    when the bracket goes, which is what a good definition with a cross-reference
    looks like. Length alone flagged it, so length alone was the wrong test.
    """
    return _rows(cur, """
        SELECT t.node_code, t.name AS node_name, t.main_products,
               (SELECT count(*) FROM tw.company_industry_exposure e
                 WHERE e.node_code = t.node_code AND e.effective_to IS NULL) AS members
        FROM tw.industry_tree t
        WHERE t.main_products IS NOT NULL
          AND t.main_products ~ '(見 ?[A-Z]|Theme|參見|同上|待[定裁補]|TBD|pending|暫)'
          AND length(btrim(regexp_replace(t.main_products,
                       '（[^）]*）|[(][^)]*[)]', '', 'g'))) < 8
        ORDER BY t.node_code
    """)


def check_definition_axis_undeclared(cur):
    """Branch definitions that never say what their children divide on.

    R031 requires sibling nodes to share one classification axis, and every
    branch fixed in the 2026-09-07 pass ended up stating that axis outright: A2
    by process stage, A5 by where the packaging happens, A6 by two declared
    dimensions, B2 by board structure, J by position in the chemical chain, K by
    material against product, V by category distributed. The ones that had not
    stated it are the ones that drifted -- B2 held five axes at once and nobody
    could see it from the tree.

    Whether an axis is actually violated needs a person; no query decides
    whether HDI is a structure or a property. What a query can decide is whether
    the parent ever declared one, which is the precondition for the person to
    have anything to check against. Named for what it tests.

    Scoped to branches whose subtree is in use and which have at least two
    children -- a lone child has no sibling to be exclusive against and its
    parent owes a scope, not an axis -- and satisfied by any of the words a
    declaration uses in this tree.
    """
    return _rows(cur, """
        SELECT t.node_code, t.name AS node_name, t.level,
               (SELECT count(*) FROM tw.company_industry_exposure e
                  JOIN tw.industry_tree d ON d.node_code = e.node_code
                 WHERE e.effective_to IS NULL
                   AND (d.parent_code = t.node_code
                     OR d.parent_code IN (SELECT g.node_code FROM tw.industry_tree g
                                           WHERE g.parent_code = t.node_code))) AS subtree_members
        FROM tw.industry_tree t
        WHERE (SELECT count(*) FROM tw.industry_tree c
                WHERE c.parent_code = t.node_code) >= 2
          AND t.main_products IS NOT NULL
          AND t.main_products !~ '(軸|維度|互斥|分界|依.{0,6}(切分|細分|分格))'
          AND EXISTS (SELECT 1 FROM tw.company_industry_exposure e
                        JOIN tw.industry_tree d ON d.node_code = e.node_code
                       WHERE e.effective_to IS NULL
                         AND (d.parent_code = t.node_code
                           OR d.parent_code IN (SELECT g.node_code FROM tw.industry_tree g
                                                 WHERE g.parent_code = t.node_code)))
        ORDER BY t.node_code
    """)


def check_definition_boundary_overlap(cur):
    """Sibling definitions that claim a sibling's own name.

    B2-01 was named 硬板 / 多層板 / HDI and its definition listed HDI and
    Server/AI Server PCB, which are the whole of B2-05 HDI / SLP and B2-06 AI
    Server PCB. K2 said it covered 所有成形方式（⋯射出⋯）, which is the whole of
    K4 塑膠射出. In both cases the empty sibling was the symptom and the defined
    one was the cause, and neither [definition_completeness] nor
    [definition_boundary] could say so: one asks whether a node has a definition,
    the other whether a set has any gaps, and this is a set where the gap was
    created by a definition that had already taken the ground.

    Matches a sibling's name, minus the generic tail that most node names carry,
    against the other siblings' definitions -- but only where the sibling's node
    code is absent. That one condition is what separates a silent claim from a
    boundary: 「與 A2-05 的分界是製造 vs 服務」 and 「不含被動元件（V2）」 name the code
    because they are handing the ground over, and they are what a good definition
    looks like. B2-01 wrote HDI and never wrote B2-05. Without the condition this
    check reported 74 rows and most of them were definitions doing their job.
    """
    return _rows(cur, """
        WITH n AS (
            SELECT node_code, parent_code, name, main_products,
                   btrim(regexp_replace(
                       split_part(regexp_replace(name, '\\(.*\\)', '', 'g'), ' / ', 1),
                       '(設備|製造|通路|服務|材料|IC|代工|加工|製品|原料|封裝|系統)$', '')) AS core_name
            FROM tw.industry_tree
            WHERE parent_code IS NOT NULL
        )
        SELECT a.node_code AS claiming_node, a.name AS claiming_name,
               b.node_code AS claimed_node, b.name AS claimed_name,
               b.core_name AS term,
               (SELECT count(*) FROM tw.company_industry_exposure e
                 WHERE e.node_code = a.node_code AND e.effective_to IS NULL) AS claiming_members,
               (SELECT count(*) FROM tw.company_industry_exposure e
                 WHERE e.node_code = b.node_code AND e.effective_to IS NULL) AS claimed_members
        FROM n a JOIN n b ON b.parent_code = a.parent_code AND b.node_code <> a.node_code
        WHERE a.main_products IS NOT NULL
          AND length(b.core_name) >= 2
          AND a.main_products NOT LIKE '%%' || b.node_code || '%%'
          -- the term has to be one of the things A lists, not a fragment inside
          -- a longer one: 不織布 contains 織布 and 貴金屬 contains 金屬, and neither
          -- is a claim on anybody
          AND EXISTS (SELECT 1 FROM regexp_split_to_table(a.main_products,
                                       '[、,，/／()（）:：;；。★「」\n]') AS w
                       WHERE length(btrim(w)) >= 2
                         AND (btrim(w) = b.core_name
                           OR b.core_name LIKE '%%' || btrim(w))
                         -- and the matched word has to be distinctive: one that
                         -- turns up in more than one sibling's name is the
                         -- branch's head noun, not a claim on anybody. 晶圓 sits
                         -- in both 矽晶圓 and 再生晶圓, so A1-04 saying 化合物晶圓
                         -- takes nothing from either; 射出 sits only in 塑膠射出,
                         -- so K2 saying 射出 does.
                         AND (SELECT count(*) FROM n c
                               WHERE c.parent_code = a.parent_code
                                 AND c.node_code <> a.node_code
                                 AND c.name LIKE '%%' || btrim(w) || '%%') < 2)
        ORDER BY a.node_code, b.node_code
    """)


def check_double(cur):
    r"""The same economic activity counted on two nodes. R004 and R020(2) both forbid it.

    Split before grouping. R020(4) lets one exposure's weight_item list its component lines with " + ", so the
    duplicate that matters never appears as two equal strings: 3374 carries
    "晶圓級尺寸封裝 + 晶圓級後護層封裝 + 測試服務" on A5-05 (98.020) while A6-01 (47.860) carries "測試服務" by
    itself. Grouping on the whole item -- what this check did until 2026-09-29 -- compares a merged string against a
    single one, finds them unequal, and lets 47.86pp that is counted twice go by.

    weight_pct IS NOT NULL is a HARD limit on scope, not a shortcut for speed. R020(2) 乙型 prescribes exactly the
    shape this check would otherwise flag: when one revenue line spans several nodes with no reliable basis to split
    it, every node gets a Direct exposure and every weight_pct is NULL. Four current rows are that prescribed
    handling and not a defect -- 3030 (A2-13 / B5-02, 自動檢測設備), 3131 (A2-05 / A2-12, 機台設備產品, a regression
    sample quoted inside R020 itself), 4968 (A4-10 / A4-13, Wi-Fi 產品) and 8054 (A4-11 / A4-19). Widening the scope
    would report all four as violating the rule they comply with.

    The separator is exactly " + ", the syntax R020(4) names. A bare "+" is not a separator and MUST NOT be split
    on: 2451 "FLASH+DRAM產品" and 2402 "機構整合元件產品(含商品+模具+模組+散熱模組)" are single product names, and
    shredding them would manufacture tokens the filing never disclosed.

    Known limit: SQL folds whitespace here, not NFKC. Verified 2026-09-29 -- no two tokens in the table differ only
    by a width variant, so both normalisations agree today. When that stops being true this belongs in Python.

    This check does not decide what to do about a hit. 3374's own adjudication runs in the ids 163/164/165 queue.
    """
    return _rows(cur, r"""
        WITH activity AS (
            SELECT e.stock_id, e.node_code, e.weight_item,
                   regexp_replace(t.token, '\s+', '', 'g') AS activity
            FROM tw.company_industry_exposure e
            CROSS JOIN LATERAL unnest(string_to_array(e.weight_item, ' + ')) AS t(token)
            WHERE e.effective_to IS NULL
              AND e.weight_pct IS NOT NULL
              AND coalesce(e.weight_item, '') <> ''
        )
        SELECT a.stock_id, s.name, a.activity,
               array_agg(DISTINCT a.node_code ORDER BY a.node_code) AS nodes,
               array_agg(DISTINCT a.weight_item ORDER BY a.weight_item) AS items
        FROM activity a
        JOIN tw.stocks s ON s.stock_id = a.stock_id
        WHERE length(a.activity) >= 2
        GROUP BY a.stock_id, s.name, a.activity
        HAVING count(DISTINCT a.node_code) > 1
        ORDER BY a.stock_id
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


def check_weight_reason(cur):
    """Rows with no weight and no recorded reason for having none.

    R020's governance line is that a NULL weight is not itself a defect -- a
    NULL without a correct reason is. Until 2026-09-08 the schema could not
    hold that distinction: the only reason it could express was
    evidence_kind='filing_too_coarse', which is a statement about the evidence
    that happens to double as a reason. Every other evidence kind could sit on
    a NULL weight saying nothing about why. That value was removed from
    evidence_kind on 2026-09-10 and the reason now lives only in weight_status;
    the two columns answer different questions and no longer share a namespace.

    So this does not report a shortage of numbers -- [below] and [coverage]
    already do that, and R020 is explicit that inventing shares to fill them
    makes the data worse. It reports rows where nobody has said which of the
    three cases applies: the filing combined several activities, the filing
    disclosed no share at all, or a revenue share is not the right measure.

    Expect this to start high. 94 rows carried an unexplained NULL on the day
    the column was added, and each needs its filing read; the count falls as
    they are adjudicated, not as weights appear.
    """
    return _rows(cur, """
        SELECT e.stock_id, s.name, e.node_code, e.evidence_kind, e.role
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        WHERE e.effective_to IS NULL
          AND e.weight_pct IS NULL
          AND e.weight_status IS NULL
        ORDER BY e.stock_id, e.node_code
    """)


def check_weight_status_consistency(cur):
    """Rows whose stated reason for having no weight contradicts something else.

    weight_status answers "why is there no share". Three of its possible states
    have consequences elsewhere, and this is where they are held together:

    (a) evidence_kind='filing_too_coarse' is no longer a legal value -- the
        reason belongs in weight_status. A row still carrying it is reported so
        the value cannot creep back in through a hand-edited file.

    (b) 'Not_Applicable' is the one value that should never accumulate quietly:
        without a check it becomes the bin unexplained NULLs get swept into, and
        [weight_reason] falls while the data gets worse. It used to be reported
        on sight, as a review queue. Four rows went through that review on
        2026-09-11 and came out as three different things -- 3055 蔚華科 and
        3093 港建* where the company does not perform the node's activity at all
        and a share of it has no object; 1595 川寶 where Not_Applicable was
        absorbing Too_Coarse (its own AVI machines, disclosed only inside a
        31.03% department line); 6138 茂達 where the measure applies perfectly
        and the reason is that a separately-listed subsidiary, 6435 大中, holds
        the same activity. All four carried the same value and the same
        sentence.

        So what is required now is the ground, not the review: 「理由＝度量不適用」
        or 「理由＝合併治理」. A row naming neither is reported, which is how a
        third kind surfaces instead of being absorbed by the first two.

    (c) A row that names a weight_item and carries a status will have that status
        wiped the next time db.fill_weight resolves the item -- unless the status
        is one fill_weight skips by design. That set is db.weight_status.
        AUTOFILL_BLOCKED and is read from there rather than repeated here, because
        repeating it is how this paragraph went stale: it named two values and
        Not_Applicable joined them on 2026-09-29. Keeping the known components in
        weight_item on such a row is what R020(4) asks for, so that combination is
        correct and only the others are reported.

    Each CASE branch carries the condition of the clause it speaks for, not a
    looser one. The third branch used to fire on weight_item alone, so a row
    admitted by (b-ground) while exempt under (c) was told it was about
    fill_weight -- true of the column, false of the row. A reorder would only
    move that fault to the other branch; the conditions are what must match.
    """
    return _rows(cur, """
        SELECT e.stock_id, s.name, e.node_code, e.role, e.evidence_kind,
               e.weight_status::text, e.weight_item, e.weight_pct,
               CASE
                 WHEN e.weight_pct IS NOT NULL AND e.weight_item IS NULL
                   THEN 'a number with no item: WEIGHTED needs (b) as well as (a)'
                 WHEN e.evidence_kind = 'filing_too_coarse'
                   THEN 'evidence_kind carrying a reason: it belongs in weight_status'
                 WHEN e.weight_status IS NOT NULL AND e.weight_item IS NOT NULL
                      AND NOT e.weight_status::text = ANY(%s)
                   THEN 'an item this reason will not survive: fill_weight will resolve it'
                 ELSE 'Not_Applicable without a stated ground: 度量不適用 or 合併治理?'
               END AS kind
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        WHERE e.effective_to IS NULL
          AND (
            -- R020 (1)-W (b): a share nobody can trace back to named revenue lines
            (e.weight_pct IS NOT NULL AND e.weight_item IS NULL)
            OR e.evidence_kind = 'filing_too_coarse'
            OR (e.weight_status IS NOT NULL AND e.weight_item IS NOT NULL
                AND NOT e.weight_status::text = ANY(%s))
            OR (e.weight_status = 'Not_Applicable'
                AND e.evidence NOT LIKE '%%理由＝度量不適用%%'
                AND e.evidence NOT LIKE '%%理由＝合併治理%%')
          )
        ORDER BY e.stock_id, e.node_code
    """, (sorted(AUTOFILL_BLOCKED), sorted(AUTOFILL_BLOCKED)))


def check_weight_status_lifecycle(cur):
    """Not_Extracted rows whose source has since become readable (R020-NE).

    Not_Extracted says "the source exists and we did not get the number out of
    it". That is a work queue, not a verdict, and the failure mode it invites is
    silence: the parser gets fixed, the filing becomes readable, and the row goes
    on saying we could not read it -- which by then is indistinguishable from
    claiming the company disclosed nothing. R020-NE forbids a row staying here
    only because it was already here, so this is where that is enforced.

    The signal is the company's own revenue pipeline: once
    tw.product_revenue_status reads 'ok' and rows exist, whatever blocked the
    extraction is gone and the row owes a fresh adjudication into WEIGHTED,
    TOO_COARSE, NOT_DISCLOSED or DISCLOSURE_INVALID.

    Zero here does not mean the queue is empty -- it means nothing in it has
    been fixed yet. The size of the queue is the count of Not_Extracted rows,
    which is reported by the distribution rather than as a finding, because a
    row waiting on a parser repair is an honest state, not a defect.
    """
    return _rows(cur, """
        SELECT e.stock_id, s.name, e.node_code, e.weight_status::text,
               st.status AS revenue_status, c.n AS revenue_rows
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        JOIN tw.product_revenue_status st
          ON st.stock_id = e.stock_id AND st.fiscal_year = %s
        CROSS JOIN LATERAL (
            SELECT count(*) AS n FROM tw.product_revenue p
             WHERE p.stock_id = e.stock_id AND p.fiscal_year = %s
        ) c
        WHERE e.effective_to IS NULL
          AND e.weight_status = 'Not_Extracted'
          AND st.status = 'ok' AND c.n > 0
        ORDER BY e.stock_id, e.node_code
    """, (FISCAL_YEAR, FISCAL_YEAR))


def check_granularity_queue(cur):
    """Every current row held at Granularity_Insufficient -- the queue itself.

    Granularity_Insufficient says "the revenue is disclosed along a product axis,
    too coarsely to compute this node's weight, and the other node the line also
    covers cannot be named" (R2, ruled 2026-09-17). Unlike Too_Coarse it is not
    terminal: it ends when (a) the definition or mapping is repaired so the line
    can be attributed, or (b) the source adds a revenue split that can be used.
    Neither happens on its own, and a value nobody lists is a value rows sit in
    forever -- the same failure Not_Extracted guards against.

    So the whole queue is the report. A row here is an honest waiting state, not
    a defect; the count should fall as (a) or (b) happen, never by relabelling.
    Compared as text because the enum does not carry the value until its
    migration is applied, and an enum comparison would then be an error.
    """
    return _rows(cur, """
        SELECT e.stock_id, s.name, e.node_code, e.role, e.weight_item
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        WHERE e.effective_to IS NULL
          AND e.weight_status::text = %s
        ORDER BY e.stock_id, e.node_code
    """, ("Granularity_Insufficient",))

def check_revenue_rows(cur):
    """The revenue source's own integrity: does the status row match the table?

    Every weight in this schema is a share of tw.product_revenue, so a company
    whose rows silently went missing produces a denominator that is wrong in a
    way no downstream check can see -- 6166 凌華 sat at an 88% denominator for
    two weeks and was only caught by reading its filing against the database by
    hand.

    Two conditions, because they fail differently:

    (a) persisted_rows disagrees with COUNT(*). save() writes that number in the
        same transaction as the rows, so any drift afterwards means something
        outside the scraper changed the table. Rows written before migration 104
        have NULL counts and are not reported -- they fill in as each filing is
        re-parsed, and asserting a count for them now would be inventing one.

    (a) is not by itself a defect. The case it was built for is
        **retained-last-known**: the parse failed or was refused this time, so
        the rows from the last successful read are kept on purpose -- a fetch or
        locator failure must not be allowed to empty a company that had a good
        read. What the check reports is that the two disagree, so nobody mistakes
        the stored rows for the current parse.

        Until migration 109 that category also held a second, different thing:
        rows the current parse had *contradicted* rather than failed to confirm.
        23 companies sat here holding what earlier parser versions produced
        before the context gate existed -- 8401 白紗科's 「整體印刷業銷售總值
        (仟元)」 and 「市場佔有率(%)」, 9941 裕融's 「汽車分期設定台數」, 3577
        泓格's 「研發費用」. Those now retract themselves when the document is
        re-read and found to hold no mix, so what remains under (a) is once
        again only the retained-last-known case -- 2302 麗正, whose filing could
        not be found this time and whose rows therefore stand unchallenged.

    (b) status is 'ok' with no rows at all. This needs no counts and is what
        catches the 2026-08 batch: 20 companies whose status claims a successful
        read while the table holds nothing. save() writes rows and status in one
        transaction, so 'ok' with zero rows cannot be produced by the current
        code path at all.
    """
    return _rows(cur, """
        SELECT st.stock_id, s.name, st.status, st.parsed_rows, st.persisted_rows,
               st.duplicate_rows, c.n AS actual_rows,
               CASE WHEN st.persisted_rows IS NOT NULL AND st.persisted_rows <> c.n
                    THEN 'persisted_rows disagrees with the table'
                    ELSE 'status ok with no rows stored' END AS kind
        FROM tw.product_revenue_status st
        JOIN tw.stocks s ON s.stock_id = st.stock_id
        CROSS JOIN LATERAL (
            -- Retracted rows are not the current parse and are not evidence;
            -- counting them here would keep reporting a disagreement that the
            -- retraction resolved, and the finding would go dead (migration 109).
            SELECT count(*) AS n FROM tw.product_revenue p
             WHERE p.stock_id = st.stock_id AND p.fiscal_year = st.fiscal_year
               AND p.retracted_at IS NULL
        ) c
        WHERE st.fiscal_year = %s
          AND ((st.persisted_rows IS NOT NULL AND st.persisted_rows <> c.n)
               OR (st.status = 'ok' AND c.n = 0))
        ORDER BY st.stock_id
    """, (FISCAL_YEAR,))


def check_known_missing(cur):
    """Rows whose evidence has a date to give and did not get it.

    Split out of the old [known] on 2026-09-07, which asked only whether
    known_date was NULL and so reported 2 while 26 rows carried a date borrowed
    from a document they were never read from. A missing date and an
    unreconstructible one are different states and only the first is a defect;
    lumping them made the honest ones look like defects and the fabricated ones
    look fine.

    Filing-dated evidence owes the filing date and deck evidence owes the
    conference date. If either is absent the row is a gap in the pipeline —
    tw.product_revenue_status has no filing_date, or the deck never landed in
    tw.conference_product_mix — not an inherent limit of the evidence.
    """
    return _rows(cur, """
        SELECT e.stock_id, s.name, e.node_code, e.evidence_kind
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        WHERE e.effective_to IS NULL AND e.known_date IS NULL
          AND e.evidence_kind IN ('annual_report_table', 'annual_report_prose',
                                  'investor_conf')
        ORDER BY e.stock_id, e.node_code
    """)


def check_known_unavailable(cur):
    """Rows whose evidence carries no publication date anyone can recover.

    A company's product page says what it says with no date on it, so there is no
    honest answer to "from when was this knowable". R030 already settled the
    shape of that answer for the other axis — 不具期間語意的 evidence,
    evidence_period_end 應為 NULL, 不得以推測日期填補 — and known_date is the same
    axis with the same argument.

    Two things come out of here and they are not the same. The NULL rows are the
    standing, correct state, listed rather than hidden because a backtest filtering
    on known_date silently drops them and someone should know how many that is:
    conservative exclusion, which is the trade R030 asks for. A row of this kind
    carrying a date is the real finding — nothing could have supplied it except a
    guess or a borrowing.
    """
    return _rows(cur, """
        SELECT e.stock_id, s.name, e.node_code, e.evidence_kind, e.known_date,
               CASE WHEN e.known_date IS NULL
                    THEN 'unavailable — excluded from any as-of query, by design'
                    ELSE 'has a date its evidence cannot supply' END AS kind
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        WHERE e.effective_to IS NULL
          AND e.evidence_kind IN ('company_site', 'no_filing', 'pending')
        ORDER BY e.known_date NULLS LAST, e.stock_id, e.node_code
    """)


def check_provenance(cur):
    """Rows whose evidence_kind names a source the database cannot show.

    evidence_kind is a claim about where a row came from, and every date on the
    row is derived from it: db.seed_industry's _dates_for sends investor_conf to
    the deck's conference date and everything else to the annual report's filing
    date, and evidence_period_end is set only for the annual-report kinds. A
    wrong label therefore does not merely mislabel — it silently dates the row
    off the wrong document, or leaves it undated and invisible to [temporal].

    Two things a query can settle, both found on 2026-09-07:

        claims a deck that is not here    6213 聯茂 and 6274 台燿 were
                                          investor_conf while tw.conference_
                                          product_mix held one company in total
                                          (3583 辛耘), and neither row's evidence
                                          mentioned a presentation at all
        weight came from the filing       6187 萬潤, 6640 均華, 7734 印能科技 and
                                          7751 竑騰 were company_site, yet their
                                          weight_item matches a line in
                                          tw.product_revenue_share verbatim and
                                          db.fill_weight read the share off that
                                          table. The filing is the source
                                          whatever the label says

    What it cannot settle is the opposite direction: a row honestly labelled
    company_site because the filing gives no breakdown still leans on the
    filing's date for known_date, and no query can tell whether the website said
    it earlier or later. That is a rule question, not a check.
    """
    return _rows(cur, """
        SELECT e.stock_id, s.name, e.node_code, e.evidence_kind, e.weight_item,
               CASE WHEN e.evidence_kind = 'investor_conf'
                    THEN 'no deck for this company in tw.conference_product_mix'
                    ELSE 'weight_item matches the annual report, so the filing is the source'
               END AS problem
        FROM tw.company_industry_exposure e
        JOIN tw.stocks s ON s.stock_id = e.stock_id
        WHERE e.effective_to IS NULL
          AND ((e.evidence_kind = 'investor_conf'
                AND NOT EXISTS (SELECT 1 FROM tw.conference_product_mix c
                                 WHERE c.stock_id = e.stock_id))
            OR (e.evidence_kind NOT IN ('annual_report_table', 'annual_report_prose')
                AND e.weight_item IS NOT NULL
                AND EXISTS (SELECT 1 FROM tw.product_revenue_share r
                             WHERE r.stock_id = e.stock_id AND r.fiscal_year = %s
                               AND r.item = ANY(string_to_array(e.weight_item, ' + ')))))
        ORDER BY e.stock_id, e.node_code
    """, (FISCAL_YEAR,))


def check_core_pending(cur):
    """Cores owed a fresh R023 adjudication and not yet given one.

    R023 obliges a re-run whenever a taxonomy action changes a company's
    candidate set — a node added, or an existing definition widened so that a
    business which had nowhere to go becomes legal. Before migration 098 the
    only trace of that obligation was a sentence someone wrote into the
    evidence, which records that it happened but cannot say that it is owed.

    A row with stock_id NULL is the taxonomy action itself: the whole table
    still owes a scan against that node, and sync_tree raises it automatically
    the moment the tree changes. A row with a stock_id is a company already
    known to be affected, written by whoever does the mapping.

    Zero here is the claim that no Core is waiting on a taxonomy change. It is
    not the same claim as [core_reason] = 0, which is about whether the reason
    was written down, not about whether it is still the right reason.
    """
    return _rows(cur, """
        SELECT q.trigger_node, t.name AS node_name, q.trigger_kind,
               q.stock_id, s.name AS stock_name, q.raised_on, q.note
        FROM tw.core_reassessment q
        JOIN tw.industry_tree t ON t.node_code = q.trigger_node
        LEFT JOIN tw.stocks s ON s.stock_id = q.stock_id
        WHERE q.resolved_on IS NULL
        ORDER BY q.raised_on, q.trigger_node, q.stock_id NULLS FIRST
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


# Words a rule uses when it is disclosing that a node is gone, not asserting that it is there. Checked against the
# text, not invented: R025 says "A5-02 降級", R031 says "把 B2-04／05／06 移出 Industry tree".
_RULE_NODE_HISTORICAL = ("移出", "移除", "降級", "已關閉", "CLOSED", "不再存在", "已廢", "已刪除", "撤除")

# An explicit tag, for the day a rule wants to keep an illustration that uses a code the tree does not carry.
# Nothing in the table uses these yet, which is why every hypothetical currently reports INVALID -- the point of the
# check is to make someone write the tag.
_RULE_NODE_HYPOTHETICAL = ("Hypothetical", "hypothetical", "假想例", "示範用", "虛構例")

# How close a marker has to sit to count as marking THIS code. Measured 2026-09-29 over every stale reference in
# the table: real markings land 6, 12, 54 and 60 characters from a mention (R025's "A5-02 降級", R031's
# "B2-04／05／06 移出"), while words that merely happen to be in the same rule land at 405 (R019 says "移除" about a
# Core ROW), 1418 and 2318. 120 sits between the two clusters rather than against either edge.
_RULE_NODE_WINDOW = 120


def _rule_node_marked(text, positions):
    """True when some marker word sits within the window of any mention of this code."""
    for w in _RULE_NODE_HISTORICAL + _RULE_NODE_HYPOTHETICAL:
        start = 0
        while True:
            i = text.find(w, start)
            if i < 0:
                break
            if any(abs(p - i) <= _RULE_NODE_WINDOW for p in positions):
                return True
            start = i + 1
    return False


def check_rule_node_reference(cur):
    r"""Industry node codes quoted in rule prose that no longer resolve against the tree.

    R027 already forbids writing live statistics into rule prose, because a number written down goes stale and the
    rule then states something false. A node code is the same kind of mutable literal and was never governed: 50.16
    is mutable data, A5-02 is a mutable taxonomy identifier. Both rot, and until now nothing looked at the second.

    What it found on the day it was written (2026-09-29): R019 walks through 精材 and concludes "故 Core 為 A5-02",
    then says A5-02 must carry weight_pct=50.16 -- and A5-02 is not in the tree at all, while 精材 actually sits on
    A5-05 at 98.020. R005 and R017 both offer "T07 與 A3-03 ／ A5-04 HBM Packaging ／ A6-07 HBM Test 同時成立" as
    the worked example of Industry and Theme coexisting; neither A5-04 nor A6-07 has ever existed, not even as a
    historical exposure row. A reader has no way to tell those apart from the codes that do resolve.

    Three states, per the 2026-09-29 ruling:
        VALID_CURRENT      the code is in tw.industry_tree
        VALID_HISTORICAL   it is not, and the rule that mentions it says so somewhere in its own text
        INVALID            it is not, and nothing in that rule discloses it -- the finding

    Marking is evaluated per (rule, code), not per mention. A rule that establishes "B2-04／05／06 移出 Industry
    tree" once and then discusses them several times has disclosed their status; demanding the marker beside every
    mention would report the disclosure itself as the defect.

    But the marker has to sit near the code, not merely inside the same rule. Asking `any(word in rule_text)` loses
    the headline case: R019 writes "移除該公司的 Core 列" about removing a Core ROW, 405 characters from any A5-02
    mention, and a rule-wide search reads that as R019 disclosing that A5-02 is gone. See _RULE_NODE_WINDOW for the
    measured separation between real markings and coincidences.

    Scope is the dashed form only (A2-12, B5-01). That is not laziness, it is the only form with one owner: the gap
    IDs in db/TAXONOMY_GAPS.md run G1..G18 and collide head-on with the live nodes G2 工業電腦, G5 工具機 and G6, so
    a bare "G5" in rule prose cannot be classified by shape or by registry lookup. Themes (T##), ecosystems (E##),
    groups (GT##) and the MOPS filing codes (F04／F19／FE4) carry no dash, so they never enter this check at all.
    Bare L1/L2 codes are therefore unverified rather than passing; when the PASS/FAIL/UNVERIFIED work lands (P0-2)
    that is the row this check should emit for them.

    This check reads rule text and the tree. It writes nothing and it does not decide how a finding gets fixed --
    the 2026-09-29 ruling is that a stale case is re-pointed at a regression fixture or tagged Historical or
    Hypothetical, never quietly updated to today's node code, which would only start the clock again.
    """
    import re as _re

    cur.execute("SELECT node_code FROM tw.industry_tree")
    tree = {r["node_code"] for r in cur.fetchall()}
    cur.execute("SELECT rule_id, coalesce(description, '') AS d FROM tw.classification_rules ORDER BY rule_id")
    rules = [dict(r) for r in cur.fetchall()]

    dashed = _re.compile(r"(?<![0-9A-Za-z-])([A-Z]{1,2}[0-9]{1,2}-[0-9]{2})(?![0-9A-Za-z-])")
    findings = []
    for r in rules:
        seen = {}
        for m in dashed.finditer(r["d"]):
            seen.setdefault(m.group(1), []).append(m.start())
        for code, at in sorted(seen.items()):
            if code in tree:
                continue
            if _rule_node_marked(r["d"], at):
                continue
            findings.append({"rule_id": r["rule_id"], "node_code": code, "mentions": len(at),
                             "state": "INVALID",
                             "why": "not in tw.industry_tree and %s does not mark it historical or hypothetical"
                                    % r["rule_id"]})
    return findings


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
