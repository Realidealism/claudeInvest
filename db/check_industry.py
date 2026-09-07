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
import datetime
import re

from db.connection import get_cursor

CHECKS = ("core", "core_overlap", "core_reason", "core_pending", "listing", "stale",
          "below", "role_numeric_consistency", "role_indirect_rationale_missing",
          "role_direct_null_minor_unsupported", "coverage",
          "temporal", "mix", "scale", "definition",
          "double", "depth",
          "unknown", "known_missing", "known_unavailable", "provenance", "official")
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
        coarse_filing       every exposure is filing_too_coarse; nothing to
                            assign, and this is a closed case not a backlog
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
                         AND e.evidence_kind <> 'filing_too_coarse')
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
    """, (FISCAL_YEAR, FISCAL_YEAR, FISCAL_YEAR, threshold, threshold))


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
               AND e.evidence_kind IN ('annual_report_table', 'annual_report_prose',
                                       'filing_too_coarse'))
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


def check_definition(cur):
    """Leaf nodes carrying members but no product definition.

    R025 and R031 say the definition comes first and the mapping second: a
    company may only be hung on a node whose product boundary is already
    written, and sibling definitions must be mutually exclusive before either
    accepts anybody. Most of that needs a person — whether two prose
    definitions overlap is a judgement, and no query decides it.

    This is the half a query can decide. A node with members and an empty
    main_products has had the sequence run backwards, and it is not a
    hypothetical: V1 held 32 companies against a definition field reading
    文曄、大聯大、至上 — three company names, a mapping result written where the
    definition belongs. That one read as a definition, which is worse than
    empty, and only a person could see it. An empty one, a query can.

    Not every undefined leaf is a finding. 70 of them have no members, and an
    empty node is a gap in the taxonomy rather than a rule being broken; it
    becomes a finding the moment someone maps a company onto it.

    An empty definition is not the only way to have none. A field holding
    company names is a mapping result written where the boundary belongs, and it
    is worse than empty because it reads as a definition: V1 said
    文曄、大聯大、至上 and 32 members were judged against it. That kind cannot be
    found by testing for NULL, so it is tested by asking whether the field is
    mostly names of listed companies -- A5-05 OSAT reads 日月光、矽品、力成、京元、
    南茂、超豐、頎邦 and A3-01 Foundry 台積電、聯電、世界、力積電.
    """
    return _rows(cur, """
        WITH leaf AS (
            SELECT t.node_code, t.name AS node_name, t.parent_code, t.main_products,
                   (SELECT count(*) FROM tw.company_industry_exposure e
                    WHERE e.node_code = t.node_code AND e.effective_to IS NULL) AS members
            FROM tw.industry_tree t
            WHERE NOT EXISTS (SELECT 1 FROM tw.industry_tree c
                              WHERE c.parent_code = t.node_code)
        ), tok AS (
            SELECT l.node_code,
                   btrim(x) AS token
            FROM leaf l,
                 regexp_split_to_table(coalesce(l.main_products, ''), '[、,，/／(（]') AS x
            WHERE btrim(x) <> ''
        ), namey AS (
            SELECT t.node_code,
                   count(*) AS tokens,
                   count(*) FILTER (WHERE EXISTS (
                       SELECT 1 FROM tw.stocks s
                       WHERE s.is_active AND s.security_type = 'STOCK'
                         AND s.name = t.token AND length(s.name) >= 2)) AS names
            FROM tok t GROUP BY t.node_code
        )
        SELECT l.node_code, l.node_name, l.parent_code, l.members,
               CASE WHEN l.main_products IS NULL THEN 'no definition'
                    ELSE 'definition is company names' END AS kind
        FROM leaf l
        LEFT JOIN namey n USING (node_code)
        WHERE l.members > 0
          AND (l.main_products IS NULL
               OR (n.names > 0 AND n.names * 2 >= n.tokens))
        ORDER BY l.members DESC, l.node_code
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
                                  'filing_too_coarse', 'investor_conf')
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
            OR (e.evidence_kind NOT IN ('annual_report_table', 'annual_report_prose',
                                        'filing_too_coarse')
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
