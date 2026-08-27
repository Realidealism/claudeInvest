"""
Read-only quality checks over tw.stock_themes, against the annual-report
product mix in tw.product_revenue.

Nothing here writes. Membership edits belong in db/themes_data.py followed by
db.seed_themes; the revenue_pct column belongs to db.fill_theme_revenue.

    python -m db.check_themes                    # the four verdict checks
    python -m db.check_themes --category 印刷電路板
    python -m db.check_themes --check missing    # candidate pool, opt-in

Checks:
  listing   member is delisted, suspended, or not an ordinary share
  mismatch  the keyword caught a side line while a LARGER line of the same
            company also belongs to this theme, so revenue_pct understates the
            exposure and must not be read as a low share
  below     the matched line is the right one and its share is under the
            threshold, so the exposure is genuinely small
  foreign   the member's largest product line reads as another industry
  missing   OPT-IN, and a candidate pool rather than a finding list. Companies
            held by no theme in the chain, whose product wording one of its
            themes matches. Companies already collected elsewhere in the chain
            are left out — those are cross-listing candidates, a different
            question. Expect heavy false positives even so: the keywords exist
            to read a KNOWN member's product mix, where the company is already
            given, so a broad word like 化學品 is harmless there and drags in
            every chemical maker here. Screen by hand, and raise
            --missing-threshold to trade recall for quiet.

A finding is a question, not a verdict: distributors list every category
separately so each share looks small, and niche themes deliberately keep
significant players whose ratio is low (see db/themes_data.py).
"""

import argparse
import re

from db.connection import get_cursor

CHECKS = ("listing", "mismatch", "below", "foreign", "missing")
DEFAULT_CHECKS = ("listing", "mismatch", "below", "foreign")

# Industries a semiconductor or PCB member's headline product should not read as.
FOREIGN = ("LCD|面板|液晶|太陽能|光伏|生技|醫療|血糖|試劑|食品|飼料|紡織|成衣|營建|"
           "建材|水泥|航運|貨櫃|輪胎|家電|運動|護具|傢俱|化妝|清潔|飲料|旅遊|保全|遊戲")


def _shares(cur):
    """(stock_id, item) -> pct, filling pct in from revenue where it is NULL."""
    cur.execute("""
        WITH tot AS (
          SELECT stock_id, sum(revenue) AS total FROM tw.product_revenue
          WHERE revenue IS NOT NULL GROUP BY 1 HAVING sum(revenue) > 0)
        SELECT pr.stock_id, pr.item,
               COALESCE(pr.pct, round(pr.revenue / tot.total * 100, 2)) AS pct
        FROM tw.product_revenue pr LEFT JOIN tot USING (stock_id)
    """)
    out = {}
    for r in cur.fetchall():
        if r["pct"] is not None:
            out.setdefault(r["stock_id"], []).append((r["item"], float(r["pct"])))
    for rows in out.values():
        rows.sort(key=lambda x: -x[1])
    return out


def _members(cur):
    cur.execute("""
        SELECT s.stock_id, s.name, s.market, s.industry, s.is_active,
               s.security_type, s.delisted_date, t.category, t.name AS theme,
               st.revenue_pct, st.pct_item
        FROM tw.stock_themes st
        JOIN tw.themes t USING (theme_id)
        JOIN tw.stocks s USING (stock_id)
        ORDER BY t.category, t.name, s.stock_id
    """)
    return [dict(r) for r in cur.fetchall()]


def check_listing(members, _shares_by_stock):
    for m in members:
        if m["is_active"] and m["security_type"] == "STOCK":
            continue
        why = "delisted %s" % m["delisted_date"] if m["delisted_date"] else \
              "inactive" if not m["is_active"] else "type=%s" % m["security_type"]
        yield m, why


def _keywords():
    try:
        from db.fill_theme_revenue import KEYWORDS
        return KEYWORDS
    except Exception:
        return None


def _bigger_line_in_theme(m, shares, keywords):
    """A larger line of the same company that also belongs to m's theme, if any.

    Without this the two cases look identical: 柏騰's largest line is PVD coating,
    which is not a compound-semiconductor material, so its 0.37% 碳化矽 really is
    the whole exposure. 新復興's largest line is high-frequency microwave board,
    which IS the rigid board business it was collected for, so its 9% understates.
    """
    words = (keywords or {}).get(m["theme"])
    if not words:
        return None
    for item, pct in shares.get(m["stock_id"]) or []:
        if pct <= float(m["revenue_pct"]) or item == m["pct_item"]:
            continue
        if any(w.lower() in item.lower() for w in words):
            return item, pct
    return None


def check_mismatch(members, shares, threshold):
    keywords = _keywords()
    for m in members:
        if m["revenue_pct"] is None or m["revenue_pct"] >= threshold:
            continue
        bigger = _bigger_line_in_theme(m, shares, keywords)
        if bigger:
            yield m, "matched %s (%.2f%%) but %s (%.2f%%) also fits this theme" % (
                m["pct_item"], m["revenue_pct"], bigger[0], bigger[1])


def check_below(members, shares, threshold):
    keywords = _keywords()
    for m in members:
        if m["revenue_pct"] is None or m["revenue_pct"] >= threshold:
            continue
        if _bigger_line_in_theme(m, shares, keywords):
            continue
        rows = shares.get(m["stock_id"]) or []
        top = " (largest line is %s %.2f%%)" % (rows[0][0], rows[0][1]) if rows and rows[0][0] != m["pct_item"] else ""
        yield m, "%s is %.2f%%%s" % (m["pct_item"], m["revenue_pct"], top)


def check_foreign(members, shares, _threshold):
    pat = re.compile(FOREIGN)
    seen = set()
    for m in members:
        if m["stock_id"] in seen:
            continue
        rows = shares.get(m["stock_id"]) or []
        if not rows:
            continue
        top_item, top_pct = rows[0]
        if top_pct >= 40 and pat.search(top_item):
            seen.add(m["stock_id"])
            yield m, "largest line is %s (%.2f%%)" % (top_item, top_pct)


def check_missing(cur, shares, threshold, category):
    """Companies outside a theme selling what that theme's keywords describe."""
    try:
        from db.fill_theme_revenue import KEYWORDS
    except Exception as exc:
        print("  (skipped: cannot import KEYWORDS from db.fill_theme_revenue: %s)" % exc)
        return
    cur.execute("""
        SELECT t.name AS theme, t.category,
               (SELECT array_agg(DISTINCT st2.stock_id)
                  FROM tw.stock_themes st2 JOIN tw.themes t2 USING (theme_id)
                 WHERE t2.category = t.category) AS chain_members
        FROM tw.themes t GROUP BY t.name, t.category
    """)
    themes = [dict(r) for r in cur.fetchall()]
    cur.execute("""SELECT stock_id, name, market FROM tw.stocks
                   WHERE is_active AND security_type = 'STOCK'""")
    listed = {r["stock_id"]: (r["name"], r["market"]) for r in cur.fetchall()}

    for t in themes:
        if category and t["category"] != category:
            continue
        words = KEYWORDS.get(t["theme"])
        if not words:
            continue
        held = set(t["chain_members"] or [])
        for stock_id, rows in shares.items():
            if stock_id in held or stock_id not in listed:
                continue
            for item, pct in rows:
                if pct < threshold:
                    break
                if any(w.lower() in item.lower() for w in words):
                    name, market = listed[stock_id]
                    yield ("%s %s (%s)" % (stock_id, name, market),
                           "%s > %s" % (t["category"], t["theme"]),
                           "%s %.2f%%" % (item, pct))
                    break


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--category", help="limit to one theme category")
    ap.add_argument("--threshold", type=float, default=20.0)
    ap.add_argument("--missing-threshold", type=float, default=50.0,
                    help="share a non-member's line must reach for the missing "
                         "check to surface it (default 50)")
    ap.add_argument("--check", default=",".join(DEFAULT_CHECKS),
                    help="comma-separated subset of: " + ", ".join(CHECKS))
    args = ap.parse_args()
    wanted = [c.strip() for c in args.check.split(",") if c.strip()]
    unknown = [c for c in wanted if c not in CHECKS]
    if unknown:
        raise SystemExit("Unknown check(s): %s. Pick from: %s" % (unknown, ", ".join(CHECKS)))

    with get_cursor(commit=False) as cur:
        shares = _shares(cur)
        members = _members(cur)
        if args.category:
            members = [m for m in members if m["category"] == args.category]

        total = 0
        for name, fn in (("listing", check_listing), ("mismatch", check_mismatch),
                         ("below", check_below), ("foreign", check_foreign)):
            if name not in wanted:
                continue
            rows = list(fn(members, shares) if name == "listing"
                        else fn(members, shares, args.threshold))
            print("\n[%s] %d finding(s)" % (name, len(rows)))
            for m, why in rows:
                print("  %s %-9s %s > %s\n        %s" % (
                    m["stock_id"], m["name"], m["category"], m["theme"], why))
            total += len(rows)

        if "missing" in wanted:
            rows = list(check_missing(cur, shares, args.missing_threshold, args.category))
            print("\n[missing] %d candidate(s) - screen by hand" % len(rows))
            for who, theme, evidence in rows:
                print("  %-22s -> %s\n        %s" % (who, theme, evidence))
            total += len(rows)

    print("\n%d finding(s) total. Nothing was written." % total)


if __name__ == "__main__":
    main()
