"""Fill weight_pct on tw.company_industry_exposure from the annual report.

This does no matching. Each exposure already names the product line or lines it
is read off (weight_item), so all that happens here is a lookup of those exact
lines in tw.product_revenue.

A node can own several of a company's reported lines, and the exposure's weight
is their total: 創意 books 委託設計(NRE) 23.5% and 晶圓產品 75.4% separately, but
both are its ASIC design service business, so A4-05 carries 98.9%. Components are
written as "line A + line B"; if any one of them fails to resolve the whole
exposure is left NULL rather than given a partial sum, which would understate it
in exactly the way the old keyword matcher did.

That is the whole point of the rewrite. The predecessor guessed the line from a
keyword table, which silently caught side businesses and missed the main one —
中美晶 came out at 1.1% when the line that mattered was 81.1%, and nothing in
the output looked wrong. A share that cannot be looked up is left NULL here
rather than approximated.

Shares come from tw.product_revenue_share rather than the base table: roughly a
quarter of reports print amounts and no ratio, and that view derives the share
from the amounts while recording that it did (pct_is_derived). weight_source
keeps the distinction so a derived share is never mistaken for a printed one.

A share is written with its type and denominator (R020). Every share here is a
revenue share against consolidated revenue, because that is what the annual
report's 營業比重 table measures; a capacity or strategic weight would be a
different kind of number and is not derivable from this source.

A second source exists for the companies the annual report cannot serve. 64 of
them file one line too coarse to split (R020), and 辛耘 is the case that shows
what that costs: its report gives only 銷貨收入 / 勞務收入 / 其他營業收入, so
nothing in it separates the equipment it builds from the equipment it
distributes, which is the R022 line deciding how much of it is a distributor.
Its investor-conference deck draws exactly that line.

The deck is a fallback, not a branch: a line is looked up in the annual report
first and in tw.conference_product_mix only when the report does not have it.
That, and not evidence_kind, is what keeps a deck from overwriting a filing —
evidence_kind says where the exposure's evidence came from, which is a different
question. 台光電, 台燿 and 聯茂 are all evidenced by a deck and all read their
weight off 銅箔基板, a line their annual reports print; keying on evidence_kind
would have sent those three to a table that has never held a row for them.

Where a deck gives several periods the most recent one wins. That is a choice
with a cost worth naming: the newer the period, the less of it can be checked.
辛耘's FY2025 share is verifiable against the filing to two decimals (6,647,828
of 11,371,368 is 58.46% against the 58 printed) and its 2026/Q2 share is not,
because no 2026 Q2 income statement exists for any company yet.

Shares are cleared before being rewritten, so a weight_item that stops matching
leaves no stale number behind.

    python -m db.fill_weight --dry-run
    python -m db.fill_weight
"""

import argparse

from db.connection import get_cursor

FISCAL_YEAR = 2025


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="report without writing")
    ap.add_argument("--year", type=int, default=FISCAL_YEAR)
    args = ap.parse_args()

    with get_cursor() as cur:
        cur.execute("""
            SELECT e.id, e.stock_id, e.node_code, e.weight_item, e.evidence_kind,
                   s.name
            FROM tw.company_industry_exposure e
            JOIN tw.stocks s ON s.stock_id = e.stock_id
            WHERE e.effective_to IS NULL
        """)
        rows = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT stock_id, item, pct_effective, pct_is_derived
            FROM tw.product_revenue_share
            WHERE fiscal_year = %s AND pct_effective IS NOT NULL
        """, (args.year,))
        shares = {(r["stock_id"], r["item"]): (r["pct_effective"], r["pct_is_derived"])
                  for r in cur.fetchall()}

        # Most recent period per line. A deck reprints several years on one
        # slide, and DISTINCT ON picks the latest rather than summing them,
        # which is the whole difference between a mix and a time series. The
        # share is against that period's own total, never a mix of periods.
        cur.execute("""
            WITH usable AS (
                SELECT * FROM tw.conference_product_mix
                WHERE axis IN ('product', 'business_unit')
            ), totals AS (
                SELECT stock_id, period_label, sum(revenue) AS total
                FROM usable WHERE revenue IS NOT NULL
                GROUP BY stock_id, period_label
            )
            SELECT DISTINCT ON (u.stock_id, u.item)
                   u.stock_id, u.item,
                   COALESCE(u.pct, round(u.revenue * 100.0 / t.total, 4)) AS pct,
                   u.pct IS NULL AS pct_is_derived
            FROM usable u
            LEFT JOIN totals t USING (stock_id, period_label)
            WHERE COALESCE(u.pct, CASE WHEN t.total > 0 THEN 1 END) IS NOT NULL
            ORDER BY u.stock_id, u.item, u.period_end DESC
        """)
        deck = {(r["stock_id"], r["item"]): (r["pct"], r["pct_is_derived"])
                for r in cur.fetchall()}

        # A weight_item is kept as evidence even when the filing is too coarse to
        # give the node a total (R020). Those rows name a real reported line, so
        # they would resolve here — and the number would be the line's, not the
        # node's. They stay NULL.
        coarse = [r for r in rows if r["weight_item"]
                  and r["evidence_kind"] == "filing_too_coarse"]
        claimed = [r for r in rows if r["weight_item"]
                   and r["evidence_kind"] != "filing_too_coarse"]
        for r in claimed:
            parts = [p.strip() for p in r["weight_item"].split(" + ")]
            found, from_deck = [], False
            for part in parts:
                got = shares.get((r["stock_id"], part))
                if got is None:
                    got = deck.get((r["stock_id"], part))
                    from_deck = from_deck or got is not None
                found.append(got)
            r["from_deck"] = from_deck
            if any(f is None for f in found):
                r["pct"] = None
                r["from_deck"] = False
                r["missing"] = [p for p, f in zip(parts, found) if f is None]
            else:
                r["pct"] = sum(f[0] for f in found)
                r["pct_is_derived"] = any(f[1] for f in found)
                r["parts"] = len(parts)
        hit = [r for r in claimed if r["pct"] is not None]
        miss = [r for r in claimed if r["pct"] is None]

        merged = sum(1 for r in hit if r.get("parts", 1) > 1)
        from_deck = sum(1 for r in hit if r["from_deck"])
        print(f"exposures {len(rows)}  naming an item {len(claimed)}  "
              f"resolved {len(hit)} (of which {merged} sum several lines)  "
              f"unresolved {len(miss)}  "
              f"held NULL as filing_too_coarse {len(coarse)}")
        # Kept out of the line above so a change in one source cannot be read as
        # a change in the other.
        print(f"  by source: annual report {len(hit) - from_deck}  "
              f"investor conference {from_deck}")
        if miss:
            print("unresolved (weight_pct stays NULL):")
            for r in miss[:20]:
                print(f"  {r['stock_id']} {r['name']} / {r['node_code']} — "
                      f"unresolved: {r['missing']}")
            if len(miss) > 20:
                print(f"  ... and {len(miss) - 20} more")

        if args.dry_run:
            print("--dry-run: nothing written")
            return

        cur.execute("""
            UPDATE tw.company_industry_exposure
               SET weight_pct = NULL, weight_source = NULL,
                   weight_type = NULL, weight_basis = NULL
             WHERE effective_to IS NULL
        """)
        for r in hit:
            if r["from_deck"]:
                source = ("investor_conf_derived" if r["pct_is_derived"]
                          else "investor_conf")
            else:
                source = ("annual_report_item_derived" if r["pct_is_derived"]
                          else "annual_report_item")
            cur.execute("""
                UPDATE tw.company_industry_exposure
                   SET weight_pct = %s, weight_source = %s,
                       weight_type = 'Revenue', weight_basis = 'consolidated_revenue'
                 WHERE id = %s
            """, (r["pct"], source, r["id"]))

    print(f"filled {len(hit)} weights")


if __name__ == "__main__":
    main()
