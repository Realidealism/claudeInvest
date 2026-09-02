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
            found = [shares.get((r["stock_id"], p)) for p in parts]
            if any(f is None for f in found):
                r["pct"] = None
                r["missing"] = [p for p, f in zip(parts, found) if f is None]
            else:
                r["pct"] = sum(f[0] for f in found)
                r["pct_is_derived"] = any(f[1] for f in found)
                r["parts"] = len(parts)
        hit = [r for r in claimed if r["pct"] is not None]
        miss = [r for r in claimed if r["pct"] is None]

        merged = sum(1 for r in hit if r.get("parts", 1) > 1)
        print(f"exposures {len(rows)}  naming an item {len(claimed)}  "
              f"resolved {len(hit)} (of which {merged} sum several lines)  "
              f"unresolved {len(miss)}  "
              f"held NULL as filing_too_coarse {len(coarse)}")
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
