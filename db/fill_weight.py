"""Fill weight_pct on tw.company_industry_exposure from the annual report.

This does no matching. Each exposure already names the product line it is read
off (weight_item), so all that happens here is a lookup of that exact line in
tw.product_revenue.

That is the whole point of the rewrite. The predecessor guessed the line from a
keyword table, which silently caught side businesses and missed the main one —
中美晶 came out at 1.1% when the line that mattered was 81.1%, and nothing in
the output looked wrong. A share that cannot be looked up is left NULL here
rather than approximated.

Shares come from tw.product_revenue_share rather than the base table: roughly a
quarter of reports print amounts and no ratio, and that view derives the share
from the amounts while recording that it did (pct_is_derived). weight_source
keeps the distinction so a derived share is never mistaken for a printed one.

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
            SELECT e.id, e.stock_id, e.node_code, e.weight_item, s.name,
                   r.pct_effective AS pct, r.pct_is_derived
            FROM tw.company_industry_exposure e
            JOIN tw.stocks s ON s.stock_id = e.stock_id
            LEFT JOIN tw.product_revenue_share r
                   ON r.stock_id = e.stock_id AND r.fiscal_year = %s
                  AND r.item = e.weight_item
            WHERE e.effective_to IS NULL
        """, (args.year,))
        rows = [dict(r) for r in cur.fetchall()]

        claimed = [r for r in rows if r["weight_item"]]
        hit = [r for r in claimed if r["pct"] is not None]
        miss = [r for r in claimed if r["pct"] is None]

        print(f"exposures {len(rows)}  naming an item {len(claimed)}  "
              f"resolved {len(hit)}  unresolved {len(miss)}")
        if miss:
            print("unresolved (weight_pct stays NULL):")
            for r in miss[:20]:
                print(f"  {r['stock_id']} {r['name']} / {r['node_code']} — {r['weight_item']!r}")
            if len(miss) > 20:
                print(f"  ... and {len(miss) - 20} more")

        if args.dry_run:
            print("--dry-run: nothing written")
            return

        cur.execute("""
            UPDATE tw.company_industry_exposure
               SET weight_pct = NULL, weight_source = NULL
             WHERE effective_to IS NULL
        """)
        for r in hit:
            source = ("annual_report_item_derived" if r["pct_is_derived"]
                      else "annual_report_item")
            cur.execute("""
                UPDATE tw.company_industry_exposure
                   SET weight_pct = %s, weight_source = %s
                 WHERE id = %s
            """, (r["pct"], source, r["id"]))

    print(f"filled {len(hit)} weights")


if __name__ == "__main__":
    main()
