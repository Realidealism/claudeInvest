"""
Revenue historic low stock screener (營收歷史新低).

Condition: current month's revenue is the lowest in the stock's
entire history (all-time low), signaling fundamental deterioration.

Usage:
  python -m analysis.revenue_historic_low                # latest month
  python -m analysis.revenue_historic_low 2026-03        # specific month
"""

import sys
from db.connection import get_cursor

# Minimum months of history required
MIN_HISTORY = 24


def screen(year_month: str = None) -> list[dict]:
    with get_cursor(commit=False) as cur:
        if not year_month:
            cur.execute("SELECT MAX(year_month) AS ym FROM tw.monthly_revenue")
            year_month = cur.fetchone()["ym"]

        print(f"Historic low screening for {year_month} ...")

        cur.execute("""
            SELECT r.stock_id, r.revenue, r.yoy_pct, r.mom_pct, r.note,
                   s.name, s.market, s.industry
            FROM tw.monthly_revenue r
            JOIN tw.stocks s ON r.stock_id = s.stock_id
            WHERE r.year_month = %s
              AND s.is_active = TRUE
        """, (year_month,))
        candidates = cur.fetchall()

        results = []
        for c in candidates:
            stock_id = c["stock_id"]
            revenue = c["revenue"]

            cur.execute("""
                SELECT MIN(revenue) AS min_rev,
                       AVG(revenue) AS avg_rev,
                       COUNT(*) AS months
                FROM tw.monthly_revenue
                WHERE stock_id = %s AND year_month < %s
            """, (stock_id, year_month))
            hist = cur.fetchone()

            if not hist["min_rev"] or hist["months"] < MIN_HISTORY:
                continue

            prev_min = hist["min_rev"]
            avg_rev = float(hist["avg_rev"])

            if revenue > prev_min:
                continue

            results.append({
                "stock_id": stock_id,
                "name": c["name"],
                "market": c["market"],
                "industry": c["industry"],
                "revenue": revenue,
                "prev_min_revenue": prev_min,
                "avg_revenue": round(avg_rev),
                "below_avg_pct": round((revenue - avg_rev) / avg_rev * 100, 2) if avg_rev else None,
                "yoy_pct": float(c["yoy_pct"]) if c["yoy_pct"] is not None else None,
                "mom_pct": float(c["mom_pct"]) if c["mom_pct"] is not None else None,
                "note": c["note"],
            })

        results.sort(key=lambda x: x["below_avg_pct"] or 0)
        print(f"Found {len(results)} stocks at historic low revenue.")
        return results


if __name__ == "__main__":
    ym = sys.argv[1] if len(sys.argv) >= 2 else None
    results = screen(ym)
    if not results:
        print("No historic low stocks found.")
