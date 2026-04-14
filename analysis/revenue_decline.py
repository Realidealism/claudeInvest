"""
Revenue consecutive decline stock screener (營收連續衰退).

Condition: YoY has been negative for N or more consecutive months
up to and including the target month, with the decline deepening
(latest YoY% worse than average of the streak).

Usage:
  python -m analysis.revenue_decline                # latest month
  python -m analysis.revenue_decline 2026-03        # specific month
"""

import sys
from db.connection import get_cursor

# Minimum consecutive months of negative YoY
MIN_STREAK = 6


def screen(year_month: str = None) -> list[dict]:
    with get_cursor(commit=False) as cur:
        if not year_month:
            cur.execute("SELECT MAX(year_month) AS ym FROM tw.monthly_revenue")
            year_month = cur.fetchone()["ym"]

        print(f"Consecutive decline screening for {year_month} ...")

        cur.execute("""
            SELECT r.stock_id, r.revenue, r.yoy_pct, r.mom_pct, r.note,
                   s.name, s.market, s.industry
            FROM tw.monthly_revenue r
            JOIN tw.stocks s ON r.stock_id = s.stock_id
            WHERE r.year_month = %s
              AND r.yoy_pct < 0
              AND s.is_active = TRUE
        """, (year_month,))
        candidates = cur.fetchall()

        results = []
        for c in candidates:
            stock_id = c["stock_id"]

            cur.execute("""
                SELECT year_month, yoy_pct
                FROM tw.monthly_revenue
                WHERE stock_id = %s AND year_month <= %s
                ORDER BY year_month DESC
                LIMIT 60
            """, (stock_id, year_month))
            history = cur.fetchall()

            streak = 0
            yoy_sum = 0
            worst_yoy = None
            for r in history:
                if r["yoy_pct"] is not None and r["yoy_pct"] < 0:
                    streak += 1
                    yoy_val = float(r["yoy_pct"])
                    yoy_sum += yoy_val
                    if worst_yoy is None or yoy_val < worst_yoy:
                        worst_yoy = yoy_val
                else:
                    break

            if streak < MIN_STREAK:
                continue

            avg_yoy = round(yoy_sum / streak, 2)
            current_yoy = float(c["yoy_pct"])

            results.append({
                "stock_id": stock_id,
                "name": c["name"],
                "market": c["market"],
                "industry": c["industry"],
                "revenue": c["revenue"],
                "yoy_pct": current_yoy,
                "streak": streak,
                "avg_yoy_pct": avg_yoy,
                "worst_yoy_pct": worst_yoy,
                "deepening": current_yoy < avg_yoy,
                "mom_pct": float(c["mom_pct"]) if c["mom_pct"] is not None else None,
                "note": c["note"],
            })

        results.sort(key=lambda x: x["yoy_pct"])
        print(f"Found {len(results)} stocks with {MIN_STREAK}+ consecutive decline months.")
        return results


if __name__ == "__main__":
    ym = sys.argv[1] if len(sys.argv) >= 2 else None
    results = screen(ym)
    if not results:
        print("No consecutive decline stocks found.")
