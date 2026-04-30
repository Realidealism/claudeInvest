"""
Peak season miss stock screener (旺季不旺).

Condition:
  1. Stock has clear seasonality (CV >= threshold)
  2. Target month is a "peak season" month (coefficient > 1.0)
  3. Revenue is below the historical same-month average

Jan+Feb are merged to neutralize Lunar New Year distortion.

Usage:
  python -m analysis.revenue_peak_miss                # latest month
  python -m analysis.revenue_peak_miss 2026-03        # specific month
"""

import sys
from db.connection import get_cursor
from analysis.revenue_off_season import (
    _compute_seasonality, _same_month_history, MIN_CV, MIN_YEARS
)
from collections import defaultdict


def screen(year_month: str = None, min_cv: float = MIN_CV) -> list[dict]:
    with get_cursor(commit=False) as cur:
        if not year_month:
            cur.execute("SELECT MAX(year_month) AS ym FROM tw.monthly_revenue")
            year_month = cur.fetchone()["ym"]

        print(f"Peak-miss screening for {year_month} ...")

        target_month = int(year_month[5:7])
        if target_month in (1, 2):
            target_period = "1+2"
        else:
            target_period = str(target_month)

        # Load all revenue data for seasonality computation
        cur.execute("""
            SELECT r.stock_id, r.year_month, r.revenue
            FROM tw.monthly_revenue r
            JOIN tw.stocks s ON r.stock_id = s.stock_id
            WHERE s.is_active = TRUE
            ORDER BY r.stock_id, r.year_month
        """)
        all_rows = cur.fetchall()

        stock_data = defaultdict(list)
        for r in all_rows:
            stock_data[r["stock_id"]].append((r["year_month"], r["revenue"]))

        # Get target month revenue and stock info
        cur.execute("""
            SELECT r.stock_id, r.revenue, r.yoy_pct, r.mom_pct, r.note,
                   s.name, s.market, s.industry
            FROM tw.monthly_revenue r
            JOIN tw.stocks s ON r.stock_id = s.stock_id
            WHERE r.year_month = %s AND s.is_active = TRUE
        """, (year_month,))
        target_rows = {r["stock_id"]: r for r in cur.fetchall()}

        # For 1+2 period, also need the other month's revenue
        partner_revenue = {}
        if target_period == "1+2":
            partner_month = 1 if target_month == 2 else 2
            partner_ym = f"{year_month[:4]}-{partner_month:02d}"
            cur.execute("""
                SELECT stock_id, revenue FROM tw.monthly_revenue
                WHERE year_month = %s
            """, (partner_ym,))
            for r in cur.fetchall():
                partner_revenue[r["stock_id"]] = r["revenue"]

        results = []
        seasonal_count = 0

        for stock_id, data in stock_data.items():
            if stock_id not in target_rows:
                continue

            seasonality = _compute_seasonality(data)
            if seasonality is None:
                continue

            cv = seasonality["__cv"]
            if cv < min_cv:
                continue

            seasonal_count += 1
            coeff = seasonality.get(target_period)
            if coeff is None:
                continue

            # Must be a peak-season month (coefficient > 1.0)
            if coeff <= 1.0:
                continue

            c = target_rows[stock_id]
            if target_period == "1+2":
                partner_rev = partner_revenue.get(stock_id)
                if partner_rev is None:
                    continue
                current_rev = float(c["revenue"] + partner_rev) / 2
            else:
                current_rev = float(c["revenue"])

            hist_avg, year_count = _same_month_history(
                cur, stock_id, target_period, year_month)
            if not hist_avg or year_count < MIN_YEARS:
                continue

            # Must be BELOW historical average (旺季不旺)
            if current_rev >= hist_avg:
                continue

            miss_pct = round((current_rev - hist_avg) / hist_avg * 100, 2)

            results.append({
                "stock_id": stock_id,
                "name": c["name"],
                "market": c["market"],
                "industry": c["industry"],
                "revenue": c["revenue"],
                "period": target_period,
                "seasonal_coeff": round(coeff, 3),
                "cv": round(cv, 4),
                "hist_avg": round(hist_avg),
                "miss_pct": miss_pct,
                "yoy_pct": float(c["yoy_pct"]) if c["yoy_pct"] is not None else None,
                "mom_pct": float(c["mom_pct"]) if c["mom_pct"] is not None else None,
                "note": c["note"],
            })

        results.sort(key=lambda x: x["miss_pct"])
        print(f"Seasonal stocks (CV >= {min_cv}): {seasonal_count}")
        print(f"Found {len(results)} peak-miss stocks.")
        return results


if __name__ == "__main__":
    ym = sys.argv[1] if len(sys.argv) >= 2 else None
    results = screen(ym)
    if not results:
        print("No peak-miss stocks found.")
