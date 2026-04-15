"""
Growth analysis — 成長力指標.

Metrics (per quarter, single-quarter basis):
  - revenue_yoy          單季營收 YoY %
  - gross_profit_yoy     單季毛利 YoY %
  - operating_income_yoy 單季營業利益 YoY %
  - net_income_yoy       單季稅後淨利 YoY %
  - eps_yoy              單季 EPS YoY %

YoY compares the same quarter against one year ago (Q3 vs Q3-last-year) to
eliminate seasonal distortion. QoQ is deliberately omitted since 台股
electronics sector has strong seasonality (Q4 旺季) that makes QoQ misleading.
"""

from db.connection import get_cursor


def _yoy(current, prior):
    if current is None or prior is None or prior == 0:
        return None
    return round((float(current) - float(prior)) / abs(float(prior)) * 100, 2)


def get_growth(stock_id: str, quarters: int = 8) -> list[dict]:
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT year, quarter,
                   revenue, gross_profit, operating_income,
                   net_income_attributable, eps
            FROM tw.income_statements
            WHERE stock_id = %s AND period_type = 'Q'
            ORDER BY year, quarter
            """,
            (stock_id,),
        )
        rows = cur.fetchall()

    # Build lookup: (year, quarter) → row
    by_yq = {(r["year"], r["quarter"]): r for r in rows}

    result = []
    for r in rows:
        prior = by_yq.get((r["year"] - 1, r["quarter"]))
        if prior is None:
            continue
        result.append({
            "year": r["year"],
            "quarter": r["quarter"],
            "revenue":              r["revenue"],
            "revenue_yoy":          _yoy(r["revenue"], prior["revenue"]),
            "gross_profit_yoy":     _yoy(r["gross_profit"], prior["gross_profit"]),
            "operating_income_yoy": _yoy(r["operating_income"], prior["operating_income"]),
            "net_income_yoy":       _yoy(r["net_income_attributable"], prior["net_income_attributable"]),
            "eps_yoy":              _yoy(r["eps"], prior["eps"]),
        })

    return result[-quarters:]


if __name__ == "__main__":
    import sys
    sid = sys.argv[1] if len(sys.argv) >= 2 else "2330"
    print(f"=== {sid} Growth (YoY %) ===")
    print(f"{'Period':<10}{'Rev':>10}{'GP':>10}{'OP':>10}{'NI':>10}{'EPS':>10}")
    for r in get_growth(sid, 12):
        p = f"{r['year']}Q{r['quarter']}"
        print(f"{p:<10}{r['revenue_yoy'] or '':>10}{r['gross_profit_yoy'] or '':>10}"
              f"{r['operating_income_yoy'] or '':>10}{r['net_income_yoy'] or '':>10}"
              f"{r['eps_yoy'] or '':>10}")
