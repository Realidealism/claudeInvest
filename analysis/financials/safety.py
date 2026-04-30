"""
Safety analysis — 安全性指標.

Metrics (point-in-time, based on balance sheet at quarter end):
  - 流動比率 current_ratio  = current_assets / current_liabilities
  - 速動比率 quick_ratio    = (current_assets - inventory) / current_liabilities
  - 負債比率 debt_ratio     = total_liabilities / total_assets
  - 負債權益比 debt_to_equity = total_liabilities / equity_attributable
  - 長期資金佔固資比 long_term_fund_ratio = (equity_attributable + long_term_debt) / ppe
      (>100% 表示長期資金足以支撐固定資產，不用短債養長投)

Rules of thumb:
  current_ratio > 150%   健康 ; < 100%   警訊
  quick_ratio   > 100%   健康 ; < 50%    警訊
  debt_ratio    < 50%    保守 ; > 70%    高槓桿
  long_term_fund_ratio > 100% 長投與長期資金匹配
"""

from db.connection import get_cursor


def _pct(n, d):
    if n is None or not d:
        return None
    return round(float(n) / float(d) * 100, 2)


def get_safety(stock_id: str, quarters: int = 8) -> list[dict]:
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT year, quarter,
                   current_assets, inventory, current_liabilities,
                   total_assets, total_liabilities,
                   equity_attributable, long_term_debt, ppe
            FROM tw.balance_sheets
            WHERE stock_id = %s
            ORDER BY year, quarter
            """,
            (stock_id,),
        )
        rows = cur.fetchall()

    result = []
    for r in rows:
        current = r["current_assets"]
        inv = r["inventory"] or 0
        cl = r["current_liabilities"]
        long_term_funds = ((r["equity_attributable"] or 0) +
                           (r["long_term_debt"] or 0)) if r["equity_attributable"] else None

        result.append({
            "year": r["year"],
            "quarter": r["quarter"],
            "current_ratio":        _pct(current, cl),
            "quick_ratio":          _pct((current - inv) if current is not None else None, cl),
            "debt_ratio":           _pct(r["total_liabilities"], r["total_assets"]),
            "debt_to_equity":       _pct(r["total_liabilities"], r["equity_attributable"]),
            "long_term_fund_ratio": _pct(long_term_funds, r["ppe"]),
        })

    return result[-quarters:]


if __name__ == "__main__":
    import sys
    sid = sys.argv[1] if len(sys.argv) >= 2 else "2330"
    print(f"=== {sid} Safety ===")
    print(f"{'Period':<10}{'Current%':>10}{'Quick%':>10}{'Debt%':>10}{'D/E%':>10}{'LTFund%':>10}")
    for r in get_safety(sid, 12):
        period = f"{r['year']}Q{r['quarter']}"
        print(f"{period:<10}{r['current_ratio'] or '':>10}{r['quick_ratio'] or '':>10}"
              f"{r['debt_ratio'] or '':>10}{r['debt_to_equity'] or '':>10}"
              f"{r['long_term_fund_ratio'] or '':>10}")
