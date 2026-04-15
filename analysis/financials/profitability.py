"""
Profitability analysis — 獲利能力指標.

Metrics (quarterly, single-quarter basis):
  - 毛利率   gross_margin     = gross_profit / revenue
  - 營業利益率 operating_margin = operating_income / revenue
  - 稅後淨利率 net_margin      = net_income_attributable / revenue
  - ROE (單季年化) = net_income_attributable / avg(equity_attributable) * 4
  - ROA (單季年化) = net_income / avg(total_assets) * 4
  - EPS (單季)

DuPont decomposition (ROE = 淨利率 × 資產週轉率 × 權益乘數):
  - net_margin      = net_income / revenue
  - asset_turnover  = revenue / avg(total_assets)   (annualised × 4)
  - equity_multiplier = avg(total_assets) / avg(equity)

ROE and ROA use *average* balance sheet values (beginning + ending) / 2 to
avoid the bias of comparing a period's income against a point-in-time snapshot.
For the first available quarter we fall back to the ending balance only.
"""

from db.connection import get_cursor


def _avg(a, b):
    if a is None or b is None:
        return a if b is None else b
    return (a + b) / 2


def _pct(numerator, denominator):
    if numerator is None or not denominator:
        return None
    return round(float(numerator) / float(denominator) * 100, 2)


def get_profitability(stock_id: str, quarters: int = 8) -> list[dict]:
    """
    Return per-quarter profitability metrics for the most recent `quarters` periods.
    Ordered oldest → newest.
    """
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT i.year, i.quarter,
                   i.revenue, i.gross_profit, i.operating_income,
                   i.net_income, i.net_income_attributable, i.eps,
                   b.total_assets, b.equity_attributable
            FROM tw.income_statements i
            LEFT JOIN tw.balance_sheets b
              ON b.stock_id = i.stock_id
             AND b.year = i.year
             AND b.quarter = i.quarter
            WHERE i.stock_id = %s AND i.period_type = 'Q'
            ORDER BY i.year, i.quarter
            """,
            (stock_id,),
        )
        rows = cur.fetchall()

    if not rows:
        return []

    result = []
    for idx, r in enumerate(rows):
        prev = rows[idx - 1] if idx > 0 else None
        avg_assets = _avg(prev["total_assets"] if prev else None, r["total_assets"])
        avg_equity = _avg(prev["equity_attributable"] if prev else None,
                          r["equity_attributable"])

        result.append({
            "year": r["year"],
            "quarter": r["quarter"],
            "revenue":           r["revenue"],
            "gross_margin":      _pct(r["gross_profit"], r["revenue"]),
            "operating_margin":  _pct(r["operating_income"], r["revenue"]),
            "net_margin":        _pct(r["net_income_attributable"], r["revenue"]),
            "roe_annualized":    _pct(r["net_income_attributable"] * 4 if r["net_income_attributable"] else None,
                                      avg_equity),
            "roa_annualized":    _pct(r["net_income"] * 4 if r["net_income"] else None,
                                      avg_assets),
            "eps":               float(r["eps"]) if r["eps"] is not None else None,
        })

    return result[-quarters:]


def get_dupont(stock_id: str, quarters: int = 8) -> list[dict]:
    """
    DuPont decomposition: ROE = net_margin × asset_turnover × equity_multiplier.
    All three are annualised on a single-quarter basis (× 4 for flow items).
    """
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT i.year, i.quarter,
                   i.revenue, i.net_income_attributable,
                   b.total_assets, b.equity_attributable
            FROM tw.income_statements i
            LEFT JOIN tw.balance_sheets b
              ON b.stock_id = i.stock_id
             AND b.year = i.year
             AND b.quarter = i.quarter
            WHERE i.stock_id = %s AND i.period_type = 'Q'
            ORDER BY i.year, i.quarter
            """,
            (stock_id,),
        )
        rows = cur.fetchall()

    result = []
    for idx, r in enumerate(rows):
        prev = rows[idx - 1] if idx > 0 else None
        avg_assets = _avg(prev["total_assets"] if prev else None, r["total_assets"])
        avg_equity = _avg(prev["equity_attributable"] if prev else None,
                          r["equity_attributable"])

        net_margin    = _pct(r["net_income_attributable"], r["revenue"])
        # asset turnover: revenue × 4 / avg_assets (annualised)
        asset_turn    = (float(r["revenue"]) * 4 / float(avg_assets)
                         if r["revenue"] and avg_assets else None)
        equity_mult   = (float(avg_assets) / float(avg_equity)
                         if avg_assets and avg_equity else None)
        roe_check     = (net_margin / 100 * asset_turn * equity_mult * 100
                         if None not in (net_margin, asset_turn, equity_mult) else None)

        result.append({
            "year": r["year"],
            "quarter": r["quarter"],
            "net_margin":        net_margin,
            "asset_turnover":    round(asset_turn, 3) if asset_turn is not None else None,
            "equity_multiplier": round(equity_mult, 3) if equity_mult is not None else None,
            "roe_decomposed":    round(roe_check, 2) if roe_check is not None else None,
        })

    return result[-quarters:]


if __name__ == "__main__":
    import sys
    sid = sys.argv[1] if len(sys.argv) >= 2 else "2330"

    print(f"=== {sid} Profitability ===")
    print(f"{'Period':<10}{'GM%':>8}{'OM%':>8}{'NM%':>8}{'ROE%':>8}{'ROA%':>8}{'EPS':>8}")
    for r in get_profitability(sid, 12):
        period = f"{r['year']}Q{r['quarter']}"
        print(f"{period:<10}{r['gross_margin'] or '':>8}{r['operating_margin'] or '':>8}"
              f"{r['net_margin'] or '':>8}{r['roe_annualized'] or '':>8}"
              f"{r['roa_annualized'] or '':>8}{r['eps'] or '':>8}")

    print(f"\n=== {sid} DuPont ===")
    print(f"{'Period':<10}{'NM%':>8}{'AT':>8}{'EM':>8}{'ROE%':>8}")
    for r in get_dupont(sid, 12):
        period = f"{r['year']}Q{r['quarter']}"
        print(f"{period:<10}{r['net_margin'] or '':>8}{r['asset_turnover'] or '':>8}"
              f"{r['equity_multiplier'] or '':>8}{r['roe_decomposed'] or '':>8}")
