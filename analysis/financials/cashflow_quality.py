"""
Cash flow & earnings quality — 現金流與盈餘品質.

Cash flow values in tw.cash_flows are YTD cumulative (period_type='A'). We
derive single-quarter flows on-the-fly by subtracting the prior quarter's
cumulative from the current quarter's cumulative within the same year (Q1 is
itself single-quarter by definition).

Metrics:
  Cash flow ratios:
    - ocf_to_ni          營業現金流 / 稅後淨利 (>1 = 盈餘有現金支撐)
    - fcf                自由現金流 = OCF − CAPEX (CAPEX 為負，故實作為 OCF + CAPEX)
    - fcf_margin         FCF / 營收
    - fcf_to_ni          FCF / 稅後淨利
    - capex_intensity    |CAPEX| / 營收 (重資產程度)

  Earnings quality / red flags (比較近一季與去年同季):
    - ar_growth_vs_rev   應收帳款成長 − 營收成長 (正值過大 = 紅燈, 塞貨嫌疑)
    - inv_growth_vs_rev  存貨成長 − 營收成長 (正值過大 = 紅燈, 需求轉弱前兆)

  Cash conversion cycle (CCC, 年化):
    - dso                應收帳款天數 = AR / (revenue × 4) × 365
    - dio                存貨天數     = Inventory / (COGS × 4) × 365
    - dpo                應付帳款天數 = AP / (COGS × 4) × 365
    - ccc                = DSO + DIO − DPO  (愈短愈好, 甚至負值最優)
"""

from db.connection import get_cursor


def _div(a, b):
    if a is None or not b:
        return None
    return float(a) / float(b)


def _pct(a, b, digits=2):
    r = _div(a, b)
    return round(r * 100, digits) if r is not None else None


def _days(num, flow_q, digits=1):
    """Annualise flow_q × 4, then compute days: num / (flow_q×4) × 365."""
    if num is None or not flow_q:
        return None
    annual = float(flow_q) * 4
    return round(float(num) / annual * 365, digits)


def _single_quarter_cashflow(rows: list[dict]) -> list[dict]:
    """
    Convert YTD cumulative cash flow rows to single-quarter values.
    Input rows assumed ordered (year, quarter) ascending.
    """
    result = []
    prev = None
    for r in rows:
        if r["quarter"] == 1 or prev is None or prev["year"] != r["year"]:
            # Q1 is already single-quarter; or reset at year boundary
            single = dict(r)
        else:
            single = {"year": r["year"], "quarter": r["quarter"]}
            for k in ("operating_cash_flow", "capex", "investing_cash_flow",
                     "financing_cash_flow", "free_cash_flow",
                     "depreciation", "amortization"):
                a = r.get(k)
                b = prev.get(k)
                single[k] = (a - b) if a is not None and b is not None else a
        result.append(single)
        prev = r
    return result


def get_cashflow_analysis(stock_id: str, quarters: int = 8) -> list[dict]:
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT i.year, i.quarter,
                   i.revenue, i.cost_of_revenue, i.net_income_attributable,
                   b.accounts_receivable, b.inventory, b.accounts_payable,
                   c.operating_cash_flow, c.capex, c.investing_cash_flow,
                   c.financing_cash_flow, c.free_cash_flow,
                   c.depreciation, c.amortization
            FROM tw.income_statements i
            LEFT JOIN tw.balance_sheets b
              ON b.stock_id = i.stock_id AND b.year = i.year AND b.quarter = i.quarter
            LEFT JOIN tw.cash_flows c
              ON c.stock_id = i.stock_id AND c.year = i.year AND c.quarter = i.quarter
             AND c.period_type = 'A'
            WHERE i.stock_id = %s AND i.period_type = 'Q'
            ORDER BY i.year, i.quarter
            """,
            (stock_id,),
        )
        rows = [dict(r) for r in cur.fetchall()]

    # Convert cumulative cash flow to single-quarter
    cf_single = _single_quarter_cashflow(rows)
    by_yq = {(r["year"], r["quarter"]): r for r in rows}

    result = []
    for idx, base in enumerate(rows):
        cf = cf_single[idx]
        prior = by_yq.get((base["year"] - 1, base["quarter"]))

        ar_growth = _pct(base["accounts_receivable"] - prior["accounts_receivable"]
                         if prior and base["accounts_receivable"] and prior["accounts_receivable"]
                         else None,
                         prior["accounts_receivable"] if prior else None)
        inv_growth = _pct(base["inventory"] - prior["inventory"]
                          if prior and base["inventory"] and prior["inventory"]
                          else None,
                          prior["inventory"] if prior else None)
        rev_growth = _pct(base["revenue"] - prior["revenue"]
                          if prior and base["revenue"] and prior["revenue"]
                          else None,
                          prior["revenue"] if prior else None)

        cogs_q = base["cost_of_revenue"]

        result.append({
            "year": base["year"],
            "quarter": base["quarter"],
            # Cash flow
            "ocf":               cf.get("operating_cash_flow"),
            "capex":             cf.get("capex"),
            "fcf":               cf.get("free_cash_flow"),
            "ocf_to_ni":         round(_div(cf.get("operating_cash_flow"),
                                            base["net_income_attributable"]), 2)
                                  if cf.get("operating_cash_flow") and base["net_income_attributable"]
                                  else None,
            "fcf_margin":        _pct(cf.get("free_cash_flow"), base["revenue"]),
            "fcf_to_ni":         round(_div(cf.get("free_cash_flow"),
                                            base["net_income_attributable"]), 2)
                                  if cf.get("free_cash_flow") and base["net_income_attributable"]
                                  else None,
            "capex_intensity":   _pct(abs(cf["capex"]) if cf.get("capex") else None,
                                      base["revenue"]),
            # Earnings quality red flags (YoY growth diff, %-points)
            "ar_growth_vs_rev":  round(ar_growth - rev_growth, 2)
                                  if ar_growth is not None and rev_growth is not None
                                  else None,
            "inv_growth_vs_rev": round(inv_growth - rev_growth, 2)
                                  if inv_growth is not None and rev_growth is not None
                                  else None,
            # Cash conversion cycle (based on single-quarter flows annualised)
            "dso":               _days(base["accounts_receivable"], base["revenue"]),
            "dio":               _days(base["inventory"], cogs_q),
            "dpo":               _days(base["accounts_payable"], cogs_q),
            "ccc":               None,  # filled below
        })
        if all(result[-1][k] is not None for k in ("dso", "dio", "dpo")):
            result[-1]["ccc"] = round(result[-1]["dso"] + result[-1]["dio"] - result[-1]["dpo"], 1)

    return result[-quarters:]


if __name__ == "__main__":
    import sys
    sid = sys.argv[1] if len(sys.argv) >= 2 else "2330"
    print(f"=== {sid} Cash Flow & Quality ===")
    print(f"{'Period':<8}{'OCF/NI':>8}{'FCF%':>8}{'CAPEX%':>8}"
          f"{'AR-Rev':>8}{'Inv-Rev':>9}{'DSO':>6}{'DIO':>6}{'DPO':>6}{'CCC':>7}")
    for r in get_cashflow_analysis(sid, 12):
        p = f"{r['year']}Q{r['quarter']}"
        print(f"{p:<8}{r['ocf_to_ni'] or '':>8}{r['fcf_margin'] or '':>8}"
              f"{r['capex_intensity'] or '':>8}{r['ar_growth_vs_rev'] or '':>8}"
              f"{r['inv_growth_vs_rev'] or '':>9}{r['dso'] or '':>6}"
              f"{r['dio'] or '':>6}{r['dpo'] or '':>6}{r['ccc'] or '':>7}")
