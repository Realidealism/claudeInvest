"""
Piotroski F-Score — 9-point financial strength score.

Originally proposed by Joseph Piotroski (2000) to distinguish strong from weak
firms among value stocks. Each criterion scores 1 or 0; total 0–9.

Interpretation:
  7–9  strong financial improvement
  4–6  mixed / stable
  0–3  weak / deteriorating

We compute F-Score on a TTM (trailing 4-quarter) basis so it updates every
quarter rather than waiting a full year. The YoY comparison baseline is the
same stock's TTM values as of four quarters ago.

Criteria (all compared to prior year's TTM / year-ago balance sheet):

  Profitability (4):
    1. ROA > 0                       (positive TTM net income)
    2. OCF > 0                       (positive TTM operating cash flow)
    3. ΔROA > 0                      (ROA higher than year ago)
    4. Accruals: OCF/assets > ROA    (earnings backed by cash, not accruals)

  Leverage, Liquidity, Funding (3):
    5. ΔLeverage < 0                 (long-term-debt/assets decreasing)
    6. ΔCurrent ratio > 0            (liquidity improving)
    7. No share issuance             (shares outstanding not increasing)

  Operating Efficiency (2):
    8. ΔGross margin > 0             (gross margin improving)
    9. ΔAsset turnover > 0           (asset turnover improving)
"""

from db.connection import get_cursor


def _ttm_sum(vals: list, key: str) -> float | None:
    vs = [v[key] for v in vals if v.get(key) is not None]
    if len(vs) < 4:
        return None
    return float(sum(vs[-4:]))


def _single_quarter_ocf(cf_rows: list[dict]) -> dict:
    """Convert YTD cumulative OCF into (year,quarter) → single-quarter OCF."""
    out = {}
    prev = None
    for r in cf_rows:
        if r["quarter"] == 1 or prev is None or prev["year"] != r["year"]:
            single = r.get("operating_cash_flow")
        else:
            a = r.get("operating_cash_flow")
            b = prev.get("operating_cash_flow")
            single = (a - b) if a is not None and b is not None else a
        out[(r["year"], r["quarter"])] = single
        prev = r
    return out


def get_piotroski(stock_id: str) -> dict:
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT year, quarter, revenue, gross_profit, net_income_attributable "
            "FROM tw.income_statements WHERE stock_id = %s AND period_type = 'Q' "
            "ORDER BY year, quarter",
            (stock_id,),
        )
        inc = [dict(r) for r in cur.fetchall()]
        cur.execute(
            "SELECT year, quarter, total_assets, current_assets, current_liabilities, "
            "long_term_debt, common_stock FROM tw.balance_sheets "
            "WHERE stock_id = %s ORDER BY year, quarter",
            (stock_id,),
        )
        bs = {(r["year"], r["quarter"]): dict(r) for r in cur.fetchall()}
        cur.execute(
            "SELECT year, quarter, operating_cash_flow FROM tw.cash_flows "
            "WHERE stock_id = %s AND period_type = 'A' ORDER BY year, quarter",
            (stock_id,),
        )
        cf_rows = [dict(r) for r in cur.fetchall()]

    if len(inc) < 8:
        return {"status": "insufficient", "need": "at least 8 quarters"}

    ocf_q = _single_quarter_ocf(cf_rows)
    # Inject single-quarter OCF into income rows for TTM summing
    for r in inc:
        r["ocf_q"] = ocf_q.get((r["year"], r["quarter"]))

    # TTM windows: current (last 4) vs prior (4 quarters before that)
    curr_inc = inc[-4:]
    prior_inc = inc[-8:-4]
    curr_end = (curr_inc[-1]["year"], curr_inc[-1]["quarter"])
    prior_end = (prior_inc[-1]["year"], prior_inc[-1]["quarter"])
    curr_bs = bs.get(curr_end)
    prior_bs = bs.get(prior_end)
    if not curr_bs or not prior_bs:
        return {"status": "insufficient", "need": "balance sheets missing"}

    ttm_ni = _ttm_sum(curr_inc, "net_income_attributable")
    prior_ni = _ttm_sum(prior_inc, "net_income_attributable")
    ttm_ocf = _ttm_sum(curr_inc, "ocf_q")
    ttm_rev = _ttm_sum(curr_inc, "revenue")
    prior_rev = _ttm_sum(prior_inc, "revenue")
    ttm_gp = _ttm_sum(curr_inc, "gross_profit")
    prior_gp = _ttm_sum(prior_inc, "gross_profit")

    curr_assets = curr_bs["total_assets"]
    prior_assets = prior_bs["total_assets"]

    def safe_div(a, b):
        return float(a) / float(b) if a is not None and b else None

    roa_curr = safe_div(ttm_ni, curr_assets)
    roa_prior = safe_div(prior_ni, prior_assets)
    gm_curr = safe_div(ttm_gp, ttm_rev)
    gm_prior = safe_div(prior_gp, prior_rev)
    at_curr = safe_div(ttm_rev, curr_assets)
    at_prior = safe_div(prior_rev, prior_assets)
    cr_curr = safe_div(curr_bs["current_assets"], curr_bs["current_liabilities"])
    cr_prior = safe_div(prior_bs["current_assets"], prior_bs["current_liabilities"])
    lev_curr = safe_div(curr_bs["long_term_debt"] or 0, curr_assets)
    lev_prior = safe_div(prior_bs["long_term_debt"] or 0, prior_assets)
    ocf_to_assets = safe_div(ttm_ocf, curr_assets)

    criteria = {
        "roa_positive":         bool(roa_curr and roa_curr > 0),
        "ocf_positive":         bool(ttm_ocf and ttm_ocf > 0),
        "roa_improving":        bool(roa_curr is not None and roa_prior is not None
                                     and roa_curr > roa_prior),
        "accruals_quality":     bool(ocf_to_assets is not None and roa_curr is not None
                                     and ocf_to_assets > roa_curr),
        "leverage_decreasing":  bool(lev_curr is not None and lev_prior is not None
                                     and lev_curr < lev_prior),
        "liquidity_improving":  bool(cr_curr is not None and cr_prior is not None
                                     and cr_curr > cr_prior),
        "no_dilution":          bool(curr_bs["common_stock"] and prior_bs["common_stock"]
                                     and curr_bs["common_stock"] <= prior_bs["common_stock"]),
        "margin_improving":     bool(gm_curr is not None and gm_prior is not None
                                     and gm_curr > gm_prior),
        "turnover_improving":   bool(at_curr is not None and at_prior is not None
                                     and at_curr > at_prior),
    }

    score = sum(1 for v in criteria.values() if v)
    return {
        "status": "ok",
        "score": score,
        "max": 9,
        "period": f"{curr_end[0]}Q{curr_end[1]} vs {prior_end[0]}Q{prior_end[1]}",
        "criteria": criteria,
    }


if __name__ == "__main__":
    import sys
    sid = sys.argv[1] if len(sys.argv) >= 2 else "2330"
    r = get_piotroski(sid)
    print(f"=== {sid} Piotroski F-Score ===")
    if r.get("status") != "ok":
        print(r)
    else:
        print(f"Score: {r['score']} / {r['max']}   Period: {r['period']}")
        for k, v in r["criteria"].items():
            print(f"  [{'V' if v else ' '}] {k}")
