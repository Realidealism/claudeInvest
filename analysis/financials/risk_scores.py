"""
Altman Z-Score & Beneish M-Score — 破產預警與財報操縱偵測.

Altman Z-Score (1968, for manufacturing firms):
  Z = 1.2·A + 1.4·B + 3.3·C + 0.6·D + 1.0·E
  A = 營運資金 / 總資產
  B = 保留盈餘 / 總資產
  C = EBIT / 總資產                (用營業利益近似 EBIT)
  D = 股票市值 / 總負債
  E = 營收 / 總資產 (TTM)

  Z > 2.99         安全
  1.81 < Z < 2.99  灰色地帶
  Z < 1.81         破產風險

Beneish M-Score (1999, earnings manipulation detector):
  Eight indices comparing latest period to prior period:
    DSRI  Days-Sales-in-Receivables index
    GMI   Gross-Margin index         (prior GM / current GM)
    AQI   Asset-Quality index
    SGI   Sales-Growth index
    DEPI  Depreciation index
    LVGI  Leverage index
    TATA  Total Accruals to Total Assets
  (SGAI 因 FinMind 未提供 SGA 明細，以 OperatingExpenses/Revenue 近似)

  M = -4.84 + 0.92·DSRI + 0.528·GMI + 0.404·AQI + 0.892·SGI
        + 0.115·DEPI - 0.172·SGAI + 4.679·TATA - 0.327·LVGI

  M > -1.78  可能操縱財報 (紅燈)
  M ≤ -1.78  通過檢測
"""

from db.connection import get_cursor


def _fetch(stock_id: str):
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT year, quarter, revenue, cost_of_revenue, gross_profit, "
            "operating_income, operating_expenses, net_income_attributable "
            "FROM tw.income_statements WHERE stock_id = %s AND period_type = 'Q' "
            "ORDER BY year, quarter",
            (stock_id,),
        )
        inc = [dict(r) for r in cur.fetchall()]
        cur.execute(
            "SELECT year, quarter, current_assets, current_liabilities, total_assets, "
            "total_liabilities, retained_earnings, accounts_receivable, "
            "ppe, long_term_debt FROM tw.balance_sheets "
            "WHERE stock_id = %s ORDER BY year, quarter",
            (stock_id,),
        )
        bs = {(r["year"], r["quarter"]): dict(r) for r in cur.fetchall()}
        cur.execute(
            "SELECT year, quarter, depreciation, operating_cash_flow "
            "FROM tw.cash_flows WHERE stock_id = %s AND period_type = 'A' "
            "ORDER BY year, quarter",
            (stock_id,),
        )
        cf = [dict(r) for r in cur.fetchall()]
        cur.execute(
            "SELECT close_price FROM tw.daily_prices "
            "WHERE stock_id = %s ORDER BY trade_date DESC LIMIT 1",
            (stock_id,),
        )
        last_price = cur.fetchone()
        cur.execute(
            "SELECT common_stock FROM tw.balance_sheets "
            "WHERE stock_id = %s ORDER BY year DESC, quarter DESC LIMIT 1",
            (stock_id,),
        )
        latest_cs = cur.fetchone()
    return inc, bs, cf, last_price, latest_cs


def _ttm(rows, key, end_idx):
    vs = [r[key] for r in rows[end_idx - 3: end_idx + 1] if r[key] is not None]
    return float(sum(vs)) if len(vs) == 4 else None


def _ttm_cf(cf_rows, year, quarter, key):
    """Derive TTM single-quarter sum from cumulative YTD cash-flow rows."""
    total = 0.0
    count = 0
    # Iterate backward: sum last 4 single-quarter values
    prev = None
    by_yq = {(r["year"], r["quarter"]): r for r in cf_rows}
    # Walk back 4 quarters
    y, q = year, quarter
    for _ in range(4):
        r = by_yq.get((y, q))
        if r and r.get(key) is not None:
            v = float(r[key])
            if q == 1:
                single = v
            else:
                prev_r = by_yq.get((y, q - 1))
                single = v - float(prev_r[key]) if prev_r and prev_r.get(key) is not None else v
            total += single
            count += 1
        q -= 1
        if q == 0:
            q = 4
            y -= 1
    return total if count == 4 else None


def altman_z(stock_id: str) -> dict:
    inc, bs, cf, last_price, latest_cs = _fetch(stock_id)
    if len(inc) < 4 or not last_price or not latest_cs:
        return {"status": "insufficient"}
    end_idx = len(inc) - 1
    last = inc[end_idx]
    b = bs.get((last["year"], last["quarter"]))
    if not b:
        return {"status": "insufficient"}

    ttm_rev = _ttm(inc, "revenue", end_idx)
    ttm_ebit = _ttm(inc, "operating_income", end_idx)
    total_assets = b["total_assets"]
    total_liab = b["total_liabilities"]
    wc = (b["current_assets"] or 0) - (b["current_liabilities"] or 0)
    re = b["retained_earnings"] or 0

    # Market value of equity = shares × price; shares = common_stock×1000 / 10
    shares = (latest_cs["common_stock"] or 0) * 1000 // 10  # par=10
    mv = shares * float(last_price["close_price"])  # NTD (not 千元)
    mv_in_kntd = mv / 1000  # convert to 千元 to match balance sheet

    if not total_assets or not total_liab:
        return {"status": "insufficient"}

    A = wc / total_assets
    B = re / total_assets
    C = (ttm_ebit / total_assets) if ttm_ebit is not None else 0
    D = mv_in_kntd / total_liab
    E = (ttm_rev / total_assets) if ttm_rev is not None else 0

    z = 1.2 * A + 1.4 * B + 3.3 * C + 0.6 * D + 1.0 * E

    if z > 2.99:
        zone = "safe"
    elif z > 1.81:
        zone = "grey"
    else:
        zone = "distress"

    return {
        "status": "ok",
        "z_score": round(z, 2),
        "zone": zone,
        "components": {
            "wc_to_ta":  round(A, 3),
            "re_to_ta":  round(B, 3),
            "ebit_to_ta": round(C, 3),
            "mv_to_tl":  round(D, 3),
            "sales_to_ta": round(E, 3),
        },
    }


def beneish_m(stock_id: str) -> dict:
    inc, bs, cf, _, _ = _fetch(stock_id)
    if len(inc) < 8:
        return {"status": "insufficient"}

    end_idx = len(inc) - 1
    curr = inc[end_idx]
    prior = inc[end_idx - 4]
    b_curr = bs.get((curr["year"], curr["quarter"]))
    b_prior = bs.get((prior["year"], prior["quarter"]))
    if not (b_curr and b_prior):
        return {"status": "insufficient"}

    # TTM flows
    rev_c = _ttm(inc, "revenue", end_idx)
    rev_p = _ttm(inc, "revenue", end_idx - 4)
    cogs_c = _ttm(inc, "cost_of_revenue", end_idx)
    cogs_p = _ttm(inc, "cost_of_revenue", end_idx - 4)
    opex_c = _ttm(inc, "operating_expenses", end_idx)
    opex_p = _ttm(inc, "operating_expenses", end_idx - 4)
    ni_c   = _ttm(inc, "net_income_attributable", end_idx)
    dep_c = _ttm_cf(cf, curr["year"], curr["quarter"], "depreciation")
    dep_p = _ttm_cf(cf, prior["year"], prior["quarter"], "depreciation")
    ocf_c = _ttm_cf(cf, curr["year"], curr["quarter"], "operating_cash_flow")

    if None in (rev_c, rev_p, cogs_c, cogs_p, ni_c, ocf_c):
        return {"status": "insufficient"}

    def sd(a, b): return float(a) / float(b) if a is not None and b else None

    # DSRI = (AR_c/Rev_c) / (AR_p/Rev_p)
    dsri_num = sd(b_curr["accounts_receivable"], rev_c)
    dsri_den = sd(b_prior["accounts_receivable"], rev_p)
    DSRI = dsri_num / dsri_den if dsri_num and dsri_den else 1

    # GMI = (prior GM) / (current GM)
    gm_c = (rev_c - cogs_c) / rev_c if rev_c else None
    gm_p = (rev_p - cogs_p) / rev_p if rev_p else None
    GMI = gm_p / gm_c if gm_c and gm_p else 1

    # AQI = non-current, non-PPE assets / total assets  (current vs prior)
    def aqi_raw(b):
        if not b["total_assets"]: return None
        nonquality = b["total_assets"] - (b["current_assets"] or 0) - (b["ppe"] or 0)
        return nonquality / b["total_assets"]
    aqi_c = aqi_raw(b_curr); aqi_p = aqi_raw(b_prior)
    AQI = aqi_c / aqi_p if aqi_c and aqi_p else 1

    # SGI = current sales / prior sales
    SGI = rev_c / rev_p if rev_p else 1

    # DEPI = (prior dep/(dep+PPE)) / (current dep/(dep+PPE))
    def dep_ratio(d, ppe):
        return d / (d + ppe) if d and ppe else None
    dep_c_r = dep_ratio(dep_c, b_curr["ppe"])
    dep_p_r = dep_ratio(dep_p, b_prior["ppe"])
    DEPI = dep_p_r / dep_c_r if dep_c_r and dep_p_r else 1

    # SGAI (proxy: OpEx / Sales)
    sgai_c = sd(opex_c, rev_c); sgai_p = sd(opex_p, rev_p)
    SGAI = sgai_c / sgai_p if sgai_c and sgai_p else 1

    # LVGI = (TL_c/TA_c) / (TL_p/TA_p)
    lv_c = sd(b_curr["total_liabilities"], b_curr["total_assets"])
    lv_p = sd(b_prior["total_liabilities"], b_prior["total_assets"])
    LVGI = lv_c / lv_p if lv_c and lv_p else 1

    # TATA = (NI - OCF) / TA
    TATA = (ni_c - ocf_c) / b_curr["total_assets"]

    M = (-4.84 + 0.92 * DSRI + 0.528 * GMI + 0.404 * AQI + 0.892 * SGI
         + 0.115 * DEPI - 0.172 * SGAI + 4.679 * TATA - 0.327 * LVGI)

    return {
        "status": "ok",
        "m_score": round(M, 2),
        "flag": "manipulation_suspected" if M > -1.78 else "clean",
        "threshold": -1.78,
        "indices": {
            "DSRI": round(DSRI, 3), "GMI": round(GMI, 3), "AQI": round(AQI, 3),
            "SGI":  round(SGI, 3),  "DEPI": round(DEPI, 3), "SGAI": round(SGAI, 3),
            "LVGI": round(LVGI, 3), "TATA": round(TATA, 3),
        },
    }


if __name__ == "__main__":
    import sys
    sid = sys.argv[1] if len(sys.argv) >= 2 else "2330"
    z = altman_z(sid)
    m = beneish_m(sid)
    print(f"=== {sid} Altman Z-Score ===")
    print(f"  Z = {z.get('z_score')}  ({z.get('zone')})")
    print(f"  components: {z.get('components')}")
    print(f"\n=== {sid} Beneish M-Score ===")
    print(f"  M = {m.get('m_score')}  flag: {m.get('flag')}")
    print(f"  indices: {m.get('indices')}")
