"""
Stock financial summary — aggregates all analysis modules into one snapshot.

Usage:
  python -m analysis.financials.summary 2330
"""

from .profitability import get_profitability, get_dupont
from .safety import get_safety
from .growth import get_growth
from .valuation import get_valuation_summary
from .cashflow_quality import get_cashflow_analysis
from .piotroski import get_piotroski
from .risk_scores import altman_z, beneish_m

from db.connection import get_cursor


def get_summary(stock_id: str) -> dict:
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT name, market, industry FROM tw.stocks WHERE stock_id = %s",
            (stock_id,),
        )
        meta = cur.fetchone()

    prof = get_profitability(stock_id, 4)
    safe = get_safety(stock_id, 4)
    grow = get_growth(stock_id, 4)
    val = get_valuation_summary(stock_id)
    cfq = get_cashflow_analysis(stock_id, 4)
    pio = get_piotroski(stock_id)
    z = altman_z(stock_id)
    m = beneish_m(stock_id)

    return {
        "stock_id": stock_id,
        "name":     meta["name"] if meta else None,
        "industry": meta["industry"] if meta else None,
        "valuation":     val,
        "profitability": prof,
        "dupont":        get_dupont(stock_id, 4),
        "safety":        safe,
        "growth":        grow,
        "cashflow":      cfq,
        "piotroski":     pio,
        "altman_z":      z,
        "beneish_m":     m,
    }


def _fmt(v, suffix=""):
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.2f}{suffix}"
    return f"{v}{suffix}"


def print_report(stock_id: str):
    s = get_summary(stock_id)
    print(f"\n{'='*70}")
    print(f"  {s['stock_id']}  {s['name'] or ''}   {s['industry'] or ''}")
    print(f"{'='*70}")

    v = s["valuation"]
    print(f"\n[ Valuation ]  {v.get('last_date')}  close={v.get('close')}")
    print(f"  PE(TTM)={_fmt(v.get('pe_ttm'))}   "
          f"PB={_fmt(v.get('pb'))}   "
          f"Yield={_fmt(v.get('dividend_yield'), '%')}")
    if v.get("pe_bands"):
        bands = v["pe_bands"]
        print(f"  PE Bands: p10={bands[10]:.1f}  p25={bands[25]:.1f}  "
              f"p50={bands[50]:.1f}  p75={bands[75]:.1f}  p90={bands[90]:.1f}")
        print(f"  Current PE percentile: {v.get('pe_percentile')}%  "
              f"(n={v.get('pe_sample_size')})")

    print(f"\n[ Profitability - latest 4 quarters ]")
    print(f"  {'Period':<10}{'GM%':>8}{'OM%':>8}{'NM%':>8}{'ROE%':>8}{'ROA%':>8}{'EPS':>8}")
    for r in s["profitability"]:
        print(f"  {r['year']}Q{r['quarter']:<6}"
              f"{_fmt(r['gross_margin']):>8}{_fmt(r['operating_margin']):>8}"
              f"{_fmt(r['net_margin']):>8}{_fmt(r['roe_annualized']):>8}"
              f"{_fmt(r['roa_annualized']):>8}{_fmt(r['eps']):>8}")

    print(f"\n[ DuPont - ROE decomposition ]")
    for r in s["dupont"]:
        print(f"  {r['year']}Q{r['quarter']}: NM={_fmt(r['net_margin'],'%')}  "
              f"AT={_fmt(r['asset_turnover'])}  EM={_fmt(r['equity_multiplier'])}  "
              f"ROE={_fmt(r['roe_decomposed'],'%')}")

    print(f"\n[ Growth - YoY % ]")
    for r in s["growth"]:
        print(f"  {r['year']}Q{r['quarter']}: Rev={_fmt(r['revenue_yoy'],'%')}  "
              f"GP={_fmt(r['gross_profit_yoy'],'%')}  "
              f"OP={_fmt(r['operating_income_yoy'],'%')}  "
              f"NI={_fmt(r['net_income_yoy'],'%')}  "
              f"EPS={_fmt(r['eps_yoy'],'%')}")

    print(f"\n[ Safety ]")
    for r in s["safety"]:
        print(f"  {r['year']}Q{r['quarter']}: Current={_fmt(r['current_ratio'],'%')}  "
              f"Quick={_fmt(r['quick_ratio'],'%')}  Debt={_fmt(r['debt_ratio'],'%')}  "
              f"D/E={_fmt(r['debt_to_equity'],'%')}")

    print(f"\n[ Cash Flow & Earnings Quality ]")
    for r in s["cashflow"]:
        print(f"  {r['year']}Q{r['quarter']}: OCF/NI={_fmt(r['ocf_to_ni'])}  "
              f"FCF%={_fmt(r['fcf_margin'])}  CAPEX%={_fmt(r['capex_intensity'])}  "
              f"AR-Rev={_fmt(r['ar_growth_vs_rev'])}  CCC={_fmt(r['ccc'])}d")

    p = s["piotroski"]
    if p.get("status") == "ok":
        print(f"\n[ Piotroski F-Score ]  {p['score']} / {p['max']}   ({p['period']})")

    z = s["altman_z"]
    if z.get("status") == "ok":
        print(f"\n[ Altman Z-Score ]  Z = {z['z_score']}   zone: {z['zone']}")

    m = s["beneish_m"]
    if m.get("status") == "ok":
        print(f"\n[ Beneish M-Score ]  M = {m['m_score']}   flag: {m['flag']}")

    print()


if __name__ == "__main__":
    import sys
    sid = sys.argv[1] if len(sys.argv) >= 2 else "2330"
    print_report(sid)
