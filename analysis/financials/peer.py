"""
Peer comparison — 同業比較.

For a given stock, find peers in the same industry (tw.stocks.industry) and
rank key valuation/profitability/growth metrics.

Methodology:
  - Peer set: active stocks with same industry, excluding the target
  - Metrics computed for the most recent period available per peer
    (typically same quarter if all have been updated)
  - Output: target's value, peer median / p25 / p75, and target's percentile

Peers with missing data for a given metric are skipped for that metric.
"""

from statistics import median

from db.connection import get_cursor


def _get_industry(stock_id: str) -> str | None:
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT industry FROM tw.stocks WHERE stock_id = %s", (stock_id,))
        r = cur.fetchone()
    return r["industry"] if r else None


def _get_peers(industry: str) -> list[str]:
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT stock_id FROM tw.stocks "
            "WHERE industry = %s AND is_active = TRUE AND security_type = 'STOCK'",
            (industry,),
        )
        return [r["stock_id"] for r in cur.fetchall()]


def _latest_metrics(stock_id: str) -> dict | None:
    """Latest quarter's key metrics from joined tables + latest price."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            WITH latest AS (
                SELECT year, quarter FROM tw.income_statements
                WHERE stock_id = %s AND period_type = 'Q'
                ORDER BY year DESC, quarter DESC LIMIT 1
            ),
            ttm AS (
                SELECT SUM(i.eps) AS ttm_eps
                FROM tw.income_statements i, latest l
                WHERE i.stock_id = %s AND i.period_type = 'Q'
                  AND (i.year * 4 + i.quarter) BETWEEN (l.year * 4 + l.quarter - 3)
                                                 AND (l.year * 4 + l.quarter)
            ),
            prior_year AS (
                SELECT i.revenue, i.net_income_attributable, i.eps
                FROM tw.income_statements i, latest l
                WHERE i.stock_id = %s AND i.period_type = 'Q'
                  AND i.year = l.year - 1 AND i.quarter = l.quarter
            )
            SELECT i.year, i.quarter,
                   i.revenue, i.gross_profit, i.operating_income, i.net_income_attributable, i.eps,
                   b.total_assets, b.equity_attributable, b.book_value_per_share,
                   b.total_liabilities, b.current_assets, b.current_liabilities,
                   p.close_price,
                   ttm.ttm_eps,
                   py.revenue AS prior_rev,
                   py.net_income_attributable AS prior_ni,
                   py.eps AS prior_eps
            FROM tw.income_statements i
            JOIN latest l ON l.year = i.year AND l.quarter = i.quarter
            LEFT JOIN tw.balance_sheets b
              ON b.stock_id = i.stock_id AND b.year = i.year AND b.quarter = i.quarter
            LEFT JOIN LATERAL (
                SELECT close_price FROM tw.daily_prices
                WHERE stock_id = i.stock_id ORDER BY trade_date DESC LIMIT 1
            ) p ON TRUE
            LEFT JOIN ttm ON TRUE
            LEFT JOIN prior_year py ON TRUE
            WHERE i.stock_id = %s AND i.period_type = 'Q'
            """,
            (stock_id, stock_id, stock_id, stock_id),
        )
        r = cur.fetchone()
    if not r or r["revenue"] is None:
        return None

    def pct(a, b):
        return round(float(a) / float(b) * 100, 2) if a is not None and b else None

    def yoy(c, p):
        if c is None or p is None or p == 0:
            return None
        return round((float(c) - float(p)) / abs(float(p)) * 100, 2)

    close = float(r["close_price"]) if r["close_price"] else None
    bvps = float(r["book_value_per_share"]) if r["book_value_per_share"] else None
    ttm_eps = float(r["ttm_eps"]) if r["ttm_eps"] else None

    return {
        "stock_id":         stock_id,
        "period":           f"{r['year']}Q{r['quarter']}",
        "close":            close,
        "pe":               round(close / ttm_eps, 2) if close and ttm_eps and ttm_eps > 0 else None,
        "pb":               round(close / bvps, 2) if close and bvps else None,
        "gross_margin":     pct(r["gross_profit"], r["revenue"]),
        "operating_margin": pct(r["operating_income"], r["revenue"]),
        "net_margin":       pct(r["net_income_attributable"], r["revenue"]),
        "roe_ann":          pct(r["net_income_attributable"] * 4 if r["net_income_attributable"] else None,
                                r["equity_attributable"]),
        "debt_ratio":       pct(r["total_liabilities"], r["total_assets"]),
        "current_ratio":    pct(r["current_assets"], r["current_liabilities"]),
        "revenue_yoy":      yoy(r["revenue"], r["prior_rev"]),
        "eps_yoy":          yoy(r["eps"], r["prior_eps"]),
    }


def _percentile_rank(values: list[float], target: float) -> float | None:
    if target is None or not values:
        return None
    below = sum(1 for v in values if v < target)
    return round(below / len(values) * 100, 1)


def _quartiles(values: list[float]) -> dict:
    if not values:
        return {"p25": None, "median": None, "p75": None, "n": 0}
    s = sorted(values)
    n = len(s)
    return {
        "p25":    s[max(0, int(n * 0.25) - 1)] if n >= 4 else None,
        "median": median(s),
        "p75":    s[min(n - 1, int(n * 0.75))] if n >= 4 else None,
        "n":      n,
    }


METRIC_KEYS = [
    "pe", "pb", "gross_margin", "operating_margin", "net_margin",
    "roe_ann", "debt_ratio", "current_ratio", "revenue_yoy", "eps_yoy",
]


def get_peer_comparison(stock_id: str, max_peers: int = 50) -> dict:
    industry = _get_industry(stock_id)
    if not industry:
        return {"status": "no_industry", "stock_id": stock_id}

    peer_ids = [p for p in _get_peers(industry) if p != stock_id]
    target = _latest_metrics(stock_id)
    if not target:
        return {"status": "target_no_data", "stock_id": stock_id, "industry": industry}

    peer_metrics = []
    for pid in peer_ids[:max_peers * 4]:   # fetch extra to allow for peers with no data
        m = _latest_metrics(pid)
        if m:
            peer_metrics.append(m)
        if len(peer_metrics) >= max_peers:
            break

    comparison = {}
    for k in METRIC_KEYS:
        peer_vals = [p[k] for p in peer_metrics if p[k] is not None]
        q = _quartiles(peer_vals)
        comparison[k] = {
            "target":     target[k],
            "peer_n":     q["n"],
            "peer_p25":   q["p25"],
            "peer_median": q["median"],
            "peer_p75":   q["p75"],
            "percentile": _percentile_rank(peer_vals, target[k]),
        }

    return {
        "status":    "ok",
        "stock_id":  stock_id,
        "industry":  industry,
        "peer_total": len(peer_ids),
        "peer_with_data": len(peer_metrics),
        "period":    target["period"],
        "comparison": comparison,
    }


if __name__ == "__main__":
    import sys
    sid = sys.argv[1] if len(sys.argv) >= 2 else "2330"
    r = get_peer_comparison(sid)
    if r["status"] != "ok":
        print(r)
    else:
        print(f"=== {sid} vs peers in {r['industry']} "
              f"(n={r['peer_with_data']}/{r['peer_total']}, {r['period']}) ===")
        print(f"  {'Metric':<18}{'Target':>10}{'P25':>10}{'Median':>10}{'P75':>10}{'Rank%':>10}")
        for k, v in r["comparison"].items():
            print(f"  {k:<18}{str(v['target']):>10}{str(v['peer_p25']):>10}"
                  f"{str(v['peer_median']):>10}{str(v['peer_p75']):>10}"
                  f"{str(v['percentile']):>10}")
