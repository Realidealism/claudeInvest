"""
Valuation analysis — 估值與本益比河流圖 (PE Band Chart).

Metrics:
  - pe_ttm   = close_price / TTM_EPS (trailing 4-quarter EPS)
  - pb       = close_price / book_value_per_share
  - dividend_yield = annual_cash_dividend / close_price
  - peg      = pe_ttm / eps_growth_pct (TTM EPS YoY growth)

PE Band Chart (本益比河流圖):
  For each trading day, compute TTM EPS using the latest 4 quarters whose
  publication date <= trading day. Publication deadlines per TWSE regulations:
    Q1 → 5/15, Q2 → 8/14, Q3 → 11/14, Q4 (annual) → next year 3/31
  The chart then plots historical PE distribution as percentile bands so the
  user can see whether today's valuation is cheap, fair, or expensive relative
  to the stock's own history.

  Why percentile (not fixed multiples): a tech growth stock's "fair PE" is
  naturally different from a bank's. Percentiles tailor the bands to each
  stock's trading history.

Edge cases:
  - Negative TTM EPS → PE undefined (returned as None, skipped in percentiles)
  - PE > 200 → capped at 200 when computing percentile bands (prevents extreme
    values from distorting the river)
  - Fewer than 60 days of valid PE history → bands marked as "insufficient"
"""

from datetime import date, timedelta

from db.connection import get_cursor


# Publication deadlines: (month, day) by which the quarter's financials must be filed
PUB_DEADLINE = {
    1: (5, 15),   # Q1 by May 15
    2: (8, 14),   # Q2 by Aug 14
    3: (11, 14),  # Q3 by Nov 14
    4: (3, 31),   # Q4 (annual) by next year March 31
}


def _publication_date(year: int, quarter: int) -> date:
    """When the market can act on this quarter's figures."""
    m, d = PUB_DEADLINE[quarter]
    report_year = year + 1 if quarter == 4 else year
    return date(report_year, m, d)


def _build_ttm_eps_timeline(stock_id: str) -> list[tuple[date, float]]:
    """
    Build a timeline of (effective_date, ttm_eps) pairs. Each entry is active
    from its effective_date until the next entry.
    TTM EPS = sum of the latest 4 consecutive single-quarter EPS values available
    at that publication date.
    """
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT year, quarter, eps FROM tw.income_statements "
            "WHERE stock_id = %s AND period_type = 'Q' AND eps IS NOT NULL "
            "ORDER BY year, quarter",
            (stock_id,),
        )
        rows = cur.fetchall()

    timeline = []
    for i in range(3, len(rows)):
        window = rows[i - 3 : i + 1]
        ttm = sum(float(r["eps"]) for r in window)
        eff = _publication_date(rows[i]["year"], rows[i]["quarter"])
        timeline.append((eff, ttm))
    return timeline


def _ttm_at(timeline: list[tuple[date, float]], d: date) -> float | None:
    """Return the TTM EPS effective on date d (latest entry with eff <= d)."""
    ttm = None
    for eff, val in timeline:
        if eff <= d:
            ttm = val
        else:
            break
    return ttm


def get_pe_history(stock_id: str, years: int = 10) -> list[dict]:
    """
    Return daily PE history: [{date, close, ttm_eps, pe}, ...].
    Skips days with missing or negative TTM EPS.
    """
    timeline = _build_ttm_eps_timeline(stock_id)
    if not timeline:
        return []

    cutoff = date.today() - timedelta(days=365 * years)
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT trade_date, close_price FROM tw.daily_prices "
            "WHERE stock_id = %s AND trade_date >= %s AND close_price IS NOT NULL "
            "ORDER BY trade_date",
            (stock_id, cutoff),
        )
        prices = cur.fetchall()

    result = []
    for p in prices:
        ttm = _ttm_at(timeline, p["trade_date"])
        if ttm is None or ttm <= 0:
            continue
        pe = round(float(p["close_price"]) / ttm, 2)
        result.append({
            "date": p["trade_date"],
            "close": float(p["close_price"]),
            "ttm_eps": round(ttm, 2),
            "pe": pe,
        })
    return result


def get_pe_bands(stock_id: str, years: int = 10,
                 percentiles: tuple[float, ...] = (10, 25, 50, 75, 90),
                 cap: float = 200.0) -> dict:
    """
    Compute historical PE percentile bands for PE river chart.
    Returns {'pe_bands': {p: value}, 'current_pe': x, 'current_percentile': y, 'n': sample_size}.
    """
    history = get_pe_history(stock_id, years)
    if len(history) < 60:
        return {"pe_bands": {}, "current_pe": None,
                "current_percentile": None, "n": len(history),
                "status": "insufficient"}

    pe_values = sorted(min(h["pe"], cap) for h in history)
    n = len(pe_values)
    bands = {}
    for p in percentiles:
        idx = min(int(n * p / 100), n - 1)
        bands[p] = pe_values[idx]

    current_pe = history[-1]["pe"]
    below = sum(1 for v in pe_values if v < current_pe)
    current_percentile = round(below / n * 100, 1)

    return {
        "pe_bands": bands,
        "current_pe": current_pe,
        "current_ttm_eps": history[-1]["ttm_eps"],
        "current_percentile": current_percentile,
        "n": n,
        "status": "ok",
    }


def get_valuation_summary(stock_id: str) -> dict:
    """Snapshot: latest PE, PB, yield, plus PE percentile vs history."""
    band = get_pe_bands(stock_id)

    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT trade_date, close_price FROM tw.daily_prices "
            "WHERE stock_id = %s ORDER BY trade_date DESC LIMIT 1",
            (stock_id,),
        )
        last = cur.fetchone()
        cur.execute(
            "SELECT book_value_per_share FROM tw.balance_sheets "
            "WHERE stock_id = %s ORDER BY year DESC, quarter DESC LIMIT 1",
            (stock_id,),
        )
        bs = cur.fetchone()
        cur.execute(
            "SELECT COALESCE(SUM(cash_dividend), 0) AS cash "
            "FROM tw.dividends WHERE stock_id = %s "
            "AND ex_date >= CURRENT_DATE - INTERVAL '400 days'",
            (stock_id,),
        )
        div = cur.fetchone()

    close = float(last["close_price"]) if last and last["close_price"] else None
    bvps = float(bs["book_value_per_share"]) if bs and bs["book_value_per_share"] else None
    annual_div = float(div["cash"]) if div and div["cash"] else 0.0

    return {
        "last_date": last["trade_date"] if last else None,
        "close": close,
        "pe_ttm": band.get("current_pe"),
        "pb": round(close / bvps, 2) if close and bvps else None,
        "dividend_yield": round(annual_div / close * 100, 2) if close and annual_div else None,
        "pe_bands": band.get("pe_bands"),
        "pe_percentile": band.get("current_percentile"),
        "pe_sample_size": band.get("n"),
    }


if __name__ == "__main__":
    import sys
    sid = sys.argv[1] if len(sys.argv) >= 2 else "2330"

    print(f"=== {sid} Valuation Summary ===")
    s = get_valuation_summary(sid)
    for k, v in s.items():
        print(f"  {k}: {v}")
