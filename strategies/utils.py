"""Shared utilities for strategy signal detection."""

from decimal import Decimal


def prev_period(period: str) -> str:
    """Return the YYYYMM string for the month before *period*."""
    y, m = int(period[:4]), int(period[4:])
    m -= 1
    if m == 0:
        m, y = 12, y - 1
    return f"{y}{m:02d}"


def month_end_prices(cur, period: str, tickers: list[str]) -> dict[str, float]:
    """Get the last closing price in *period* (YYYYMM) for each ticker."""
    if not tickers:
        return {}
    y, m = int(period[:4]), int(period[4:])
    start = f"{y}-{m:02d}-01"
    if m == 12:
        end = f"{y + 1}-01-01"
    else:
        end = f"{y}-{m + 1:02d}-01"
    cur.execute("""
        SELECT DISTINCT ON (stock_id) stock_id, close_price
        FROM tw.daily_prices
        WHERE stock_id = ANY(%s)
          AND trade_date >= %s AND trade_date < %s
          AND close_price IS NOT NULL
        ORDER BY stock_id, trade_date DESC
    """, (tickers, start, end))
    return {r["stock_id"]: float(r["close_price"]) for r in cur.fetchall()}


def estimate_shares(amount, price) -> int | None:
    """Estimate share count from holding market value and stock price."""
    if amount and price and price > 0:
        return round(amount / price)
    return None


def fund_aum_changes(cur, period: str) -> dict[int, dict]:
    """Estimate per-fund AUM for current and previous period.

    Derives AUM from any Top-10 holding: amount / (weight / 100).
    Uses the median across holdings to reduce noise.

    Returns: {fund_id: {"curr_aum": float, "prev_aum": float, "change_pct": float}}
    """
    prev = prev_period(period)
    cur.execute("""
        SELECT fund_id, period, amount, weight
        FROM tw.fund_holdings_monthly
        WHERE period IN (%s, %s)
          AND amount IS NOT NULL AND amount > 0
          AND weight IS NOT NULL AND weight > 0
    """, (period, prev))

    # Collect AUM estimates per fund per period
    estimates: dict[tuple[int, str], list[float]] = {}
    for r in cur.fetchall():
        key = (r["fund_id"], r["period"])
        w = float(r["weight"]) if isinstance(r["weight"], Decimal) else r["weight"]
        aum = r["amount"] / (w / 100)
        estimates.setdefault(key, []).append(aum)

    def _median(vals: list[float]) -> float:
        s = sorted(vals)
        n = len(s)
        if n % 2 == 1:
            return s[n // 2]
        return (s[n // 2 - 1] + s[n // 2]) / 2

    result = {}
    fund_ids = {k[0] for k in estimates}
    for fid in fund_ids:
        curr_vals = estimates.get((fid, period))
        prev_vals = estimates.get((fid, prev))
        if not curr_vals or not prev_vals:
            continue
        curr_aum = _median(curr_vals)
        prev_aum = _median(prev_vals)
        change_pct = (curr_aum - prev_aum) / prev_aum if prev_aum > 0 else 0
        result[fid] = {
            "curr_aum": curr_aum,
            "prev_aum": prev_aum,
            "change_pct": change_pct,
        }
    return result
