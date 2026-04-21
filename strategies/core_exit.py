"""Signal: ticker was in Top 10 for consecutive months then suddenly disappeared.

Three layers of false-exit filtering:
1. Share-count estimation: if hypothetical weight (unchanged shares) is close
   to last weight, the disappearance is price/AUM driven, not actual selling.
2. AUM fallback: when amount data is unavailable, filter if fund AUM grew
   significantly and the stock was near the bottom of Top 10.
3. Period guard: skip if current period has no monthly data yet.
"""

from strategies.base import BaseStrategy
from strategies.registry import register
from strategies.utils import prev_period, month_end_prices, estimate_shares, fund_aum_changes


def _prev_periods(period: str, n: int) -> list[str]:
    """Return the previous n periods in reverse chronological order."""
    result = []
    y, m = int(period[:4]), int(period[4:])
    for _ in range(n):
        m -= 1
        if m == 0:
            m, y = 12, y - 1
        result.append(f"{y}{m:02d}")
    return result


@register
class CoreExit(BaseStrategy):
    signal_type = "core_exit"

    # Must have been in Top 10 for at least this many consecutive months
    MIN_CONSECUTIVE = 2
    # If hypothetical weight >= last_weight * this ratio, shares likely unchanged
    WEIGHT_RETENTION_RATIO = 0.7
    # Fallback: AUM growth threshold for filtering without amount data
    AUM_GROWTH_THRESHOLD = 0.10  # 10%
    # Fallback: rank at or below this is considered "near bottom" of Top 10
    BOTTOM_WEIGHT_RANK = 8

    def scan(self, period: str, cur) -> list[dict]:
        """Find tickers that were in Top 10 for MIN_CONSECUTIVE+ months
        but disappeared in the current period.

        period: 'YYYYMM' monthly period.
        """
        # Guard: skip if current period has no monthly data
        cur.execute("""
            SELECT 1 FROM tw.fund_holdings_monthly WHERE period = %s LIMIT 1
        """, (period,))
        if not cur.fetchone():
            return []

        prev_list = _prev_periods(period, self.MIN_CONSECUTIVE)
        if len(prev_list) < self.MIN_CONSECUTIVE:
            return []

        prev1 = prev_list[0]  # immediately previous month

        # Tickers in previous month but NOT in current month, per fund
        cur.execute("""
            SELECT p.ticker, p.ticker_name, p.weight AS last_weight,
                   p.rank AS last_rank, p.amount AS last_amount,
                   p.fund_id, f.code AS fund_code, f.name AS fund_name
            FROM tw.fund_holdings_monthly p
            JOIN tw.funds f ON p.fund_id = f.id
            WHERE p.period = %s
              AND NOT EXISTS (
                  SELECT 1 FROM tw.fund_holdings_monthly c
                  WHERE c.fund_id = p.fund_id AND c.ticker = p.ticker
                    AND c.period = %s
              )
        """, (prev1, period))
        disappeared = cur.fetchall()
        if not disappeared:
            return []

        # Price + AUM data for share-based verification
        tickers_with_amount = {r["ticker"] for r in disappeared if r["last_amount"]}
        prev_prices = month_end_prices(cur, prev1, list(tickers_with_amount))
        curr_prices = month_end_prices(cur, period, list(tickers_with_amount))
        aum_changes = fund_aum_changes(cur, period)

        ticker_signals = {}
        for r in disappeared:
            aum = aum_changes.get(r["fund_id"])
            filtered = False

            # Layer 1: share-based verification
            if r["last_amount"] and aum and aum.get("curr_aum"):
                prev_price = prev_prices.get(r["ticker"])
                curr_price = curr_prices.get(r["ticker"])
                if prev_price and curr_price:
                    est_shares = estimate_shares(r["last_amount"], prev_price)
                    if est_shares:
                        # Hypothetical current value if shares unchanged
                        hypo_value = est_shares * curr_price
                        hypo_weight = hypo_value / aum["curr_aum"] * 100
                        last_w = float(r["last_weight"]) if r["last_weight"] else 0
                        # If hypothetical weight is still close to last weight,
                        # shares haven't been sold — disappearance is from
                        # price drop, AUM expansion, or other stocks rising
                        if last_w > 0 and hypo_weight >= last_w * self.WEIGHT_RETENTION_RATIO:
                            filtered = True

            # Layer 2: AUM fallback when no amount data
            if not filtered and not r["last_amount"]:
                if aum and aum["change_pct"] > self.AUM_GROWTH_THRESHOLD:
                    last_rank = r["last_rank"]
                    if last_rank and last_rank >= self.BOTTOM_WEIGHT_RANK:
                        filtered = True

            if filtered:
                continue

            # Check consecutive months
            streak = 1  # already confirmed in prev1
            for older in prev_list[1:]:
                cur.execute("""
                    SELECT 1 FROM tw.fund_holdings_monthly
                    WHERE fund_id = %s AND ticker = %s AND period = %s
                """, (r["fund_id"], r["ticker"], older))
                if cur.fetchone():
                    streak += 1
                else:
                    break

            if streak < self.MIN_CONSECUTIVE:
                continue

            ticker = r["ticker"]
            if ticker not in ticker_signals:
                ticker_signals[ticker] = {
                    "ticker_name": r["ticker_name"],
                    "funds": [],
                    "evidence": {"details": []},
                }
            entry = ticker_signals[ticker]
            entry["funds"].append(r["fund_name"])
            detail = {
                "fund": r["fund_code"],
                "last_weight": float(r["last_weight"]) if r["last_weight"] else None,
                "last_rank": r["last_rank"],
                "consecutive_months": streak,
            }
            if aum:
                detail["fund_aum_change_pct"] = round(aum["change_pct"] * 100, 1)
            entry["evidence"]["details"].append(detail)

        return [
            self._make_signal(
                ticker=ticker,
                ticker_name=info["ticker_name"],
                funds=info["funds"],
                trigger_period=f"{period}M",
                evidence=info["evidence"],
            )
            for ticker, info in ticker_signals.items()
        ]
