"""Signal: ticker was in Top 10 for consecutive months then suddenly disappeared.

Uses share-count estimation to filter out false exits caused by fund AUM
expansion diluting the stock below the Top 10 threshold.
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

    def scan(self, period: str, cur) -> list[dict]:
        """Find tickers that were in Top 10 for MIN_CONSECUTIVE+ months
        but disappeared in the current period.

        period: 'YYYYMM' monthly period.
        """
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

        # Check how many consecutive previous months each ticker was held
        ticker_signals = {}
        for r in disappeared:
            # Share-based filter: if we can estimate shares, check whether
            # the stock at current price still represents meaningful weight.
            # If "hypothetical weight" (assuming shares unchanged) is still
            # above the fund's current #10 weight, it's likely just dilution.
            aum = aum_changes.get(r["fund_id"])
            if r["last_amount"] and aum:
                prev_price = prev_prices.get(r["ticker"])
                curr_price = curr_prices.get(r["ticker"])
                if prev_price and curr_price and aum.get("curr_aum"):
                    est_shares = estimate_shares(r["last_amount"], prev_price)
                    if est_shares:
                        # Hypothetical current value if shares unchanged
                        hypo_value = est_shares * curr_price
                        hypo_weight = hypo_value / aum["curr_aum"] * 100
                        # Get current period's #10 weight for this fund
                        cur.execute("""
                            SELECT MIN(weight) AS min_w FROM (
                                SELECT weight FROM tw.fund_holdings_monthly
                                WHERE fund_id = %s AND period = %s
                                ORDER BY weight DESC LIMIT 10
                            ) sub
                        """, (r["fund_id"], period))
                        row = cur.fetchone()
                        threshold = float(row["min_w"]) if row and row["min_w"] else 0
                        if hypo_weight >= threshold * 0.8:
                            # Shares haven't really been sold — just diluted out
                            continue

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
