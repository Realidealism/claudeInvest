"""Signal: ticker was in Top 10 for consecutive months then suddenly disappeared."""

from strategies.base import BaseStrategy
from strategies.registry import register


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

        # Check how many consecutive previous months each ticker was held
        ticker_signals = {}
        for r in disappeared:
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
            entry["evidence"]["details"].append({
                "fund": r["fund_code"],
                "last_weight": float(r["last_weight"]) if r["last_weight"] else None,
                "consecutive_months": streak,
            })

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
