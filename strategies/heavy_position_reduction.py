"""Signal: high-weight position shows weight decline."""

from strategies.base import BaseStrategy
from strategies.registry import register


def _prev_period(period: str) -> str:
    y, m = int(period[:4]), int(period[4:])
    m -= 1
    if m == 0:
        m, y = 12, y - 1
    return f"{y}{m:02d}"


@register
class HeavyPositionReduction(BaseStrategy):
    signal_type = "heavy_position_reduction"

    # Previous-month weight must have been at least this to qualify as "heavy"
    HEAVY_THRESHOLD = 8.0  # %

    def scan(self, period: str, cur) -> list[dict]:
        """Find tickers with high previous weight that declined this month.

        period: 'YYYYMM' monthly period.
        """
        prev = _prev_period(period)

        cur.execute("""
            SELECT c.ticker, c.ticker_name,
                   c.weight AS curr_weight, p.weight AS prev_weight,
                   f.code AS fund_code, f.name AS fund_name
            FROM tw.fund_holdings_monthly c
            JOIN tw.fund_holdings_monthly p
                ON c.fund_id = p.fund_id AND c.ticker = p.ticker AND p.period = %s
            JOIN tw.funds f ON c.fund_id = f.id
            WHERE c.period = %s
              AND p.weight >= %s
              AND c.weight < p.weight
        """, (prev, period, self.HEAVY_THRESHOLD))
        rows = cur.fetchall()

        ticker_signals = {}
        for r in rows:
            ticker = r["ticker"]
            change = float(r["curr_weight"] - r["prev_weight"]) if r["curr_weight"] and r["prev_weight"] else None

            if ticker not in ticker_signals:
                ticker_signals[ticker] = {
                    "ticker_name": r["ticker_name"],
                    "funds": [],
                    "max_drop": 0,
                    "evidence": {"prev_period": prev, "details": []},
                }
            entry = ticker_signals[ticker]
            entry["funds"].append(r["fund_name"])
            if change and change < entry["max_drop"]:
                entry["max_drop"] = change
            entry["evidence"]["details"].append({
                "fund": r["fund_code"],
                "prev_weight": float(r["prev_weight"]) if r["prev_weight"] else None,
                "curr_weight": float(r["curr_weight"]) if r["curr_weight"] else None,
            })

        return [
            self._make_signal(
                ticker=ticker,
                ticker_name=info["ticker_name"],
                funds=info["funds"],
                trigger_period=f"{period}M",
                weight_change=info["max_drop"],
                evidence=info["evidence"],
            )
            for ticker, info in ticker_signals.items()
        ]
