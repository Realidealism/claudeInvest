"""Signal: ticker held by 3+ funds in the same monthly period."""

from strategies.base import BaseStrategy
from strategies.registry import register


@register
class MultiFundConsensus(BaseStrategy):
    signal_type = "multi_fund_consensus"

    MIN_FUNDS = 3

    def scan(self, period: str, cur) -> list[dict]:
        """Find tickers held in monthly Top 10 by >= MIN_FUNDS funds.

        period: 'YYYYMM' monthly period.
        """
        cur.execute("""
            SELECT m.ticker, m.ticker_name,
                   ARRAY_AGG(f.name ORDER BY m.weight DESC) AS fund_names,
                   ARRAY_AGG(f.code ORDER BY m.weight DESC) AS fund_codes,
                   ARRAY_AGG(m.weight ORDER BY m.weight DESC) AS weights,
                   COUNT(*) AS fund_count,
                   AVG(m.weight) AS avg_weight
            FROM tw.fund_holdings_monthly m
            JOIN tw.funds f ON m.fund_id = f.id
            WHERE m.period = %s
            GROUP BY m.ticker, m.ticker_name
            HAVING COUNT(*) >= %s
            ORDER BY COUNT(*) DESC, AVG(m.weight) DESC
        """, (period, self.MIN_FUNDS))

        return [
            self._make_signal(
                ticker=r["ticker"],
                ticker_name=r["ticker_name"],
                funds=list(r["fund_names"]),
                trigger_period=f"{period}M",
                evidence={
                    "fund_count": r["fund_count"],
                    "avg_weight": float(r["avg_weight"]) if r["avg_weight"] else None,
                    "per_fund": [
                        {"fund": c, "weight": float(w) if w else None}
                        for c, w in zip(r["fund_codes"], r["weights"])
                    ],
                },
            )
            for r in cur.fetchall()
        ]
