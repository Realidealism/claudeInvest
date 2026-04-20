"""Signal: ETF recently established a new position in a stock that funds
have historically held (confirming manager conviction)."""

from strategies.base import BaseStrategy
from strategies.registry import register


@register
class ConsensusFormation(BaseStrategy):
    signal_type = "consensus_formation"

    ETF_LOOKBACK_DAYS = 30

    def scan(self, period: str, cur) -> list[dict]:
        """Find tickers where an ETF recently added a new position AND
        a fund (any, not just same-manager) has held it historically.

        period: 'YYYYMM' monthly period.
        """
        # Recent ETF new additions
        cur.execute("""
            SELECT d.etf_id, d.stock_id, d.stock_name, d.trade_date,
                   d.curr_shares, d.curr_weight,
                   f.name AS etf_name
            FROM tw.etf_holdings_diff d
            JOIN tw.funds f ON d.etf_id = f.code
            WHERE d.change_type = 'added'
              AND d.trade_date >= (
                  SELECT MAX(trade_date) - %s FROM tw.etf_holdings_diff
              )
        """, (self.ETF_LOOKBACK_DAYS,))
        etf_adds = cur.fetchall()
        if not etf_adds:
            return []

        # Tickers that appeared in fund monthly Top 10 in any recent period
        cur.execute("""
            SELECT DISTINCT m.ticker,
                   ARRAY_AGG(DISTINCT f.name) AS fund_names,
                   ARRAY_AGG(DISTINCT m.period) AS periods
            FROM tw.fund_holdings_monthly m
            JOIN tw.funds f ON m.fund_id = f.id
            GROUP BY m.ticker
        """)
        fund_history = {}
        for r in cur.fetchall():
            fund_history[r["ticker"]] = {
                "funds": list(r["fund_names"]),
                "periods": list(r["periods"]),
            }

        ticker_signals = {}
        for add in etf_adds:
            ticker = add["stock_id"]
            hist = fund_history.get(ticker)
            if not hist:
                continue

            if ticker not in ticker_signals:
                ticker_signals[ticker] = {
                    "ticker_name": add["stock_name"],
                    "funds": hist["funds"],
                    "evidence": {
                        "etf_adds": [],
                        "fund_history_periods": hist["periods"],
                    },
                }
            entry = ticker_signals[ticker]
            entry["evidence"]["etf_adds"].append({
                "etf": add["etf_id"],
                "etf_name": add["etf_name"],
                "date": str(add["trade_date"]),
                "weight": float(add["curr_weight"]) if add["curr_weight"] else None,
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
