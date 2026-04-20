"""Signal: stock held in quarterly report + same-manager ETF recently bought it."""

from strategies.base import BaseStrategy
from strategies.registry import register


@register
class QuarterlyDormantEtfActive(BaseStrategy):
    signal_type = "quarterly_dormant_etf_active"

    # How many recent trading days of ETF diff to check
    ETF_LOOKBACK_DAYS = 20

    def scan(self, period: str, cur) -> list[dict]:
        """Find tickers held in quarterly report where the same manager's
        ETF has recently added or increased the position.

        period: 'YYYYMM' quarterly period to check.
        """
        # Get same-manager fund-ETF pairs
        cur.execute("""
            SELECT f1.id AS fund_id, f1.code AS fund_code, f1.name AS fund_name,
                   f2.code AS etf_code, f2.name AS etf_name,
                   fm.name AS mgr_name
            FROM tw.funds f1
            JOIN tw.funds f2 ON f1.manager_id = f2.manager_id AND f1.id != f2.id
            JOIN tw.fund_managers fm ON f1.manager_id = fm.id
            WHERE f1.fund_type = 'fund' AND f2.fund_type = 'etf'
        """)
        pairs = cur.fetchall()
        if not pairs:
            return []

        # Quarterly holdings for this period
        cur.execute("""
            SELECT q.fund_id, q.ticker, q.ticker_name, q.weight
            FROM tw.fund_holdings_quarterly q
            WHERE q.period = %s
        """, (period,))
        q_holdings = {}
        for r in cur.fetchall():
            q_holdings.setdefault(r["fund_id"], {})[r["ticker"]] = {
                "name": r["ticker_name"], "weight": r["weight"],
            }

        # Recent ETF additions/increases
        cur.execute("""
            SELECT etf_id, stock_id, stock_name, change_type,
                   curr_shares, share_diff, trade_date
            FROM tw.etf_holdings_diff
            WHERE change_type IN ('added', 'increased')
              AND trade_date >= (
                  SELECT MAX(trade_date) - %s FROM tw.etf_holdings_diff
              )
            ORDER BY trade_date DESC
        """, (self.ETF_LOOKBACK_DAYS,))
        etf_activity = {}
        for r in cur.fetchall():
            key = (r["etf_id"], r["stock_id"])
            if key not in etf_activity:
                etf_activity[key] = r

        # Match: fund quarterly holding × same-manager ETF recent buy
        ticker_signals = {}
        for pair in pairs:
            fund_holdings = q_holdings.get(pair["fund_id"], {})
            for ticker, info in fund_holdings.items():
                etf_key = (pair["etf_code"], ticker)
                etf_act = etf_activity.get(etf_key)
                if not etf_act:
                    continue

                if ticker not in ticker_signals:
                    ticker_signals[ticker] = {
                        "ticker_name": info["name"],
                        "funds": [],
                        "evidence": {"quarterly_period": period, "etf_actions": []},
                    }
                entry = ticker_signals[ticker]
                entry["funds"].append(pair["fund_name"])
                entry["evidence"]["etf_actions"].append({
                    "etf": pair["etf_code"],
                    "manager": pair["mgr_name"],
                    "action": etf_act["change_type"],
                    "date": str(etf_act["trade_date"]),
                    "q_weight": float(info["weight"]) if info["weight"] else None,
                })

        return [
            self._make_signal(
                ticker=ticker,
                ticker_name=info["ticker_name"],
                funds=info["funds"],
                trigger_period=f"{period}Q",
                evidence=info["evidence"],
            )
            for ticker, info in ticker_signals.items()
        ]
