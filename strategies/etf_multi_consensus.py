"""Signal: multiple ETFs add or significantly increase the same stock
within a rolling 5-trading-day window.

Pure ETF strategy — 5-day rolling window.
"""

from datetime import date

from strategies.base import BaseStrategy
from strategies.registry import register


@register
class EtfMultiConsensus(BaseStrategy):
    signal_type = "etf_multi_consensus"

    MIN_ETFS = 2
    WINDOW_DAYS = 5
    MIN_WEIGHT = 1.0  # ETF must hold >= 1% weight in the stock

    def scan(self, period: str, cur) -> list[dict]:
        """Monthly fallback — not used for daily ETF scan."""
        return []

    def scan_daily(self, trade_date: date, cur) -> list[dict]:
        """Find stocks that multiple ETFs added or increased
        within the last WINDOW_DAYS trading days ending at trade_date."""

        # Get the last N trading days up to trade_date from etf_holdings_diff
        cur.execute("""
            SELECT DISTINCT trade_date FROM tw.etf_holdings_diff
            WHERE trade_date <= %s
            ORDER BY trade_date DESC
            LIMIT %s
        """, (trade_date, self.WINDOW_DAYS))
        dates = [r["trade_date"] for r in cur.fetchall()]
        if not dates:
            return []
        window_start = min(dates)

        # Aggregate ETF actions per stock within the window
        cur.execute("""
            SELECT stock_id, stock_name, etf_id,
                   SUM(COALESCE(share_diff, curr_shares, 0)) AS total_shares
            FROM tw.etf_holdings_diff
            WHERE trade_date >= %s AND trade_date <= %s
              AND change_type IN ('added', 'increased')
            GROUP BY stock_id, stock_name, etf_id
            HAVING SUM(COALESCE(share_diff, curr_shares, 0)) > 0
        """, (window_start, trade_date))
        rows = cur.fetchall()

        # ETF name lookup
        cur.execute("SELECT code, name FROM tw.funds WHERE fund_type = 'etf'")
        etf_names = {r["code"]: r["name"] for r in cur.fetchall()}

        # Latest weight per (etf, stock)
        cur.execute("""
            SELECT etf_id, stock_id, weight
            FROM tw.etf_holdings
            WHERE trade_date = (SELECT MAX(trade_date) FROM tw.etf_holdings)
        """)
        latest_weight = {(r["etf_id"], r["stock_id"]): float(r["weight"]) for r in cur.fetchall()}

        # Group by stock, filtering by minimum weight
        stock_data: dict[str, dict] = {}
        for r in rows:
            sid = r["stock_id"]
            etf_id = r["etf_id"]
            w = latest_weight.get((etf_id, sid), 0)
            if w < self.MIN_WEIGHT:
                continue
            if sid not in stock_data:
                stock_data[sid] = {"stock_name": r["stock_name"], "etfs": {}}
            stock_data[sid]["etfs"][etf_id] = int(r["total_shares"])

        iso = trade_date.isocalendar()
        period_str = f"{iso[0]}W{iso[1]:02d}"
        signals = []
        for ticker, info in stock_data.items():
            if len(info["etfs"]) < self.MIN_ETFS:
                continue

            etf_list = []
            details = []
            total_shares = 0
            for etf_id, shares in info["etfs"].items():
                etf_list.append(etf_names.get(etf_id, etf_id))
                w = latest_weight.get((etf_id, ticker), 0)
                details.append({"etf": etf_id, "total_shares": shares, "weight": round(w, 2)})
                total_shares += shares

            signals.append(self._make_signal(
                ticker=ticker,
                ticker_name=info["stock_name"],
                funds=etf_list,
                trigger_period=period_str,
                weight_change=None,
                evidence={
                    "window": f"{window_start} ~ {trade_date}",
                    "etf_count": len(info["etfs"]),
                    "total_shares": total_shares,
                    "details": details,
                },
            ))

        return signals
