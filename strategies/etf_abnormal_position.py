"""Signal: ETF establishes an unusually large new position or increase in a stock.

Pure ETF strategy — daily trigger.
Compares today's share addition against the ETF's historical daily average
to detect outlier moves.
"""

from datetime import date

from strategies.base import BaseStrategy
from strategies.registry import register


@register
class EtfAbnormalPosition(BaseStrategy):
    signal_type = "etf_abnormal_position"

    # Current addition must be >= MULTIPLE times the ETF's historical average
    MULTIPLE = 3.0
    # Minimum history days for meaningful average
    MIN_HISTORY_DAYS = 5
    # Fallback: if no history, require at least this many shares added
    MIN_SHARES_ABSOLUTE = 500_000  # 500張
    MIN_AMOUNT = 500_000  # minimum change amount in TWD

    def scan(self, period: str, cur) -> list[dict]:
        """Monthly fallback — not used for daily ETF scan."""
        return []

    def scan_daily(self, trade_date: date, cur) -> list[dict]:
        """Find stocks where an ETF added/increased shares far above
        that ETF's historical daily average on trade_date."""

        # Today's additions
        cur.execute("""
            SELECT etf_id, stock_id, stock_name,
                   COALESCE(share_diff, curr_shares, 0) AS shares_added
            FROM tw.etf_holdings_diff
            WHERE trade_date = %s
              AND change_type IN ('added', 'increased')
              AND COALESCE(share_diff, curr_shares, 0) > 0
        """, (trade_date,))
        today = cur.fetchall()
        if not today:
            return []

        # Historical daily average addition per ETF (before today)
        cur.execute("""
            SELECT etf_id,
                   AVG(daily_add) AS avg_add,
                   COUNT(*) AS days
            FROM (
                SELECT etf_id, trade_date,
                       SUM(COALESCE(share_diff, curr_shares, 0)) AS daily_add
                FROM tw.etf_holdings_diff
                WHERE trade_date < %s
                  AND change_type IN ('added', 'increased')
                GROUP BY etf_id, trade_date
            ) sub
            GROUP BY etf_id
        """, (trade_date,))
        hist = {r["etf_id"]: {
            "avg": float(r["avg_add"]),
            "days": r["days"],
        } for r in cur.fetchall()}

        # ETF name lookup
        cur.execute("SELECT code, name FROM tw.funds WHERE fund_type = 'etf'")
        etf_names = {r["code"]: r["name"] for r in cur.fetchall()}

        # Latest close price per stock for amount calculation
        cur.execute("""
            SELECT DISTINCT ON (stock_id) stock_id, close_price
            FROM tw.daily_prices
            WHERE close_price IS NOT NULL
            ORDER BY stock_id, trade_date DESC
        """)
        prices = {r["stock_id"]: float(r["close_price"]) for r in cur.fetchall()}

        ticker_signals: dict[str, dict] = {}
        for r in today:
            etf_id = r["etf_id"]
            shares = int(r["shares_added"])
            h = hist.get(etf_id)

            is_abnormal = False
            multiple = None
            if h and h["days"] >= self.MIN_HISTORY_DAYS and h["avg"] > 0:
                multiple = shares / h["avg"]
                if multiple >= self.MULTIPLE:
                    is_abnormal = True
            elif not h or h["days"] < self.MIN_HISTORY_DAYS:
                if shares >= self.MIN_SHARES_ABSOLUTE:
                    is_abnormal = True

            if not is_abnormal:
                continue

            ticker = r["stock_id"]
            amount = shares * prices.get(ticker, 0)
            if amount < self.MIN_AMOUNT:
                continue
            if ticker not in ticker_signals:
                ticker_signals[ticker] = {
                    "stock_name": r["stock_name"],
                    "etfs": [],
                    "details": [],
                    "total_shares": 0,
                }
            info = ticker_signals[ticker]
            info["etfs"].append(etf_names.get(etf_id, etf_id))
            info["total_shares"] += shares
            detail = {"etf": etf_id, "shares_added": shares}
            if multiple is not None:
                detail["multiple"] = round(multiple, 1)
                detail["hist_avg"] = round(h["avg"])
                detail["hist_days"] = h["days"]
            info["details"].append(detail)

        iso = trade_date.isocalendar()
        period_str = f"{iso[0]}W{iso[1]:02d}"
        return [
            self._make_signal(
                ticker=ticker,
                ticker_name=info["stock_name"],
                funds=info["etfs"],
                trigger_period=period_str,
                weight_change=None,
                evidence={
                    "trade_date": str(trade_date),
                    "total_shares": info["total_shares"],
                    "details": info["details"],
                },
            )
            for ticker, info in ticker_signals.items()
        ]
