"""Signal: ticker in Top 10 for 2+ consecutive months with rising weight.

Uses amount / month-end close to verify actual share increase,
filtering out weight gains caused by fund size shrinkage.
"""

from strategies.base import BaseStrategy
from strategies.registry import register
from strategies.utils import prev_period, month_end_prices, estimate_shares, fund_aum_changes


@register
class ConsecutiveAccumulation(BaseStrategy):
    signal_type = "consecutive_accumulation"

    # Shares must increase by at least this ratio to count as real accumulation
    SHARE_RISE_THRESHOLD = 0.02  # 2%

    def scan(self, period: str, cur) -> list[dict]:
        """Find tickers in Top 10 for both current and previous month
        with weight increase, verified by share count when available.

        period: 'YYYYMM' monthly period.
        """
        prev = prev_period(period)

        cur.execute("""
            SELECT c.ticker, c.ticker_name, c.weight AS curr_weight,
                   p.weight AS prev_weight,
                   c.amount AS curr_amount, p.amount AS prev_amount,
                   c.fund_id, f.code AS fund_code, f.name AS fund_name
            FROM tw.fund_holdings_monthly c
            JOIN tw.fund_holdings_monthly p
                ON c.fund_id = p.fund_id AND c.ticker = p.ticker AND p.period = %s
            JOIN tw.funds f ON c.fund_id = f.id
            WHERE c.period = %s
              AND c.weight > p.weight
        """, (prev, period))
        rows = cur.fetchall()

        # Price lookup for share estimation
        tickers_with_amount = {r["ticker"] for r in rows if r["curr_amount"] and r["prev_amount"]}
        curr_prices = month_end_prices(cur, period, list(tickers_with_amount))
        prev_prices = month_end_prices(cur, prev, list(tickers_with_amount))
        aum_changes = fund_aum_changes(cur, period)

        ticker_signals = {}
        for r in rows:
            ticker = r["ticker"]

            # Filter: if share data derivable, require actual share increase
            if r["curr_amount"] and r["prev_amount"]:
                cs = estimate_shares(r["curr_amount"], curr_prices.get(ticker))
                ps = estimate_shares(r["prev_amount"], prev_prices.get(ticker))
                if cs is not None and ps is not None and ps > 0:
                    if (cs - ps) / ps < self.SHARE_RISE_THRESHOLD:
                        # Shares barely changed — weight rise is from fund shrinkage
                        continue

            change = float(r["curr_weight"] - r["prev_weight"]) if r["curr_weight"] and r["prev_weight"] else None

            if ticker not in ticker_signals:
                ticker_signals[ticker] = {
                    "ticker_name": r["ticker_name"],
                    "funds": [],
                    "max_change": 0,
                    "evidence": {"prev_period": prev, "details": []},
                }
            entry = ticker_signals[ticker]
            entry["funds"].append(r["fund_name"])
            if change and change > entry["max_change"]:
                entry["max_change"] = change
            detail = {
                "fund": r["fund_code"],
                "prev_weight": float(r["prev_weight"]) if r["prev_weight"] else None,
                "curr_weight": float(r["curr_weight"]) if r["curr_weight"] else None,
            }
            if r["curr_amount"] and r["prev_amount"]:
                ps = estimate_shares(r["prev_amount"], prev_prices.get(ticker))
                cs = estimate_shares(r["curr_amount"], curr_prices.get(ticker))
                if ps is not None and cs is not None:
                    detail["est_prev_shares"] = ps
                    detail["est_curr_shares"] = cs
            aum = aum_changes.get(r["fund_id"])
            if aum:
                detail["fund_aum_change_pct"] = round(aum["change_pct"] * 100, 1)
            entry["evidence"]["details"].append(detail)

        return [
            self._make_signal(
                ticker=ticker,
                ticker_name=info["ticker_name"],
                funds=info["funds"],
                trigger_period=f"{period}M",
                weight_change=info["max_change"],
                evidence=info["evidence"],
            )
            for ticker, info in ticker_signals.items()
        ]
