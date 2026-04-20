"""Signal: stock promoted from low-weight quarterly holding to monthly Top 10."""

from strategies.base import BaseStrategy
from strategies.registry import register


@register
class QuarterlyToMonthlyTop10(BaseStrategy):
    signal_type = "quarterly_to_monthly_top10"

    # Quarter-end month → previous quarter-end month
    PREV_QUARTER = {3: 12, 6: 3, 9: 6, 12: 9}

    def scan(self, period: str, cur) -> list[dict]:
        """Find tickers that were low-weight in last quarterly report
        but entered monthly Top 10 in the given period.

        period: 'YYYYMM' monthly period to check.
        """
        year, month = int(period[:4]), int(period[4:])

        # Determine the most recent quarterly period before this month
        q_month = None
        for qm in [12, 9, 6, 3]:
            qy = year if qm < month else year - 1
            q_period = f"{qy}{qm:02d}"
            if q_period < period:
                q_month = q_period
                break

        if not q_month:
            return []

        # Tickers in monthly Top 10 for this period
        cur.execute("""
            SELECT m.ticker, m.ticker_name, m.weight, m.rank,
                   f.code AS fund_code, f.name AS fund_name
            FROM tw.fund_holdings_monthly m
            JOIN tw.funds f ON m.fund_id = f.id
            WHERE m.period = %s
        """, (period,))
        monthly_rows = cur.fetchall()

        # Tickers in quarterly holdings for the previous quarter
        cur.execute("""
            SELECT q.ticker, q.weight, f.code AS fund_code
            FROM tw.fund_holdings_quarterly q
            JOIN tw.funds f ON q.fund_id = f.id
            WHERE q.period = %s
        """, (q_month,))
        quarterly = {}
        for r in cur.fetchall():
            quarterly.setdefault(r["fund_code"], {})[r["ticker"]] = r["weight"]

        # Find promotions: in quarterly with low weight, now in monthly Top 10
        # Group by ticker across funds
        ticker_signals = {}  # ticker -> {funds, evidence, ...}

        for r in monthly_rows:
            ticker = r["ticker"]
            fund = r["fund_code"]
            q_weights = quarterly.get(fund, {})
            q_weight = q_weights.get(ticker)

            # Must exist in quarterly with weight below monthly Top-10 threshold
            # (i.e. was a minor position that got promoted)
            if q_weight is None:
                continue
            if q_weight >= r["weight"]:
                continue  # was already heavy — not a promotion

            key = ticker
            if key not in ticker_signals:
                ticker_signals[key] = {
                    "ticker_name": r["ticker_name"],
                    "funds": [],
                    "weight_change": 0,
                    "evidence": {"quarterly_period": q_month, "details": []},
                }
            entry = ticker_signals[key]
            entry["funds"].append(r["fund_name"])
            change = float(r["weight"] - q_weight) if r["weight"] and q_weight else None
            if change and change > entry["weight_change"]:
                entry["weight_change"] = change
            entry["evidence"]["details"].append({
                "fund": fund,
                "q_weight": float(q_weight) if q_weight else None,
                "m_weight": float(r["weight"]) if r["weight"] else None,
                "m_rank": r["rank"],
            })

        return [
            self._make_signal(
                ticker=ticker,
                ticker_name=info["ticker_name"],
                funds=info["funds"],
                trigger_period=f"{period}M",
                weight_change=info["weight_change"],
                evidence=info["evidence"],
            )
            for ticker, info in ticker_signals.items()
        ]
