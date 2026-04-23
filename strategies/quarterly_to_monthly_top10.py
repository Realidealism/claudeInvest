"""Signal: stock promoted from low-weight quarterly holding to monthly Top 10.

Uses share-count estimation to filter out false promotions caused by
fund AUM shrinkage inflating weight of unchanged positions.
"""

from strategies.base import BaseStrategy
from strategies.registry import register
from strategies.utils import month_end_prices, estimate_shares, fund_aum_changes


@register
class QuarterlyToMonthlyTop10(BaseStrategy):
    signal_type = "quarterly_to_monthly_top10"

    # Quarter-end month → previous quarter-end month
    PREV_QUARTER = {3: 12, 6: 3, 9: 6, 12: 9}

    def scan(self, period: str, cur, trade_date=None) -> list[dict]:
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
                   m.amount, m.fund_id,
                   f.code AS fund_code, f.name AS fund_name
            FROM tw.fund_holdings_monthly m
            JOIN tw.funds f ON m.fund_id = f.id
            WHERE m.period = %s
        """, (period,))
        monthly_rows = cur.fetchall()

        # Tickers in quarterly holdings for the previous quarter
        cur.execute("""
            SELECT q.ticker, q.weight, q.fund_id, f.code AS fund_code
            FROM tw.fund_holdings_quarterly q
            JOIN tw.funds f ON q.fund_id = f.id
            WHERE q.period = %s
        """, (q_month,))
        quarterly = {}
        quarterly_by_fund_id = {}
        for r in cur.fetchall():
            quarterly.setdefault(r["fund_code"], {})[r["ticker"]] = r["weight"]
            quarterly_by_fund_id.setdefault(r["fund_id"], {})[r["ticker"]] = r["weight"]

        # AUM changes and price data for share-based verification
        aum_changes = fund_aum_changes(cur, period)
        tickers_with_amount = {r["ticker"] for r in monthly_rows if r["amount"]}
        curr_prices = month_end_prices(cur, period, list(tickers_with_amount))

        # Find promotions: in quarterly with low weight, now in monthly Top 10
        ticker_signals = {}

        for r in monthly_rows:
            ticker = r["ticker"]
            fund = r["fund_code"]
            q_weights = quarterly.get(fund, {})
            q_weight = q_weights.get(ticker)

            # Must exist in quarterly with weight below monthly Top-10 threshold
            if q_weight is None:
                continue
            if q_weight >= r["weight"]:
                continue  # was already heavy — not a promotion

            # Share-based filter: if fund AUM shrank, check whether the weight
            # increase is real (more shares) or just AUM-driven inflation.
            # Compare hypothetical weight (quarterly shares at current price / current AUM)
            # against current weight. If similar, shares didn't actually increase.
            aum = aum_changes.get(r["fund_id"])
            if r["amount"] and aum and aum.get("curr_aum") and aum.get("prev_aum"):
                curr_price = curr_prices.get(ticker)
                if curr_price:
                    curr_shares = estimate_shares(r["amount"], curr_price)
                    if curr_shares:
                        # Estimate quarterly shares: q_weight% of prev_aum / prev_price
                        # We don't have quarterly amount, so derive from weight + AUM
                        q_w = float(q_weight) if q_weight else 0
                        if q_w > 0:
                            q_value = aum["prev_aum"] * q_w / 100
                            # Use current price to estimate quarterly shares
                            # (imperfect, but directionally correct)
                            q_shares = q_value / curr_price
                            if q_shares > 0:
                                share_change = (curr_shares - q_shares) / q_shares
                                # If shares barely changed (<10%), it's just AUM shrinkage
                                if share_change < 0.10:
                                    continue

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
            detail = {
                "fund": fund,
                "q_weight": float(q_weight) if q_weight else None,
                "m_weight": float(r["weight"]) if r["weight"] else None,
                "m_rank": r["rank"],
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
                trigger_date=trade_date,
                weight_change=info["weight_change"],
                evidence=info["evidence"],
            )
            for ticker, info in ticker_signals.items()
        ]
