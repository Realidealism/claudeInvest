"""Signal: both fund and same-manager ETF are accumulating a position.

Uses amount / month-end close to verify actual share increase,
filtering out weight gains caused by fund size shrinkage.
When amount data is unavailable, falls back to AUM-based filtering.
"""

from strategies.base import BaseStrategy
from strategies.registry import register
from strategies.utils import prev_period, month_end_prices, estimate_shares, fund_aum_changes


@register
class DualTrackAccumulation(BaseStrategy):
    signal_type = "dual_track_accumulation"

    ETF_WINDOW_DAYS = 30
    SHARE_RISE_THRESHOLD = 0.02  # 2%
    # Fallback: if fund AUM shrank more than this, weight rise may be passive
    AUM_SHRINK_THRESHOLD = -0.05  # -5%

    def scan(self, period: str, cur, trade_date=None) -> list[dict]:
        """Find tickers where fund weight increased month-over-month AND
        same-manager ETF also increased holdings recently.
        Verified by share count when available.

        period: 'YYYYMM' monthly period.
        """
        prev = prev_period(period)

        # Fund weight increases
        cur.execute("""
            SELECT c.ticker, c.ticker_name,
                   c.weight AS curr_weight, p.weight AS prev_weight,
                   c.amount AS curr_amount, p.amount AS prev_amount,
                   c.fund_id, f.code AS fund_code, f.name AS fund_name, f.manager_id
            FROM tw.fund_holdings_monthly c
            JOIN tw.fund_holdings_monthly p
                ON c.fund_id = p.fund_id AND c.ticker = p.ticker AND p.period = %s
            JOIN tw.funds f ON c.fund_id = f.id
            WHERE c.period = %s AND c.weight > p.weight
        """, (prev, period))
        fund_increases = cur.fetchall()
        if not fund_increases:
            return []

        # Price lookup for share estimation
        tickers_with_amount = {r["ticker"] for r in fund_increases if r["curr_amount"] and r["prev_amount"]}
        curr_prices = month_end_prices(cur, period, list(tickers_with_amount))
        prev_prices = month_end_prices(cur, prev, list(tickers_with_amount))
        aum_changes = fund_aum_changes(cur, period)

        # Same-manager ETFs
        cur.execute("""
            SELECT code AS etf_code, manager_id FROM tw.funds
            WHERE fund_type = 'etf'
        """)
        etf_by_mgr = {}
        for r in cur.fetchall():
            etf_by_mgr.setdefault(r["manager_id"], []).append(r["etf_code"])

        # Recent ETF increases
        cur.execute("""
            SELECT etf_id, stock_id, share_diff, trade_date
            FROM tw.etf_holdings_diff
            WHERE change_type = 'increased'
              AND trade_date >= (
                  SELECT MAX(trade_date) - %s FROM tw.etf_holdings_diff
              )
        """, (self.ETF_WINDOW_DAYS,))
        etf_inc = set()
        for r in cur.fetchall():
            etf_inc.add((r["etf_id"], r["stock_id"]))

        ticker_signals = {}
        for r in fund_increases:
            aum = aum_changes.get(r["fund_id"])

            # Filter: if share data derivable, require actual share increase
            if r["curr_amount"] and r["prev_amount"]:
                cs = estimate_shares(r["curr_amount"], curr_prices.get(r["ticker"]))
                ps = estimate_shares(r["prev_amount"], prev_prices.get(r["ticker"]))
                if cs is not None and ps is not None and ps > 0:
                    if (cs - ps) / ps < self.SHARE_RISE_THRESHOLD:
                        continue
            else:
                # AUM fallback: if fund shrank, weight rise may be passive
                if aum and aum["change_pct"] < self.AUM_SHRINK_THRESHOLD:
                    prev_w = float(r["prev_weight"]) if r["prev_weight"] else 0
                    curr_w = float(r["curr_weight"]) if r["curr_weight"] else 0
                    if curr_w - prev_w < 1.5:
                        continue

            mgr_etfs = etf_by_mgr.get(r["manager_id"], [])
            for etf_code in mgr_etfs:
                if (etf_code, r["ticker"]) not in etf_inc:
                    continue

                ticker = r["ticker"]
                change = float(r["curr_weight"] - r["prev_weight"]) if r["curr_weight"] and r["prev_weight"] else None

                if ticker not in ticker_signals:
                    ticker_signals[ticker] = {
                        "ticker_name": r["ticker_name"],
                        "funds": [],
                        "max_change": 0,
                        "evidence": {"details": []},
                    }
                entry = ticker_signals[ticker]
                entry["funds"].append(r["fund_name"])
                if change and change > entry["max_change"]:
                    entry["max_change"] = change
                detail = {
                    "fund": r["fund_code"],
                    "etf": etf_code,
                    "prev_weight": float(r["prev_weight"]) if r["prev_weight"] else None,
                    "curr_weight": float(r["curr_weight"]) if r["curr_weight"] else None,
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
                weight_change=info["max_change"],
                evidence=info["evidence"],
            )
            for ticker, info in ticker_signals.items()
        ]
