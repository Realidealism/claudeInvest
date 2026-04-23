"""Signal: high-weight position shows actual share reduction.

Two detection paths:
1. Still in Top 10 but weight declined — verified by share count.
2. Was high-weight in Top 10 last month but completely disappeared.

Filtering layers:
- Share-based: only trigger if estimated shares actually decreased >= 2%.
- AUM fallback: when no amount data, filter if fund AUM shrank (weight drop
  is passive from other stocks rising in relative terms).
- Period guard: skip if current period has no monthly data.
"""

from strategies.base import BaseStrategy
from strategies.registry import register
from strategies.utils import prev_period, month_end_prices, estimate_shares, fund_aum_changes


@register
class HeavyPositionReduction(BaseStrategy):
    signal_type = "heavy_position_reduction"

    # Previous-month weight must have been at least this to qualify as "heavy"
    HEAVY_THRESHOLD = 8.0  # %
    # Shares must drop by at least this ratio to count as real reduction
    SHARE_DROP_THRESHOLD = 0.02  # 2%
    # Fallback: if fund AUM shrank more than this, weight drop may be passive
    AUM_SHRINK_THRESHOLD = -0.05  # -5%

    def scan(self, period: str, cur, trade_date=None) -> list[dict]:
        """Find high-weight positions with actual share reductions.

        Detects both weight declines (still in Top 10) and complete
        disappearances (dropped out of Top 10).
        """
        # Period guard
        cur.execute("""
            SELECT 1 FROM tw.fund_holdings_monthly WHERE period = %s LIMIT 1
        """, (period,))
        if not cur.fetchone():
            return []

        prev = prev_period(period)

        # Path 1: still in Top 10 but weight declined
        cur.execute("""
            SELECT c.ticker, c.ticker_name,
                   c.weight AS curr_weight, p.weight AS prev_weight,
                   c.amount AS curr_amount, p.amount AS prev_amount,
                   c.fund_id, f.code AS fund_code, f.name AS fund_name,
                   'declined' AS exit_type
            FROM tw.fund_holdings_monthly c
            JOIN tw.fund_holdings_monthly p
                ON c.fund_id = p.fund_id AND c.ticker = p.ticker AND p.period = %s
            JOIN tw.funds f ON c.fund_id = f.id
            WHERE c.period = %s
              AND p.weight >= %s
              AND c.weight < p.weight
        """, (prev, period, self.HEAVY_THRESHOLD))
        declined = cur.fetchall()

        # Path 2: was heavy in Top 10 but completely disappeared
        cur.execute("""
            SELECT p.ticker, p.ticker_name,
                   NULL AS curr_weight, p.weight AS prev_weight,
                   NULL AS curr_amount, p.amount AS prev_amount,
                   p.fund_id, f.code AS fund_code, f.name AS fund_name,
                   'disappeared' AS exit_type
            FROM tw.fund_holdings_monthly p
            JOIN tw.funds f ON p.fund_id = f.id
            WHERE p.period = %s
              AND p.weight >= %s
              AND NOT EXISTS (
                  SELECT 1 FROM tw.fund_holdings_monthly c
                  WHERE c.fund_id = p.fund_id AND c.ticker = p.ticker
                    AND c.period = %s
              )
        """, (prev, self.HEAVY_THRESHOLD, period))
        disappeared = cur.fetchall()

        rows = list(declined) + list(disappeared)
        if not rows:
            return []

        # Price + AUM data
        tickers_with_amount = set()
        for r in rows:
            if r["prev_amount"]:
                tickers_with_amount.add(r["ticker"])

        curr_prices = month_end_prices(cur, period, list(tickers_with_amount))
        prev_prices = month_end_prices(cur, prev, list(tickers_with_amount))
        aum_changes = fund_aum_changes(cur, period)

        ticker_signals = {}
        for r in rows:
            ticker = r["ticker"]
            aum = aum_changes.get(r["fund_id"])
            filtered = False

            if r["exit_type"] == "declined":
                # Share-based filter
                if r["curr_amount"] and r["prev_amount"]:
                    cs = estimate_shares(r["curr_amount"], curr_prices.get(ticker))
                    ps = estimate_shares(r["prev_amount"], prev_prices.get(ticker))
                    if cs is not None and ps is not None and ps > 0:
                        if (ps - cs) / ps < self.SHARE_DROP_THRESHOLD:
                            filtered = True
                elif not r["curr_amount"] or not r["prev_amount"]:
                    # AUM fallback: if fund AUM expanded, weight drop may be
                    # from dilution, not selling
                    if aum and aum["change_pct"] > -self.AUM_SHRINK_THRESHOLD:
                        # Fund grew — weight drop is likely passive
                        # Only filter if weight drop is small (< 1.5%)
                        prev_w = float(r["prev_weight"]) if r["prev_weight"] else 0
                        curr_w = float(r["curr_weight"]) if r["curr_weight"] else 0
                        if prev_w - curr_w < 1.5:
                            filtered = True

            elif r["exit_type"] == "disappeared":
                # For disappeared: use same logic as core_exit layer 1
                if r["prev_amount"] and aum and aum.get("curr_aum"):
                    prev_price = prev_prices.get(ticker)
                    curr_price = curr_prices.get(ticker)
                    if prev_price and curr_price:
                        est_shares = estimate_shares(r["prev_amount"], prev_price)
                        if est_shares:
                            hypo_value = est_shares * curr_price
                            hypo_weight = hypo_value / aum["curr_aum"] * 100
                            last_w = float(r["prev_weight"]) if r["prev_weight"] else 0
                            if last_w > 0 and hypo_weight >= last_w * 0.7:
                                filtered = True
                elif not r["prev_amount"]:
                    # AUM fallback
                    if aum and aum["change_pct"] > 0.10:
                        filtered = True

            if filtered:
                continue

            change = None
            if r["curr_weight"] and r["prev_weight"]:
                change = float(r["curr_weight"] - r["prev_weight"])

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
            detail = {
                "fund": r["fund_code"],
                "prev_weight": float(r["prev_weight"]) if r["prev_weight"] else None,
                "curr_weight": float(r["curr_weight"]) if r["curr_weight"] else None,
                "exit_type": r["exit_type"],
            }
            if r["prev_amount"]:
                ps = estimate_shares(r["prev_amount"], prev_prices.get(ticker))
                cs = estimate_shares(r.get("curr_amount"), curr_prices.get(ticker)) if r["curr_amount"] else None
                if ps is not None:
                    detail["est_prev_shares"] = ps
                if cs is not None:
                    detail["est_curr_shares"] = cs
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
                weight_change=info["max_drop"],
                evidence=info["evidence"],
            )
            for ticker, info in ticker_signals.items()
        ]
