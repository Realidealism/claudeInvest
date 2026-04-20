"""Signal: both fund and same-manager ETF are accumulating a position."""

from strategies.base import BaseStrategy
from strategies.registry import register


def _prev_period(period: str) -> str:
    y, m = int(period[:4]), int(period[4:])
    m -= 1
    if m == 0:
        m, y = 12, y - 1
    return f"{y}{m:02d}"


@register
class DualTrackAccumulation(BaseStrategy):
    signal_type = "dual_track_accumulation"

    ETF_WINDOW_DAYS = 30

    def scan(self, period: str, cur) -> list[dict]:
        """Find tickers where fund weight increased month-over-month AND
        same-manager ETF also increased holdings recently.

        period: 'YYYYMM' monthly period.
        """
        prev = _prev_period(period)

        # Fund weight increases
        cur.execute("""
            SELECT c.ticker, c.ticker_name,
                   c.weight AS curr_weight, p.weight AS prev_weight,
                   f.code AS fund_code, f.name AS fund_name, f.manager_id
            FROM tw.fund_holdings_monthly c
            JOIN tw.fund_holdings_monthly p
                ON c.fund_id = p.fund_id AND c.ticker = p.ticker AND p.period = %s
            JOIN tw.funds f ON c.fund_id = f.id
            WHERE c.period = %s AND c.weight > p.weight
        """, (prev, period))
        fund_increases = cur.fetchall()
        if not fund_increases:
            return []

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
                entry["evidence"]["details"].append({
                    "fund": r["fund_code"],
                    "etf": etf_code,
                    "prev_weight": float(r["prev_weight"]) if r["prev_weight"] else None,
                    "curr_weight": float(r["curr_weight"]) if r["curr_weight"] else None,
                })

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
