"""Signal: ETF and fund both establish new positions in the same month."""

from strategies.base import BaseStrategy
from strategies.registry import register


def _prev_period(period: str) -> str:
    y, m = int(period[:4]), int(period[4:])
    m -= 1
    if m == 0:
        m, y = 12, y - 1
    return f"{y}{m:02d}"


@register
class DualTrackEntry(BaseStrategy):
    signal_type = "dual_track_entry"

    # How many trading days around month-end to look for ETF additions
    ETF_WINDOW_DAYS = 30

    def scan(self, period: str, cur) -> list[dict]:
        """Find tickers newly appearing in both fund monthly Top 10
        and same-manager ETF holdings in the same month.

        period: 'YYYYMM' monthly period.
        """
        prev = _prev_period(period)

        # New entries in monthly Top 10 (in current period but not previous)
        cur.execute("""
            SELECT m.ticker, m.ticker_name, m.weight, m.rank,
                   f.code AS fund_code, f.name AS fund_name, f.manager_id
            FROM tw.fund_holdings_monthly m
            JOIN tw.funds f ON m.fund_id = f.id
            WHERE m.period = %s
              AND m.ticker NOT IN (
                  SELECT m2.ticker FROM tw.fund_holdings_monthly m2
                  WHERE m2.fund_id = m.fund_id AND m2.period = %s
              )
        """, (period, prev))
        new_monthly = cur.fetchall()
        if not new_monthly:
            return []

        # Same-manager ETFs
        cur.execute("""
            SELECT f.code AS etf_code, f.manager_id
            FROM tw.funds f
            WHERE f.fund_type = 'etf'
        """)
        etf_by_mgr = {}
        for r in cur.fetchall():
            etf_by_mgr.setdefault(r["manager_id"], []).append(r["etf_code"])

        # Recent ETF additions
        cur.execute("""
            SELECT etf_id, stock_id, stock_name, trade_date, curr_shares
            FROM tw.etf_holdings_diff
            WHERE change_type = 'added'
              AND trade_date >= (
                  SELECT MAX(trade_date) - %s FROM tw.etf_holdings_diff
              )
        """, (self.ETF_WINDOW_DAYS,))
        etf_adds = {}
        for r in cur.fetchall():
            etf_adds.setdefault((r["etf_id"], r["stock_id"]), r)

        ticker_signals = {}
        for r in new_monthly:
            mgr_etfs = etf_by_mgr.get(r["manager_id"], [])
            for etf_code in mgr_etfs:
                etf_add = etf_adds.get((etf_code, r["ticker"]))
                if not etf_add:
                    continue

                ticker = r["ticker"]
                if ticker not in ticker_signals:
                    ticker_signals[ticker] = {
                        "ticker_name": r["ticker_name"],
                        "funds": [],
                        "weight_change": None,
                        "evidence": {"details": []},
                    }
                entry = ticker_signals[ticker]
                entry["funds"].append(r["fund_name"])
                entry["evidence"]["details"].append({
                    "fund": r["fund_code"],
                    "m_weight": float(r["weight"]) if r["weight"] else None,
                    "m_rank": r["rank"],
                    "etf": etf_code,
                    "etf_add_date": str(etf_add["trade_date"]),
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
