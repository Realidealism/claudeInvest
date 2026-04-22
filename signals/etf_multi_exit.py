"""Signal: multiple ETFs remove or significantly decrease the same stock
within a rolling 5-trading-day window.

Pure ETF short signal — 5-day rolling window.
"""

from datetime import date


SIGNAL_TYPE = "etf_multi_exit"
MIN_ETFS = 2
WINDOW_DAYS = 5
MIN_AMOUNT = 500_000  # minimum change amount in TWD


def scan(trade_date: date, cur) -> list[dict]:
    """Find stocks that multiple ETFs removed or decreased
    within the last WINDOW_DAYS trading days ending at trade_date."""

    cur.execute("""
        SELECT DISTINCT trade_date FROM tw.etf_holdings_diff
        WHERE trade_date <= %s
        ORDER BY trade_date DESC
        LIMIT %s
    """, (trade_date, WINDOW_DAYS))
    dates = [r["trade_date"] for r in cur.fetchall()]
    if not dates:
        return []
    window_start = min(dates)

    cur.execute("""
        SELECT stock_id, stock_name, etf_id,
               SUM(CASE
                   WHEN change_type = 'removed' THEN COALESCE(prev_shares, 0)
                   ELSE ABS(COALESCE(share_diff, 0))
               END) AS total_shares
        FROM tw.etf_holdings_diff
        WHERE trade_date >= %s AND trade_date <= %s
          AND change_type IN ('removed', 'decreased')
        GROUP BY stock_id, stock_name, etf_id
        HAVING SUM(CASE
                   WHEN change_type = 'removed' THEN COALESCE(prev_shares, 0)
                   ELSE ABS(COALESCE(share_diff, 0))
               END) > 0
    """, (window_start, trade_date))
    rows = cur.fetchall()

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

    stock_data: dict[str, dict] = {}
    for r in rows:
        sid = r["stock_id"]
        etf_id = r["etf_id"]
        shares = int(r["total_shares"])
        amount = shares * prices.get(sid, 0)
        if amount < MIN_AMOUNT:
            continue
        if sid not in stock_data:
            stock_data[sid] = {"stock_name": r["stock_name"], "etfs": {}}
        stock_data[sid]["etfs"][etf_id] = shares

    iso = trade_date.isocalendar()
    period_str = f"{iso[0]}W{iso[1]:02d}"
    signals = []
    for ticker, info in stock_data.items():
        if len(info["etfs"]) < MIN_ETFS:
            continue

        etf_list = []
        details = []
        total_shares = 0
        for etf_id, shares in info["etfs"].items():
            etf_list.append(etf_names.get(etf_id, etf_id))
            amount = shares * prices.get(ticker, 0)
            details.append({"etf": etf_id, "total_shares": shares, "amount": round(amount)})
            total_shares += shares

        signals.append({
            "signal_type": SIGNAL_TYPE,
            "ticker": ticker,
            "ticker_name": info["stock_name"],
            "funds": etf_list,
            "trigger_date": date.today(),
            "trigger_period": period_str,
            "weight_change": None,
            "evidence": {
                "window": f"{window_start} ~ {trade_date}",
                "etf_count": len(info["etfs"]),
                "total_shares": total_shares,
                "details": details,
            },
        })

    return signals
