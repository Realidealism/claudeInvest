"""Signal: ETF removes or reduces a stock by an unusually large amount.

Pure ETF short signal — daily trigger.
Compares today's share reduction against the ETF's historical daily average.
"""

from datetime import date


SIGNAL_TYPE = "etf_abnormal_exit"
MULTIPLE = 3.0
MIN_HISTORY_DAYS = 5
MIN_AMOUNT_FALLBACK = 5_000_000  # fallback: change amount >= 5M TWD
MIN_WEIGHT_DIFF_FALLBACK = 0.3  # fallback: weight change >= 0.3%
MIN_AMOUNT = 500_000  # minimum change amount in TWD


def scan(trade_date: date, cur) -> list[dict]:
    """Find stocks where an ETF reduced/removed shares far above
    that ETF's historical daily average on trade_date."""

    cur.execute("""
        SELECT etf_id, stock_id, stock_name,
               CASE WHEN change_type = 'removed' THEN COALESCE(prev_shares, 0)
                    ELSE ABS(COALESCE(share_diff, 0))
               END AS shares_reduced
        FROM tw.etf_holdings_diff
        WHERE trade_date = %s
          AND change_type IN ('removed', 'decreased')
          AND CASE WHEN change_type = 'removed' THEN COALESCE(prev_shares, 0)
                   ELSE ABS(COALESCE(share_diff, 0))
              END > 0
    """, (trade_date,))
    today = cur.fetchall()
    if not today:
        return []

    # Historical daily average reduction per ETF
    cur.execute("""
        SELECT etf_id,
               AVG(daily_red) AS avg_red,
               COUNT(*) AS days
        FROM (
            SELECT etf_id, trade_date,
                   SUM(CASE WHEN change_type = 'removed' THEN COALESCE(prev_shares, 0)
                            ELSE ABS(COALESCE(share_diff, 0))
                       END) AS daily_red
            FROM tw.etf_holdings_diff
            WHERE trade_date < %s
              AND change_type IN ('removed', 'decreased')
            GROUP BY etf_id, trade_date
        ) sub
        GROUP BY etf_id
    """, (trade_date,))
    hist = {r["etf_id"]: {
        "avg": float(r["avg_red"]),
        "days": r["days"],
    } for r in cur.fetchall()}

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

    # Weight diff from today's diff for fallback check
    cur.execute("""
        SELECT etf_id, stock_id,
               COALESCE(ABS(weight_diff), prev_weight, 0) AS wd
        FROM tw.etf_holdings_diff
        WHERE trade_date = %s
          AND change_type IN ('removed', 'decreased')
    """, (trade_date,))
    weight_diffs = {(r["etf_id"], r["stock_id"]): float(r["wd"]) for r in cur.fetchall()}

    ticker_signals: dict[str, dict] = {}
    for r in today:
        etf_id = r["etf_id"]
        shares = int(r["shares_reduced"])
        h = hist.get(etf_id)

        is_abnormal = False
        multiple = None
        if h and h["days"] >= MIN_HISTORY_DAYS and h["avg"] > 0:
            multiple = shares / h["avg"]
            if multiple >= MULTIPLE:
                is_abnormal = True
        elif not h or h["days"] < MIN_HISTORY_DAYS:
            # No history: require both large amount and meaningful weight change
            ticker = r["stock_id"]
            amt = shares * prices.get(ticker, 0)
            wd = weight_diffs.get((etf_id, ticker), 0)
            if amt >= MIN_AMOUNT_FALLBACK and wd >= MIN_WEIGHT_DIFF_FALLBACK:
                is_abnormal = True

        if not is_abnormal:
            continue

        ticker = r["stock_id"]
        amount = shares * prices.get(ticker, 0)
        if amount < MIN_AMOUNT:
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
        detail = {"etf": etf_id, "shares_reduced": shares}
        if multiple is not None:
            detail["multiple"] = round(multiple, 1)
            detail["hist_avg"] = round(h["avg"])
            detail["hist_days"] = h["days"]
        info["details"].append(detail)

    iso = trade_date.isocalendar()
    period_str = f"{iso[0]}W{iso[1]:02d}"
    return [
        {
            "signal_type": SIGNAL_TYPE,
            "ticker": ticker,
            "ticker_name": info["stock_name"],
            "funds": info["etfs"],
            "trigger_date": date.today(),
            "trigger_period": period_str,
            "weight_change": None,
            "evidence": {
                "trade_date": str(trade_date),
                "total_shares": info["total_shares"],
                "details": info["details"],
            },
        }
        for ticker, info in ticker_signals.items()
    ]
