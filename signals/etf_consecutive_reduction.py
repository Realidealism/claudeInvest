"""Signal: a single ETF decreases holdings of the same stock for N consecutive weeks.

Pure ETF short signal — weekly granularity.
"""

from datetime import date, timedelta


SIGNAL_TYPE = "etf_consecutive_reduction"
MIN_STREAK = 2
LOOKBACK_WEEKS = 4


def _iso_week_label(d: date) -> str:
    iso = d.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def scan(trade_date: date, cur) -> list[dict]:
    """Find stocks where any ETF has decreased holdings for MIN_STREAK
    consecutive weeks ending at the week of trade_date."""

    current_monday = trade_date - timedelta(days=trade_date.weekday())
    lookback_start = current_monday - timedelta(weeks=LOOKBACK_WEEKS - 1)

    cur.execute("""
        SELECT etf_id, stock_id, stock_name,
               date_trunc('week', trade_date)::date AS week_start,
               SUM(CASE
                   WHEN change_type = 'added' THEN COALESCE(curr_shares, 0)
                   WHEN change_type = 'removed' THEN -COALESCE(prev_shares, 0)
                   ELSE COALESCE(share_diff, 0)
               END) AS net_shares
        FROM tw.etf_holdings_diff
        WHERE trade_date >= %s AND trade_date <= %s
        GROUP BY etf_id, stock_id, stock_name, date_trunc('week', trade_date)
    """, (lookback_start, trade_date))
    rows = cur.fetchall()

    weekly: dict[tuple[str, str], dict] = {}
    stock_names: dict[str, str] = {}
    for r in rows:
        key = (r["etf_id"], r["stock_id"])
        if key not in weekly:
            weekly[key] = {}
        wl = _iso_week_label(r["week_start"])
        weekly[key][wl] = int(r["net_shares"])
        stock_names[r["stock_id"]] = r["stock_name"]

    week_labels = []
    d = lookback_start
    while d <= current_monday:
        week_labels.append(_iso_week_label(d))
        d += timedelta(weeks=1)

    cur.execute("SELECT code, name FROM tw.funds WHERE fund_type = 'etf'")
    etf_names = {r["code"]: r["name"] for r in cur.fetchall()}

    current_week = week_labels[-1] if week_labels else None
    if not current_week:
        return []

    ticker_signals: dict[str, dict] = {}
    for (etf_id, stock_id), weeks_data in weekly.items():
        streak = 0
        streak_weeks = []
        for wl in reversed(week_labels):
            net = weeks_data.get(wl, 0)
            if net < 0:
                streak += 1
                streak_weeks.append({"week": wl, "net_shares": net})
            else:
                break

        if streak < MIN_STREAK:
            continue

        streak_weeks.reverse()
        if stock_id not in ticker_signals:
            ticker_signals[stock_id] = {
                "stock_name": stock_names.get(stock_id, ""),
                "etfs": [],
                "details": [],
                "total_shares": 0,
            }
        info = ticker_signals[stock_id]
        total = sum(w["net_shares"] for w in streak_weeks)
        info["etfs"].append(etf_names.get(etf_id, etf_id))
        info["total_shares"] += total
        info["details"].append({
            "etf": etf_id,
            "streak": streak,
            "weeks": streak_weeks,
        })

    current_week_str = current_week.replace("-", "")
    return [
        {
            "signal_type": SIGNAL_TYPE,
            "ticker": ticker,
            "ticker_name": info["stock_name"],
            "funds": info["etfs"],
            "trigger_date": date.today(),
            "trigger_period": current_week_str,
            "weight_change": None,
            "evidence": {
                "total_shares": info["total_shares"],
                "details": info["details"],
            },
        }
        for ticker, info in ticker_signals.items()
    ]
