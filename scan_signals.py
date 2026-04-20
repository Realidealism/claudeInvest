"""
Scan fund/ETF holdings for strategy signals and persist to DB.

Usage:
  python scan_signals.py              # scan latest monthly period
  python scan_signals.py 202603       # scan specific period
  python scan_signals.py 202510 202603  # scan range of periods
"""

import sys

from db.connection import get_cursor, init_db
from strategies.registry import get_strategies, scan_all, save_signals


def scan_period(period: str):
    print(f"\n--- Scanning signals for {period} ---")
    with get_cursor() as cur:
        signals = scan_all(period, cur)
        if not signals:
            print("  No signals found.")
            return

        n = save_signals(signals, cur)
        print(f"  {n} signals saved.")

        # Summary by type
        by_type = {}
        for s in signals:
            by_type.setdefault(s["signal_type"], []).append(s)
        for stype, items in sorted(by_type.items()):
            tickers = ", ".join(s["ticker"] for s in items[:5])
            suffix = f" +{len(items)-5}" if len(items) > 5 else ""
            print(f"  {stype}: {len(items)} ({tickers}{suffix})")


def _period_range(start: str, end: str) -> list[str]:
    """Generate monthly periods from start to end inclusive."""
    periods = []
    y, m = int(start[:4]), int(start[4:])
    ey, em = int(end[:4]), int(end[4:])
    while (y, m) <= (ey, em):
        periods.append(f"{y}{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return periods


if __name__ == "__main__":
    init_db()
    args = sys.argv[1:]

    if len(args) == 0:
        # Find latest period in DB
        with get_cursor(commit=False) as cur:
            cur.execute("SELECT MAX(period) FROM tw.fund_holdings_monthly")
            latest = cur.fetchone()[0]
        if not latest:
            print("No monthly holdings data found.")
            sys.exit(1)
        scan_period(latest)
    elif len(args) == 1:
        scan_period(args[0])
    elif len(args) == 2:
        for p in _period_range(args[0], args[1]):
            scan_period(p)
    else:
        print("Usage: python scan_signals.py [period] [end_period]")
        sys.exit(1)
