"""
Test turn-point scoring for 2330 — prints last 10 days of scores.
"""

import sys
import os
import io

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from backtest.data import load_stock_data
from analysis.score import build_turn_scoreboard


def main():
    stock_id = "2330"
    print(f"載入 {stock_id} 資料中...")
    data = load_stock_data(stock_id)
    print(f"資料: {data.dates[0]} ~ {data.dates[-1]} ({data.n} 天)")

    board = build_turn_scoreboard()
    show_days = min(10, data.n)
    start = data.n - show_days

    # Header
    print()
    print(f"{'日期':>12s}  {'收盤':>8s}  "
          f"{'短多':>6s} {'短空':>6s}  "
          f"{'中多':>6s} {'中空':>6s}  "
          f"{'長多':>6s} {'長空':>6s}  "
          f"{'總多':>6s} {'總空':>6s}")
    print("-" * 110)

    for i in range(start, data.n):
        r = board.evaluate(data, i)
        print(f"{str(data.dates[i]):>12s}  {data.close[i]:8.2f}  "
              f"{r.short.long_score:>+6.1f} {r.short.short_score:>+6.1f}  "
              f"{r.medium.long_score:>+6.1f} {r.medium.short_score:>+6.1f}  "
              f"{r.long.long_score:>+6.1f} {r.long.short_score:>+6.1f}  "
              f"{r.total.long_score:>+6.1f} {r.total.short_score:>+6.1f}")

    # Detail for last day
    r = board.evaluate(data, data.n - 1)
    print()
    print(f"=== {data.dates[-1]} 明細 ===")
    print()

    for label, tf in [("短週期", r.short), ("中週期", r.medium), ("長週期", r.long)]:
        print(f"【{label}】多方={tf.long_score:+.1f} ({tf.long.pct:+.0f}%)  "
              f"空方={tf.short_score:+.1f} ({tf.short.pct:+.0f}%)")
        for d in tf.long.details:
            if d.triggered:
                print(f"  多方 {d.name}: {d.points:+.1f}")
        for d in tf.short.details:
            if d.triggered:
                print(f"  空方 {d.name}: {d.points:+.1f}")
        print()

    print(f"【綜合】多方={r.total.long_score:+.1f}  空方={r.total.short_score:+.1f}")


if __name__ == "__main__":
    main()
