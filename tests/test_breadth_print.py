"""
Print market breadth data for the most recent trading days.
"""

import sys
import os
import io

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from analysis.market_breadth import calculate_market_breadth


def main():
    results = calculate_market_breadth(last_n_days=10)

    if not results:
        print("無資料")
        return

    print()
    print("=" * 120)
    print("  市場強度 — 均線排列比例（排除死魚股）")
    print("=" * 120)
    print()
    print(f"{'日期':>12s}  {'活躍':>5s} {'有效':>5s} {'活魚%':>5s}  "
          f"{'短多':>5s} {'短中':>5s} {'短空':>5s}  "
          f"{'中多':>5s} {'中中':>5s} {'中空':>5s}  "
          f"{'長多':>5s} {'長中':>5s} {'長空':>5s}")
    print("-" * 110)

    for r in results:
        print(f"{str(r.trade_date):>12s}  {r.active_stocks:5d} {r.total_stocks:5d} "
              f"{r.alive_pct:4.1f}%  "
              f"{r.short_up_pct:4.1f}% {r.short_neutral_pct:4.1f}% {r.short_down_pct:4.1f}%  "
              f"{r.medium_up_pct:4.1f}% {r.medium_neutral_pct:4.1f}% {r.medium_down_pct:4.1f}%  "
              f"{r.long_up_pct:4.1f}% {r.long_neutral_pct:4.1f}% {r.long_down_pct:4.1f}%")

    # Latest day summary
    latest = results[-1]
    print()
    print(f"=== {latest.trade_date} 市場強度 ===")
    print(f"  活躍股票: {latest.active_stocks}  非死魚: {latest.total_stocks} ({latest.alive_pct:.1f}%)")
    print()
    print(f"  短期 (3>8>21):   多排 {latest.short_up_pct:5.1f}%  中性 {latest.short_neutral_pct:5.1f}%  空排 {latest.short_down_pct:5.1f}%")
    print(f"  中期 (5>13>34):  多排 {latest.medium_up_pct:5.1f}%  中性 {latest.medium_neutral_pct:5.1f}%  空排 {latest.medium_down_pct:5.1f}%")
    print(f"  長期 (8>21>55):  多排 {latest.long_up_pct:5.1f}%  中性 {latest.long_neutral_pct:5.1f}%  空排 {latest.long_down_pct:5.1f}%")
    print()
    print(f"  預估總趨勢（實排 + 成形中）:")
    for label, up, neu, dn, upf, dnf in [
        ("短期", latest.short_up_pct, latest.short_neutral_pct, latest.short_down_pct,
         latest.short_up_forming_pct, latest.short_down_forming_pct),
        ("中期", latest.medium_up_pct, latest.medium_neutral_pct, latest.medium_down_pct,
         latest.medium_up_forming_pct, latest.medium_down_forming_pct),
        ("長期", latest.long_up_pct, latest.long_neutral_pct, latest.long_down_pct,
         latest.long_up_forming_pct, latest.long_down_forming_pct),
    ]:
        total_up = up + upf
        total_dn = dn + dnf
        total_neu = neu - upf - dnf
        print(f"  {label}:  多勢 {total_up:5.1f}%  中性 {total_neu:5.1f}%  空勢 {total_dn:5.1f}%")


if __name__ == "__main__":
    main()
