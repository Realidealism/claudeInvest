"""Backtest demo: run the placeholder MA-cross strategy on synthetic 5m bars
through the shared engine + cost model, and print the result.

Run: python run_backtest_demo.py
This needs no broker / network -- it exercises the same strategy/risk/position
code the live system uses, only the bar source is synthetic.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from broker.types import Bar                       # noqa: E402
from backtest.replay import CostModel, replay      # noqa: E402
from risk.risk_manager import RiskConfig           # noqa: E402
from strategy.strategies.engulfing import EngulfingStrategy  # noqa: E402

# OHLC path: downtrend (black) -> bullish engulfing at a new low -> rally ->
# pullback that trips the trailing stop. Shows entry pattern + Chandelier exit.
OHLC = [
    (120, 121, 118, 119), (119, 120, 117, 118), (118, 119, 116, 117),
    (117, 118, 115, 116), (116, 117, 114, 115),     # 5 prior black bars
    (115, 115.5, 112, 113),                          # prev: black
    (112, 118, 111, 117),                            # curr: bullish engulfing, new low -> LONG
    (117, 122, 116, 121), (121, 126, 120, 125),
    (125, 130, 124, 129), (129, 134, 128, 133),
    (133, 138, 132, 137), (137, 140, 136, 139),      # strong rally (trail ratchets to ~140)
    (139, 140, 130, 131), (131, 132, 126, 127),      # pullback -> trailing stop locks profit
]


def make_bars():
    base = datetime(2024, 12, 18, 9, 0)
    return [Bar("TMF00", base + timedelta(minutes=5 * i), o, h, l, c, 10, "5m")
            for i, (o, h, l, c) in enumerate(OHLC)]


def main():
    bars = make_bars()
    cost = CostModel(fee_per_lot=20, tax_rate=0.00002, point_value=10)
    res = replay(bars, EngulfingStrategy(lookback=5),
                 risk_cfg=RiskConfig(stop_loss_atr_mult=2.0), cost=cost, atr_period=5)

    print(f"bars={len(bars)}  trades={res.n_trades}  win_rate={res.win_rate:.0%}")
    print(f"gross={res.gross_pnl:+.1f}  cost={res.cost:.1f}  net={res.net_pnl:+.1f} NT$")
    print("-" * 56)
    for rt in res.round_trips:
        print(f"  {rt.side.value:4}  {rt.entry_ts:%H:%M} {rt.entry_price:>6.0f}"
              f"  ->  {rt.exit_ts:%H:%M} {rt.exit_price:>6.0f}   {rt.points:+.0f} pts")


if __name__ == "__main__":
    main()
