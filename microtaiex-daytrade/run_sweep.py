"""Parameter sweep for the engulfing strategy over historical bars.

Runs a grid of (lookback x atr_mult) on the merged CSV data and prints a table
sorted by net PnL, so you can see parameter sensitivity at a glance.

Usage:
    python run_sweep.py "reports\\tm_*.csv" [atr_period=14]
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from backtest.replay import CostModel, replay          # noqa: E402
from data.csv_loader import load_bars_glob             # noqa: E402
from risk.risk_manager import RiskConfig               # noqa: E402
from strategy.strategies.engulfing import EngulfingStrategy  # noqa: E402

LOOKBACKS = [5, 10, 15, 20]
ATR_MULTS = [1.0, 1.5, 2.0, 2.5, 3.0]


def _profit_factor(round_trips) -> float:
    wins = sum(rt.points for rt in round_trips if rt.points > 0)
    losses = -sum(rt.points for rt in round_trips if rt.points < 0)
    if losses <= 0:
        return float("inf") if wins > 0 else 0.0
    return wins / losses


def main(argv) -> int:
    if len(argv) < 2:
        print('usage: run_sweep.py "CSV-or-glob" [atr_period=14] [trend_sma=120]')
        return 2
    pattern = argv[1]
    atr_period = int(argv[2]) if len(argv) > 2 else 14
    trend_sma = int(argv[3]) if len(argv) > 3 else 0   # 0 = no trend filter (default)

    bars, files = load_bars_glob(pattern)
    if not bars:
        print("no bars loaded"); return 1
    print(f"files={len(files)} bars={len(bars)} "
          f"range={bars[0].ts:%Y-%m-%d}~{bars[-1].ts:%Y-%m-%d} "
          f"atr_period={atr_period} trend_sma={trend_sma}\n")

    rows = []
    for lb in LOOKBACKS:
        for k in ATR_MULTS:
            res = replay(bars, EngulfingStrategy(lookback=lb, trend_sma=trend_sma),
                         risk_cfg=RiskConfig(stop_loss_atr_mult=k),
                         cost=CostModel(), atr_period=atr_period)
            rows.append((lb, k, res.n_trades, res.win_rate,
                         _profit_factor(res.round_trips), res.net_pnl))

    rows.sort(key=lambda r: r[5], reverse=True)   # by net PnL desc
    print(f"{'lookback':>8} {'atr_mult':>8} {'trades':>6} {'win%':>5} {'PF':>5} {'net NT$':>10}")
    for lb, k, n, wr, pf, net in rows:
        pf_s = "inf" if pf == float("inf") else f"{pf:.2f}"
        print(f"{lb:>8} {k:>8.1f} {n:>6} {wr*100:>4.0f}% {pf_s:>5} {net:>+10.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
