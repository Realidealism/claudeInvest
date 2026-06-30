"""Backtest the engulfing strategy on one or more CSVs of historical bars.

Usage:
    python run_backtest_csv.py "reports\\tm_*.csv" [lookback] [atr_mult] [atr_period]
    python run_backtest_csv.py reports\\recent.csv 10 2.0 14

The first arg may be a single file or a glob (quote it so the shell doesn't
expand it). All matched files are merged, de-duplicated by timestamp, sorted.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from backtest.replay import CostModel, replay          # noqa: E402
from data.csv_loader import load_bars_glob             # noqa: E402
from risk.risk_manager import RiskConfig               # noqa: E402
from strategy.base import StrategyContext              # noqa: E402
from strategy.strategies.engulfing import EngulfingStrategy  # noqa: E402


def _count_signals(bars, lookback, trend_sma=0):
    strat = EngulfingStrategy(lookback=lookback, trend_sma=trend_sma)
    ctx = StrategyContext()
    longs = shorts = 0
    for b in bars:
        ctx.bars.append(b)
        s = strat.on_bar_close(b, ctx)
        if s is None:
            continue
        if s.type.value == "long":
            longs += 1
        elif s.type.value == "short":
            shorts += 1
    return longs, shorts


def main(argv) -> int:
    if len(argv) < 2:
        print('usage: run_backtest_csv.py "CSV-or-glob" [lookback=10] [atr_mult=2.0] [atr_period=14]')
        return 2
    pattern = argv[1]
    lookback = int(argv[2]) if len(argv) > 2 else 10
    atr_mult = float(argv[3]) if len(argv) > 3 else 2.0
    atr_period = int(argv[4]) if len(argv) > 4 else 14
    trend_sma = int(argv[5]) if len(argv) > 5 else 0   # 0 = no trend filter (default)

    bars, files = load_bars_glob(pattern)
    if not bars:
        print("no bars loaded"); return 1
    longs, shorts = _count_signals(bars, lookback, trend_sma)
    res = replay(bars, EngulfingStrategy(lookback=lookback, trend_sma=trend_sma),
                 risk_cfg=RiskConfig(stop_loss_atr_mult=atr_mult),
                 cost=CostModel(), atr_period=atr_period)

    print(f"files={len(files)}  bars={len(bars)}  "
          f"range={bars[0].ts:%Y-%m-%d}~{bars[-1].ts:%Y-%m-%d}")
    print(f"params: lookback={lookback} atr_mult={atr_mult} atr_period={atr_period} trend_sma={trend_sma}")
    print(f"signals: long={longs} short={shorts} (raw, pre-risk)")
    print(f"trades={res.n_trades}  win_rate={res.win_rate:.0%}  "
          f"gross={res.gross_pnl:+.0f}  cost={res.cost:.0f}  net={res.net_pnl:+.0f} NT$")
    for rt in res.round_trips:
        et = rt.entry_ts.strftime("%m-%d %H:%M") if rt.entry_ts else "?"
        print(f"  {rt.side.value:5} {et} {rt.entry_price:.0f} -> "
              f"{rt.exit_ts:%m-%d %H:%M} {rt.exit_price:.0f}  {rt.points:+.0f} pts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
