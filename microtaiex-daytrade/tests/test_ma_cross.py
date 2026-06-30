from datetime import datetime, timedelta

from broker.types import Bar
from strategy.base import SignalType, StrategyContext
from strategy.strategies.ma_cross import MaCrossStrategy


def _drive(strat, closes):
    ctx = StrategyContext()
    base = datetime(2024, 12, 18, 9, 0)
    out = []
    for i, c in enumerate(closes):
        bar = Bar("TMFR1", base + timedelta(minutes=i), c, c, c, c, 1, strat.timeframe)
        ctx.bars.append(bar)
        out.append(strat.on_bar_close(bar, ctx))
    return out


def test_warmup_returns_none():
    strat = MaCrossStrategy(fast=2, slow=3)
    sigs = _drive(strat, [5, 4])
    assert sigs == [None, None]


def test_bullish_then_bearish_cross():
    strat = MaCrossStrategy(fast=2, slow=3)
    sigs = _drive(strat, [5, 4, 3, 2, 3, 5, 7, 9, 7, 5, 3, 1])
    types = [s.type for s in sigs if s is not None]
    assert types == [SignalType.LONG, SignalType.SHORT]


def test_invalid_periods():
    try:
        MaCrossStrategy(fast=5, slow=5)
    except ValueError:
        return
    raise AssertionError("expected ValueError for fast >= slow")
