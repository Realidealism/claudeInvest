"""Placeholder strategy: fast/slow SMA crossover on 5m closes.

This is a stand-in to exercise the pipeline end-to-end. The real strategy (the
"red-black box" tape-reading approach) waits on strategy_redblack_box_spec.md.
LONG on a bullish cross, SHORT on a bearish cross; flattening is handled by the
risk layer (stop / session force-close).
"""
from __future__ import annotations

from typing import Optional

from broker.types import Bar
from strategy.base import Signal, SignalType, Strategy, StrategyContext
from strategy.indicators import sma


class MaCrossStrategy(Strategy):
    def __init__(self, fast: int = 5, slow: int = 20, timeframe: str = "5m") -> None:
        if fast >= slow:
            raise ValueError("fast period must be < slow period")
        self.fast = fast
        self.slow = slow
        self.timeframe = timeframe
        self._prev_diff: Optional[float] = None

    def on_bar_close(self, bar: Bar, ctx: StrategyContext) -> Optional[Signal]:
        closes = ctx.closes()
        f = sma(closes, self.fast)
        s = sma(closes, self.slow)
        if f is None or s is None:
            return None
        diff = f - s
        signal: Optional[Signal] = None
        if self._prev_diff is not None:
            if self._prev_diff <= 0 < diff:
                signal = Signal(bar.symbol, SignalType.LONG, bar.ts, bar.close,
                                "fast SMA crossed above slow", abs(diff))
            elif self._prev_diff >= 0 > diff:
                signal = Signal(bar.symbol, SignalType.SHORT, bar.ts, bar.close,
                                "fast SMA crossed below slow", abs(diff))
        self._prev_diff = diff
        return signal
