"""Engulfing-pattern reversal strategy with a relative-position filter.

Entry (5m closed bars):
- Bullish engulfing at a relative LOW  -> LONG
- Bearish engulfing at a relative HIGH -> SHORT

红黑 (candle color): red = close > open, black = close < open.
Engulfing: current body fully covers the previous body.
Relative low/high: the pattern's extreme breaks the lowest/highest of the prior
``lookback`` bars (a fresh local extreme = reversal-from-edge).

Exit is handled by the risk layer (Chandelier trailing stop) + opposite signal
flip + session force-close.
"""
from __future__ import annotations

from typing import Optional

from broker.types import Bar
from strategy.base import Signal, SignalType, Strategy, StrategyContext


def _is_red(b: Bar) -> bool:
    return b.close > b.open


def _is_black(b: Bar) -> bool:
    return b.close < b.open


class EngulfingStrategy(Strategy):
    def __init__(self, lookback: int = 10, timeframe: str = "5m",
                 trend_sma: int = 0) -> None:
        if lookback < 1:
            raise ValueError("lookback must be >= 1")
        self.lookback = lookback
        self.timeframe = timeframe
        # trend_sma > 0: only take WITH-TREND signals (long above SMA, short below).
        # Backtest shows counter-trend engulfing bleeds; with-trend is the edge.
        self.trend_sma = trend_sma

    def on_bar_close(self, bar: Bar, ctx: StrategyContext) -> Optional[Signal]:
        bars = ctx.bars
        if len(bars) < max(self.lookback + 2, self.trend_sma):
            return None
        prev, curr = bars[-2], bars[-1]
        prior = bars[-(self.lookback + 2):-2]   # N bars before the 2-bar pattern

        signal: Optional[Signal] = None
        # bullish engulfing -> long, only at a relative low
        if (_is_black(prev) and _is_red(curr)
                and curr.open <= prev.close and curr.close >= prev.open
                and min(prev.low, curr.low) <= min(b.low for b in prior)):
            signal = Signal(bar.symbol, SignalType.LONG, bar.ts, bar.close,
                            "bullish engulfing at relative low")
        # bearish engulfing -> short, only at a relative high
        elif (_is_red(prev) and _is_black(curr)
                and curr.open >= prev.close and curr.close <= prev.open
                and max(prev.high, curr.high) >= max(b.high for b in prior)):
            signal = Signal(bar.symbol, SignalType.SHORT, bar.ts, bar.close,
                            "bearish engulfing at relative high")

        # trend filter: drop counter-trend signals
        if signal is not None and self.trend_sma > 0:
            sma = sum(b.close for b in bars[-self.trend_sma:]) / self.trend_sma
            if signal.type is SignalType.LONG and bar.close <= sma:
                return None
            if signal.type is SignalType.SHORT and bar.close >= sma:
                return None
        return signal
