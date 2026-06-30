"""Six price-pattern day-trade signals, mirroring the daily signal factory's
taxonomy: {reversal, momentum, flee} x {long, short}.

Each function takes ``bars`` (oldest..newest, last element = current closed bar)
and returns True iff the signal fires on the current bar. Pure price action, no
look-ahead. Flee signals are dual-role: a fast reversal that is simultaneously
the opposite side's entry and this side's exit (resolved by the composite).

  pick       長 反轉  bullish engulfing at a relative low
  touch      空 反轉  bearish engulfing at a relative high
  buy        長 順勢  close breaks above the prior-N-bar high (Donchian up)
  sell       空 順勢  close breaks below the prior-N-bar low (Donchian down)
  buy_flee   下殺反轉  new N-bar high then closes below prev close (bull trap)
  sell_flee  上拉反轉  new N-bar low then closes above prev close (bear trap)
"""
from __future__ import annotations

from typing import Sequence

from broker.types import Bar


def _is_red(b: Bar) -> bool:
    return b.close > b.open


def _is_black(b: Bar) -> bool:
    return b.close < b.open


def pick(bars: Sequence[Bar], lookback: int = 20) -> bool:
    """長 反轉: bullish engulfing whose extreme breaks the prior-N-bar low."""
    if len(bars) < lookback + 2:
        return False
    prev, curr = bars[-2], bars[-1]
    prior = bars[-(lookback + 2):-2]
    return (_is_black(prev) and _is_red(curr)
            and curr.open <= prev.close and curr.close >= prev.open
            and min(prev.low, curr.low) <= min(b.low for b in prior))


def touch(bars: Sequence[Bar], lookback: int = 20) -> bool:
    """空 反轉: bearish engulfing whose extreme breaks the prior-N-bar high."""
    if len(bars) < lookback + 2:
        return False
    prev, curr = bars[-2], bars[-1]
    prior = bars[-(lookback + 2):-2]
    return (_is_red(prev) and _is_black(curr)
            and curr.open >= prev.close and curr.close <= prev.open
            and max(prev.high, curr.high) >= max(b.high for b in prior))


def buy(bars: Sequence[Bar], breakout_lb: int = 20) -> bool:
    """長 順勢: close breaks above the highest high of the prior N bars."""
    if len(bars) < breakout_lb + 1:
        return False
    prior = bars[-(breakout_lb + 1):-1]
    return bars[-1].close > max(b.high for b in prior)


def sell(bars: Sequence[Bar], breakout_lb: int = 20) -> bool:
    """空 順勢: close breaks below the lowest low of the prior N bars."""
    if len(bars) < breakout_lb + 1:
        return False
    prior = bars[-(breakout_lb + 1):-1]
    return bars[-1].close < min(b.low for b in prior)


def buy_flee(bars: Sequence[Bar], flee_lb: int = 10) -> bool:
    """下殺反轉 (bull trap): new prior-N-bar high intrabar, then closes below the
    previous close. Longs flee, shorts enter."""
    if len(bars) < flee_lb + 1:
        return False
    prev, curr = bars[-2], bars[-1]
    prior = bars[-(flee_lb + 1):-1]
    return curr.high > max(b.high for b in prior) and curr.close < prev.close


def sell_flee(bars: Sequence[Bar], flee_lb: int = 10) -> bool:
    """上拉反轉 (bear trap): new prior-N-bar low intrabar, then closes above the
    previous close. Shorts flee, longs enter."""
    if len(bars) < flee_lb + 1:
        return False
    prev, curr = bars[-2], bars[-1]
    prior = bars[-(flee_lb + 1):-1]
    return curr.low < min(b.low for b in prior) and curr.close > prev.close
