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

Session-anchored day-trade signals (reset each TAIFEX session):
  vwap       長 反轉  bar wicks > k*MAD below session VWAP and closes green
  vwap_s     空 反轉  mirror (above VWAP, closes red)
  orb        長 順勢  close breaks above the opening-range (first N bars) high
  orb_s      空 順勢  close breaks below the opening-range low
"""
from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import Sequence

from broker.types import Bar
from strategy.indicators import atr

_DAY_OPEN = time(8, 45)
_DAY_CLOSE = time(13, 45)
_NIGHT_OPEN = time(15, 0)
_NIGHT_CLOSE = time(5, 0)


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
    """長 順勢: close breaks above the prior-N-bar high by a real margin (>=
    0.75*ATR21). The margin rejects marginal pokes just above the Donchian line —
    the fake-breakout traps that reverse at once — while keeping breakouts with
    genuine thrust. Threshold from an 8-file sweep (0.5-1.5*ATR all beat the
    unfiltered breakout; 0.75 = pool-net peak). ATR warmup -> no margin (base only)."""
    if len(bars) < breakout_lb + 1:
        return False
    prior = bars[-(breakout_lb + 1):-1]
    prior_high = max(b.high for b in prior)
    curr = bars[-1]
    if curr.close <= prior_high:
        return False
    a = atr(bars, 21)
    return a is None or curr.close >= prior_high + 0.75 * a


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
    previous close AND in the upper 40% of its own range. Shorts flee, longs enter.

    The range-position filter (close >= low + 0.6*range) rejects weak fake
    rebounds — a bar that plunges to a new low but only edges a point or two above
    the prior close on a soft finish is not a real reversal and gets skipped in a
    sustained sell-off. Threshold from an 8-file sweep (0.6 = robust ridge centre)."""
    if len(bars) < flee_lb + 1:
        return False
    prev, curr = bars[-2], bars[-1]
    prior = bars[-(flee_lb + 1):-1]
    return (curr.low < min(b.low for b in prior) and curr.close > prev.close
            and curr.close >= curr.low + 0.6 * (curr.high - curr.low))


# ── session-anchored helpers ─────────────────────────────────────────────

def _session_open(ts: datetime):
    """Start datetime of the TAIFEX session containing ts (None if off-session).
    Night session (15:00 evening) leads through to 05:00 the next morning."""
    t, d = ts.time(), ts.date()
    if _DAY_OPEN <= t < _DAY_CLOSE:
        return datetime.combine(d, _DAY_OPEN)
    if t >= _NIGHT_OPEN:
        return datetime.combine(d, _NIGHT_OPEN)
    if t < _NIGHT_CLOSE:
        return datetime.combine(d - timedelta(days=1), _NIGHT_OPEN)
    return None


def _session_bars(bars: Sequence[Bar]) -> list:
    """Bars belonging to the current session (oldest..newest), current included."""
    so = _session_open(bars[-1].ts)
    if so is None:
        return []
    out = []
    for b in reversed(bars):
        if b.ts < so:
            break
        out.append(b)
    out.reverse()
    return out


def _vwap(sbars: Sequence[Bar]):
    tv = sum(b.volume for b in sbars)
    if tv <= 0:
        return None
    return sum(((b.high + b.low + b.close) / 3.0) * b.volume for b in sbars) / tv


def vwap(bars: Sequence[Bar], k: float = 1.5) -> bool:
    """長 反轉: bar wicks > k*MAD below the session VWAP and closes green
    (over-extended below VWAP, rejected up)."""
    sb = _session_bars(bars)
    if len(sb) < 5:
        return False
    v = _vwap(sb)
    if v is None:
        return False
    band = k * (sum(abs(b.close - v) for b in sb) / len(sb))
    curr = bars[-1]
    return curr.low < v - band and curr.close > curr.open


def vwap_s(bars: Sequence[Bar], k: float = 1.5) -> bool:
    """空 反轉: mirror — wicks > k*MAD above VWAP and closes red."""
    sb = _session_bars(bars)
    if len(sb) < 5:
        return False
    v = _vwap(sb)
    if v is None:
        return False
    band = k * (sum(abs(b.close - v) for b in sb) / len(sb))
    curr = bars[-1]
    return curr.high > v + band and curr.close < curr.open


def orb(bars: Sequence[Bar], opening: int = 6) -> bool:
    """長 順勢: close breaks above the opening-range (first N bars) high."""
    sb = _session_bars(bars)
    if len(sb) <= opening:
        return False
    return bars[-1].close > max(b.high for b in sb[:opening])


def orb_s(bars: Sequence[Bar], opening: int = 6) -> bool:
    """空 順勢: close breaks below the opening-range low."""
    sb = _session_bars(bars)
    if len(sb) <= opening:
        return False
    return bars[-1].close < min(b.low for b in sb[:opening])
