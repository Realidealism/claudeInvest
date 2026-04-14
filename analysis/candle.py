"""
Candlestick technical analysis — ported from Go CalculateCandle.

Computes candle body, shadows, HL (peak), stick length, cuts (fibonacci
retracement), jump/squat gaps, trigger/creep patterns, and rolling
high/low of High and Low prices.

Usage:
    from analysis.candle import calculate_candle
    result = calculate_candle(open_, high, low, close)
    result.candle.top        # candle top (max of open, close)
    result.hl.short_hl       # short peak flag
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from numpy.typing import NDArray

from analysis.indicators import (
    sma,
    rolling_std,
    rolling_highest,
    rolling_lowest,
    compare_ge,
)

F32 = np.float32
F32Array = NDArray[np.float32]
BoolArray = NDArray[np.bool_]

# Periods used for averaged std/ma in HL and stick length
_AVG_PERIODS = (21, 34, 55)


# ── Dataclasses ─────────────────────────────────────────────────────────────


@dataclass
class CandleBody:
    """Basic candle body information."""
    top: F32Array           # max(open, close)
    bottom: F32Array        # min(open, close)
    # NOTE: Go naming is inverted — BlackCandle = Close > Open there.
    # We follow standard convention: red = bullish, black = bearish.
    red: BoolArray          # close > open (bullish)
    black: BoolArray        # open > close (bearish)


@dataclass
class HLResult:
    """High-Low range (峰) analysis."""
    hl: F32Array            # high - low
    std: F32Array           # averaged std over 21/34/55
    ma: F32Array            # averaged ma over 21/34/55
    hlb: F32Array           # averaged rolling-max HL over 21/34/55
    short_hl: BoolArray     # hl > ma + std * 0.95
    medium_hl: BoolArray    # hl > ma + std * 1.45
    long_hl: BoolArray      # hl > ma + std * 1.95


@dataclass
class StickLengthResult:
    """Candle body length (棒長) analysis."""
    length: F32Array        # top - bottom
    std: F32Array           # averaged std over 21/34/55
    ma: F32Array            # averaged ma over 21/34/55
    red_short: BoolArray
    black_short: BoolArray
    red_medium: BoolArray
    black_medium: BoolArray
    red_long: BoolArray
    black_long: BoolArray


@dataclass
class ShadowResult:
    """Upper/lower shadow (影) analysis."""
    upper_length: F32Array
    lower_length: F32Array
    upper: BoolArray        # significant upper shadow
    lower: BoolArray        # significant lower shadow


@dataclass
class HighLowRolling:
    """Rolling high/low for raw High or Low price."""
    high: dict[int, F32Array]   # rolling highest
    low: dict[int, F32Array]    # rolling lowest


@dataclass
class TopBottomRolling:
    """Rolling high/low for candle Top or Bottom."""
    values: dict[int, F32Array]


@dataclass
class CutResult:
    """Fibonacci-based price cuts (切)."""
    top_cut: F32Array       # high - hl * 0.191
    up_cut: F32Array        # high - hl * 0.382
    mid_cut: F32Array       # (high + low) / 2
    down_cut: F32Array      # low + hl * 0.382
    bottom_cut: F32Array    # low + hl * 0.191
    close_ge_top: BoolArray
    close_ge_up: BoolArray
    close_le_down: BoolArray
    close_le_bottom: BoolArray


@dataclass
class CandleResult:
    """Complete candlestick analysis result."""
    candle: CandleBody
    hl: HLResult
    stick_length: StickLengthResult
    shadow: ShadowResult
    high_rolling: HighLowRolling    # rolling extremes of High price
    low_rolling: HighLowRolling     # rolling extremes of Low price
    top_rolling: TopBottomRolling   # rolling highest of candle Top
    bottom_rolling: TopBottomRolling  # rolling lowest of candle Bottom
    cut: CutResult

    # Gap patterns
    jump: BoolArray         # bottom > prev top (gap up)
    squat: BoolArray        # top < prev bottom (gap down)

    # Trigger (觸) — today breaks previous extreme
    trigger_high1: BoolArray    # H > H[1]
    trigger_high2: BoolArray    # H > HD2B[1]
    trigger_high3: BoolArray    # H > HD3B[1]
    trigger_low1: BoolArray     # L < L[1]
    trigger_low2: BoolArray     # L < LD2S[1]
    trigger_low3: BoolArray     # L < LD3S[1]

    # Creep (爬) — today stays within previous extreme
    creep_high1: BoolArray      # H < H[1]
    creep_high2: BoolArray      # H < HD2S[1]
    creep_high3: BoolArray      # H < HD3S[1]
    creep_low1: BoolArray       # L > L[1]
    creep_low2: BoolArray       # L > LD2B[1]
    creep_low3: BoolArray       # L > LD3B[1]


# ── Main Entry ──────────────────────────────────────────────────────────────


def calculate_candle(
    open_: F32Array, high: F32Array, low: F32Array, close: F32Array,
) -> CandleResult:
    """
    Main entry point — equivalent to Go GetCalculateCandle.

    Parameters
    ----------
    open_, high, low, close : float32 numpy arrays (oldest first).
    """
    n = len(close)
    open_ = open_.astype(F32)
    high = high.astype(F32)
    low = low.astype(F32)
    close = close.astype(F32)

    # ── Candle body ─────────────────────────────────────────────────────
    top = np.maximum(open_, close)
    bottom = np.minimum(open_, close)
    # Go: BlackCandle = Close > Open, RedCandle = Open > Close (naming inverted)
    # We use standard convention here.
    red = close > open_
    black = open_ > close
    candle = CandleBody(top=top, bottom=bottom, red=red, black=black)

    # ── Rolling Top / Bottom ────────────────────────────────────────────
    top_rolling = TopBottomRolling(values={
        p: rolling_highest(top, p) for p in (2, 3, 5)
    })
    bottom_rolling = TopBottomRolling(values={
        p: rolling_lowest(bottom, p) for p in (2, 3, 5)
    })

    # ── HL (峰) ─────────────────────────────────────────────────────────
    hl_arr = high - low
    hl_result = _calc_hl(hl_arr, n)

    # ── Stick Length (棒長) ──────────────────────────────────────────────
    stick_len = top - bottom
    stick_length = _calc_stick_length(stick_len, hl_result, candle, n)

    # ── Shadow (影) ─────────────────────────────────────────────────────
    shadow = _calc_shadow(high, low, top, bottom, hl_result)

    # ── Jump / Squat ────────────────────────────────────────────────────
    prev_top = _shift1(top)
    prev_bottom = _shift1(bottom)
    valid1 = np.arange(n) >= 1
    jump = valid1 & (bottom > prev_top)
    squat = valid1 & (top < prev_bottom)

    # ── High / Low rolling extremes ─────────────────────────────────────
    high_rolling = HighLowRolling(
        high={p: rolling_highest(high, p) for p in (2, 3, 4, 5)},
        low={p: rolling_lowest(high, p) for p in (2, 3)},
    )
    low_rolling = HighLowRolling(
        high={p: rolling_highest(low, p) for p in (2, 3)},
        low={p: rolling_lowest(low, p) for p in (2, 3, 4, 5)},
    )

    # ── Trigger / Creep ─────────────────────────────────────────────────
    prev_high = _shift1(high)
    prev_low = _shift1(low)

    trigger_high1 = valid1 & (high > prev_high)
    trigger_low1 = valid1 & (low < prev_low)
    trigger_high2 = valid1 & (high > _shift1(high_rolling.high[2]))
    trigger_low2 = valid1 & (low < _shift1(low_rolling.low[2]))
    trigger_high3 = valid1 & (high > _shift1(high_rolling.high[3]))
    trigger_low3 = valid1 & (low < _shift1(low_rolling.low[3]))

    creep_high1 = valid1 & (high < prev_high)
    creep_low1 = valid1 & (low > prev_low)
    creep_high2 = valid1 & (high < _shift1(high_rolling.low[2]))
    creep_low2 = valid1 & (low > _shift1(low_rolling.high[2]))
    creep_high3 = valid1 & (high < _shift1(high_rolling.low[3]))
    creep_low3 = valid1 & (low > _shift1(low_rolling.high[3]))

    # ── Cut (切) ────────────────────────────────────────────────────────
    cut = _calc_cut(high, low, close, hl_arr)

    return CandleResult(
        candle=candle,
        hl=hl_result,
        stick_length=stick_length,
        shadow=shadow,
        high_rolling=high_rolling,
        low_rolling=low_rolling,
        top_rolling=top_rolling,
        bottom_rolling=bottom_rolling,
        cut=cut,
        jump=jump,
        squat=squat,
        trigger_high1=trigger_high1,
        trigger_high2=trigger_high2,
        trigger_high3=trigger_high3,
        trigger_low1=trigger_low1,
        trigger_low2=trigger_low2,
        trigger_low3=trigger_low3,
        creep_high1=creep_high1,
        creep_high2=creep_high2,
        creep_high3=creep_high3,
        creep_low1=creep_low1,
        creep_low2=creep_low2,
        creep_low3=creep_low3,
    )


# ── Helpers ─────────────────────────────────────────────────────────────────


def _shift1(arr: F32Array) -> F32Array:
    out = np.roll(arr, 1)
    out[0] = 0
    return out


def _avg_stat(data: F32Array, func, periods: tuple = _AVG_PERIODS) -> F32Array:
    """Average of a rolling statistic across multiple periods."""
    return sum(func(data, p) for p in periods) / len(periods)


# ── HL (峰) ─────────────────────────────────────────────────────────────────


def _calc_hl(hl: F32Array, n: int) -> HLResult:
    hl_std = _avg_stat(hl, rolling_std)
    hl_ma = _avg_stat(hl, sma)

    hlb = _avg_stat(hl, rolling_highest)

    return HLResult(
        hl=hl,
        std=hl_std,
        ma=hl_ma,
        hlb=hlb,
        short_hl=hl > hl_ma + hl_std * 0.95,
        medium_hl=hl > hl_ma + hl_std * 1.45,
        long_hl=hl > hl_ma + hl_std * 1.95,
    )


# ── Stick Length (棒長) ─────────────────────────────────────────────────────


def _calc_stick_length(
    stick_len: F32Array, hl: HLResult, candle: CandleBody, n: int,
) -> StickLengthResult:
    sl_std = _avg_stat(stick_len, rolling_std)
    sl_ma = _avg_stat(stick_len, sma)

    # Candlestick size classification
    short_cond = (
        (stick_len > sl_ma + sl_std * 0.95)
        & (stick_len > hl.ma + hl.std * 0.45)
    )
    medium_cond = (
        (stick_len > sl_ma + sl_std * 1.45)
        & (stick_len > hl.ma + hl.std * 0.95)
    )
    long_cond = (
        (stick_len > sl_ma + sl_std * 1.95)
        & (stick_len > hl.ma + hl.std * 1.45)
    )

    return StickLengthResult(
        length=stick_len,
        std=sl_std,
        ma=sl_ma,
        red_short=short_cond & candle.red,
        black_short=short_cond & candle.black,
        red_medium=medium_cond & candle.red,
        black_medium=medium_cond & candle.black,
        red_long=long_cond & candle.red,
        black_long=long_cond & candle.black,
    )


# ── Shadow (影) ─────────────────────────────────────────────────────────────


def _calc_shadow(
    high: F32Array, low: F32Array,
    top: F32Array, bottom: F32Array,
    hl: HLResult,
) -> ShadowResult:
    upper_len = high - top
    lower_len = bottom - low
    threshold = (hl.ma - hl.std * 0.1) * 0.5

    return ShadowResult(
        upper_length=upper_len,
        lower_length=lower_len,
        upper=upper_len > threshold,
        lower=lower_len > threshold,
    )


# ── Cut (切) ────────────────────────────────────────────────────────────────


def _calc_cut(
    high: F32Array, low: F32Array, close: F32Array, hl: F32Array,
) -> CutResult:
    top_cut = high - hl * 0.191
    up_cut = high - hl * 0.382
    mid_cut = (high + low) / 2
    down_cut = low + hl * 0.382
    bottom_cut = low + hl * 0.191

    return CutResult(
        top_cut=top_cut,
        up_cut=up_cut,
        mid_cut=mid_cut,
        down_cut=down_cut,
        bottom_cut=bottom_cut,
        close_ge_top=close >= top_cut,
        close_ge_up=close >= up_cut,
        close_le_down=close <= down_cut,
        close_le_bottom=close <= bottom_cut,
    )
