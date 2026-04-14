"""
OBV (On-Balance Volume) technical analysis — ported from Go CalculateOBV.

Computes a modified OBV indicator with shadow OBV (price-projected),
MACD-style divergence, and a staircase signal line with buy/sell signals.

Usage:
    from analysis.obv import calculate_obv
    result = calculate_obv(close, limit_refer, high, low, volume)
    result.signal_up     # buy signal
    result.signal_down   # sell signal
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from numpy.typing import NDArray

from analysis.indicators import (
    sma,
    ema,
    dema,
    rolling_std,
    linear_regression,
)

F32 = np.float32
F32Array = NDArray[np.float32]
BoolArray = NDArray[np.bool_]

# Configuration constants (matching Go defaults)
OBV_MA_LEN = 13
WINDOW_LEN = 26
SHADOW_EMA_LEN1 = 1
SHADOW_EMA_LEN2 = 2
SHADOW_DEMA_LEN = 8
SLOW_LEN = 55
SLOPE_LEN = 3


I8Array = NDArray[np.int8]


@dataclass
class OBVResult:
    """Complete OBV analysis result."""
    obv: F32Array               # raw OBV
    obv_ma: F32Array            # SMA of OBV
    shadow_obv: F32Array        # price-projected OBV
    shadow_obv_ema: F32Array    # EMA-smoothed shadow OBV
    macd: F32Array              # shadow DEMA - close slow EMA
    step_line: F32Array         # staircase line
    signal_up: BoolArray        # buy signal (cross up)
    signal_down: BoolArray      # sell signal (cross down)
    trend: I8Array              # +1=bullish, -1=bearish, 0=undetermined


def calculate_obv(
    close: F32Array,
    limit_refer: F32Array,
    high: F32Array,
    low: F32Array,
    volume: F32Array,
) -> OBVResult:
    """
    Main entry point — equivalent to Go GetCalculateOBV.

    Parameters
    ----------
    close : close prices
    limit_refer : reference price for up/down determination
    high, low : high and low prices
    volume : daily volume
    """
    n = len(close)
    close = close.astype(F32)
    limit_refer = limit_refer.astype(F32)
    high = high.astype(F32)
    low = low.astype(F32)
    volume = volume.astype(F32)

    # 1. Standard OBV
    obv = _calc_obv(close, limit_refer, volume, n)

    # 2. OBVMA
    obv_ma = sma(obv, OBV_MA_LEN)

    # OBV diff and its rolling std
    obv_diff = obv - obv_ma
    obv_diff_std = rolling_std(obv_diff, WINDOW_LEN)

    # HL diff and its rolling std
    hl_diff = (high - low).astype(F32)
    hl_std = rolling_std(hl_diff, WINDOW_LEN)

    # 3. Shadow OBV
    shadow_obv = _calc_shadow_obv(close, high, low, obv, obv_ma, obv_diff_std, hl_std, n)

    # EMA smoothing: average of EMA(1) and EMA(2)
    shadow_ema1 = ema(shadow_obv, SHADOW_EMA_LEN1)
    shadow_ema2 = ema(shadow_obv, SHADOW_EMA_LEN2)
    shadow_obv_ema = ((shadow_ema1 + shadow_ema2) / 2).astype(F32)

    # 4. MACD: DEMA of shadow - slow EMA of close
    shadow_dema = dema(shadow_obv_ema, SHADOW_DEMA_LEN)
    close_slow_ema = ema(close, SLOW_LEN)
    macd = (shadow_dema - close_slow_ema).astype(F32)

    # 5. Linear regression projection
    _, tt1 = linear_regression(macd, SLOPE_LEN)

    # 6. Staircase line with signals
    step_line, signal_up, signal_down = _calc_staircase(tt1, n)

    # 7. Trend state: latches +1 on signal_up, -1 on signal_down
    trend = _calc_trend(signal_up, signal_down, n)

    return OBVResult(
        obv=obv,
        obv_ma=obv_ma,
        shadow_obv=shadow_obv,
        shadow_obv_ema=shadow_obv_ema,
        macd=macd,
        step_line=step_line,
        signal_up=signal_up,
        signal_down=signal_down,
        trend=trend,
    )


def _calc_obv(
    close: F32Array, limit_refer: F32Array, volume: F32Array, n: int,
) -> F32Array:
    """Standard OBV: accumulate volume based on close vs reference."""
    obv = np.zeros(n, dtype=F32)
    current = F32(0)
    for i in range(n):
        if close[i] > limit_refer[i]:
            current += volume[i]
        elif close[i] < limit_refer[i]:
            current -= volume[i]
        obv[i] = current
    return obv


def _calc_shadow_obv(
    close: F32Array, high: F32Array, low: F32Array,
    obv: F32Array, obv_ma: F32Array,
    obv_diff_std: F32Array, hl_std: F32Array, n: int,
) -> F32Array:
    """Project OBV deviation onto price scale using std ratio."""
    out = np.copy(close)
    safe = (obv_diff_std != 0) & ~np.isnan(hl_std)
    shadow = np.where(
        safe,
        (obv - obv_ma) / np.where(obv_diff_std != 0, obv_diff_std, 1) * hl_std,
        F32(0),
    )
    # Positive shadow → add to high; negative → add to low
    pos = shadow > 0
    out[safe & pos] = high[safe & pos] + shadow[safe & pos]
    out[safe & ~pos] = low[safe & ~pos] + shadow[safe & ~pos]
    return out.astype(F32)


def _calc_staircase(
    tt1: F32Array, n: int,
) -> tuple[F32Array, BoolArray, BoolArray]:
    """
    Compute the staircase step line and cross signals.

    Uses cumulative average absolute deviation as the step threshold.
    """
    step_line = np.zeros(n, dtype=F32)
    signal_up = np.zeros(n, dtype=np.bool_)
    signal_down = np.zeros(n, dtype=np.bool_)

    if n == 0:
        return step_line, signal_up, signal_down

    step_line[0] = tt1[0]
    prev = float(tt1[0])
    prev_oc = 0
    cum_abs_diff = 0.0

    for i in range(1, n):
        src = float(tt1[i])

        # Cumulative average absolute deviation
        cum_abs_diff += abs(src - prev)
        a15 = cum_abs_diff / i

        # Step logic: only move if deviation exceeds threshold
        current = prev
        if src > prev + a15:
            current = src
        elif src < prev - a15:
            current = src

        step_line[i] = F32(current)

        # Direction
        current_oc = prev_oc
        if current > prev:
            current_oc = 1
        elif current < prev:
            current_oc = -1

        # Cross signals
        signal_up[i] = (current_oc == 1) and (prev_oc != 1)
        signal_down[i] = (current_oc == -1) and (prev_oc != -1)

        prev = current
        prev_oc = current_oc

    return step_line, signal_up, signal_down


def _calc_trend(
    signal_up: BoolArray, signal_down: BoolArray, n: int,
) -> I8Array:
    """Latch +1 on signal_up, -1 on signal_down, 0 before first signal."""
    trend = np.zeros(n, dtype=np.int8)
    state: int = 0
    for i in range(n):
        if signal_up[i]:
            state = 1
        elif signal_down[i]:
            state = -1
        trend[i] = state
    return trend
