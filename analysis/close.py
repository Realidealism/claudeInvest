"""
Close-price technical analysis — ported from Go CalculateClose.

Computes MA, Bollinger Bands, rolling high/low, turn points (扣抵),
EMA, and knot (均線糾結) indicators for a given close-price series.

Usage:
    from analysis.close import calculate_close
    result = calculate_close(close_prices)   # numpy float32 array
    result.ma.sma[21]                        # 21-day SMA array
    result.boll[21].u1                       # 21-day upper Bollinger band 1
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from numpy.typing import NDArray

from analysis.indicators import (
    sma,
    pre_sma,
    ema,
    rolling_std,
    bias_ratio,
    rolling_highest,
    rolling_lowest,
    compare_ge,
    compare_gt,
    compare_lt,
)
from analysis.constants import (
    SMA_PERIODS,
    SMA_PERIODS_SHORT,
    BOLL_PERIODS,
    BS_PERIODS,
    SORT_NORMAL,
    SORT_LP,
    TURN_CONFIGS,
    VALUE_THRESHOLDS,
    KNOT_DEFS,
    KNOT_PURE_DEFS,
)

F32 = np.float32
F32Array = NDArray[np.float32]
BoolArray = NDArray[np.bool_]
U8Array = NDArray[np.uint8]


# ── Dataclasses ─────────────────────────────────────────────────────────────


@dataclass
class SortResult:
    """Trend alignment for one timeframe (e.g. short/medium/long)."""
    up: BoolArray
    down: BoolArray


@dataclass
class MAResult:
    sma: dict[int, F32Array]
    pre_sma: dict[int, F32Array]
    close_on_ma: dict[int, BoolArray]
    bias: dict[int, F32Array]
    sort_normal: dict[str, SortResult]
    sort_predicted: dict[str, SortResult]
    sort_lp: dict[str, SortResult]


@dataclass
class BollBand:
    """Bollinger band data for a single period."""
    std: F32Array
    u1: F32Array
    u2: F32Array
    u3: F32Array
    d1: F32Array
    d2: F32Array
    d3: F32Array
    close_gt_u1: BoolArray
    close_gt_u2: BoolArray
    close_gt_u3: BoolArray
    close_lt_d1: BoolArray
    close_lt_d2: BoolArray
    close_lt_d3: BoolArray


@dataclass
class BSResult:
    high: dict[int, F32Array]
    low: dict[int, F32Array]


@dataclass
class KnotEntry:
    """Knot data for one timeframe."""
    bias: F32Array
    std: F32Array
    ma: F32Array
    flag: BoolArray
    duration: U8Array          # consecutive days in knot state
    break_up: BoolArray        # knot ended + upward breakout
    break_down: BoolArray      # knot ended + downward breakout


@dataclass
class CloseResult:
    """Complete close-price analysis result."""
    ma: MAResult
    boll: dict[int, BollBand]
    bs: BSResult
    turn: dict[int, U8Array]
    ema: dict[int, F32Array]
    knot: dict[str, KnotEntry]
    knot_level: U8Array            # 0–3: how many of short/medium/long are in knot
    value_level: U8Array


# ── Main Entry ──────────────────────────────────────────────────────────────


def calculate_close(close: F32Array) -> CloseResult:
    """
    Main entry point — equivalent to Go GetCalculateClose.

    Parameters
    ----------
    close : float32 numpy array of close prices (oldest first).
    """
    n = len(close)
    close = close.astype(F32)

    ma = _calc_ma(close, n)
    bs = _calc_bs(close, n)

    knot = _calc_knot(ma.sma, close, n)

    # knot_level: count of short/medium/long simultaneously in knot (0–3)
    knot_level = (
        knot["short"].flag.astype(np.uint8)
        + knot["medium"].flag.astype(np.uint8)
        + knot["long"].flag.astype(np.uint8)
    )

    return CloseResult(
        ma=ma,
        boll=_calc_boll(close, n),
        bs=bs,
        ema=_calc_ema(close),
        value_level=_calc_value_level(ma.sma[8], n),
        turn=_calc_turn(close, bs.high, bs.low, n),
        knot=knot,
        knot_level=knot_level,
    )


# ── MA block ────────────────────────────────────────────────────────────────


def _calc_ma(close: F32Array, n: int) -> MAResult:
    sma_d: dict[int, F32Array] = {p: sma(close, p) for p in SMA_PERIODS}
    pre_sma_d: dict[int, F32Array] = {p: pre_sma(close, p) for p in SMA_PERIODS_SHORT}

    close_on_ma: dict[int, BoolArray] = {}
    bias_d: dict[int, F32Array] = {}
    for p in SMA_PERIODS:
        close_on_ma[p] = compare_ge(close, sma_d[p])
        bias_d[p] = bias_ratio(close, sma_d[p])

    return MAResult(
        sma=sma_d,
        pre_sma=pre_sma_d,
        close_on_ma=close_on_ma,
        bias=bias_d,
        sort_normal=_calc_sort(sma_d, SORT_NORMAL),
        sort_predicted=_calc_sort(pre_sma_d, SORT_NORMAL),
        sort_lp=_calc_sort(sma_d, SORT_LP),
    )


def _calc_sort(
    ma_data: dict[int, F32Array], definitions: dict,
) -> dict[str, SortResult]:
    """Compute up/down trend alignment for each timeframe."""
    out: dict[str, SortResult] = {}
    for label, (p1, p2, p3) in definitions.items():
        m1, m2, m3 = ma_data[p1], ma_data[p2], ma_data[p3]
        out[label] = SortResult(
            up=(m1 > m2) & (m2 > m3),
            down=(m1 < m2) & (m2 < m3),
        )
    return out


# ── Sort Forming (成形中排列) ───────────────────────────────────────────────


def calc_sort_forming(
    close_result: CloseResult,
    volume_status: U8Array,
) -> dict[str, SortResult]:
    """
    Compute "forming" trend alignment for short/medium/long timeframes.

    Detects cases where the predicted MAs (pre_sma) have formed a sorted
    alignment but the current SMAs have not. Intended as a strength-state
    indicator, NOT as a buy/sell signal.

    Conditions per timeframe (up variant, mirrored for down):
      1. pre[p1] > pre[p2] * 1.003            (predicted sort + 0.3% min gap)
      2. pre[p2] > pre[p3] * 1.003
      3. pre[p1] > sma[p3]                    (predicted short crosses current long)
      4. pre[p1] today > pre[p1] yesterday    (slope confirmation)
      5. NOT (sma[p1] > sma[p2] > sma[p3])    (exclude already-aligned)
      6. volume_status < 3                    (only flood/big/high; exclude normal/low/shrink/sleep)

    Day 0 has no previous pre_sma — slope check yields False.
    """
    out: dict[str, SortResult] = {}
    pre = close_result.ma.pre_sma
    cd = close_result.ma.sma
    vol_ok = volume_status < 3
    GAP = F32(1.003)

    for label, (p1, p2, p3) in SORT_NORMAL.items():
        pre1, pre2, pre3 = pre[p1], pre[p2], pre[p3]
        cd3 = cd[p3]

        n = len(pre1)
        pre1_up = np.zeros(n, dtype=np.bool_)
        pre1_dn = np.zeros(n, dtype=np.bool_)
        if n > 1:
            pre1_up[1:] = pre1[1:] > pre1[:-1]
            pre1_dn[1:] = pre1[1:] < pre1[:-1]

        up_forming = (
            (pre1 > pre2 * GAP)
            & (pre2 > pre3 * GAP)
            & (pre1 > cd3)
            & pre1_up
            & vol_ok
        )

        down_forming = (
            (pre1 * GAP < pre2)
            & (pre2 * GAP < pre3)
            & (pre1 < cd3)
            & pre1_dn
            & vol_ok
        )

        out[label] = SortResult(up=up_forming, down=down_forming)

    return out


# ── Bollinger Bands ─────────────────────────────────────────────────────────


def _calc_boll(close: F32Array, n: int) -> dict[int, BollBand]:
    result: dict[int, BollBand] = {}
    for p in BOLL_PERIODS:
        std = rolling_std(close, p)
        ma = sma(close, p)
        u1, u2, u3 = ma + std, ma + std * 2, ma + std * 3
        d1, d2, d3 = ma - std, ma - std * 2, ma - std * 3

        result[p] = BollBand(
            std=std,
            u1=u1, u2=u2, u3=u3,
            d1=d1, d2=d2, d3=d3,
            close_gt_u1=compare_gt(close, u1),
            close_gt_u2=compare_gt(close, u2),
            close_gt_u3=compare_gt(close, u3),
            close_lt_d1=compare_lt(close, d1),
            close_lt_d2=compare_lt(close, d2),
            close_lt_d3=compare_lt(close, d3),
        )
    return result


# ── Rolling High / Low (BS) ────────────────────────────────────────────────


def _calc_bs(close: F32Array, n: int) -> BSResult:
    high_d: dict[int, F32Array] = {}
    low_d: dict[int, F32Array] = {}
    for p in BS_PERIODS:
        high_d[p] = rolling_highest(close, p)
        low_d[p] = rolling_lowest(close, p)
    return BSResult(high=high_d, low=low_d)


# ── Turn Points (漲跌扣抵) ──────────────────────────────────────────────────


def _calc_turn(
    close: F32Array,
    bs_high: dict[int, F32Array],
    bs_low: dict[int, F32Array],
    n: int,
) -> dict[int, U8Array]:
    """
    For each MA period, determine whether today's close is above (2),
    below (0), or within (1) the historical high/low range at the
    look-back offset.

    Values: 2 = bullish (漲), 1 = neutral, 0 = bearish (跌)
    """
    turn: dict[int, U8Array] = {}

    for ma_period, offset, bs_period in TURN_CONFIGS:
        out = np.ones(n, dtype=np.uint8)

        if bs_period is None:
            valid = np.arange(n) >= offset
            shifted = np.roll(close, offset)
            out[valid & (close > shifted)] = 2
            out[valid & (close < shifted)] = 0
        else:
            hi = bs_high[bs_period]
            lo = bs_low[bs_period]
            valid = np.arange(n) >= (offset + 1)
            shifted_hi = np.roll(hi, offset)
            shifted_lo = np.roll(lo, offset)
            out[valid & (close > shifted_hi)] = 2
            out[valid & (close < shifted_lo)] = 0

        turn[ma_period] = out

    return turn


# ── Value Level ─────────────────────────────────────────────────────────────


def _calc_value_level(sma8: F32Array, n: int) -> U8Array:
    """Classify price level 0–15 based on 8-day SMA."""
    out = np.full(n, len(VALUE_THRESHOLDS), dtype=np.uint8)
    for i, threshold in reversed(list(enumerate(VALUE_THRESHOLDS))):
        out[sma8 < threshold] = i
    return out


# ── EMA ─────────────────────────────────────────────────────────────────────


def _calc_ema(close: F32Array) -> dict[int, F32Array]:
    return {p: ema(close, p) for p in SMA_PERIODS_SHORT}


# ── Knot (均線糾結) ─────────────────────────────────────────────────────────


def _calc_knot(
    sma_d: dict[int, F32Array], close: F32Array, n: int,
) -> dict[str, KnotEntry]:
    """
    Detect MA convergence (糾結) at short/medium/long timeframes,
    plus medium_pure / long_pure variants.

    Flag rule rationale (ported from Go, threshold = bias < ma - std * X):
      Short:  OR aggregation — rapid detection, any single sub-rule fires.
              0.9(<=1d) OR 0.8(<1d) OR 0.7(<2d) OR 0.6(<3d) OR 0.5(<5d)
      Medium: trigger OR + 5-day persistence AND gate.
              (0.9(<1d) OR 0.8(<2d) OR 0.7(<3d)) AND 0.6(<5d)
      Long:   trigger OR + 3-day AND + 5-day AND (strictest).
              (0.9(<1d) OR 0.8(<2d)) AND 0.7(<3d) AND 0.6(<5d)

    Threshold semantics:
      0.9 = loose (mild convergence)
      0.7 = moderate
      0.5 = tight (strong convergence)
    Higher thresholds are easier to trigger; longer day requirements
    ensure persistence. Short timeframe uses loose OR for speed;
    longer timeframes add AND gates to filter noise.

    Pure variants (medium_pure, long_pure) use only the MAs native to
    that timeframe, detecting convergence without short-MA interference.
    """
    result: dict[str, KnotEntry] = {}

    all_defs = KNOT_DEFS + KNOT_PURE_DEFS

    for label, periods, std_window in all_defs:
        # Check that all required MA periods exist
        if not all(p in sma_d for p in periods):
            continue

        stack = np.stack([sma_d[p] for p in periods], axis=0)
        hi = np.max(stack, axis=0)
        lo = np.min(stack, axis=0)

        with np.errstate(divide="ignore", invalid="ignore"):
            knot_bias = np.where(lo != 0, (hi - lo) / lo, F32(0)).astype(F32)

        knot_std = rolling_std(knot_bias, std_window)
        knot_ma = sma(knot_bias, std_window)

        knot_flag = _calc_knot_flag(label, knot_bias, knot_ma, knot_std, n)

        # Duration: consecutive days with flag=True
        duration = _calc_knot_duration(knot_flag, n)

        # Break up/down: knot ended yesterday + close breakout today
        break_up, break_down = _calc_knot_break(knot_flag, close, n)

        result[label] = KnotEntry(
            bias=knot_bias,
            std=knot_std,
            ma=knot_ma,
            flag=knot_flag,
            duration=duration,
            break_up=break_up,
            break_down=break_down,
        )

    return result


def _calc_knot_flag(
    label: str,
    knot_bias: F32Array,
    knot_ma: F32Array,
    knot_std: F32Array,
    n: int,
) -> BoolArray:
    """Compute knot flag using timeframe-specific rules."""
    knot_flag = np.zeros(n, dtype=np.bool_)
    if n <= 4:
        return knot_flag

    prev_ma = np.roll(knot_ma, 1)
    prev_std = np.roll(knot_std, 1)
    prev_ma[:1] = 0
    prev_std[:1] = 0

    shifted = [np.roll(knot_bias, i) for i in range(5)]
    valid = np.arange(n) >= 4

    def _days_below(thresh: float, days: int, use_le: bool = False) -> np.ndarray:
        bound = prev_ma - prev_std * thresh
        cond = valid.copy()
        op = np.less_equal if use_le else np.less
        for d in range(days):
            cond = cond & op(shifted[d], bound)
        return cond

    # Pure variants reuse the base label's rule pattern
    base = label.replace("_pure", "")

    if base == "short":
        knot_flag = (
            _days_below(0.9, 1, use_le=True)
            | _days_below(0.8, 1)
            | _days_below(0.7, 2)
            | _days_below(0.6, 3)
            | _days_below(0.5, 5)
        )
    elif base == "medium":
        trigger = (
            _days_below(0.9, 1)
            | _days_below(0.8, 2)
            | _days_below(0.7, 3)
        )
        knot_flag = trigger & _days_below(0.6, 5)
    else:  # long
        trigger = _days_below(0.9, 1) | _days_below(0.8, 2)
        knot_flag = trigger & _days_below(0.7, 3) & _days_below(0.6, 5)

    return knot_flag


def _calc_knot_duration(flag: BoolArray, n: int) -> U8Array:
    """Count consecutive True days in flag, resetting on False."""
    duration = np.zeros(n, dtype=np.uint8)
    for i in range(n):
        if flag[i]:
            duration[i] = min(255, (int(duration[i - 1]) + 1) if i > 0 else 1)
    return duration


def _calc_knot_break(
    flag: BoolArray, close: F32Array, n: int,
) -> tuple[BoolArray, BoolArray]:
    """
    Detect breakout on the first day after knot ends.

    break_up:   flag[i-1]=True, flag[i]=False, close[i] > close[i-1]
    break_down: flag[i-1]=True, flag[i]=False, close[i] < close[i-1]
    """
    break_up = np.zeros(n, dtype=np.bool_)
    break_down = np.zeros(n, dtype=np.bool_)
    if n < 2:
        return break_up, break_down

    prev_flag = np.roll(flag, 1)
    prev_flag[0] = False
    ended = prev_flag & ~flag  # knot was on, now off

    break_up[1:] = ended[1:] & (close[1:] > close[:-1])
    break_down[1:] = ended[1:] & (close[1:] < close[:-1])

    return break_up, break_down
