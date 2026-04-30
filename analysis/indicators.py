"""
Core technical indicator functions using numpy vectorized operations.

Ported from Go: invest/internal/calculate
"""

import numpy as np
from numpy.typing import NDArray

F32 = np.float32
F32Array = NDArray[np.float32]
BoolArray = NDArray[np.bool_]
U8Array = NDArray[np.uint8]


# ── Moving Averages ──────────────────────────────────────────────────────────


def sma(close: F32Array, period: int) -> F32Array:
    """
    Simple Moving Average.

    When insufficient data exists (i < period), uses available days as
    the window. E.g. SMA(13) at day 7 (8 data points) computes the
    average of those 8 points.
    """
    n = len(close)
    out = np.zeros(n, dtype=F32)
    if n == 0:
        return out
    cumsum = np.cumsum(close, dtype=np.float64)
    cumsum = np.insert(cumsum, 0, 0.0)
    # Full-period SMA where enough data exists
    if period <= n:
        out[period - 1 :] = (cumsum[period:] - cumsum[:-period]) / period
    # Partial-period SMA for early days: average of all available data
    for i in range(min(period - 1, n)):
        out[i] = F32(cumsum[i + 1] / (i + 1))
    return out


def pre_sma(close: F32Array, period: int) -> F32Array:
    """
    Predicted SMA: average of the most recent (period-1) values,
    i.e. what the SMA would become if today's close equals the average of
    the last (period-1) days.

    When insufficient data exists (i < period-1), uses all available
    prior values as the window.
    """
    n = len(close)
    out = np.zeros(n, dtype=F32)
    if n == 0 or period < 2:
        return out
    p1 = period - 1
    cumsum = np.cumsum(close, dtype=np.float64)
    cumsum = np.insert(cumsum, 0, 0.0)
    # Full-period pre_sma where enough data exists
    if p1 < n:
        out[p1:] = (cumsum[p1 + 1 :] - cumsum[1 : n - p1 + 1]) / p1
    # Partial: use available values (at day i, average of days 0..i-1)
    # day 0 has no prior data → use close[0] itself
    out[0] = F32(close[0])
    for i in range(1, min(p1, n)):
        out[i] = F32(cumsum[i] / i)
    return out.astype(F32)


def ema(close: F32Array, period: int) -> F32Array:
    """Exponential Moving Average."""
    n = len(close)
    out = np.zeros(n, dtype=F32)
    if n == 0 or period < 1:
        return out
    k = np.float64(2.0 / (period + 1))
    # seed with first value
    prev = np.float64(close[0])
    out[0] = F32(prev)
    for i in range(1, n):
        prev = close[i] * k + prev * (1 - k)
        out[i] = F32(prev)
    return out


# ── Statistical ──────────────────────────────────────────────────────────────


def rolling_std(data: F32Array, period: int) -> F32Array:
    """
    Population standard deviation over a rolling window.

    When insufficient data exists (i < period), uses available days.
    Day 0 always returns 0 (single value has no deviation).
    """
    n = len(data)
    out = np.zeros(n, dtype=F32)
    if n < 2 or period < 2:
        return out
    d = data.astype(np.float64)
    cumsum = np.cumsum(d)
    cumsum2 = np.cumsum(d * d)
    cumsum = np.insert(cumsum, 0, 0.0)
    cumsum2 = np.insert(cumsum2, 0, 0.0)
    # Full-period std
    if period <= n:
        s = cumsum[period:] - cumsum[:-period]
        s2 = cumsum2[period:] - cumsum2[:-period]
        var = s2 / period - (s / period) ** 2
        np.maximum(var, 0, out=var)
        out[period - 1 :] = np.sqrt(var).astype(F32)
    # Partial-period std for early days (day 0 stays 0)
    for i in range(1, min(period - 1, n)):
        w = i + 1
        s = cumsum[w]
        s2 = cumsum2[w]
        var = s2 / w - (s / w) ** 2
        if var > 0:
            out[i] = F32(np.sqrt(var))
    return out


# ── Comparison / Boolean ─────────────────────────────────────────────────────


def compare_ge(a: F32Array, b: F32Array) -> BoolArray:
    """Element-wise a >= b."""
    return a >= b


def compare_gt(a: F32Array, b: F32Array) -> BoolArray:
    """Element-wise a > b."""
    return a > b


def compare_lt(a: F32Array, b: F32Array) -> BoolArray:
    """Element-wise a < b."""
    return a < b


# ── Bias ─────────────────────────────────────────────────────────────────────


def bias_ratio(close: F32Array, ma: F32Array) -> F32Array:
    """(close - ma) / ma, safe division (0 where ma is 0)."""
    with np.errstate(divide="ignore", invalid="ignore"):
        out = np.where(ma != 0, (close - ma) / ma, F32(0))
    return out.astype(F32)


# ── Rolling Highest / Lowest ─────────────────────────────────────────────────


def _rolling_extreme(
    data: F32Array, period: int, func: str
) -> F32Array:
    """
    Vectorised rolling highest/lowest using stride tricks.
    For positions with fewer than `period` values, use whatever is available.
    """
    n = len(data)
    out = np.zeros(n, dtype=F32)
    if n == 0:
        return out
    d = data.astype(np.float64)
    fn = np.max if func == "max" else np.min
    # prefix: fewer than period bars available
    for i in range(min(period - 1, n)):
        out[i] = F32(fn(d[: i + 1]))
    if period <= n:
        # sliding window via stride tricks
        shape = (n - period + 1, period)
        strides = (d.strides[0], d.strides[0])
        windows = np.lib.stride_tricks.as_strided(d, shape=shape, strides=strides)
        if func == "max":
            out[period - 1 :] = np.max(windows, axis=1).astype(F32)
        else:
            out[period - 1 :] = np.min(windows, axis=1).astype(F32)
    return out


def rolling_highest(data: F32Array, period: int) -> F32Array:
    return _rolling_extreme(data, period, "max")


def rolling_lowest(data: F32Array, period: int) -> F32Array:
    return _rolling_extreme(data, period, "min")


# ── Rolling Mean (arbitrary offset-based, for knot bias) ─────────────────────


def rolling_mean(data: F32Array, period: int) -> F32Array:
    """Same as sma but used on derived series (knot bias etc.)."""
    return sma(data, period)


# ── DEMA ────────────────────────────────────────────────────────────────────


def dema(data: F32Array, period: int) -> F32Array:
    """Double Exponential Moving Average: 2*EMA(data) - EMA(EMA(data))."""
    ema1 = ema(data, period)
    ema2 = ema(ema1, period)
    return (2 * ema1 - ema2).astype(data.dtype)


# ── Linear Regression Slope ─────────────────────────────────────────────────


def linear_regression(data: F32Array, length: int) -> tuple[F32Array, F32Array]:
    """
    Rolling linear regression matching Pine Script's calcSlope convention.

    Pine uses X = [2, 3, ..., length+1] over the last `length` bars.
    Returns (slope, intercept + slope * length) — the projected value
    used in the Go OBV staircase calculation.

    Returns
    -------
    slope : array of slopes
    projected : array of intercept + slope * length (the tt1 value)
    """
    n = len(data)
    slope_out = np.zeros(n, dtype=data.dtype)
    proj_out = np.zeros(n, dtype=data.dtype)

    if length < 1:
        return slope_out, proj_out

    # X values per Pine convention: i goes 1..length, per = i + 1
    x_vals = np.arange(2, length + 2, dtype=np.float64)
    sum_x = x_vals.sum()
    sum_x2 = (x_vals * x_vals).sum()
    L = float(length)

    for i in range(n):
        if i < length - 1:
            proj_out[i] = data[i]
            continue

        window = data[i - length + 1: i + 1].astype(np.float64)
        sum_y = window.sum()
        sum_xy = (window * x_vals).sum()

        denom = L * sum_x2 - sum_x * sum_x
        if denom == 0:
            proj_out[i] = data[i]
            continue

        s = (L * sum_xy - sum_x * sum_y) / denom
        avg = sum_y / L
        intercept = avg - s * sum_x / L + s

        slope_out[i] = s
        proj_out[i] = intercept + s * L

    return slope_out.astype(data.dtype), proj_out.astype(data.dtype)


# ── Diff / Day-by-Day ──────────────────────────────────────────────────────


def diff(data: F32Array) -> F32Array:
    """Day-over-day difference: out[i] = data[i] - data[i-1], out[0] = 0."""
    out = np.zeros(len(data), dtype=data.dtype)
    if len(data) > 1:
        out[1:] = data[1:] - data[:-1]
    return out


def compare_dbd(data: F32Array) -> BoolArray:
    """Day-by-day: out[i] = data[i] >= data[i-1], out[0] = False."""
    out = np.zeros(len(data), dtype=np.bool_)
    if len(data) > 1:
        out[1:] = data[1:] >= data[:-1]
    return out


# ── Rolling Count ──────────────────────────────────────────────────────────


def rolling_count_ge(flags: BoolArray, window: int, threshold: int) -> BoolArray:
    """
    For each position, count True values in the last `window` elements.
    Return True where count >= threshold.
    Equivalent to Go RangeBool(target, true, window, i, threshold).
    """
    n = len(flags)
    out = np.zeros(n, dtype=np.bool_)
    if n == 0 or window < 1:
        return out
    cumsum = np.cumsum(flags.astype(np.int32))
    cumsum = np.insert(cumsum, 0, 0)
    for i in range(n):
        start = max(0, i + 1 - window)
        count = cumsum[i + 1] - cumsum[start]
        out[i] = count >= threshold
    return out
