"""
Volume technical analysis — ported from Go CalculateVolume.

Computes volume MA, rolling extremes, volume diff, burst/sleep/flood
detection, and overall volume status classification.

Usage:
    from analysis.volume import calculate_volume
    result = calculate_volume(volume)   # numpy float32 array
    result.sma[21]                      # 21-day volume SMA
    result.volume_status                # 0–6 classification
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from numpy.typing import NDArray

from analysis.indicators import (
    sma,
    diff,
    compare_dbd,
    compare_gt,
    compare_lt,
    rolling_highest,
    rolling_lowest,
    rolling_count_ge,
)
from analysis.constants import (
    VOLUME_SMA_PERIODS,
    VOLUME_HIGH_PERIODS,
    VOLUME_LOW_PERIODS,
    VX_HIGH_PERIODS,
    VOLUME_LEVEL_THRESHOLDS,
    NO_DEAL_CONFIGS,
    VOLUME_EXTREME_GROUPS,
)

F32 = np.float32
F32Array = NDArray[np.float32]
BoolArray = NDArray[np.bool_]
U8Array = NDArray[np.uint8]


# ── Dataclasses ─────────────────────────────────────────────────────────────


@dataclass
class VolumeExtremes:
    """Max/min of grouped volume SMAs for one timeframe."""
    max_vol: F32Array
    min_vol: F32Array
    big: BoolArray     # volume > max_vol
    small: BoolArray   # volume < min_vol


@dataclass
class VolumeResult:
    """Complete volume analysis result."""
    # Basic
    sma: dict[int, F32Array]
    volume_up: BoolArray              # V[i] >= V[i-1]
    volume_level: U8Array             # 0–11

    # Volume diff (VX)
    vx: F32Array                      # day-over-day diff
    vx_high: dict[int, F32Array]      # rolling max of VX

    # Rolling high/low of volume
    high: dict[int, F32Array]
    low: dict[int, F32Array]

    # MA34 seasonal range
    ma34_high55: F32Array             # rolling max of SMA(34) over 55 days
    ma34_low55: F32Array              # rolling min of SMA(34) over 55 days
    volume_range: F32Array            # ma34_high55 - ma34_low55

    # No-deal detection
    deal_no: BoolArray                # volume == 0
    no_deal: BoolArray                # composite no-deal flag

    # Extremes per timeframe
    extremes: dict[str, VolumeExtremes]

    # Burst / Sleep / Flood / MessUp
    vx_burst: BoolArray
    v_short_burst: BoolArray
    burst: BoolArray
    sleep: BoolArray
    flood: BoolArray
    mess_up: BoolArray

    # Overall status: 0=flood, 1=big, 2=high, 3=normal, 4=low, 5=shrink, 6=sleep
    volume_status: U8Array

    # Flood reference: tier k = last k flood events aggregated
    # tier 1 = most recent flood (equivalent to old flood_high / flood_low)
    flood_high_tier: dict[int, F32Array]   # {1..5} max of last k floods' high values
    flood_low_tier: dict[int, F32Array]    # {1..5} min of last k floods' low values
    above_tier: dict[int, BoolArray]       # {1..5} close >= flood_high_tier[k]
    below_tier: dict[int, BoolArray]       # {1..5} close <  flood_low_tier[k]

    # Scoring signals for future scoring system — each independently ±15
    flood_short_score: NDArray[np.int8]    # tier 1: +15 above, -15 below, 0 neutral
    flood_medium_score: NDArray[np.int8]   # tier 2: +15 above, -15 below, 0 neutral
    flood_long_score: NDArray[np.int8]     # tier 3: +15 above, -15 below, 0 neutral


# ── Main Entry ──────────────────────────────────────────────────────────────


def calculate_volume(
    volume: F32Array,
    open_: F32Array | None = None,
    close: F32Array | None = None,
    high: F32Array | None = None,
    low: F32Array | None = None,
) -> VolumeResult:
    """
    Main entry point — equivalent to Go GetCalculateVolume.

    Parameters
    ----------
    volume : float32 numpy array of daily volume (oldest first).
    close, high, low : optional price arrays for flood reference signals.
    """
    n = len(volume)
    vol = volume.astype(F32)

    # SMA
    sma_d = {p: sma(vol, p) for p in VOLUME_SMA_PERIODS}

    # Volume diff
    vx = diff(vol)
    vx_high = {p: rolling_highest(vx, p) for p in VX_HIGH_PERIODS}

    # Volume up (day-by-day)
    volume_up = compare_dbd(vol)

    # Rolling high/low of raw volume
    high_d = {p: rolling_highest(vol, p) for p in VOLUME_HIGH_PERIODS}
    low_d = {p: rolling_lowest(vol, p) for p in VOLUME_LOW_PERIODS}

    # MA34 seasonal range
    ma34_high55 = rolling_highest(sma_d[34], 55)
    ma34_low55 = rolling_lowest(sma_d[34], 55)
    volume_range = ma34_high55 - ma34_low55

    # Volume level
    volume_level = _calc_volume_level(sma_d[8], n)

    # No-deal
    deal_no = vol == 0
    no_deal = _calc_no_deal(deal_no, n)

    # Extremes
    extremes = _calc_extremes(vol, sma_d)

    # Burst
    prev_vr = _shift1(volume_range)
    vx_burst = _calc_vx_burst(vol, vx, vx_high, volume_up, prev_vr)
    v_short_burst = _calc_v_short_burst(vol, volume_up, extremes["short"].max_vol, prev_vr)
    burst = vx_burst | v_short_burst

    # Sleep / Flood / MessUp
    prev_high = {p: _shift1(high_d[p]) for p in VOLUME_HIGH_PERIODS}
    prev_low = {p: _shift1(low_d[p]) for p in VOLUME_LOW_PERIODS}
    prev_ext = {
        label: (_shift1(ext.max_vol), _shift1(ext.min_vol))
        for label, ext in extremes.items()
    }

    sleep_flag = _calc_sleep(vol, low_d, prev_high, prev_low, prev_vr, prev_ext, extremes, burst)
    flood_flag = _calc_flood(vol, prev_high, prev_vr, prev_ext, extremes, burst)
    mess_up = _calc_mess_up(vol, high_d)

    # Volume status
    prev_vol = _shift1(vol)
    prev2_vol_idx = np.arange(n)  # for [i-2] access
    volume_status = _calc_volume_status(
        vol, n, high_d, prev_high, prev_vr, prev_ext,
        extremes, sleep_flag, flood_flag, burst,
    )

    # Flood reference signals (tiered: last 1..5 flood events)
    MAX_TIER = 5
    if open_ is not None and close is not None and high is not None and low is not None:
        flood_high_tier, flood_low_tier, above_tier, below_tier = _calc_flood_ref(
            open_.astype(F32), close.astype(F32), high.astype(F32), low.astype(F32),
            flood_flag, n, max_tier=MAX_TIER,
        )
    else:
        flood_high_tier = {k: np.zeros(n, dtype=F32) for k in range(1, MAX_TIER + 1)}
        flood_low_tier = {k: np.zeros(n, dtype=F32) for k in range(1, MAX_TIER + 1)}
        above_tier = {k: np.zeros(n, dtype=np.bool_) for k in range(1, MAX_TIER + 1)}
        below_tier = {k: np.zeros(n, dtype=np.bool_) for k in range(1, MAX_TIER + 1)}

    # Per-tier ±15 scores for scoring system
    def _score(above, below):
        s = np.zeros(n, dtype=np.int8)
        s[above] = 15
        s[below] = -15
        return s
    flood_short_score = _score(above_tier[1], below_tier[1])
    flood_medium_score = _score(above_tier[2], below_tier[2])
    flood_long_score = _score(above_tier[3], below_tier[3])

    return VolumeResult(
        sma=sma_d,
        volume_up=volume_up,
        volume_level=volume_level,
        vx=vx,
        vx_high=vx_high,
        high=high_d,
        low=low_d,
        ma34_high55=ma34_high55,
        ma34_low55=ma34_low55,
        volume_range=volume_range,
        deal_no=deal_no,
        no_deal=no_deal,
        extremes=extremes,
        vx_burst=vx_burst,
        v_short_burst=v_short_burst,
        burst=burst,
        sleep=sleep_flag,
        flood=flood_flag,
        mess_up=mess_up,
        volume_status=volume_status,
        flood_high_tier=flood_high_tier,
        flood_low_tier=flood_low_tier,
        above_tier=above_tier,
        below_tier=below_tier,
        flood_short_score=flood_short_score,
        flood_medium_score=flood_medium_score,
        flood_long_score=flood_long_score,
    )


# ── Helpers ─────────────────────────────────────────────────────────────────


def _shift1(arr: F32Array) -> F32Array:
    """Shift array by 1 (arr[i] becomes arr[i-1]). Position 0 = 0."""
    out = np.roll(arr, 1)
    out[0] = 0
    return out


def _shift2(arr: F32Array) -> F32Array:
    """Shift array by 2 (arr[i] becomes arr[i-2]). Positions 0-1 = 0."""
    out = np.roll(arr, 2)
    out[:2] = 0
    return out


# ── Volume Level ────────────────────────────────────────────────────────────


def _calc_volume_level(sma8: F32Array, n: int) -> U8Array:
    """Classify volume level 0–11 based on 8-day volume SMA."""
    out = np.full(n, len(VOLUME_LEVEL_THRESHOLDS), dtype=np.uint8)
    for i, threshold in reversed(list(enumerate(VOLUME_LEVEL_THRESHOLDS))):
        out[sma8 < threshold] = i
    return out


# ── No Deal ─────────────────────────────────────────────────────────────────


def _calc_no_deal(deal_no: BoolArray, n: int) -> BoolArray:
    """
    Composite no-deal flag: true if any of the rolling-window checks trigger.
    Configs: (21,1), (34,2), (55,3) — within N days, >= M zero-volume days.
    """
    result = np.zeros(n, dtype=np.bool_)
    for window, threshold in NO_DEAL_CONFIGS:
        result |= rolling_count_ge(deal_no, window, threshold)
    return result


# ── Extremes ────────────────────────────────────────────────────────────────


def _calc_extremes(vol: F32Array, sma_d: dict[int, F32Array]) -> dict[str, VolumeExtremes]:
    """Max/min of grouped volume SMAs; compare raw volume against them."""
    result: dict[str, VolumeExtremes] = {}
    for label, periods in VOLUME_EXTREME_GROUPS.items():
        stack = np.stack([sma_d[p] for p in periods], axis=0)
        max_vol = np.max(stack, axis=0).astype(F32)
        min_vol = np.min(stack, axis=0).astype(F32)
        result[label] = VolumeExtremes(
            max_vol=max_vol,
            min_vol=min_vol,
            big=compare_gt(vol, max_vol),
            small=compare_lt(vol, min_vol),
        )
    return result


# ── Burst ───────────────────────────────────────────────────────────────────


def _calc_vx_burst(
    vol: F32Array, vx: F32Array, vx_high: dict[int, F32Array],
    volume_up: BoolArray, prev_vr: F32Array,
) -> BoolArray:
    """
    VX burst: large volume diff relative to historical max diffs.
    volume_up AND vx > volume_range[i-1]*0.3
      AND vx == vx_high[3]
      AND (vx >= vx_high[5]*0.8 OR vx >= vx_high[8]*0.6 OR vx >= vx_high[13]*0.4)
      AND vx >= vx_high[21]*0.3
      AND vx >= vx_high[34]*0.2
      AND vx >= vx_high[55]*0.1
    """
    return (
        volume_up
        & (vx > prev_vr * 0.3)
        & (vx == vx_high[3])
        & ((vx >= vx_high[5] * 0.8) | (vx >= vx_high[8] * 0.6) | (vx >= vx_high[13] * 0.4))
        & (vx >= vx_high[21] * 0.3)
        & (vx >= vx_high[34] * 0.2)
        & (vx >= vx_high[55] * 0.1)
    )


def _calc_v_short_burst(
    vol: F32Array, volume_up: BoolArray,
    short_max: F32Array, prev_vr: F32Array,
) -> BoolArray:
    """V short burst: volume_up AND volume > (short_max + volume_range[i-1]*0.7)."""
    return volume_up & (vol > (short_max + prev_vr * 0.7))


# ── Sleep ───────────────────────────────────────────────────────────────────


def _calc_sleep(
    vol: F32Array,
    low_d: dict[int, F32Array],
    prev_high: dict[int, F32Array],
    prev_low: dict[int, F32Array],
    prev_vr: F32Array,
    prev_ext: dict[str, tuple[F32Array, F32Array]],
    extremes: dict[str, VolumeExtremes],
    burst: BoolArray,
) -> BoolArray:
    """Sleep (息): extremely low volume relative to historical context."""
    prev_long_max, prev_long_min = prev_ext["long"]
    prev_burst = _shift1(burst.astype(F32)).astype(np.bool_)
    prev_big_long = _shift1(extremes["long"].big.astype(F32)).astype(np.bool_)
    prev_vol = _shift1(vol)

    all_small = (
        extremes["long"].small
        & extremes["medium"].small
        & extremes["short"].small
    )

    # Main condition block
    cond_a = (
        ((vol < prev_high[5] / 2) & (vol == low_d[5]))
        | (vol < prev_low[8])
    )
    cond_b = vol < (prev_low[13] + prev_vr * 2)
    cond_c = (
        (vol < (prev_long_max - prev_vr))
        | (vol < (prev_long_min - prev_vr * 0.5))
        | (vol < prev_low[13])
        | (vol < prev_high[8] * 0.2)
    )
    cond_main = cond_a & cond_b & cond_c

    # Alternative: post-burst collapse
    cond_alt = (vol < prev_vol / 3) & prev_big_long & prev_burst

    return (
        all_small
        & (vol < (prev_long_max - prev_vr * 0.5))
        & (cond_main | cond_alt)
    )


# ── Flood ───────────────────────────────────────────────────────────────────


def _calc_flood(
    vol: F32Array,
    prev_high: dict[int, F32Array],
    prev_vr: F32Array,
    prev_ext: dict[str, tuple[F32Array, F32Array]],
    extremes: dict[str, VolumeExtremes],
    burst: BoolArray,
) -> BoolArray:
    """Flood (洪): extremely high volume relative to historical context."""
    prev_long_max, _ = prev_ext["long"]

    all_big = (
        extremes["long"].big
        & extremes["medium"].big
        & extremes["short"].big
    )

    # Short-term top
    cond_short_top = (
        (vol > prev_high[3] * 0.9)
        & (vol > prev_high[5] * 0.85)
        & (vol > prev_high[8] * 0.8)
    )
    cond_long_top = (
        (vol > prev_high[21] * 0.9)
        | (vol > prev_high[34] * 0.8)
        | (vol > prev_high[55] * 0.7)
    )

    # Sustained high
    cond_sustained = (
        (vol > prev_high[13] * 0.7)
        & (vol > prev_high[21] * 0.6)
        & (vol > prev_high[34] * 0.5)
        & (vol > prev_high[55] * 0.4)
    )

    # Burst or exceeds 34-day high
    cond_burst_or_34 = burst | (vol > prev_high[34])

    # Range breakout
    cond_range = (
        (vol > prev_long_max + prev_vr * 0.8)
        | ((vol > prev_long_max + prev_vr * 0.7) & (vol > prev_high[8]))
        | ((vol > prev_long_max + prev_vr * 0.6) & (vol > prev_high[13]))
    )

    return (
        all_big
        & (cond_short_top & cond_long_top)
        & cond_sustained
        & cond_burst_or_34
        & cond_range
    )


# ── MessUp ──────────────────────────────────────────────────────────────────


def _calc_mess_up(vol: F32Array, high_d: dict[int, F32Array]) -> BoolArray:
    """MessUp (打混): volume distribution is extremely uneven."""
    h2, h3, h5, h8 = high_d[2], high_d[3], high_d[5], high_d[8]
    return (
        (h2 < h3 * 0.3) | (vol < h3 * 0.2)
        | (h3 < h5 * 0.3) | (h2 < h5 * 0.2)
        | (h5 < h8 * 0.3) | (h3 < h8 * 0.2)
    )


# ── Volume Status ───────────────────────────────────────────────────────────


def _calc_volume_status(
    vol: F32Array, n: int,
    high_d: dict[int, F32Array],
    prev_high: dict[int, F32Array],
    prev_vr: F32Array,
    prev_ext: dict[str, tuple[F32Array, F32Array]],
    extremes: dict[str, VolumeExtremes],
    sleep_flag: BoolArray,
    flood_flag: BoolArray,
    burst: BoolArray,
) -> U8Array:
    """
    Classify volume status:
      0=flood(洪), 1=big(大量), 2=high(量多),
      3=normal(正常), 4=low(量少), 5=shrink(量縮), 6=sleep(窒息)
    """
    out = np.full(n, 3, dtype=np.uint8)  # default normal

    prev_long_max, prev_long_min = prev_ext["long"]
    prev_low55 = _shift1(extremes["long"].small.astype(F32))  # not used directly

    all_big = extremes["long"].big & extremes["medium"].big & extremes["short"].big
    all_small = extremes["long"].small & extremes["medium"].small & extremes["short"].small

    prev_short_big1 = _shift1(extremes["short"].big.astype(F32)).astype(np.bool_)
    prev_short_big2 = _shift2(extremes["short"].big.astype(F32)).astype(np.bool_)
    prev_short_small1 = _shift1(extremes["short"].small.astype(F32)).astype(np.bool_)
    prev_short_small2 = _shift2(extremes["short"].small.astype(F32)).astype(np.bool_)

    # Status 6: sleep
    out[sleep_flag] = 6

    # Status 0: flood (overwrites sleep if both true — Go checks sleep first)
    out[flood_flag] = 0

    # Status 1: big volume — all_big + sustained high + recent confirmation
    cond_big_sustained = (
        (vol > high_d[55] * 0.4)
        & (vol > high_d[34] * 0.5)
        & (vol > high_d[21] * 0.6)
        & (vol > high_d[13] * 0.7)
        & (vol > high_d[8] * 0.8)
        & (vol > high_d[5] * 0.9)
    )
    cond_big_confirm = (
        prev_short_big1
        | prev_short_big2
        | (vol > (prev_long_min + prev_vr * 0.5))
    )
    is_big = all_big & cond_big_sustained & cond_big_confirm & ~sleep_flag & ~flood_flag
    out[is_big] = 1

    # Status 5: shrink — all_small + recent confirmation
    cond_shrink_confirm = (
        prev_short_small1
        | prev_short_small2
        | (vol < (prev_long_max - prev_vr * 0.5))
        | (
            (vol < (prev_long_max - prev_vr * 0.3))
            & (vol < _shift1(extremes["long"].min_vol) * 1.3)
        )
    )
    is_shrink = (
        all_small & cond_shrink_confirm
        & ~sleep_flag & ~flood_flag & ~is_big
    )
    out[is_shrink] = 5

    # Status 2: high volume — all_big but didn't qualify for big/flood
    is_high = all_big & ~flood_flag & ~is_big & ~sleep_flag
    out[is_high] = 2

    # Status 4: low volume — all_small but didn't qualify for shrink/sleep
    is_low = all_small & ~sleep_flag & ~is_shrink
    out[is_low] = 4

    return out


# ── Flood Reference ────────────────────────────────────────────────────────


def _calc_flood_ref(
    open_: F32Array, close: F32Array, high: F32Array, low: F32Array,
    flood_flag: BoolArray, n: int,
    max_tier: int = 5,
) -> tuple[dict[int, F32Array], dict[int, F32Array],
           dict[int, BoolArray], dict[int, BoolArray]]:
    """
    Track price references for the last `max_tier` flood events.

    Gap-aware latching (same as before):
      - high_value: prev_bottom if gap_down else flood day's high
      - low_value:  prev_top    if gap_up   else flood day's low

    For tier k (1..max_tier):
      - flood_high_tier[k] = max of the last k latched high values
      - flood_low_tier[k]  = min of the last k latched low values
      - above_tier[k] requires at least k floods seen AND close >= flood_high_tier[k]
      - below_tier[k] requires at least k floods seen AND close <  flood_low_tier[k]
      - Signals are suppressed on flood days (reference just updated)
    """
    candle_top = np.maximum(open_, close)
    candle_bottom = np.minimum(open_, close)

    flood_high_tier = {k: np.zeros(n, dtype=F32) for k in range(1, max_tier + 1)}
    flood_low_tier = {k: np.zeros(n, dtype=F32) for k in range(1, max_tier + 1)}

    # Deque of (high_value, low_value) for last max_tier floods, newest first
    recent: list[tuple[float, float]] = []
    # Count of floods seen so far (inclusive of today)
    flood_count = 0
    flood_count_before = np.zeros(n, dtype=np.int32)

    for i in range(n):
        flood_count_before[i] = flood_count  # count before today's processing

        if flood_flag[i] and i > 0:
            prev_top = candle_top[i - 1]
            prev_bottom = candle_bottom[i - 1]
            gap_up = candle_bottom[i] > prev_top
            gap_down = candle_top[i] < prev_bottom

            hv = float(prev_bottom) if gap_down else float(high[i])
            lv = float(prev_top) if gap_up else float(low[i])

            recent.insert(0, (hv, lv))
            if len(recent) > max_tier:
                recent.pop()
            flood_count += 1

        # Write tier values based on current deque state
        if recent:
            for k in range(1, max_tier + 1):
                avail = min(k, len(recent))
                flood_high_tier[k][i] = max(r[0] for r in recent[:avail])
                flood_low_tier[k][i] = min(r[1] for r in recent[:avail])

    above_tier = {k: np.zeros(n, dtype=np.bool_) for k in range(1, max_tier + 1)}
    below_tier = {k: np.zeros(n, dtype=np.bool_) for k in range(1, max_tier + 1)}

    if flood_count == 0:
        return flood_high_tier, flood_low_tier, above_tier, below_tier

    not_flood_day = ~flood_flag
    for k in range(1, max_tier + 1):
        # Need at least k floods observed strictly before today, and today not a flood day
        active_k = not_flood_day & (flood_count_before >= k)
        above_tier[k] = active_k & (close >= flood_high_tier[k])
        below_tier[k] = active_k & (close < flood_low_tier[k])

    return flood_high_tier, flood_low_tier, above_tier, below_tier
