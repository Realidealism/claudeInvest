"""
Wave (波浪) technical analysis — ported from Go CalculateWave.

Stateful wave detection: identifies alternating up/down waves based on
convex/concave conditions, then classifies each wave as waterfall (浪瀑)
or ditch (浪溝) based on relative length.

Depends on CandleResult and close BSResult.

Usage:
    from analysis.wave import calculate_wave
    result = calculate_wave(open_, high, low, close, candle, close_bs)
    result.direction        # current wave direction per day
    result.red_waterfall0   # bullish waterfall on current wave
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

import numpy as np
from numpy.typing import NDArray

from analysis.candle import CandleResult
from analysis.close import BSResult

F32 = np.float32
F32Array = NDArray[np.float32]
BoolArray = NDArray[np.bool_]
U8Array = NDArray[np.uint8]


# ── Wave list (growing per detected wave) ───────────────────────────────────


@dataclass
class WaveList:
    """Dynamic list of detected waves."""
    wave: List[bool] = field(default_factory=list)       # True=UP, False=DOWN
    tip: List[float] = field(default_factory=list)
    top: List[float] = field(default_factory=list)       # candle top
    bottom: List[float] = field(default_factory=list)    # candle bottom
    day_idx: List[int] = field(default_factory=list)     # day index of wave pivot
    day: List[int] = field(default_factory=list)         # days since wave started
    length: List[float] = field(default_factory=list)    # abs tip difference
    up_price: List[float] = field(default_factory=list)
    mid_price: List[float] = field(default_factory=list)
    down_price: List[float] = field(default_factory=list)
    waterfall: List[bool] = field(default_factory=list)
    water_ditch: List[bool] = field(default_factory=list)
    avg_volume: List[float] = field(default_factory=list)
    _count: int = 0

    def count(self) -> int:
        return self._count

    def append_wave(
        self, is_up: bool, tip: float, hi: float, lo: float,
        top: float, bottom: float, day_idx: int,
    ):
        """Append a new wave entry and compute length/prices from previous tip."""
        self._count += 1
        self.wave.append(is_up)
        self.tip.append(tip)
        self.top.append(top)
        self.bottom.append(bottom)
        self.day_idx.append(day_idx)
        self.day.append(0)

        wc = self.count()
        if wc >= 2:
            prev_tip = self.tip[wc - 2]
            if is_up:
                wl = tip - prev_tip
            else:
                wl = prev_tip - tip
        else:
            wl = 0.0
        self.length.append(wl)

        if wc >= 2:
            prev_tip = self.tip[wc - 2]
            self.up_price.append(max(tip, prev_tip) - abs(wl) * 0.382)
            self.mid_price.append((tip + prev_tip) / 2)
            self.down_price.append(min(tip, prev_tip) + abs(wl) * 0.382)
        else:
            self.up_price.append(hi)
            self.mid_price.append((hi + lo) / 2)
            self.down_price.append(lo)

        self.waterfall.append(False)
        self.water_ditch.append(False)
        self.avg_volume.append(0.0)

    def finalize_wave_volume(self, wave_idx: int, volume: F32Array):
        """Compute volume stats for a completed wave using day_idx boundaries."""
        start = self.day_idx[wave_idx]
        end = self.day_idx[wave_idx + 1] if wave_idx + 1 < self._count else start
        if end <= start:
            return
        self.avg_volume[wave_idx] = float(np.mean(volume[start:end]))

    def update_current(
        self, tip: float, hi: float, lo: float,
        top: float, bottom: float, day_idx: int,
    ):
        """Update the current (last) wave's tip and recompute length/prices."""
        i = self.count() - 1
        self.tip[i] = tip
        self.top[i] = top
        self.bottom[i] = bottom
        self.day_idx[i] = day_idx

        if i >= 1:
            prev_tip = self.tip[i - 1]
            if self.wave[i]:  # UP
                wl = tip - prev_tip
            else:
                wl = prev_tip - tip
            self.length[i] = wl
            self.up_price[i] = max(tip, prev_tip) - abs(wl) * 0.382
            self.mid_price[i] = (tip + prev_tip) / 2
            self.down_price[i] = min(tip, prev_tip) + abs(wl) * 0.382

    def check_waterfall_ditch(self, idx: int):
        """Classify the wave at `idx` as waterfall or ditch."""
        if idx < 11:
            return
        wl = self.length[idx]

        w12 = self.length[max(0, idx - 11):idx + 1]
        w10 = self.length[max(0, idx - 9):idx + 1]
        w8 = self.length[max(0, idx - 7):idx + 1]
        w6 = self.length[max(0, idx - 5):idx + 1]
        w4 = self.length[max(0, idx - 3):idx + 1]

        wld4b = max(w4)
        wld6b = max(w6)
        wld8b = max(w8)
        wld10b = max(w10)
        wld12s = min(w12)
        wld12ma = sum(w12) / len(w12)

        # Waterfall
        self.waterfall[idx] = (
            (wl == wld4b or wl > wld6b * 0.9 or wl > wld8b * 0.8
             or wl > wld12ma * 2.1 or wl > wld10b * 0.7)
            and wl > wld12ma * 1.3
            and wl > wld6b * 0.5
            and wl > wld8b * 0.3
            and wl > wld10b * 0.2
            and (wl > wld12s * 3 or (wl > wld12s * 2 and wl == wld4b))
        )

        # Ditch
        self.water_ditch[idx] = (
            wl < wld12ma * 0.8
            and wl < wld10b * 0.5
            and wl < wld12s * 5
        )


# ── Wave trend ─────────────────────────────────────────────────────────────


@dataclass
class WaveTrend:
    """Wave-based trend scores at different lookback depths.

    Each score ranges from -1.0 (strong downtrend) to +1.0 (strong uptrend),
    weighted by percentage wave length so large waves dominate small ones.
    """
    short: F32Array     # last 4 waves
    medium: F32Array    # last 8 waves
    long: F32Array      # last 12 waves
    composite: F32Array # weighted average of short/medium/long


# ── Day-indexed result ──────────────────────────────────────────────────────


@dataclass
class WaveResult:
    """Complete wave analysis result (day-indexed arrays)."""
    waves: WaveList             # the raw wave list

    # Conditions (day-indexed)
    convex_i: BoolArray
    convex_ii: BoolArray
    convex_iii: BoolArray
    convex_iv: BoolArray
    concave_i: BoolArray
    concave_ii: BoolArray
    concave_iii: BoolArray
    concave_iv: BoolArray

    direction: BoolArray        # True=UP, False=DOWN
    tip0: F32Array
    tip1: F32Array
    tip2: F32Array

    up_price0: F32Array
    mid_price0: F32Array
    down_price0: F32Array
    up_price1: F32Array
    mid_price1: F32Array
    down_price1: F32Array

    close_cross_wave_d2ma: BoolArray

    red_waterfall0: BoolArray
    black_waterfall0: BoolArray
    red_waterfall1: BoolArray
    black_waterfall1: BoolArray

    red_sizable_wave1: BoolArray
    black_sizable_wave1: BoolArray

    black_wf_top_tip: F32Array
    black_wf_bottom_tip: F32Array
    red_wf_top_tip: F32Array
    red_wf_bottom_tip: F32Array
    black_wf_up_price: F32Array
    red_wf_down_price: F32Array

    close_big_black_wf_up: BoolArray
    close_small_red_wf_down: BoolArray
    close_break_black_wf_up: BoolArray
    close_break_red_wf_down: BoolArray

    sink: BoolArray

    # Breakthrough signals
    tip_breakout_up: BoolArray      # higher high: current UP tip > previous UP tip
    tip_breakout_down: BoolArray    # lower low: current DN tip < previous DN tip
    sink_reversal: BoolArray        # first day exiting sink state
    length_expansion: BoolArray     # wave length breaks declining trend
    wave_volume_up: BoolArray       # current wave avg_vol > prev same-direction wave

    # Trend
    wave_trend: WaveTrend           # multi-period wave trend scores


# ── Main Entry ──────────────────────────────────────────────────────────────


def calculate_wave(
    open_: F32Array, high: F32Array, low: F32Array, close: F32Array,
    candle: CandleResult, close_bs: BSResult,
    volume: F32Array | None = None,
) -> WaveResult:
    """
    Main entry point — equivalent to Go GetCalculateWave.

    Parameters
    ----------
    open_, high, low, close : OHLC float32 arrays (oldest first)
    candle : CandleResult from calculate_candle
    close_bs : BSResult from calculate_close (the bs field)
    volume : daily volume array (optional, for wave volume stats)
    """
    n = len(close)
    W = WaveList()

    # Allocate day-indexed arrays
    convex_i = np.zeros(n, dtype=np.bool_)
    convex_ii = np.zeros(n, dtype=np.bool_)
    convex_iii = np.zeros(n, dtype=np.bool_)
    convex_iv = np.zeros(n, dtype=np.bool_)
    concave_i = np.zeros(n, dtype=np.bool_)
    concave_ii = np.zeros(n, dtype=np.bool_)
    concave_iii = np.zeros(n, dtype=np.bool_)
    concave_iv = np.zeros(n, dtype=np.bool_)

    direction = np.zeros(n, dtype=np.bool_)
    tip0 = np.zeros(n, dtype=F32)
    tip1 = np.zeros(n, dtype=F32)
    tip2 = np.zeros(n, dtype=F32)

    up_price0 = np.zeros(n, dtype=F32)
    mid_price0 = np.zeros(n, dtype=F32)
    down_price0 = np.zeros(n, dtype=F32)
    up_price1 = np.zeros(n, dtype=F32)
    mid_price1 = np.zeros(n, dtype=F32)
    down_price1 = np.zeros(n, dtype=F32)

    close_cross_d2ma = np.zeros(n, dtype=np.bool_)

    red_wf0 = np.zeros(n, dtype=np.bool_)
    black_wf0 = np.zeros(n, dtype=np.bool_)
    red_wf1 = np.zeros(n, dtype=np.bool_)
    black_wf1 = np.zeros(n, dtype=np.bool_)
    red_wd1 = np.zeros(n, dtype=np.bool_)
    black_wd1 = np.zeros(n, dtype=np.bool_)

    bwf_top = np.zeros(n, dtype=F32)
    bwf_bot = np.zeros(n, dtype=F32)
    rwf_top = np.zeros(n, dtype=F32)
    rwf_bot = np.zeros(n, dtype=F32)
    bwf_up = np.zeros(n, dtype=F32)
    rwf_down = np.zeros(n, dtype=F32)

    close_big_bwf = np.zeros(n, dtype=np.bool_)
    close_small_rwf = np.zeros(n, dtype=np.bool_)
    close_break_bwf = np.zeros(n, dtype=np.bool_)
    close_break_rwf = np.zeros(n, dtype=np.bool_)

    sink = np.zeros(n, dtype=np.bool_)

    tip_breakout_up = np.zeros(n, dtype=np.bool_)
    tip_breakout_down = np.zeros(n, dtype=np.bool_)
    sink_reversal = np.zeros(n, dtype=np.bool_)
    length_expansion = np.zeros(n, dtype=np.bool_)
    wave_volume_up = np.zeros(n, dtype=np.bool_)

    if n == 0:
        empty_trend = WaveTrend(
            short=np.zeros(0, dtype=F32), medium=np.zeros(0, dtype=F32),
            long=np.zeros(0, dtype=F32), composite=np.zeros(0, dtype=F32),
        )
        return _build_result(
            waves=W,
            convex_i=convex_i, convex_ii=convex_ii,
            convex_iii=convex_iii, convex_iv=convex_iv,
            concave_i=concave_i, concave_ii=concave_ii,
            concave_iii=concave_iii, concave_iv=concave_iv,
            direction=direction, tip0=tip0, tip1=tip1, tip2=tip2,
            up_price0=up_price0, mid_price0=mid_price0, down_price0=down_price0,
            up_price1=up_price1, mid_price1=mid_price1, down_price1=down_price1,
            close_cross_d2ma=close_cross_d2ma,
            red_wf0=red_wf0, black_wf0=black_wf0,
            red_wf1=red_wf1, black_wf1=black_wf1,
            red_wd1=red_wd1, black_wd1=black_wd1,
            bwf_top=bwf_top, bwf_bot=bwf_bot,
            rwf_top=rwf_top, rwf_bot=rwf_bot,
            bwf_up=bwf_up, rwf_down=rwf_down,
            close_big_bwf=close_big_bwf, close_small_rwf=close_small_rwf,
            close_break_bwf=close_break_bwf, close_break_rwf=close_break_rwf,
            sink=sink,
            tip_breakout_up=tip_breakout_up, tip_breakout_down=tip_breakout_down,
            sink_reversal=sink_reversal, length_expansion=length_expansion,
            wave_volume_up=wave_volume_up,
            wave_trend=empty_trend,
        )

    # Shorthand accessors for candle data
    c_top = candle.candle.top
    c_bot = candle.candle.bottom
    hd2b = candle.high_rolling.high[2]
    hd3b = candle.high_rolling.high[3]
    hd2s = candle.high_rolling.low[2]
    hd3s = candle.high_rolling.low[3]
    ld2s = candle.low_rolling.low[2]
    ld3s = candle.low_rolling.low[3]
    ld2b = candle.low_rolling.high[2]
    ld3b = candle.low_rolling.high[3]
    td2b = candle.top_rolling.values[2]
    bd2s = candle.bottom_rolling.values[2]
    trig_h1 = candle.trigger_high1
    trig_h3 = candle.trigger_high3
    trig_l1 = candle.trigger_low1
    trig_l3 = candle.trigger_low3
    cd2b = close_bs.high[2]
    cd2s = close_bs.low[2]

    # ── Initialize first wave pair ──────────────────────────────────────
    if close[0] >= (high[0] + low[0]) / 2:  # bullish first bar
        W.append_wave(False, float(low[0]), float(high[0]), float(low[0]),
                       float(c_top[0]), float(c_bot[0]), 0)
        W.append_wave(True, float(high[0]), float(high[0]), float(low[0]),
                       float(c_top[0]), float(c_bot[0]), 0)
        tip0[0] = float(low[0])
        tip1[0] = float(high[0])
    else:
        W.append_wave(True, float(high[0]), float(high[0]), float(low[0]),
                       float(c_top[0]), float(c_bot[0]), 0)
        W.append_wave(False, float(low[0]), float(high[0]), float(low[0]),
                       float(c_top[0]), float(c_bot[0]), 0)
        tip0[0] = float(high[0])
        tip1[0] = float(low[0])

    # Populate remaining Day 0 arrays from initialized waves
    wc0 = W.count()
    direction[0] = W.wave[wc0 - 1]
    up_price0[0] = W.up_price[wc0 - 1]
    mid_price0[0] = W.mid_price[wc0 - 1]
    down_price0[0] = W.down_price[wc0 - 1]
    up_price1[0] = W.up_price[wc0 - 2]
    mid_price1[0] = W.mid_price[wc0 - 2]
    down_price1[0] = W.down_price[wc0 - 2]
    close_cross_d2ma[0] = close[0] > (tip0[0] + tip1[0]) / 2

    # Pre-compute volume flag for finalization
    _has_vol = volume is not None

    # ── Main loop ───────────────────────────────────────────────────────
    for i in range(1, n):
        wc = W.count()

        # Convex / Concave conditions
        last_day_idx = W.day_idx[wc - 1]

        if i > 2:
            convex_i[i] = (high[i - 3] > hd3b[i]) and (i - 3 > last_day_idx)
            concave_i[i] = (low[i - 3] < ld3s[i]) and (i - 3 > last_day_idx)

        if i > 1:
            cur_tip = W.tip[wc - 1]
            prev_tip = W.tip[wc - 2] if wc > 1 else cur_tip

            convex_ii[i] = (
                (high[i - 2] > hd2b[i])
                and (high[i - 2] > high[i - 3])
                and not (high[i - 2] < td2b[i - 3]
                         and (high[i - 2] - cur_tip) < (prev_tip - cur_tip) * 0.5)
                and (i - 2 > last_day_idx)
            )
            concave_ii[i] = (
                (low[i - 2] < ld2s[i])
                and (low[i - 2] < low[i - 3])
                and not (low[i - 2] > bd2s[i - 3]
                         and (cur_tip - low[i - 2]) < (cur_tip - prev_tip) * 0.5)
                and (i - 2 > last_day_idx)
            )

            convex_iii[i] = (
                (high[i - 1] > high[i])
                and (high[i - 1] > high[i - 2])
                and not (high[i - 1] < td2b[i - 2]
                         and (high[i - 1] - cur_tip) < (prev_tip - cur_tip) * 0.5)
                and (i - 1 > last_day_idx)
            )
            concave_iii[i] = (
                (low[i - 1] < low[i])
                and (low[i - 1] < low[i - 2])
                and not (low[i - 1] > bd2s[i - 2]
                         and (cur_tip - low[i - 1]) < (cur_tip - prev_tip) * 0.5)
                and (i - 1 > last_day_idx)
            )

            convex_iv[i] = (
                ((high[i] > hd2b[i - 1] and close[i] > cd2b[i - 1])
                 or (trig_h1[i] and close[i] > W.top[wc - 1]
                     and not (hd2b[i] < ld2s[i - 2])))
                and not (low[i] == W.tip[wc - 1] and high[i] < c_top[i - 2])
            )
            concave_iv[i] = (
                ((low[i] < ld2s[i - 1] and close[i] < cd2s[i - 1])
                 or (trig_l1[i] and close[i] < W.bottom[wc - 1]
                     and not (ld2s[i] > hd2b[i - 2])))
                and not (high[i] == W.tip[wc - 1] and low[i] > c_bot[i - 2])
            )

        # ── Wave update logic ───────────────────────────────────────────
        W.day[wc - 1] += 1

        any_concave = concave_i[i] or concave_ii[i] or concave_iii[i]
        any_convex = convex_i[i] or convex_ii[i] or convex_iii[i]

        if W.wave[wc - 1]:  # Current wave is UP
            _process_up_wave(
                W, i, high, low, close, c_top, c_bot,
                trig_h1, trig_h3, trig_l3,
                concave_i, concave_ii, concave_iii, concave_iv,
                convex_iv, any_concave, any_convex,
                tip0,
            )
        else:  # Current wave is DOWN
            _process_down_wave(
                W, i, high, low, close, c_top, c_bot,
                trig_l1, trig_l3, trig_h3,
                convex_i, convex_ii, convex_iii, convex_iv,
                concave_iv, any_convex, any_concave,
                tip0,
            )

        # ── Finalize volume stats for completed waves ─────────────────
        new_wc = W.count()
        if _has_vol and new_wc > wc:
            for wi in range(wc - 1, new_wc - 1):
                W.finalize_wave_volume(wi, volume)

        # ── Map wave state to day arrays ────────────────────────────────
        wc = new_wc
        direction[i] = W.wave[wc - 1]
        tip0[i] = W.tip[wc - 1]
        up_price0[i] = W.up_price[wc - 1]
        mid_price0[i] = W.mid_price[wc - 1]
        down_price0[i] = W.down_price[wc - 1]

        if wc > 1:
            tip1[i] = W.tip[wc - 2]
            up_price1[i] = W.up_price[wc - 2]
            mid_price1[i] = W.mid_price[wc - 2]
            down_price1[i] = W.down_price[wc - 2]
            d2ma = (W.tip[wc - 1] + W.tip[wc - 2]) / 2
            close_cross_d2ma[i] = close[i] > d2ma

        if wc > 2:
            tip2[i] = W.tip[wc - 3]

        if wc > 3:
            sink[i] = (W.tip[wc - 1] < W.tip[wc - 3]
                        and W.tip[wc - 2] <= W.tip[wc - 4])

            # Tip breakout: current wave tip exceeds same-direction tip 2 waves ago
            if W.wave[wc - 1]:  # current is UP
                tip_breakout_up[i] = (
                    W.tip[wc - 1] > W.tip[wc - 3]
                    and not tip_breakout_up[i - 1]
                )
            else:  # current is DOWN
                tip_breakout_down[i] = (
                    W.tip[wc - 1] < W.tip[wc - 3]
                    and not tip_breakout_down[i - 1]
                )

            # Sink reversal: transition from sink to non-sink
            sink_reversal[i] = sink[i - 1] and not sink[i]

            # Length expansion: current wave length breaks out of a
            # contracting pattern (prev 3 waves shrinking, or current
            # wave > 1.5x the average of previous 3 waves)
            if wc > 4:
                l1 = W.length[wc - 2]
                l2 = W.length[wc - 3]
                l3 = W.length[wc - 4]
                avg3 = (l1 + l2 + l3) / 3
                cur_len = W.length[wc - 1]
                contracting = l1 < l2 and l2 < l3
                length_expansion[i] = (
                    cur_len > avg3 * 1.5
                    and (contracting or cur_len > max(l1, l2, l3))
                    and W.day[wc - 2] == 0
                )

        # Wave volume up: last completed wave avg_vol > prev same-direction wave
        # wc-2 is the most recently finalized wave; wc-4 is same direction 2 waves back
        if _has_vol and wc > 3:
            prev_avg = W.avg_volume[wc - 2]
            if wc > 4 and W.wave[wc - 4] == W.wave[wc - 2]:
                wave_volume_up[i] = prev_avg > W.avg_volume[wc - 4] and prev_avg > 0
            elif wc > 5 and W.wave[wc - 5] == W.wave[wc - 2]:
                wave_volume_up[i] = prev_avg > W.avg_volume[wc - 5] and prev_avg > 0

        if wc > 11:
            # Waterfall / Ditch flags
            red_wf0[i] = W.waterfall[wc - 1] and W.wave[wc - 1]
            black_wf0[i] = W.waterfall[wc - 1] and not W.wave[wc - 1]
            red_wf1[i] = (W.waterfall[wc - 2] and W.day[wc - 2] == 0
                          and W.wave[wc - 2])
            black_wf1[i] = (W.waterfall[wc - 2] and W.day[wc - 2] == 0
                            and not W.wave[wc - 2])
            red_wd1[i] = (not W.water_ditch[wc - 2] and W.day[wc - 2] == 0
                          and W.wave[wc - 2])
            black_wd1[i] = (not W.water_ditch[wc - 2] and W.day[wc - 2] == 0
                            and not W.wave[wc - 2])

            # Black waterfall tips
            if black_wf0[i]:
                bwf_top[i] = W.tip[wc - 2]
                bwf_bot[i] = W.tip[wc - 1]
            elif black_wf1[i]:
                bwf_top[i] = W.tip[wc - 3]
                bwf_bot[i] = W.tip[wc - 2]
            else:
                bwf_top[i] = bwf_top[i - 1]
                bwf_bot[i] = (W.tip[wc - 1]
                              if bwf_bot[i - 1] > W.tip[wc - 1]
                              else bwf_bot[i - 1])

            # Red waterfall tips
            if red_wf0[i]:
                rwf_bot[i] = W.tip[wc - 2]
                rwf_top[i] = W.tip[wc - 1]
            elif red_wf1[i]:
                rwf_bot[i] = W.tip[wc - 3]
                rwf_top[i] = W.tip[wc - 2]
            else:
                rwf_bot[i] = rwf_bot[i - 1]
                rwf_top[i] = (W.tip[wc - 1]
                              if rwf_top[i - 1] < W.tip[wc - 1]
                              else rwf_top[i - 1])

            retrace = (bwf_top[i] - bwf_bot[i]) * 0.382
            bwf_up[i] = bwf_top[i] - retrace
            rwf_down[i] = rwf_bot[i] + retrace

            close_big_bwf[i] = close[i] > bwf_up[i]
            close_small_rwf[i] = close[i] < rwf_down[i]
            close_break_bwf[i] = close_big_bwf[i] and not close_big_bwf[i - 1]
            close_break_rwf[i] = close_small_rwf[i] and not close_small_rwf[i - 1]

    # Compute wave trend scores
    wave_trend = _calc_wave_trend(W, n)

    # Finalize the last (still-open) wave's volume stats
    if _has_vol and W.count() > 0:
        last_wi = W.count() - 1
        start = W.day_idx[last_wi]
        vol_slice = volume[start:n]
        if len(vol_slice) > 0:
            W.avg_volume[last_wi] = float(np.mean(vol_slice))

    return _build_result(
        waves=W,
        convex_i=convex_i, convex_ii=convex_ii,
        convex_iii=convex_iii, convex_iv=convex_iv,
        concave_i=concave_i, concave_ii=concave_ii,
        concave_iii=concave_iii, concave_iv=concave_iv,
        direction=direction, tip0=tip0, tip1=tip1, tip2=tip2,
        up_price0=up_price0, mid_price0=mid_price0, down_price0=down_price0,
        up_price1=up_price1, mid_price1=mid_price1, down_price1=down_price1,
        close_cross_d2ma=close_cross_d2ma,
        red_wf0=red_wf0, black_wf0=black_wf0,
        red_wf1=red_wf1, black_wf1=black_wf1,
        red_wd1=red_wd1, black_wd1=black_wd1,
        bwf_top=bwf_top, bwf_bot=bwf_bot,
        rwf_top=rwf_top, rwf_bot=rwf_bot,
        bwf_up=bwf_up, rwf_down=rwf_down,
        close_big_bwf=close_big_bwf, close_small_rwf=close_small_rwf,
        close_break_bwf=close_break_bwf, close_break_rwf=close_break_rwf,
        sink=sink,
        tip_breakout_up=tip_breakout_up, tip_breakout_down=tip_breakout_down,
        sink_reversal=sink_reversal, length_expansion=length_expansion,
        wave_volume_up=wave_volume_up,
        wave_trend=wave_trend,
    )


# ── UP wave processing ──────────────────────────────────────────────────────


def _process_up_wave(
    W, i, high, low, close, c_top, c_bot,
    trig_h1, trig_h3, trig_l3,
    concave_i, concave_ii, concave_iii, concave_iv,
    convex_iv, any_concave, any_convex,
    tip0,
):
    wc = W.count()

    # Try to extend current UP wave's tip higher
    extended = False
    if (high[i] > W.tip[wc - 1] and trig_h1[i]
            and not any_concave
            and not (trig_l3[i] and close[i] > c_top[i - 1])):
        W.update_current(float(high[i]), float(high[i]), float(low[i]),
                         float(c_top[i]), float(c_bot[i]), i)
        W.check_waterfall_ditch(wc - 1)
        extended = True
    elif (i > 0 and high[i - 1] > W.tip[wc - 1]
          and not (concave_i[i] or concave_ii[i])):
        W.update_current(float(high[i - 1]), float(high[i - 1]), float(low[i - 1]),
                         float(c_top[i - 1]), float(c_bot[i - 1]), i - 1)
        W.check_waterfall_ditch(wc - 1)
        extended = True

    # Check for new DOWN wave (concave conditions, priority I > II > III > IV/trigL3)
    new_wave_offset = None
    if concave_i[i]:
        new_wave_offset = 3
    elif concave_ii[i]:
        new_wave_offset = 2
    elif concave_iii[i]:
        new_wave_offset = 1
    elif concave_iv[i] or trig_l3[i]:
        new_wave_offset = 0

    if new_wave_offset is not None:
        j = i - new_wave_offset
        W.append_wave(False, float(low[j]), float(high[j]), float(low[j]),
                       float(c_top[j]), float(c_bot[j]), j)
        W.check_waterfall_ditch(W.count() - 1)

        # Immediately check if an UP wave starts on same day
        new_wc = W.count()
        if (not W.wave[new_wc - 1]  # just added DOWN
                and (convex_iv[i] or trig_h3[i])
                and W.day_idx[new_wc - 1] != W.day_idx[new_wc - 2]):
            W.append_wave(True, float(high[i]), float(high[i]), float(low[i]),
                           float(c_top[i]), float(c_bot[i]), i)
            W.check_waterfall_ditch(W.count() - 1)


# ── DOWN wave processing ────────────────────────────────────────────────────


def _process_down_wave(
    W, i, high, low, close, c_top, c_bot,
    trig_l1, trig_l3, trig_h3,
    convex_i, convex_ii, convex_iii, convex_iv,
    concave_iv, any_convex, any_concave,
    tip0,
):
    wc = W.count()

    # Try to extend current DOWN wave's tip lower
    extended = False
    if (low[i] < W.tip[wc - 1] and trig_l1[i]
            and not any_convex
            and not (trig_h3[i] and close[i] < c_bot[i - 1])):
        W.update_current(float(low[i]), float(high[i]), float(low[i]),
                         float(c_top[i]), float(c_bot[i]), i)
        W.check_waterfall_ditch(wc - 1)
        extended = True
    elif (i > 0 and low[i - 1] < W.tip[wc - 1]
          and not (convex_i[i] or convex_ii[i])):
        W.update_current(float(low[i - 1]), float(high[i - 1]), float(low[i - 1]),
                         float(c_top[i - 1]), float(c_bot[i - 1]), i - 1)
        W.check_waterfall_ditch(wc - 1)
        extended = True

    # Check for new UP wave (convex conditions, priority I > II > III > IV/trigH3)
    new_wave_offset = None
    if convex_i[i]:
        new_wave_offset = 3
    elif convex_ii[i]:
        new_wave_offset = 2
    elif convex_iii[i]:
        new_wave_offset = 1
    elif convex_iv[i] or trig_h3[i]:
        new_wave_offset = 0

    if new_wave_offset is not None:
        j = i - new_wave_offset
        W.append_wave(True, float(high[j]), float(high[j]), float(low[j]),
                       float(c_top[j]), float(c_bot[j]), j)
        W.check_waterfall_ditch(W.count() - 1)

        # Immediately check if a DOWN wave starts on same day
        new_wc = W.count()
        if (W.wave[new_wc - 1]  # just added UP
                and (concave_iv[i] or trig_l3[i])
                and W.day_idx[new_wc - 1] != W.day_idx[new_wc - 2]):
            W.append_wave(False, float(low[i]), float(high[i]), float(low[i]),
                           float(c_top[i]), float(c_bot[i]), i)
            W.check_waterfall_ditch(W.count() - 1)


# ── Wave trend calculation ──────────────────────────────────────────────────


_TREND_DEPTHS = {"short": 4, "medium": 6, "long": 8}
_TREND_WEIGHTS = {"short": 0.5, "medium": 0.3, "long": 0.2}


def _calc_wave_trend(W: WaveList, n: int) -> WaveTrend:
    """Compute length-weighted wave trend scores at multiple lookback depths.

    For each day, finds which wave it belongs to, then looks back D waves.
    Each wave contributes +pct_length (UP) or -pct_length (DOWN).
    Score = sum of signed pct_lengths / sum of abs pct_lengths → range [-1, +1].
    """
    wc = W.count()

    # Build day → wave index mapping
    wave_for_day = np.zeros(n, dtype=np.int32)
    for wi in range(wc):
        start = W.day_idx[wi]
        end = W.day_idx[wi + 1] if wi + 1 < wc else n
        wave_for_day[start:end] = wi

    # Precompute percentage lengths: length / avg_price * 100
    pct_lengths = np.zeros(wc, dtype=F32)
    for wi in range(1, wc):
        avg_price = (W.tip[wi] + W.tip[wi - 1]) / 2
        if avg_price > 0:
            pct_lengths[wi] = abs(W.length[wi]) / avg_price * 100

    # Signed contributions: +pct for UP, -pct for DOWN
    signed = np.zeros(wc, dtype=F32)
    for wi in range(wc):
        signed[wi] = pct_lengths[wi] if W.wave[wi] else -pct_lengths[wi]

    # Compute trend scores per depth
    scores = {}
    for label, depth in _TREND_DEPTHS.items():
        arr = np.zeros(n, dtype=F32)
        for i in range(n):
            wi = int(wave_for_day[i])
            start_wi = max(0, wi - depth + 1)
            if start_wi >= wi:
                continue
            window_signed = signed[start_wi:wi + 1]
            window_abs = pct_lengths[start_wi:wi + 1]
            total_abs = float(np.sum(window_abs))
            if total_abs > 0:
                arr[i] = F32(float(np.sum(window_signed)) / total_abs)
        scores[label] = arr

    # Composite: weighted average
    composite = np.zeros(n, dtype=F32)
    for label, weight in _TREND_WEIGHTS.items():
        composite += scores[label] * weight

    return WaveTrend(
        short=scores["short"],
        medium=scores["medium"],
        long=scores["long"],
        composite=composite,
    )


# ── Build result ────────────────────────────────────────────────────────────


def _build_result(
    *, waves, convex_i, convex_ii, convex_iii, convex_iv,
    concave_i, concave_ii, concave_iii, concave_iv,
    direction, tip0, tip1, tip2,
    up_price0, mid_price0, down_price0,
    up_price1, mid_price1, down_price1,
    close_cross_d2ma, red_wf0, black_wf0, red_wf1, black_wf1,
    red_wd1, black_wd1,
    bwf_top, bwf_bot, rwf_top, rwf_bot, bwf_up, rwf_down,
    close_big_bwf, close_small_rwf, close_break_bwf, close_break_rwf,
    sink,
    tip_breakout_up, tip_breakout_down, sink_reversal, length_expansion,
    wave_volume_up,
    wave_trend,
) -> WaveResult:
    return WaveResult(
        waves=waves,
        convex_i=convex_i, convex_ii=convex_ii,
        convex_iii=convex_iii, convex_iv=convex_iv,
        concave_i=concave_i, concave_ii=concave_ii,
        concave_iii=concave_iii, concave_iv=concave_iv,
        direction=direction,
        tip0=tip0, tip1=tip1, tip2=tip2,
        up_price0=up_price0, mid_price0=mid_price0, down_price0=down_price0,
        up_price1=up_price1, mid_price1=mid_price1, down_price1=down_price1,
        close_cross_wave_d2ma=close_cross_d2ma,
        red_waterfall0=red_wf0, black_waterfall0=black_wf0,
        red_waterfall1=red_wf1, black_waterfall1=black_wf1,
        red_sizable_wave1=red_wd1, black_sizable_wave1=black_wd1,
        black_wf_top_tip=bwf_top, black_wf_bottom_tip=bwf_bot,
        red_wf_top_tip=rwf_top, red_wf_bottom_tip=rwf_bot,
        black_wf_up_price=bwf_up, red_wf_down_price=rwf_down,
        close_big_black_wf_up=close_big_bwf,
        close_small_red_wf_down=close_small_rwf,
        close_break_black_wf_up=close_break_bwf,
        close_break_red_wf_down=close_break_rwf,
        sink=sink,
        tip_breakout_up=tip_breakout_up,
        tip_breakout_down=tip_breakout_down,
        sink_reversal=sink_reversal,
        length_expansion=length_expansion,
        wave_volume_up=wave_volume_up,
        wave_trend=wave_trend,
    )
