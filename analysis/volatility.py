"""Parkinson high-low volatility.

Used by the ScoreBoard's low-volatility cell (score.py `_add_volatility_rules`).

Why Parkinson rather than the close-to-close standard deviation: the 2026-07-11 window
sweep (`analysis/_score_vol_window_sweep.py`) measured both across Fibonacci windows and
Parkinson separated the *reliable* part of the volatility effect better at every single
window -- at H=60 it lifted the median decile spread from +3.427% to +3.517% at the chosen
233-day window, and by far more at short windows, where its per-observation efficiency
advantage matters most.  It also carries less of the right-tail contamination that makes a
mean-based metric look good for the wrong reason.

Window 233 is the plateau peak (233/377/610 all within 0.13pp; close-to-close falls away
past 233).  It is a Fibonacci number, matching the SMA period family used elsewhere.

Note on limit-locked days: high == low gives log(H/L) = 0, so a locked bar contributes no
range.  Over a 233-day window a handful of such bars is immaterial, and deflating the
volatility of a stock that keeps locking limit-up would be wrong anyway -- those are the
lottery names this cell exists to hold back.
"""

from __future__ import annotations

import numpy as np

from analysis.indicators import F32, F32Array

PARKINSON_WINDOW = 233
_PARK_C = 1.0 / (4.0 * np.log(2.0))


def calculate_parkinson_vol(
    high: F32Array,
    low: F32Array,
    window: int = PARKINSON_WINDOW,
) -> F32Array:
    """Rolling Parkinson volatility (per-bar sigma) from the high-low range.

    sigma = sqrt( mean(ln(H/L)^2) / (4 * ln 2) )

    Bars with a non-positive or inverted range are excluded from the window rather
    than treated as zero range.  Returns NaN until half the window is available,
    mirroring the min_periods=window//2 rule the calibration used.
    """
    h = np.asarray(high, dtype=np.float64)
    lo = np.asarray(low, dtype=np.float64)
    n = len(h)
    out = np.full(n, np.nan, dtype=np.float64)
    if n == 0:
        return out.astype(F32)

    valid = (h > 0) & (lo > 0) & (h >= lo)
    hl2 = np.zeros(n, dtype=np.float64)
    np.log(np.divide(h, lo, out=np.ones(n), where=valid), out=hl2, where=valid)
    hl2 = np.where(valid, hl2 * hl2, 0.0)

    csum = np.concatenate(([0.0], np.cumsum(hl2)))
    ccnt = np.concatenate(([0.0], np.cumsum(valid.astype(np.float64))))
    idx = np.arange(n)
    start = np.maximum(0, idx - window + 1)
    win_sum = csum[idx + 1] - csum[start]
    win_cnt = ccnt[idx + 1] - ccnt[start]

    enough = win_cnt >= max(1, window // 2)
    mean_hl2 = np.divide(win_sum, win_cnt, out=np.zeros(n), where=win_cnt > 0)
    np.sqrt(_PARK_C * mean_hl2, out=out, where=enough)
    out[~enough] = np.nan
    return out.astype(F32)
