"""Evaluation metrics (SPEC §4.3): XIRR, drawdown, decay gap — pure functions."""
from __future__ import annotations

import numpy as np
import pandas as pd


def xirr(dates, amounts, guess: float = 0.1) -> float | None:
    """Annualized internal rate of return for dated cashflows.

    Contributions negative, terminal value positive. Newton with bisection
    fallback. Returns None if it can't bracket a root.
    """
    dates = [pd.Timestamp(d) for d in dates]
    t0 = dates[0]
    years = np.array([(d - t0).days / 365.0 for d in dates])
    cf = np.array(amounts, dtype=np.float64)

    def npv(r):
        return np.sum(cf / (1.0 + r) ** years)

    def dnpv(r):
        return np.sum(-years * cf / (1.0 + r) ** (years + 1))

    r = guess
    for _ in range(100):
        f = npv(r)
        d = dnpv(r)
        if abs(d) < 1e-12:
            break
        step = f / d
        r_new = r - step
        if r_new <= -0.9999:
            r_new = (r - 0.9999) / 2
        if abs(r_new - r) < 1e-8:
            return r_new
        r = r_new
    # bisection fallback
    lo, hi = -0.9999, 10.0
    flo, fhi = npv(lo), npv(hi)
    if flo * fhi > 0:
        return None
    for _ in range(200):
        mid = (lo + hi) / 2
        fm = npv(mid)
        if abs(fm) < 1e-6:
            return mid
        if flo * fm < 0:
            hi, fhi = mid, fm
        else:
            lo, flo = mid, fm
    return (lo + hi) / 2


def max_drawdown(values: np.ndarray) -> float:
    """Largest peak-to-trough fractional drawdown of a value series (negative)."""
    if len(values) == 0:
        return 0.0
    peak = np.maximum.accumulate(values)
    dd = values / np.where(peak > 0, peak, np.nan) - 1.0
    return float(np.nanmin(dd))


def max_drawdown_duration(values: np.ndarray) -> int:
    """Longest stretch (in bars) the value stays below a prior peak."""
    if len(values) == 0:
        return 0
    peak = values[0]
    longest = cur = 0
    for v in values:
        if v >= peak:
            peak = v
            cur = 0
        else:
            cur += 1
            longest = max(longest, cur)
    return longest


def worst_rolling_return(values: np.ndarray, window: int = 252) -> float:
    """Worst fractional return over any rolling `window` bars."""
    if len(values) <= window:
        return float(values[-1] / values[0] - 1.0) if len(values) > 1 else 0.0
    ratios = values[window:] / values[:-window] - 1.0
    return float(np.min(ratios))


def leverage_decay_gap(base_1x_close: np.ndarray, target_close: np.ndarray):
    """DIAGNOSTIC ONLY: matched-underlying×2 daily-compounded theoretical vs
    00631L actual.

    `base_1x_close` must be the matched 1× underlying (0050 / 台灣50指數,
    split-adjusted) — NOT TAIEX 大盤, which has a different basket and would
    confound the basis mismatch with real decay. Both normalized to 1.0 at the
    first bar. The gap measures beta decay + fees; NEVER used as strategy return.
    Returns (theoretical_2x, actual_norm, final_gap_pct).
    """
    ret = np.zeros(len(base_1x_close))
    ret[1:] = base_1x_close[1:] / base_1x_close[:-1] - 1.0
    theo = np.cumprod(1.0 + 2.0 * ret)
    theo = theo / theo[0]
    actual = target_close / target_close[0]
    gap = float(actual[-1] / theo[-1] - 1.0)
    return theo, actual, gap
