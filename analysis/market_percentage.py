"""
Market percentage hot indicator — port of Go's MarketArrangement
{Short|Medium|Long}{Up|Down}PercentageHot from calculatetrade3.go.

Inputs are six daily fractions (0..1) of dead-fish-excluded stocks aligned
on the short/medium/long sort:
   short_up_pct, short_down_pct, medium_up_pct, medium_down_pct,
   long_up_pct, long_down_pct.

Output is six bool arrays — one per scope/direction — flagging days when
that scope shows BOTH "extreme directional accumulation" (A) AND
"weakening momentum" (B). The sell_condition uses
   short_down_hot AND medium_down_hot AND long_down_hot
to skip short entries during the tail of a sell-off (mirrors Go's
SellCondition exclusion at lines 8038-8040).

Mirrors Go behaviour 1:1, including the lone short-direction typo at
line 3117 where ShortDownPercentageAbsXD21MA reads from
ShortUpPercentageAbsX (medium/long use their own direction's AbsX).

Window pairs:
   short:  D21 / D55
   medium: D34 / D89
   long:   D55 / D144

WeakK uses thresholds 0.8 / 0.5 / 0.3 for K = I / II / III.
"""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray

F32Array = NDArray[np.float32]
BoolArray = NDArray[np.bool_]


# ── Rolling helpers (partial-window for early bars, matches Go MovingAverageFloat32A) ──


def _rolling_mean(arr: F32Array, n: int) -> F32Array:
    out = np.empty_like(arr, dtype=np.float32)
    cum = np.cumsum(arr, dtype=np.float64)
    for i in range(len(arr)):
        if i < n:
            out[i] = cum[i] / (i + 1)
        else:
            out[i] = (cum[i] - cum[i - n]) / n
    return out


def _rolling_max(arr: F32Array, n: int) -> F32Array:
    out = np.empty_like(arr, dtype=np.float32)
    for i in range(len(arr)):
        start = max(0, i - n + 1)
        out[i] = arr[start : i + 1].max()
    return out


def _rolling_min(arr: F32Array, n: int) -> F32Array:
    out = np.empty_like(arr, dtype=np.float32)
    for i in range(len(arr)):
        start = max(0, i - n + 1)
        out[i] = arr[start : i + 1].min()
    return out


def _shift1(arr: np.ndarray) -> np.ndarray:
    """Shift right by 1; head padded with the value at index 0."""
    out = np.empty_like(arr)
    if len(arr) == 0:
        return out
    out[0] = arr[0]
    out[1:] = arr[:-1]
    return out


def _x(base: F32Array) -> F32Array:
    """Daily change. Day 0 = 0 (no prior day)."""
    out = np.zeros_like(base, dtype=np.float32)
    out[1:] = base[1:] - base[:-1]
    return out


# ── Weak-K (momentum slowing) ─────────────────────────────────────────────────


def _weak(direction: BoolArray, absx: F32Array, threshold: float) -> BoolArray:
    """direction AND (AbsX[i] < AbsX[i-k]*threshold for any k in 1..3).

    Early bars (i < 3) compare against whatever predecessors exist; head
    falls back to AbsX[0] which makes the multiplier comparison trivial,
    matching Go's behaviour where DayCount-k indexes into pre-zeroed slots.
    """
    n = len(absx)
    out = np.zeros(n, dtype=np.bool_)
    for k in (1, 2, 3):
        prev = np.empty_like(absx)
        prev[:k] = absx[0]
        prev[k:] = absx[:-k]
        out |= absx < prev * threshold
    return out & direction


# ── Hot per scope/direction ───────────────────────────────────────────────────


def _hot(
    base: F32Array,
    opp: F32Array,
    base_dnshort_ma: F32Array,
    base_dnshort_b: F32Array,
    base_dnlong_b: F32Array,
    base_absx_dnshort_ma: F32Array,
    opp_dnshort_ma: F32Array,
    opp_dnshort_s: F32Array,
    opp_absx_dnshort_ma: F32Array,
    base_weak1: BoolArray,
    base_weak2: BoolArray,
    opp_dir: BoolArray,
    opp_weak3: BoolArray,
) -> BoolArray:
    """Mirror of Go's PercentageHot = A AND B.

    A_short: base extreme high relative to short window AND opp suppressed
    A_long:  base above (long-window high - 3 * short-window AbsXMA)
    A = A_short OR A_long

    B = (base_weak1[i] AND base_weak1[i-1])
        OR base_weak2[i]
        OR (opp_dir[i] AND NOT opp_weak3[i])
    """
    a_short = (
        (base > base_dnshort_ma)
        & (base > base_dnshort_b - base_absx_dnshort_ma * 2)
        & (opp < opp_dnshort_ma)
        & (opp < opp_dnshort_s + opp_absx_dnshort_ma * 3)
    )
    a_long = base > base_dnlong_b - base_absx_dnshort_ma * 3
    a = a_short | a_long

    weak1_prev = _shift1(base_weak1)
    b = (base_weak1 & weak1_prev) | base_weak2 | (opp_dir & ~opp_weak3)

    return a & b


# ── Main entry: compute six Hot arrays in one pass ────────────────────────────


def _calc_scope(
    up: F32Array,
    down: F32Array,
    n_short: int,
    n_long: int,
    swap_short_down_absxma: bool = False,
) -> dict[str, BoolArray]:
    """Compute up_hot / down_hot for one scope.

    swap_short_down_absxma: if True, Down's AbsXD{n_short}MA reads from Up's
    AbsX (mirrors Go calculatetrade3.go:3117 — applies only to short scope).
    """
    up_x = _x(up)
    down_x = _x(down)
    up_absx = np.abs(up_x).astype(np.float32)
    down_absx = np.abs(down_x).astype(np.float32)

    up_dir = up > _shift1(up)
    down_dir = down > _shift1(down)

    up_dnshort_ma = _rolling_mean(up, n_short)
    up_dnshort_b = _rolling_max(up, n_short)
    up_dnshort_s = _rolling_min(up, n_short)
    up_dnlong_b = _rolling_max(up, n_long)
    up_absx_dnshort_ma = _rolling_mean(up_absx, n_short)

    down_dnshort_ma = _rolling_mean(down, n_short)
    down_dnshort_b = _rolling_max(down, n_short)
    down_dnshort_s = _rolling_min(down, n_short)
    down_dnlong_b = _rolling_max(down, n_long)
    if swap_short_down_absxma:
        # Mirror Go line 3117: Short Down uses Up's AbsX (likely typo, kept).
        down_absx_dnshort_ma = up_absx_dnshort_ma
    else:
        down_absx_dnshort_ma = _rolling_mean(down_absx, n_short)

    up_weak1 = _weak(up_dir, up_absx, 0.8)
    up_weak2 = _weak(up_dir, up_absx, 0.5)
    up_weak3 = _weak(up_dir, up_absx, 0.3)
    down_weak1 = _weak(down_dir, down_absx, 0.8)
    down_weak2 = _weak(down_dir, down_absx, 0.5)
    down_weak3 = _weak(down_dir, down_absx, 0.3)

    up_hot = _hot(
        base=up,
        opp=down,
        base_dnshort_ma=up_dnshort_ma,
        base_dnshort_b=up_dnshort_b,
        base_dnlong_b=up_dnlong_b,
        base_absx_dnshort_ma=up_absx_dnshort_ma,
        opp_dnshort_ma=down_dnshort_ma,
        opp_dnshort_s=down_dnshort_s,
        opp_absx_dnshort_ma=down_absx_dnshort_ma,
        base_weak1=up_weak1,
        base_weak2=up_weak2,
        opp_dir=down_dir,
        opp_weak3=down_weak3,
    )
    down_hot = _hot(
        base=down,
        opp=up,
        base_dnshort_ma=down_dnshort_ma,
        base_dnshort_b=down_dnshort_b,
        base_dnlong_b=down_dnlong_b,
        base_absx_dnshort_ma=down_absx_dnshort_ma,
        opp_dnshort_ma=up_dnshort_ma,
        opp_dnshort_s=up_dnshort_s,
        opp_absx_dnshort_ma=up_absx_dnshort_ma,
        base_weak1=down_weak1,
        base_weak2=down_weak2,
        opp_dir=up_dir,
        opp_weak3=up_weak3,
    )
    return {
        "up_hot": up_hot,
        "down_hot": down_hot,
        "up_dir": up_dir,
        "down_dir": down_dir,
    }


def calculate_pct_status(
    up: F32Array, flat: F32Array, down: F32Array
) -> NDArray[np.uint8]:
    """Per-day Up/Flat/Down ordinal 0-5 (mirrors Go ShortPercentageStatus).

    Encoding (Go calculatetrade3.go:3143-3165):
       5: U >= F >= D    4: U >= D >= F    3: F >= U >= D
       2: F >= D >= U    1: D >= U >= F    0: D >= F >= U
    """
    n = len(up)
    out = np.full(n, 3, dtype=np.uint8)  # default to 3 (no clear ordering)
    for i in range(n):
        u, f, d = float(up[i]), float(flat[i]), float(down[i])
        if u >= f >= d:
            out[i] = 5
        elif u >= d >= f:
            out[i] = 4
        elif f >= u >= d:
            out[i] = 3
        elif f >= d >= u:
            out[i] = 2
        elif d >= u >= f:
            out[i] = 1
        elif d >= f >= u:
            out[i] = 0
    return out


def calculate_percentage_hots(
    short_up: F32Array,
    short_down: F32Array,
    medium_up: F32Array,
    medium_down: F32Array,
    long_up: F32Array,
    long_down: F32Array,
) -> dict[str, np.ndarray]:
    """Compute PercentageHot + Direction bool arrays for all three scopes.

    Returns keys (each value is a numpy array):
      {short|medium|long}_{up|down}_hot   — bool, the Hot trigger
      {short|medium|long}_{up|down}_dir   — bool, today > yesterday
    """
    out: dict[str, np.ndarray] = {}
    s = _calc_scope(short_up, short_down, n_short=21, n_long=55, swap_short_down_absxma=True)
    m = _calc_scope(medium_up, medium_down, n_short=34, n_long=89)
    l = _calc_scope(long_up, long_down, n_short=55, n_long=144)
    for prefix, scope in (("short", s), ("medium", m), ("long", l)):
        out[f"{prefix}_up_hot"] = scope["up_hot"]
        out[f"{prefix}_down_hot"] = scope["down_hot"]
        out[f"{prefix}_up_dir"] = scope["up_dir"]
        out[f"{prefix}_down_dir"] = scope["down_dir"]
    return out
