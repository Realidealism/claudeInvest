"""
Multi-period strong breakout / breakdown flags — ports OverUpper3/5/8 (人/走/召 漲)
and OverLower3/5/8 (人/走/召 跌) from CalculateTrade2.go (lines 16165-16315).

Each of the six final flags fires only when:
  1. Today's high (or recent N-day high) reaches yesterday's N-day high MA.
  2. The overshoot above the N-day price MA is at least a fraction of the
     34-day H-L range (0.5 / 0.8 / 1.3 for periods 3/5/8).
  3. Today's overshoot is significant relative to the recent overshoot
     history (≥80% of 5-day max, ≥50% of 8-day max, etc., with several
     long-history sanity checks).
  4. Momentum is confirmed via HighChange (today's high − 2-day low)
     vs Bollinger std on close, with crossover/Convex alternatives.

This module replaces the simplistic "close >= bs_high[5/8]" gate that
the v0 touch_condition / pick_condition used; that gate let any close
at a multi-day high through, including weak sideways breakouts. The
Go composite is much stricter and is what touch / pick were originally
meant to filter on.

Depends on CandleResult and CloseResult for shared MAs and rolling
extremes — keeps redundant computation minimal.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from numpy.typing import NDArray

from analysis.indicators import sma, rolling_std, rolling_highest, rolling_lowest

F32 = np.float32
F32Array = NDArray[np.float32]
BoolArray = NDArray[np.bool_]


@dataclass
class OverBreakoutResult:
    """6 strong breakout/breakdown flags + underlying overshoot ratios."""
    # Core overshoot ratios (kept for diagnostics / future use)
    over_upper_d3: F32Array     # (High - CD3MA) / CD3MA
    over_upper_d5: F32Array     # (HD2B - CD5MA) / CD5MA
    over_upper_d8: F32Array     # (HD3B - CD8MA) / CD8MA
    over_lower_d3: F32Array     # (Low - CD3MA) / CD3MA
    over_lower_d5: F32Array     # (LD2S - CD5MA) / CD5MA
    over_lower_d8: F32Array     # (LD3S - CD8MA) / CD8MA

    # Composite "strong breakout" flags (人/走/召)
    over_upper_3: BoolArray     # 人漲
    over_upper_5: BoolArray     # 走漲
    over_upper_8: BoolArray     # 召漲
    # Mirror "strong breakdown" flags
    over_lower_3: BoolArray     # 人跌
    over_lower_5: BoolArray     # 走跌
    over_lower_8: BoolArray     # 召跌


def _shift1(arr: np.ndarray) -> np.ndarray:
    """Shift right by 1; first element carries its own value (no spurious zero)."""
    out = np.empty_like(arr)
    out[0] = arr[0]
    out[1:] = arr[:-1]
    return out


def calculate_over_breakout(
    high: F32Array,
    low: F32Array,
    close: F32Array,
    candle_result,
    close_result,
) -> OverBreakoutResult:
    """Compute the 6 OverUpper / OverLower flags."""
    high = high.astype(F32)
    low = low.astype(F32)
    close = close.astype(F32)
    n = len(close)

    # ── Pre-computed inputs from existing modules ───────────────────────
    cd3ma  = close_result.ma.sma[3]
    cd5ma  = close_result.ma.sma[5]
    cd8ma  = close_result.ma.sma[8]
    cd13ma = close_result.ma.sma[13]
    cd21ma = close_result.ma.sma[21]
    hd2b = candle_result.high_rolling.high[2]
    hd3b = candle_result.high_rolling.high[3]
    ld2s = candle_result.low_rolling.low[2]
    ld3s = candle_result.low_rolling.low[3]
    convex8 = candle_result.convex_n[8]
    convex13 = candle_result.convex_n[13]
    convex21 = candle_result.convex_n[21]
    concave8 = candle_result.concave_n[8]
    concave13 = candle_result.concave_n[13]
    concave21 = candle_result.concave_n[21]

    # ── Fresh inputs not pre-computed elsewhere ─────────────────────────
    mahd3 = sma(high, 3)
    mahd5 = sma(high, 5)
    mahd8 = sma(high, 8)
    mald3 = sma(low, 3)
    mald5 = sma(low, 5)
    mald8 = sma(low, 8)
    hl = high - low
    hld34ma = sma(hl, 34)

    # Bollinger std on close at the periods we need
    cboll3  = rolling_std(close, 3)
    cboll5  = rolling_std(close, 5)
    cboll8  = rolling_std(close, 8)
    cboll13 = rolling_std(close, 13)
    cboll21 = rolling_std(close, 21)

    # Bollinger bands needed for absolute-breakout checks
    cboll3_u2 = cd3ma + cboll3 * 2
    cboll3_d2 = cd3ma - cboll3 * 2
    cboll5_u2 = cd5ma + cboll5 * 2
    cboll5_u3 = cd5ma + cboll5 * 3
    cboll5_d2 = cd5ma - cboll5 * 2
    cboll5_d3 = cd5ma - cboll5 * 3
    cboll8_u2 = cd8ma + cboll8 * 2
    cboll8_u3 = cd8ma + cboll8 * 3
    cboll8_d2 = cd8ma - cboll8 * 2
    cboll8_d3 = cd8ma - cboll8 * 3

    # ── Core ratios ─────────────────────────────────────────────────────
    with np.errstate(divide="ignore", invalid="ignore"):
        ou_d3 = np.where(cd3ma != 0, (high - cd3ma) / cd3ma, F32(0)).astype(F32)
        ou_d5 = np.where(cd5ma != 0, (hd2b - cd5ma) / cd5ma, F32(0)).astype(F32)
        ou_d8 = np.where(cd8ma != 0, (hd3b - cd8ma) / cd8ma, F32(0)).astype(F32)
        ol_d3 = np.where(cd3ma != 0, (low  - cd3ma) / cd3ma, F32(0)).astype(F32)
        ol_d5 = np.where(cd5ma != 0, (ld2s - cd5ma) / cd5ma, F32(0)).astype(F32)
        ol_d8 = np.where(cd8ma != 0, (ld3s - cd8ma) / cd8ma, F32(0)).astype(F32)

    # ── Rolling extrema on those ratios ─────────────────────────────────
    # OverUpperD3 highs (B = "Big" rolling max)
    ou_d3_b3, ou_d3_b5, ou_d3_b8, ou_d3_b13, ou_d3_b21, ou_d3_b34 = (
        rolling_highest(ou_d3, p) for p in (3, 5, 8, 13, 21, 34)
    )
    # OverLowerD3 lows
    ol_d3_s3, ol_d3_s5, ol_d3_s8, ol_d3_s13, ol_d3_s21, ol_d3_s34 = (
        rolling_lowest(ol_d3, p) for p in (3, 5, 8, 13, 21, 34)
    )
    # OverUpperD5 / OverLowerD5
    ou_d5_b3, ou_d5_b5, ou_d5_b8, ou_d5_b13, ou_d5_b21, ou_d5_b34, ou_d5_b55 = (
        rolling_highest(ou_d5, p) for p in (3, 5, 8, 13, 21, 34, 55)
    )
    ol_d5_s3, ol_d5_s5, ol_d5_s8, ol_d5_s13, ol_d5_s21, ol_d5_s34, ol_d5_s55 = (
        rolling_lowest(ol_d5, p) for p in (3, 5, 8, 13, 21, 34, 55)
    )
    # OverUpperD8 / OverLowerD8
    ou_d8_b5, ou_d8_b8, ou_d8_b13, ou_d8_b21, ou_d8_b34, ou_d8_b55, ou_d8_b89 = (
        rolling_highest(ou_d8, p) for p in (5, 8, 13, 21, 34, 55, 89)
    )
    ol_d8_s5, ol_d8_s8, ol_d8_s13, ol_d8_s21, ol_d8_s34, ol_d8_s55, ol_d8_s89 = (
        rolling_lowest(ol_d8, p) for p in (5, 8, 13, 21, 34, 55, 89)
    )

    # ── Change indicators ───────────────────────────────────────────────
    high_change = high - ld2s        # today's high minus 2-day low (always >= 0 in normal case)
    low_change  = hd2b - low         # 2-day high minus today's low (mirror)
    high_change_d2b = rolling_highest(high_change, 2)
    low_change_d2b  = rolling_highest(low_change, 2)

    # ── "Yesterday" shifts ──────────────────────────────────────────────
    mahd3_p = _shift1(mahd3)
    mahd5_p = _shift1(mahd5)
    mahd8_p = _shift1(mahd8)
    mald3_p = _shift1(mald3)
    mald5_p = _shift1(mald5)
    mald8_p = _shift1(mald8)
    high_p  = _shift1(high)
    low_p   = _shift1(low)
    cd8ma_p  = _shift1(cd8ma)
    cd13ma_p = _shift1(cd13ma)
    cd21ma_p = _shift1(cd21ma)

    abs_ol_d3 = np.abs(ol_d3)
    abs_ol_d5 = np.abs(ol_d5)
    abs_ol_d8 = np.abs(ol_d8)

    # ── OverUpper3 (人漲) ──────────────────────────────────────────────
    ou3 = (
        (high >= mahd3_p)
        & ((high - cd3ma) > hld34ma * 0.5)
        & (
            (ou_d3 >= ou_d3_b5 * 0.8)
            | ((ou_d3 == ou_d3_b3) & (high > cboll3_u2))
        )
        & (ou_d3 >= ou_d3_b8  * 0.5)
        & (ou_d3 >= ou_d3_b13 * 0.3)
        & (ou_d3 >= ou_d3_b21 * 0.2)
        & (
            ((ou_d3 >= ou_d3_b34 * 0.07) & (ou_d3 >= np.abs(ol_d3_s34) * 0.13))
            | (ou_d3 == ou_d3_b8)
        )
        & (
            (high_change >= cboll8 * 0.8)
            | ((high > high_p) & (high_change_d2b >= cboll8 * 0.8))
            | (
                (convex8 | ((high_p <= cd8ma_p) & (high > cd8ma)))
                & (high_change_d2b >= cboll8 * 0.5)
            )
        )
    )

    # ── OverLower3 (人跌) — mirror ─────────────────────────────────────
    ol3 = (
        (low <= mald3_p)
        & ((cd3ma - low) > hld34ma * 0.5)
        & (
            (ol_d3 <= ol_d3_s5 * 0.8)
            | ((ol_d3 == ol_d3_s3) & (low < cboll3_d2))
        )
        & (ol_d3 <= ol_d3_s8  * 0.5)
        & (ol_d3 <= ol_d3_s13 * 0.3)
        & (ol_d3 <= ol_d3_s21 * 0.2)
        & (
            ((ol_d3 <= ol_d3_s34 * 0.07) & (abs_ol_d3 >= ou_d3_b34 * 0.13))
            | (ol_d3 == ol_d3_s8)
        )
        & (
            (np.abs(low_change) >= cboll8 * 0.8)
            | ((low < low_p) & (np.abs(low_change_d2b) >= cboll8 * 0.8))
            | (
                (concave8 | ((low_p >= cd8ma_p) & (low < cd8ma)))
                & (np.abs(low_change_d2b) >= cboll8 * 0.5)
            )
        )
    )

    # ── OverUpper5 (走漲) ──────────────────────────────────────────────
    ou5 = (
        (high >= mahd5_p)
        & ((hd2b - cd5ma) > hld34ma * 0.8)
        & (
            (ou_d5 >= ou_d5_b8 * 0.8)
            | ((ou_d5 == ou_d5_b5) & (high > cboll5_u2))
            | ((ou_d5 == ou_d5_b3) & (high > cboll5_u3))
        )
        & (
            (ou_d5 >= ou_d5_b13 * 0.5)
            | (ou_d5 == ou_d5_b8)
        )
        & (ou_d5 >= ou_d5_b21 * 0.3)
        & (ou_d5 >= ou_d5_b34 * 0.2)
        & (
            ((ou_d5 >= ou_d5_b55 * 0.07) & (ou_d5 >= np.abs(ol_d5_s55) * 0.13))
            | (ou_d5 == ou_d5_b13)
        )
        & (
            (high_change >= cboll13 * 0.8)
            | ((high > high_p) & (high_change_d2b >= cboll13 * 0.8))
            | (
                (convex13 | ((high_p <= cd13ma_p) & (high > cd13ma)))
                & (high_change_d2b >= cboll13 * 0.5)
            )
        )
    )

    # ── OverLower5 (走跌) ──────────────────────────────────────────────
    ol5 = (
        (low <= mald5_p)
        & ((cd5ma - ld2s) > hld34ma * 0.8)
        & (
            (ol_d5 <= ol_d5_s8 * 0.8)
            | ((ol_d5 == ol_d5_s5) & (low < cboll5_d2))
            | ((ol_d5 == ol_d5_s3) & (low < cboll5_d3))
        )
        & (
            (ol_d5 <= ol_d5_s13 * 0.5)
            | (ol_d5 == ol_d5_s8)
        )
        & (ol_d5 <= ol_d5_s21 * 0.3)
        & (ol_d5 <= ol_d5_s34 * 0.2)
        & (
            ((ol_d5 <= ol_d5_s55 * 0.07) & (abs_ol_d5 >= ou_d5_b55 * 0.13))
            | (ol_d5 == ol_d5_s13)
        )
        & (
            (low_change >= cboll13 * 0.8)
            | ((low < low_p) & (low_change_d2b >= cboll13 * 0.8))
            | (
                (concave13 | ((low_p >= cd13ma_p) & (low < cd13ma)))
                & (low_change_d2b >= cboll13 * 0.5)
            )
        )
    )

    # ── OverUpper8 (召漲) ──────────────────────────────────────────────
    ou8 = (
        (high >= mahd8_p)
        & ((hd3b - cd8ma) > hld34ma * 1.3)
        & (
            (ou_d8 >= ou_d8_b13 * 0.8)
            | ((ou_d8 == ou_d8_b8) & (high > cboll8_u2))
            | ((ou_d8 == ou_d8_b5) & (high > cboll8_u3))
        )
        & (
            (ou_d8 >= ou_d8_b21 * 0.5)
            | (ou_d8 == ou_d8_b13)
        )
        & (ou_d8 >= ou_d8_b34 * 0.3)
        & (ou_d8 >= ou_d8_b55 * 0.2)
        & (
            ((ou_d8 >= ou_d8_b89 * 0.07) & (ou_d8 >= np.abs(ol_d8_s89) * 0.13))
            | (ou_d8 == ou_d8_b21)
        )
        & (
            (high_change >= cboll21 * 0.8)
            | ((high > high_p) & (high_change_d2b >= cboll21 * 0.8))
            | (
                (convex21 | ((high_p <= cd21ma_p) & (high > cd21ma)))
                & (high_change_d2b >= cboll21 * 0.5)
            )
        )
    )

    # ── OverLower8 (召跌) ──────────────────────────────────────────────
    ol8 = (
        (low <= mald8_p)
        & ((cd8ma - ld3s) > hld34ma * 1.3)
        & (
            (ol_d8 <= ol_d8_s13 * 0.8)
            | ((ol_d8 == ol_d8_s8) & (low < cboll8_d2))
            | ((ol_d8 == ol_d8_s5) & (low < cboll8_d3))
        )
        & (
            (ol_d8 <= ol_d8_s21 * 0.5)
            | (ol_d8 == ol_d8_s13)
        )
        & (ol_d8 <= ol_d8_s34 * 0.3)
        & (ol_d8 <= ol_d8_s55 * 0.2)
        & (
            ((ol_d8 <= ol_d8_s89 * 0.07) & (abs_ol_d8 >= ou_d8_b89 * 0.13))
            | (ol_d8 == ol_d8_s21)
        )
        & (
            (low_change >= cboll21 * 0.8)
            | ((low < low_p) & (low_change_d2b >= cboll21 * 0.8))
            | (
                (concave21 | ((low_p >= cd21ma_p) & (low < cd21ma)))
                & (low_change_d2b >= cboll21 * 0.5)
            )
        )
    )

    return OverBreakoutResult(
        over_upper_d3=ou_d3, over_upper_d5=ou_d5, over_upper_d8=ou_d8,
        over_lower_d3=ol_d3, over_lower_d5=ol_d5, over_lower_d8=ol_d8,
        over_upper_3=ou3, over_upper_5=ou5, over_upper_8=ou8,
        over_lower_3=ol3, over_lower_5=ol5, over_lower_8=ol8,
    )
