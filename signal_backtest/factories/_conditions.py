"""
Six per-day boolean condition arrays, ported in spirit (not literally) from
Go's CalculateTrade2:

    PickCondition  / TouchCondition  — 抄底 / 摸頭        (mirror pair)
    BuyCondition   / SellCondition   — 波段多 / 波段空     (mirror pair)
    BuyFleeSignal  / SellFleeSignal  — 多翻空 / 空翻多     (reversal pair)

The Go originals layer dozens of exclusion filters on top of a leverage-value
state machine. We deliberately drop the leverage-value spine (no ScoreBoard
mapping yet — see project memory) and keep ~5–7 core rules per condition.

Market-breadth filtering (大盤過濾) is also dropped here because per-stock
factories don't get market context. TODO: pre-compute market state and feed it
in once we want fidelity.

Defense rules: none currently. The flood-volume rule (v1-v3 in
data/signal_versions.db) was proven not to materially improve PF and even
hurt 空翻多, so it was reverted at v0.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
from numpy.typing import NDArray

if TYPE_CHECKING:
    from backtest.data import StockData

BoolArray = NDArray[np.bool_]


# ── Helpers ─────────────────────────────────────────────────────────────────


def _shift(arr: np.ndarray, k: int) -> np.ndarray:
    """Shift right by k positions; head padded with zeros / False."""
    out = np.zeros_like(arr)
    if k < len(arr):
        out[k:] = arr[:len(arr) - k]
    return out


def _last_n_any(arr: BoolArray, n: int) -> BoolArray:
    """True at i if any of arr[i-n+1 .. i] is True."""
    out = arr.copy()
    for k in range(1, n):
        out = out | _shift(arr, k)
    return out


def _last_n_all(arr: BoolArray, n: int) -> BoolArray:
    """True at i if all of arr[i-n+1 .. i] are True."""
    out = arr.copy()
    for k in range(1, n):
        out = out & _shift(arr, k)
    return out


def _turn_change_up(turn: NDArray[np.uint8]) -> BoolArray:
    """Detect the bar where turn flips from 0 (bearish) to >= 1.

    Approximates Go's '中低變 / 長低變' — the bar a turn-point newly
    stops contributing to a downward MA pull.
    """
    prev = _shift(turn, 1)
    return (turn >= 1) & (prev == 0)


def _turn_change_down(turn: NDArray[np.uint8]) -> BoolArray:
    """Mirror of _turn_change_up — the bar a high turn-point newly appears."""
    prev = _shift(turn, 1)
    return (turn <= 1) & (prev == 2)


def _market_strongly_bullish(data: "StockData") -> BoolArray:
    """Strong bull at any scope (trend code >= 2). Filter for short entries."""
    ms = data.market_state
    return (ms.short_trend >= 2) | (ms.medium_trend >= 2) | (ms.long_trend >= 2)


def _market_strongly_bearish(data: "StockData") -> BoolArray:
    """Strong bear at any scope (trend code <= -2). Filter for long entries."""
    ms = data.market_state
    return (ms.short_trend <= -2) | (ms.medium_trend <= -2) | (ms.long_trend <= -2)


# ── PickCondition / TouchCondition (抄底 / 摸頭) ────────────────────────────


def pick_condition(data: "StockData") -> BoolArray:
    """抄底訊號 — long_entry candidate.

    Rules:
      1. 觸 / 跌破 5 或 8 日低 (close <= bs.low[5/8])
         — v4 嘗試過用 OverLower3/5/8 複合 pattern，PF 反而下降（輸均惡化），
           已回退簡單版。touch 採 OverUpper 是有效的 (long-bias 市場下找頭適用)，
           但 pick 找底用簡單條件反而較好。
      2. 最近 3 日有出現中或長期扣抵翻轉 (turn[34/55] 從 0 翻 >=1)
      3. 量能未乾枯：今日量 >= 前日量 * 0.5 AND not (連 3 日 SmallLongMinVolume)
      4. 不在 tier-1 洪量區下方 (非 Flood 斷頭段)
      5. 非長均糾結連 2 日 (不在僵死段)
      6. 不在連續 3 日凹13（避免接持續創新低的下跌段）
      7. 大盤非強空（任一尺度 trend <= -2）— 不在崩盤段抄底
    """
    n = data.n
    close = data.close
    bs_low = data.close_result.bs.low
    turn = data.close_result.turn

    # 1. 觸或跌破 5/8 日低
    rule_pos = (close <= bs_low[5]) | (close <= bs_low[8])

    # 7. 大盤過濾
    rule_market = ~_market_strongly_bearish(data)

    # 2. 中/長扣抵翻轉（3 日內任一日）
    long_change = _turn_change_up(turn[34]) | _turn_change_up(turn[55])
    rule_change = _last_n_any(long_change, 3)

    # 3. 量能未乾枯
    vol = data.volume
    prev_vol = _shift(vol, 1)
    long_min = data.volume_result.extremes["long"].small
    rule_vol = (vol >= prev_vol * 0.5) & ~_last_n_all(long_min, 3)

    # 4. 非 tier-1 洪量斷頭
    rule_flood = ~data.volume_result.below_tier[1]

    # 5. 非長均糾結連 2 日
    long_knot = data.close_result.knot["long"].flag
    prev_long_knot = _shift(long_knot, 1)
    rule_knot = ~(long_knot & prev_long_knot)

    # 6. 不在連續 3 日凹13
    concave13 = data.candle_result.concave_n[13]
    rule_concave = ~_last_n_all(concave13, 3)

    return rule_pos & rule_change & rule_vol & rule_flood & rule_knot & rule_concave & rule_market


def touch_condition(data: "StockData") -> BoolArray:
    """摸頭訊號 — short_entry candidate; mirror of pick_condition.

    Rules:
      1. 強力上漲存在性：(走漲[0..2] OR 召漲[0..2] OR 連 2 日人漲[1..2]) AND
         今/昨日仍人漲 — 對應 Go OverUpper3/5/8 複合 pattern
      2. 最近 3 日有出現中或長期高拐 (turn[34/55] 從 2 翻 <=1)
      3. 量能未爆衝：今日量 < 前日量 * 1.5 AND not (連 3 日 BigLongMaxVolume)
      4. 不在 tier-1 洪量區上方
      5. 非長均糾結連 2 日
      6. 不在連續 3 日凸13
      7. 大盤非強多（任一尺度 trend >= 2）— 不在強多段摸頭
    """
    n = data.n
    close = data.close
    turn = data.close_result.turn
    ob = data.over_breakout

    # 1. 強力上漲存在性（Go pattern：走漲[0..2] OR 召漲[0..2] OR 連 2 日人漲[1..2]）
    #    AND 今/昨日仍人漲 — 確認漲勢仍在持續
    ou3, ou5, ou8 = ob.over_upper_3, ob.over_upper_5, ob.over_upper_8
    walked_or_called = (
        ou5 | _shift(ou5, 1) | _shift(ou5, 2)
        | ou8 | _shift(ou8, 1) | _shift(ou8, 2)
        | (_shift(ou3, 1) & _shift(ou3, 2))
    )
    rule_pos = walked_or_called & (ou3 | _shift(ou3, 1))

    long_change = _turn_change_down(turn[34]) | _turn_change_down(turn[55])
    rule_change = _last_n_any(long_change, 3)

    vol = data.volume
    prev_vol = _shift(vol, 1)
    long_max = data.volume_result.extremes["long"].big
    rule_vol = (vol < prev_vol * 1.5) & ~_last_n_all(long_max, 3)

    rule_flood = ~data.volume_result.above_tier[1]

    long_knot = data.close_result.knot["long"].flag
    prev_long_knot = _shift(long_knot, 1)
    rule_knot = ~(long_knot & prev_long_knot)

    convex13 = data.candle_result.convex_n[13]
    rule_convex = ~_last_n_all(convex13, 3)

    rule_market = ~_market_strongly_bullish(data)

    return rule_pos & rule_change & rule_vol & rule_flood & rule_knot & rule_convex & rule_market


# ── BuyCondition / SellCondition (波段多 / 波段空) ──────────────────────────


def buy_condition(data: "StockData") -> BoolArray:
    """波段多訊 — long_entry candidate.

    Rules:
      1. MA 排列：close > SMA8 AND SMA8 > SMA21
      2. 上扣 5 (turn[5] == 2) AND not 下扣 5 (turn[5] != 0)
      3. 突破或新高：close > prev close.bs.high[8]
      4. 量能配合：今日量 >= VD5 * 1.0 (站上 5 日均量)
      5. 非長均糾結
      6. 不在連續 3 日凸21（避免追在頂部凸點）
      7. 大盤非強空 — 不在崩盤段做多
    """
    close = data.close
    sma = data.close_result.ma.sma
    turn = data.close_result.turn

    rule_ma = (close > sma[8]) & (sma[8] > sma[21])
    rule_market = ~_market_strongly_bearish(data)

    rule_turn = (turn[5] == 2)

    bs_high = data.close_result.bs.high
    prev_h8 = _shift(bs_high[8], 1)
    rule_break = close > prev_h8

    vol = data.volume
    vd5 = data.volume_result.sma[5]
    rule_vol = vol >= vd5

    long_knot = data.close_result.knot["long"].flag
    rule_knot = ~long_knot

    convex21 = data.candle_result.convex_n[21]
    rule_convex = ~_last_n_all(convex21, 3)

    return rule_ma & rule_turn & rule_break & rule_vol & rule_knot & rule_convex & rule_market


def sell_condition(data: "StockData") -> BoolArray:
    """波段空訊 — short_entry candidate; mirror of buy_condition.

    Rules:
      1. MA 排列：close < SMA8 AND SMA8 < SMA21
      2. 下扣 5 (turn[5] == 0)
      3. 跌破：close < prev close.bs.low[8]
      4. 量能配合：今日量 >= VD5
      5. 非長均糾結
      6. 不在連續 3 日凹21
      7. 大盤非強多
    """
    close = data.close
    sma = data.close_result.ma.sma
    turn = data.close_result.turn

    rule_ma = (close < sma[8]) & (sma[8] < sma[21])
    rule_market = ~_market_strongly_bullish(data)

    rule_turn = (turn[5] == 0)

    bs_low = data.close_result.bs.low
    prev_l8 = _shift(bs_low[8], 1)
    rule_break = close < prev_l8

    vol = data.volume
    vd5 = data.volume_result.sma[5]
    rule_vol = vol >= vd5

    long_knot = data.close_result.knot["long"].flag
    rule_knot = ~long_knot

    concave21 = data.candle_result.concave_n[21]
    rule_concave = ~_last_n_all(concave21, 3)

    return rule_ma & rule_turn & rule_break & rule_vol & rule_knot & rule_concave & rule_market


# ── BuyFleeSignal / SellFleeSignal (多翻空 / 空翻多) ────────────────────────


def buy_flee_signal(data: "StockData") -> BoolArray:
    """多翻空訊號 — short_entry candidate.

    Structure: 前 N 日強勢 → 今日急轉直下.

    Rules:
      1. 前提強勢：最近 5 日有過 (close > prev Donchian-20-up OR SMA 多頭排列)
         — Donchian Python 模組是 lazy compute，簡化為 close > bs.high[20] (5 日內)
      2. 崩斷觸發：跳空跌破前日低 AND close < prev candle.cut.down_cut
         OR 大黑K 跌破 SMA8
      3. 量增下跌：vol > prev_vol AND close < prev close
      4. 非長均糾結 2 日
    """
    close = data.close
    sma = data.close_result.ma.sma
    bs_high = data.close_result.bs.high

    # 1. 前提強勢（5 日內出現）
    prev_close = _shift(close, 1)
    bs21 = bs_high.get(21) if 21 in bs_high else bs_high[13]
    prev_bs21 = _shift(bs21, 1)
    breakout = close > prev_bs21
    ma_bull = (sma[3] > sma[5]) & (sma[5] > sma[8]) & (sma[8] > sma[13])
    prior_strong = _last_n_any(breakout | ma_bull, 5)

    # 2. 崩斷觸發
    open_ = data.open
    low = data.low
    prev_low = _shift(low, 1)
    down_cut = data.candle_result.cut.down_cut
    prev_down_cut = _shift(down_cut, 1)
    gap_down = (open_ < prev_low) & (close < prev_down_cut)

    black = data.candle_result.candle.black
    big_black = data.candle_result.stick_length.black_medium | data.candle_result.stick_length.black_long
    break_sma = close < sma[8]
    breakdown = big_black & break_sma

    rule_trigger = gap_down | breakdown

    # 3. 量增下跌
    vol = data.volume
    prev_vol = _shift(vol, 1)
    rule_vol = (vol > prev_vol) & (close < prev_close)

    # 4. 非長均糾結 2 日
    long_knot = data.close_result.knot["long"].flag
    prev_long_knot = _shift(long_knot, 1)
    rule_knot = ~(long_knot & prev_long_knot)

    # 5. 大盤非強多 — 不在強多段做空
    rule_market = ~_market_strongly_bullish(data)

    return prior_strong & rule_trigger & rule_vol & rule_knot & rule_market


def sell_flee_signal(data: "StockData") -> BoolArray:
    """空翻多訊號 — long_entry candidate; mirror of buy_flee_signal.

    Rules:
      1. 前提弱勢：最近 5 日有過 (close < prev Donchian-21-down OR SMA 空頭排列)
      2. 翻轉觸發：跳空站上前日高 AND close > prev up_cut
         OR 大紅K 站上 SMA8
      3. 量增上漲
      4. 非長均糾結 2 日
    """
    close = data.close
    sma = data.close_result.ma.sma
    bs_low = data.close_result.bs.low

    prev_close = _shift(close, 1)
    bs21 = bs_low.get(21) if 21 in bs_low else bs_low[13]
    prev_bs21 = _shift(bs21, 1)
    breakdown_prior = close < prev_bs21
    ma_bear = (sma[3] < sma[5]) & (sma[5] < sma[8]) & (sma[8] < sma[13])
    prior_weak = _last_n_any(breakdown_prior | ma_bear, 5)

    open_ = data.open
    high = data.high
    prev_high = _shift(high, 1)
    up_cut = data.candle_result.cut.up_cut
    prev_up_cut = _shift(up_cut, 1)
    gap_up = (open_ > prev_high) & (close > prev_up_cut)

    big_red = data.candle_result.stick_length.red_medium | data.candle_result.stick_length.red_long
    breakup = big_red & (close > sma[8])

    rule_trigger = gap_up | breakup

    vol = data.volume
    prev_vol = _shift(vol, 1)
    rule_vol = (vol > prev_vol) & (close > prev_close)

    long_knot = data.close_result.knot["long"].flag
    prev_long_knot = _shift(long_knot, 1)
    rule_knot = ~(long_knot & prev_long_knot)

    # 大盤非強空 — 不在崩盤段做多
    rule_market = ~_market_strongly_bearish(data)

    return prior_weak & rule_trigger & rule_vol & rule_knot & rule_market
