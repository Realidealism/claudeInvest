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


def _market_any_bear(data: "StockData") -> BoolArray:
    """At least one scope shows bear (trend code <= -1). Stricter than
    ~strongly_bullish — also filters out neutral markets."""
    ms = data.market_state
    return (ms.short_trend <= -1) | (ms.medium_trend <= -1) | (ms.long_trend <= -1)


# ── MarketShortTrendStrong / Weak (mirror Go calculatetrade3.go:4151-4188) ──
#
# These are "trend-reversal early warning" flags computed per stock-day from
# (a) the stock's own SMA-sort code (Go ShortTrend 0/1/2, Python's
# sort_normal[scope].up/.down) and (b) the market's percentage status /
# direction. Naming caveat: "Strong" = strong bear-onset signal, "Weak" =
# strong bull-onset signal — they flag the inflection, not the steady state.


def _stock_trend_code(data: "StockData", scope: str) -> NDArray[np.int8]:
    """Map sort_normal up/down to Go's ShortTrend 0/1/2 encoding.

    2 = SMA bullish alignment (sort_normal.up)
    0 = SMA bearish alignment (sort_normal.down)
    1 = neither (transitional)
    """
    sort_normal = data.close_result.ma.sort_normal[scope]
    out = np.full(data.n, 1, dtype=np.int8)
    out[sort_normal.up] = 2
    out[sort_normal.down] = 0
    return out


def _market_trend_strong(data: "StockData", scope: str) -> BoolArray:
    """Bear-onset early warning: stock SMA bullish (or transitional + market
    rolling over) AND market down-direction is rising while up-direction is
    not. Mirrors Go MarketShortTrendStrong."""
    ms = data.market_state
    if scope == "short":
        status = ms.short_pct_status
        up_dir = ms.short_up_dir
        down_dir = ms.short_down_dir
    elif scope == "medium":
        status = ms.medium_pct_status
        up_dir = ms.medium_up_dir
        down_dir = ms.medium_down_dir
    else:
        status = ms.long_pct_status
        up_dir = ms.long_up_dir
        down_dir = ms.long_down_dir
    trend = _stock_trend_code(data, scope)
    cond_a = (trend == 2) | ((trend == 1) & (status < 2))
    cond_b = (~up_dir) & (status < 4) & down_dir
    return cond_a & cond_b


def _market_trend_weak(data: "StockData", scope: str) -> BoolArray:
    """Bull-onset early warning: stock SMA bearish (or transitional + market
    rolling over) AND market up-direction is rising while down-direction is
    not. Mirrors Go MarketShortTrendWeak."""
    ms = data.market_state
    if scope == "short":
        status = ms.short_pct_status
        up_dir = ms.short_up_dir
        down_dir = ms.short_down_dir
    elif scope == "medium":
        status = ms.medium_pct_status
        up_dir = ms.medium_up_dir
        down_dir = ms.medium_down_dir
    else:
        status = ms.long_pct_status
        up_dir = ms.long_up_dir
        down_dir = ms.long_down_dir
    trend = _stock_trend_code(data, scope)
    cond_a = (trend == 0) | ((trend == 1) & (status > 3))
    cond_b = up_dir & (status > 1) & (~down_dir)
    return cond_a & cond_b


# ── OSC defense triggers (mirror Go Pick/Buy 7618-7630 and Touch/Sell 7822-7832) ──
#
# Both directions share a "trigger AND NOT (fail-safe stack)" pattern.
# Direction differs only by which OSCStatus value lights up (true for long
# entries, false for short) and which side of PercentageStatus / Hot the
# fail-safes target.


def _osc_long_trigger(data: "StockData") -> BoolArray:
    """Pick / Buy OSC defense (long entry side)."""
    ms = data.market_state
    osc = data.macd.short
    direction = osc.osc_direction
    prev_direction = _shift(direction, 1)
    up_weak2 = osc.osc_status_up_weak2
    down_weak2 = osc.osc_status_down_weak2
    prev_down_weak2 = _shift(down_weak2, 1)

    short_trend = _stock_trend_code(data, "short")
    market_strong = _market_trend_strong(data, "short")
    short_dn_hot = ms.short_down_hot
    short_dn_hot_2d = short_dn_hot | _shift(short_dn_hot, 1)

    fail_safe = (
        (ms.short_pct_status < 2)
        & ~(direction & prev_direction & ~up_weak2)
        & ~((short_trend == 2) & direction)
        & ~(down_weak2 & prev_down_weak2)
        & ~market_strong
        & ~(short_dn_hot_2d & direction)
    )
    trigger = osc.osc_status
    return trigger & ~fail_safe


def _osc_short_trigger(data: "StockData") -> BoolArray:
    """Touch / Sell OSC defense (short entry side)."""
    ms = data.market_state
    osc = data.macd.short
    direction = osc.osc_direction
    prev_direction = _shift(direction, 1)
    up_weak2 = osc.osc_status_up_weak2
    prev_up_weak2 = _shift(up_weak2, 1)
    down_weak2 = osc.osc_status_down_weak2

    short_trend = _stock_trend_code(data, "short")
    market_weak = _market_trend_weak(data, "short")
    short_up_hot = ms.short_up_hot
    short_up_hot_2d = short_up_hot | _shift(short_up_hot, 1)

    fail_safe = (
        (ms.short_pct_status > 3)
        & ~(~direction & ~prev_direction & ~down_weak2)
        & ~((short_trend == 0) & ~direction)
        & ~(up_weak2 & prev_up_weak2)
        & ~market_weak
        & ~(short_up_hot_2d & ~direction)
    )
    trigger = ~osc.osc_status
    return trigger & ~fail_safe


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
      8. MACD short 底背離 (convergence_nte)：死叉狀態但動能轉強
         — touch v12 用 PTE (頂背離) 從 PF 0.86→1.00 成功，這版鏡像試 NTE 給 pick
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

    # 8. MACD short 底背離 — 死叉狀態但動能轉強 = 底部 likely
    rule_macd = data.macd.short.macd_convergence_nte

    # OSC defense from Go PickCondition was tried in v20 and gutted PF
    # (2.91→1.54): Go's pick "trigger = OSCStatus true" expects momentum
    # already accelerating, which contradicts our "buy the lowest tick"
    # design. Reverted in v21.

    return (rule_pos & rule_change & rule_vol & rule_flood & rule_knot
            & rule_concave & rule_market & rule_macd)


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
      8. MACD short 頂背離 (convergence_pte)：金叉狀態但動能轉弱
         — Go ShortMACDConvergencePTE 用於 BuyCondition exclusion，touch 反向用為
           positive trigger。v11 用 ~death_gold (狀態確認) 失敗，改用 PTE 早期偵測。
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

    # v31 鏡像 sell v24：從「~strongly_bullish」改「any_bear」，要求市場已有空方順風
    rule_market = _market_any_bear(data)

    # v32 鏡像 sell v25：加短+中尺度 down_hot 排除，避免在底部過熱時摸頭做空
    ms = data.market_state
    rule_not_double_down_hot = ~(ms.short_down_hot & ms.medium_down_hot)

    # 8. MACD short 頂背離 — 早期偵測「金叉狀態但動能轉弱」= 頂部 likely
    rule_macd = data.macd.short.macd_convergence_pte

    # OSC defense from Go TouchCondition reverted in v21 — same reason as
    # pick: Go's "trigger = OSCStatus false" finds momentum-fading entries,
    # not topping-tail entries which is what our touch is hunting.

    return (rule_pos & rule_change & rule_vol & rule_flood & rule_knot
            & rule_convex & rule_market & rule_not_double_down_hot & rule_macd)


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

    # 8. OSC 防禦觸發（Go BuyCondition line 7866-7877，trigger_main 部分）
    rule_osc = _osc_long_trigger(data)

    return (rule_ma & rule_turn & rule_break & rule_vol & rule_knot
            & rule_convex & rule_market & rule_osc)


def sell_condition(data: "StockData") -> BoolArray:
    """波段空訊 — short_entry candidate; mirror of buy_condition.

    Rules:
      1. MA 排列：close < SMA8 AND SMA8 < SMA21
      2. 下扣 5 (turn[5] == 0)
      3. 跌破：close < prev close.bs.low[8]
      4. 量能配合：今日量 >= VD5
      5. 非長均糾結
      6. 不在連續 3 日凹21
      7. 大盤要求至少一尺度 bear (trend <= -1)：強化過濾，排除中性市場下的隨機跌破
         （v24 從「非強多」改「至少一尺度 bear」）
      8. 排除短+中尺度 down_hot 末端追空陷阱（v25 從三尺度 AND 縮為短+中尺度，
         觸發率從 1.1% 拉到 ~3-5%；原 Go 規則 line 8038-8040 為三尺度 AND）
      9. v58 加個股 medium scope MA 全空頭：SMA5 < SMA13 < SMA34
         （sort_normal["medium"].down 確認中期 MA 結構也已轉空）
    """
    close = data.close
    sma = data.close_result.ma.sma
    turn = data.close_result.turn

    rule_ma = (close < sma[8]) & (sma[8] < sma[21])
    rule_market = _market_any_bear(data)

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

    ms = data.market_state
    rule_not_double_down_hot = ~(ms.short_down_hot & ms.medium_down_hot)

    # 9. OSC 防禦觸發（Go SellCondition line 8003-8012）
    rule_osc = _osc_short_trigger(data)

    # 10. v58 個股 medium scope MA 全空頭排列（SMA5<SMA13<SMA34）
    rule_medium_ma_bear = data.close_result.ma.sort_normal["medium"].down

    return (rule_ma & rule_turn & rule_break & rule_vol & rule_knot
            & rule_concave & rule_market & rule_not_double_down_hot & rule_osc
            & rule_medium_ma_bear)


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

    # 6. MACD short 頂背離 — 鏡像 v12 touch+PTE 成功 pattern，給 reversal 短做空
    rule_macd = data.macd.short.macd_convergence_pte

    return prior_strong & rule_trigger & rule_vol & rule_knot & rule_market & rule_macd


def sell_flee_signal(data: "StockData") -> BoolArray:
    """空翻多訊號 — long_entry candidate; mirror of buy_flee_signal.

    Rules:
      1. 前提弱勢：最近 5 日有過 (close < prev Donchian-21-down OR SMA 空頭排列)
      2. 翻轉觸發：跳空站上前日高 AND close > prev up_cut
         OR 大紅K 站上 SMA8
      3. 量增上漲
      4. 非長均糾結 2 日

    Plus Go subclause A (v36): yesterday's BuyFlee + today's gap_up
    fires regardless of the other rules — captures "failed top reversal
    bounces back" pattern (Go CalculateTrade2.go:8098-8100).
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

    # MACD short 底背離 — 鏡像 v13 pick+NTE 成功 pattern，給 reversal 短做多
    rule_macd = data.macd.short.macd_convergence_nte

    main_clause = (
        prior_weak & rule_trigger & rule_vol & rule_knot & rule_market & rule_macd
    )

    # Go 子句 A：昨日 BuyFlee 觸發後今日 gap_up → 假崩跌反彈再翻多
    # gap_up 已包含 open>prev_high AND close>prev_up_cut，等同 Go 8098-8100
    prev_buy_flee = _shift(buy_flee_signal(data), 1)
    flip_after_bf = prev_buy_flee & gap_up

    return main_clause | flip_after_bf
