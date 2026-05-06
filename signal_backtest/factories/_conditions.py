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


def _long_score_array(data: "StockData") -> NDArray[np.float32]:
    """Compute long_score for every bar, cached on the StockData instance.

    Score system is not vectorized — calls board.evaluate(i) per bar, so we
    pay O(n) once per stock then reuse. Cache lives on `data` so it dies with
    the instance (no cross-process / cross-test leakage).
    """
    cached = getattr(data, "_long_score_cache", None)
    if cached is not None:
        return cached
    from analysis.score import build_scoreboard
    board = build_scoreboard()
    n = data.n
    out = np.zeros(n, dtype=np.float32)
    for i in range(60, n):
        try:
            out[i] = board.evaluate(data, i).total.long.score
        except Exception:
            pass
    data._long_score_cache = out
    return out


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
         AND v81: 最近 3 日內 OverLower3 (Go 原版 #21 雙重位置確認)
         — v4 嘗試過用 OverLower3/5/8 複合 pattern 取代 bs_low，PF 反而下降；
           v81 改用「保留 bs_low + AND 上 OL3 雙重確認」鏡像 Go #20+#21
      2. 最近 3 日有出現中或長期扣抵翻轉 (turn[34/55] 從 0 翻 >=1)
      3. 量能未乾枯：今日量 >= 前日量 * 0.5 AND not (連 3 日 SmallLongMinVolume)
      4. 不在 tier-1 洪量區下方 (非 Flood 斷頭段)
      5. v82 改用 Go G19: 排除「短均糾結連2 + 中均糾結任一 + 不在凹13」
         — Go 邏輯：凹13 反而是底部 cycle 訊號，糾結但不在凹底才是真壞
         — 取代 v78-v81 的「連3日凹13 排除」(概念相反)
      6. 大盤非強空（任一尺度 trend <= -2）— 不在崩盤段抄底
      7. MACD short 底背離 (convergence_nte)：死叉狀態但動能轉強
         — touch v12 用 PTE (頂背離) 從 PF 0.86→1.00 成功，這版鏡像試 NTE 給 pick
      v79: 試拿掉「不在連續 3 日凹13」失敗（trades +50% 但 PF 0.34→0.29），已還原
      v80: 拿掉原規則 5「非長均糾結連 2 日」— knot 多代表盤整尾聲反而接近反彈點
      v81: rule_pos 加 OL3 雙重位置確認（Go #21）
      v82: rule_concave (禁凹13) 改成 Go G19 (糾結+不凹13 才禁) — 凹13 反向為正
      v83: rule_change 補 Go G22 (medium_hl 任一[0..2] OR long_hl 任一[0..4])
           — 動能轉強來源加入「峰的高低變」
      v84: 加 Go G10 排除 — 量衰退 (sma5<sma13) AND 多重量縮比例 (VD13<VD21*0.55 OR ...)
           — 排除「縮量陰跌」段
    """
    n = data.n
    close = data.close
    bs_low = data.close_result.bs.low
    turn = data.close_result.turn

    # 1. 觸或跌破 5/8 日低 AND v81: 最近 3 日內 OverLower3 雙重確認
    bs58 = (close <= bs_low[5]) | (close <= bs_low[8])
    ol3 = data.over_breakout.over_lower_3
    rule_pos = bs58 & _last_n_any(ol3, 3)

    # 6. 大盤過濾
    rule_market = ~_market_strongly_bearish(data)

    # 2. v83 補 Go G22: 扣抵翻轉(3日內) OR 中峰任一[0..2] OR 長峰任一[0..4]
    long_change = _turn_change_up(turn[34]) | _turn_change_up(turn[55])
    medium_hl = data.candle_result.hl.medium_hl
    long_hl = data.candle_result.hl.long_hl
    rule_change = (_last_n_any(long_change, 3)
                   | _last_n_any(medium_hl, 3)
                   | _last_n_any(long_hl, 5))

    # 3. 量能未乾枯
    vol = data.volume
    prev_vol = _shift(vol, 1)
    long_min = data.volume_result.extremes["long"].small
    rule_vol = (vol >= prev_vol * 0.5) & ~_last_n_all(long_min, 3)

    # 4. 非 tier-1 洪量斷頭
    rule_flood = ~data.volume_result.below_tier[1]

    # 5. v82 Go G19: 排除「短均糾結連2 + 中均糾結任一 + 不在凹13」
    concave13 = data.candle_result.concave_n[13]
    short_knot = data.close_result.knot["short"].flag
    medium_knot = data.close_result.knot["medium"].flag
    short_knot_2 = short_knot & _shift(short_knot, 1)
    medium_knot_any3 = medium_knot | _shift(medium_knot, 1) | _shift(medium_knot, 2)
    rule_g19 = ~(short_knot_2 & medium_knot_any3 & ~concave13)

    # v84 Go G10: 排除「量衰退 (sma5<sma13) AND 多重量縮比例任一」
    vs = data.volume_result.sma
    vh = data.volume_result.high
    weakening = vs[5] < vs[13]
    shrink_ratio = ((vh[13] < vh[21] * 0.55) |
                    (vh[8] < vh[13] * 0.34) |
                    (vh[5] < vh[8] * 0.21))
    rule_g10 = ~(weakening & shrink_ratio)

    # 7. MACD short 底背離 — 死叉狀態但動能轉強 = 底部 likely
    rule_macd = data.macd.short.macd_convergence_nte

    # OSC defense from Go PickCondition was tried in v20 and gutted PF
    # (2.91→1.54): Go's pick "trigger = OSCStatus true" expects momentum
    # already accelerating, which contradicts our "buy the lowest tick"
    # design. Reverted in v21.

    return (rule_pos & rule_change & rule_vol & rule_flood & rule_g19
            & rule_g10 & rule_market & rule_macd)


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
      6. 大盤非強空 — 不在崩盤段做多
      (v63 移除原規則 6「不在連續 3 日凸21」: 對直線飆漲股會卡死所有進場)
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

    # 7. OSC 防禦觸發（Go BuyCondition line 7866-7877，trigger_main 部分）
    rule_osc = _osc_long_trigger(data)

    return (rule_ma & rule_turn & rule_break & rule_vol & rule_knot
            & rule_market & rule_osc)


def sell_condition(data: "StockData") -> BoolArray:
    """波段空訊 — short_entry candidate; mirror of buy_condition.

    Rules:
      1. MA 排列：close < SMA8 AND SMA8 < SMA21
      2. 下扣 5 (turn[5] == 0)
      3. 跌破：close < prev close.bs.low[8]
      4. 量能配合：今日量 >= VD5
      5. 非長均糾結
      6. 大盤要求至少一尺度 bear (trend <= -1)：強化過濾，排除中性市場下的隨機跌破
         （v24 從「非強多」改「至少一尺度 bear」）
      7. 排除短+中尺度 down_hot 末端追空陷阱（v25 從三尺度 AND 縮為短+中尺度，
         觸發率從 1.1% 拉到 ~3-5%；原 Go 規則 line 8038-8040 為三尺度 AND）
      8. v58 加個股 medium scope MA 全空頭：SMA5 < SMA13 < SMA34
         （sort_normal["medium"].down 確認中期 MA 結構也已轉空）
      (v63 移除原規則 6「不在連續 3 日凹21」: 對直線崩跌股會卡死所有進場)
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

    ms = data.market_state
    rule_not_double_down_hot = ~(ms.short_down_hot & ms.medium_down_hot)

    # 9. OSC 防禦觸發（Go SellCondition line 8003-8012）
    rule_osc = _osc_short_trigger(data)

    # 10. v58 個股 medium scope MA 全空頭排列（SMA5<SMA13<SMA34）
    rule_medium_ma_bear = data.close_result.ma.sort_normal["medium"].down

    return (rule_ma & rule_turn & rule_break & rule_vol & rule_knot
            & rule_market & rule_not_double_down_hot & rule_osc
            & rule_medium_ma_bear)


# ── BuyFleeSignal / SellFleeSignal (多翻空 / 空翻多) ────────────────────────


def _buy_flee_main(data: "StockData") -> BoolArray:
    """多翻空 main — 純 score-based (鏡像 sell_flee 的 P0.5 級閾值).

    long_score 從中性/正分急降到負 = 多頭翻空頭：
      fall1 = delta1 <= -75 & prev1 >= 0
      fall3 = delta3 <= -115 & prev3 >= 0
    """
    long_score = _long_score_array(data)
    prev1 = _shift(long_score, 1)
    prev3 = _shift(long_score, 3)
    delta1 = long_score - prev1
    delta3 = long_score - prev3

    fall1 = (delta1 <= -75) & (prev1 >= 0)
    fall3 = (delta3 <= -115) & (prev3 >= 0)
    return fall1 | fall3


def _sell_flee_main(data: "StockData") -> BoolArray:
    """空翻多 main 子句 (v91 score-rise, 不含 bait_flip).

    P0.5 級閾值: rise1 = delta1>=75 & prev1<=0, rise3 = delta3>=115 & prev3<=0.
    """
    long_score = _long_score_array(data)
    prev1 = _shift(long_score, 1)
    prev3 = _shift(long_score, 3)
    delta1 = long_score - prev1
    delta3 = long_score - prev3

    rise1 = (delta1 >= 75) & (prev1 <= 0)
    rise3 = (delta3 >= 115) & (prev3 <= 0)
    return rise1 | rise3


def _gap_up_today(data: "StockData") -> BoolArray:
    """Today gap up: open > prev_high AND close > prev candle.cut.up_cut."""
    open_ = data.open
    high = data.high
    close = data.close
    prev_high = _shift(high, 1)
    up_cut = data.candle_result.cut.up_cut
    prev_up_cut = _shift(up_cut, 1)
    return (open_ > prev_high) & (close > prev_up_cut)


def _gap_down_today(data: "StockData") -> BoolArray:
    """Today gap down: open < prev_low AND close < prev candle.cut.down_cut."""
    open_ = data.open
    low = data.low
    close = data.close
    prev_low = _shift(low, 1)
    down_cut = data.candle_result.cut.down_cut
    prev_down_cut = _shift(down_cut, 1)
    return (open_ < prev_low) & (close < prev_down_cut)


def buy_flee_signal(data: "StockData") -> BoolArray:
    """多翻空訊號 — short_entry candidate.

    純 score-based main (P0.5 閾值，鏡像 sell_flee).
    """
    return _buy_flee_main(data)


def sell_flee_signal(data: "StockData") -> BoolArray:
    """空翻多訊號 — long_entry candidate.

    v93 = main (v91 score-rise) OR bait_flip_up (誘空翻多, 鏡像 buy_flee)

    bait_flip_up: 昨日 buy_flee 觸發 (誘空訊號) + 今日 gap_up (翻多打臉)
      → Go SellFlee subclause v36 的「假崩跌反彈再翻多」事件.
    """
    main = _sell_flee_main(data)

    prev_buy_flee_main = _shift(_buy_flee_main(data), 1)
    gap_up = _gap_up_today(data)
    bait_flip_up = prev_buy_flee_main & gap_up

    return main | bait_flip_up
