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


def _eval_pct_arrays(
    data: "StockData",
) -> tuple[NDArray[np.float32], NDArray[np.float32]]:
    """Compute (long_pct, short_pct) arrays in one evaluate cycle, cached.

    Currently short_pct ≡ -long_pct (~99.98% mirror pair) because every
    ScoreItem registers symmetric ±pts on long/short sides. They diverge
    only in rare knot-rescue cap edge cases. Kept as separate caches so
    callers don't have to know about the symmetry — future ScoreItem or
    rescue changes may break the mirror without rewriting signal code.

    Score system is not vectorized — calls board.evaluate(i) per bar, so we
    pay O(n) once per stock then reuse. Cache lives on `data` so it dies
    with the instance (no cross-process / cross-test leakage).
    """
    long_cache = getattr(data, "_long_pct_cache", None)
    short_cache = getattr(data, "_short_pct_cache", None)
    if long_cache is not None and short_cache is not None:
        return long_cache, short_cache
    from analysis.score import build_scoreboard
    board = build_scoreboard()
    n = data.n
    long_out = np.zeros(n, dtype=np.float32)
    short_out = np.zeros(n, dtype=np.float32)
    for i in range(60, n):
        try:
            r = board.evaluate(data, i)
            long_out[i] = r.total.long.pct
            short_out[i] = r.total.short.pct
        except Exception:
            pass
    data._long_pct_cache = long_out
    data._short_pct_cache = short_out
    return long_out, short_out


def _long_pct_array(data: "StockData") -> NDArray[np.float32]:
    """total.long.pct (-100~+100) per bar. Use for 多方訊號 score conditions."""
    return _eval_pct_arrays(data)[0]


def _short_pct_array(data: "StockData") -> NDArray[np.float32]:
    """total.short.pct (-100~+100) per bar. Use for 空方訊號 score conditions."""
    return _eval_pct_arrays(data)[1]


def _eval_overheat_pct_arrays(
    data: "StockData",
) -> tuple[NDArray[np.float32], NDArray[np.float32]]:
    """Compute (long_pct, short_pct) overheat arrays in one cycle, cached.

    Mirror of _eval_pct_arrays but for OverheatBoard — short-period reversal
    detection channel independent of ScoreBoard. Used by flee post_strength
    gates (and optionally pick/touch) for short-term overheat semantics.
    """
    long_cache = getattr(data, "_overheat_long_pct_cache", None)
    short_cache = getattr(data, "_overheat_short_pct_cache", None)
    if long_cache is not None and short_cache is not None:
        return long_cache, short_cache
    from analysis.overheat_board import build_overheat_board
    board = build_overheat_board()
    n = data.n
    long_out = np.zeros(n, dtype=np.float32)
    short_out = np.zeros(n, dtype=np.float32)
    for i in range(60, n):
        try:
            r = board.evaluate(data, i)
            long_out[i] = r.long.pct
            short_out[i] = r.short.pct
        except Exception:
            pass
    data._overheat_long_pct_cache = long_out
    data._overheat_short_pct_cache = short_out
    return long_out, short_out


def _overheat_long_pct_array(data: "StockData") -> NDArray[np.float32]:
    """overheat long.pct (-100~+100) per bar. Use for 多方訊號 short-term overheat."""
    return _eval_overheat_pct_arrays(data)[0]


def _overheat_short_pct_array(data: "StockData") -> NDArray[np.float32]:
    """overheat short.pct (-100~+100) per bar. Use for 空方訊號 short-term overheat."""
    return _eval_overheat_pct_arrays(data)[1]


# ── Tomorrow-prediction pre ScoreBoard (v191) ──────────────────────────────
# Re-evaluate ScoreBoard assuming close[i+1] = close[i]. Only close-dependent
# cells (turn, sort_normal, distance via SMA) change; events/breadth/volume
# stay frozen. Caller decides whether to gate entries on pre_long/short_pct.

def _compute_pre_sma(data: "StockData") -> dict:
    from analysis.constants import SMA_PERIODS
    close = data.close
    sma = data.close_result.ma.sma
    return {p: sma[p] + (close - _shift(close, p - 1)) / float(p)
            for p in SMA_PERIODS}


def _compute_pre_turn(data: "StockData", n: int) -> dict:
    from analysis.constants import TURN_CONFIGS
    close = data.close
    bs_high = data.close_result.bs.high
    bs_low = data.close_result.bs.low
    pre = {}
    for ma_period, offset, bs_period in TURN_CONFIGS:
        out = np.ones(n, dtype=np.uint8)
        new_offset = offset - 1
        if bs_period is None:
            shifted = _shift(close, new_offset)
            valid = np.arange(n) >= new_offset
            out[valid & (close > shifted)] = 2
            out[valid & (close < shifted)] = 0
        else:
            shifted_hi = _shift(bs_high[bs_period], new_offset)
            shifted_lo = _shift(bs_low[bs_period], new_offset)
            valid = np.arange(n) >= (new_offset + 1)
            out[valid & (close > shifted_hi)] = 2
            out[valid & (close < shifted_lo)] = 0
        pre[ma_period] = out
    return pre


def _compute_pre_sort_normal(pre_sma) -> dict:
    from analysis.close import SortResult
    from analysis.constants import SORT_NORMAL
    out = {}
    for label, (p1, p2, p3) in SORT_NORMAL.items():
        m1, m2, m3 = pre_sma[p1], pre_sma[p2], pre_sma[p3]
        out[label] = SortResult(
            up=(m1 > m2) & (m2 > m3),
            down=(m1 < m2) & (m2 < m3),
        )
    return out


class _PreMAResult:
    __slots__ = ("sma", "pre_sma", "close_on_ma", "bias", "sort_normal",
                 "sort_predicted", "sort_lp")
    def __init__(self, orig_ma, pre_sma, pre_sort_normal):
        self.sma = pre_sma
        self.pre_sma = orig_ma.pre_sma
        self.close_on_ma = orig_ma.close_on_ma
        self.bias = orig_ma.bias
        self.sort_normal = pre_sort_normal
        self.sort_predicted = orig_ma.sort_predicted
        self.sort_lp = orig_ma.sort_lp


class _PreCloseResult:
    __slots__ = ("ma", "boll", "bs", "turn", "ema", "knot",
                 "knot_level", "value_level")
    def __init__(self, orig_cr, pre_ma, pre_turn):
        self.ma = pre_ma
        self.boll = orig_cr.boll
        self.bs = orig_cr.bs
        self.turn = pre_turn
        self.ema = orig_cr.ema
        self.knot = orig_cr.knot
        self.knot_level = orig_cr.knot_level
        self.value_level = orig_cr.value_level


class _PreData:
    """Wraps a StockData for tomorrow-prediction ScoreBoard evaluation."""
    def __init__(self, orig, pre_close_result):
        self._orig = orig
        self.close_result = pre_close_result

    def __getattr__(self, name):
        return getattr(self._orig, name)


def _eval_pre_pct_arrays(
    data: "StockData",
) -> tuple[NDArray[np.float32], NDArray[np.float32]]:
    """Compute (pre_long_pct, pre_short_pct) — ScoreBoard re-evaluated under
    tomorrow's close = today's close assumption. Cached on data instance.
    """
    long_cache = getattr(data, "_pre_long_pct_cache", None)
    short_cache = getattr(data, "_pre_short_pct_cache", None)
    if long_cache is not None and short_cache is not None:
        return long_cache, short_cache
    from analysis.score import build_scoreboard
    n = data.n
    pre_sma = _compute_pre_sma(data)
    pre_turn = _compute_pre_turn(data, n)
    pre_sort = _compute_pre_sort_normal(pre_sma)
    pre_ma = _PreMAResult(data.close_result.ma, pre_sma, pre_sort)
    pre_cr = _PreCloseResult(data.close_result, pre_ma, pre_turn)
    pre_data = _PreData(data, pre_cr)

    board = build_scoreboard()
    long_out = np.zeros(n, dtype=np.float32)
    short_out = np.zeros(n, dtype=np.float32)
    for i in range(60, n):
        try:
            r = board.evaluate(pre_data, i)
            long_out[i] = r.total.long.pct
            short_out[i] = r.total.short.pct
        except Exception:
            pass
    data._pre_long_pct_cache = long_out
    data._pre_short_pct_cache = short_out
    return long_out, short_out


def _pre_long_pct_array(data: "StockData") -> NDArray[np.float32]:
    """Tomorrow-prediction long.pct (-100~+100)."""
    return _eval_pre_pct_arrays(data)[0]


def _pre_short_pct_array(data: "StockData") -> NDArray[np.float32]:
    """Tomorrow-prediction short.pct (-100~+100)."""
    return _eval_pre_pct_arrays(data)[1]


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
      v110: Port Go (D) 嚴閾值版本：階梯改 40/35/30（v109 用 Go 原 35.5/30.5/25.5
           觸發過低，filter 掉的 74 筆反而是好的；提高閾值看是否反轉成功）
      (v111 試移除 rule_macd: pick 自身 PF 0.95→1.20 但 trades 475→17382 過量稀釋
       unified_long PF 1.46→1.42, portfolio -0.018, 已 revert 回 v110)
      (v112 在 v111 基礎再移除 turn[34/55] 翻轉: 邊際改善 pick PF +0.01，
       但 portfolio 仍輸 v110, 已 revert)
      (v113 試 3日窗口: pick PF 0.95→1.15, trades 475→2831, portfolio -0.0035 仍輸 v110)
      (v114 試 2日窗口: pick PF 0.95→1.19, trades 475→1878, portfolio 1.3101 雜訊持平)
      (v115 試 nte 2日 OR gold 2日: pick PF 1.18, portfolio 1.3092 雜訊輸 v110)
      (v116 試 nte 當天 OR gold 當天: macd_gold 是 nte 子集，trades +1 等同 v110)
      (v117: short.nte AND ~weak1, pick trades 313, portfolio peak 1.3111)
      (v120 加 medium.nte OR: trades 313→375, portfolio -0.0019, 已 revert)
      (v121 試 nte 2日 AND ~weak1: pick trades 1759 PF 1.20, portfolio 1.3088)
      v123: v121 + carryover exclude — 今日下降量 > 2*|昨日動能變化| 才排除
            (osc_x < -2 * |prev_osc_x|，「強逆轉」exclude pattern)
            pick trades 811 / PF 1.12 / 勝率 25.2 — pick 自身最佳設計
            (v122 0.97x = status_down 砍光 v121 全部 / v124 3x 砍太少 / v125 1.5x 不
             如 2x — ratio sweep 顯示 2x 是 pick 自身 sweet spot)
            (Portfolio PF 1.3091，輸 v117 peak 1.3111 雜訊 -0.0020)
      (v145 試「反彈無量」排除 (5日內反彈日量<前日量): pick PF 1.14→0.82
       砍 6129 那種 -17.9% extreme loss, max_loss -17.9→-12.5
       但 pick 自身 destructive (-0.32 PF), 即使 unified_long +0.011 / portfolio +0.008
       仍因 pick 訊號自身犧牲太大，已 revert)
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

    # 7. v123: nte 2 日窗口 AND ~weak1 AND ~(carryover-strong-reversal)
    # carryover-strong-reversal: 昨日 nte + 今日無 nte + 今日下降量 > 2*|昨日動能變化|
    # ratio sweep 顯示 2x 是 pick 自身 sweet spot (PF 1.12, 勝率 25.2)
    macd_short = data.macd.short
    nte = macd_short.macd_convergence_nte
    prev_nte = _shift(nte, 1)
    today_no_nte = ~nte
    osc_x = macd_short.osc_x
    prev_osc_x = _shift(osc_x, 1)
    strong_drop = osc_x < -2 * np.abs(prev_osc_x)
    exclude_strong_breakdown = prev_nte & today_no_nte & strong_drop
    rule_macd = (_last_n_any(nte, 2)
                 & ~macd_short.osc_status_up_weak1
                 & ~exclude_strong_breakdown)

    # 8. v110 port Go (D) 嚴閾值版本：階梯 40/35/30（Go 原為 35.5/30.5/25.5）
    # Go (CalculateTrade2.go:7548-7553):
    #   ~(SellAlone[t]>0.355 & SellAlone[t-1]>0.305 & SellAlone[t-2]>0.255
    #     & (close[t]<LD55S[t-1] | close[t-1]<LD55S[t-2] | close[t-2]<LD55S[t-3]))
    short_pct = _short_pct_array(data)
    sp_t = short_pct
    sp_t1 = _shift(short_pct, 1)
    sp_t2 = _shift(short_pct, 2)
    ratchet = (sp_t >= 40) & (sp_t1 >= 35) & (sp_t2 >= 30)

    ld55 = data.close_result.bs.low[55]
    close_t = data.close
    close_t1 = _shift(data.close, 1)
    close_t2 = _shift(data.close, 2)
    break_55 = ((close_t < _shift(ld55, 1)) |
                (close_t1 < _shift(ld55, 2)) |
                (close_t2 < _shift(ld55, 3)))
    rule_not_strong_bear = ~(ratchet & break_55)

    # OSC defense from Go PickCondition was tried in v20 and gutted PF
    # (2.91→1.54): Go's pick "trigger = OSCStatus true" expects momentum
    # already accelerating, which contradicts our "buy the lowest tick"
    # design. Reverted in v21.

    return (rule_pos & rule_change & rule_vol & rule_flood & rule_g19
            & rule_g10 & rule_market & rule_macd & rule_not_strong_bear)


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
         v126 鏡像 pick v123: pte 2日窗口 AND ~osc_status_down_weak1 AND
              ~(prev_pte AND today_no_pte AND strong_rise) carryover-strong-rise
              排除「昨日頂背離今日無 pte 但今日強烈向上反彈」假頂部
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
    # (v178 試 zone-based 鏡像 sell v171，Portfolio PF -0.0068 稀釋失敗已 revert)
    rule_market = _market_any_bear(data)

    # v32 鏡像 sell v25：加短+中尺度 down_hot 排除，避免在底部過熱時摸頭做空
    ms = data.market_state
    rule_not_double_down_hot = ~(ms.short_down_hot & ms.medium_down_hot)

    # 8. v126: 鏡像 pick v123 — pte 2 日窗口 AND ~weak_down1 AND ~carryover_strong_rise
    macd_short = data.macd.short
    pte = macd_short.macd_convergence_pte
    prev_pte = _shift(pte, 1)
    today_no_pte = ~pte
    osc_x = macd_short.osc_x
    prev_osc_x = _shift(osc_x, 1)
    strong_rise = osc_x > 2 * np.abs(prev_osc_x)
    exclude_strong_breakup = prev_pte & today_no_pte & strong_rise
    rule_macd = (_last_n_any(pte, 2)
                 & ~macd_short.osc_status_down_weak1
                 & ~exclude_strong_breakup)

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
      7. OSC 長部位防禦觸發
      8. v106 long_pct gate：long_pct >= 35（v107 試 40 失敗，portfolio -0.014 退步，已 revert）
      9. v131 鏡像 v130 sell：~macd_convergence_pte (不在頂背離)
         （sell 用 ~nte 排除底背離；buy 對偶 ~pte 排除頂背離 = 動能轉弱的金叉狀態）
      (v63 移除原規則 6「不在連續 3 日凸21」: 對直線飆漲股會卡死所有進場)
      OBV 系列對 buy 全部 destructive (5 次驗證, vs v141 base):
        v135 signal_up 5日 → -0.080
        v136 sig_up | (shift & ~down) → -0.138
        v142 signal_up 2日+~down → -0.129
        v143 trend short == 1 → -0.005 (最接近但仍負)
        v144 trend medium == 1 → -0.013
      → buy 不該加 OBV gate (不對稱於 sell 用 signal_down)
    """
    close = data.close
    sma = data.close_result.ma.sma
    turn = data.close_result.turn

    # v188: rule_ma 加 pre 模式 — 量配合時用「明日 sma 預測」提前通過 8>21 排列
    # pre_sma_n[i] = sma_n[i] + (close[i] - close[i-(n-1)]) / n
    volume_status = data.volume_result.volume_status
    vol_strong = volume_status <= 2  # flood / big / high
    pre_sma8 = sma[8] + (close - _shift(close, 7)) / 8.0
    pre_sma21 = sma[21] + (close - _shift(close, 20)) / 21.0
    ma_strict = (close > sma[8]) & (sma[8] > sma[21])
    ma_pre = (close > sma[8]) & vol_strong & (pre_sma8 > pre_sma21)
    rule_ma = ma_strict | ma_pre

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

    # 8 + 6 combined: 3-tier gate by market state (v234)
    #   強空 (any <= -2): long_pct >= 45 (最嚴)
    #   偏空 (any -1, 沒到 -2): long_pct >= 40 (中等)
    #   default (no bear): long_pct >= 35 (最寬鬆)
    # rule_market 取代為 tier gate（取代 v229 之前的 ~strongly_bearish + v230 override）
    long_pct = _long_pct_array(data)
    strongly_bear = _market_strongly_bearish(data)
    moderate_bear_only = _market_any_bear(data) & ~strongly_bear
    rule_long_pct_gate = np.where(
        strongly_bear, long_pct >= 45,
        np.where(moderate_bear_only, long_pct >= 40, long_pct >= 35),
    )

    # 9. v131: ~pte (不在頂背離)
    rule_not_pte = ~data.macd.short.macd_convergence_pte

    return (rule_ma & rule_turn & rule_break & rule_vol & rule_knot
            & rule_osc & rule_long_pct_gate & rule_not_pte)


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
      9. v106 short_pct gate：short_pct >= 35（鏡像 v105 buy 的 long_pct >= 35）
         （v102/v103 sweep 0/20/30 單調改善，鏡像 buy 的閾值往同方向推）
      10. v130 port Go GS11：~macd_convergence_nte（不在底背離）
          （Go SellCondition line 8014 設計核心；sell 完全沒 MACD 條件補上）
      (v132 試 obv.short.trend == -1: portfolio +0.0059 但 sell PF 0.98→0.95 退步)
      (v133 試 obv.medium.trend == -1: 全面更糟 sell PF 0.91 unified_short 0.976)
      (v134 試 OBV signal_down 5日: portfolio +0.068 大躍進 1.4276 peak)
      (v137 試加 ~today_signal_up: 僅砍 4 筆 dead filter，等同 v134)
      (v138: OBV medium signal_down 5日 portfolio 1.4495)
      (v139 試 medium + ~signal_up: 0 筆變化, dead filter, 完全等同 v138)
      (v140: short signal_down 3日 AND ~today signal_up, portfolio 1.4526 peak)
      11. v141: 縮至 2 日窗口 (今日 OR 昨日有 signal_down AND ~today signal_up)
      (v63 移除原規則 6「不在連續 3 日凹21」: 對直線崩跌股會卡死所有進場)
      (v96 嘗試加 short_ma_bear 失敗，trades 只 -0.94%、PF 無變化，已 revert)
      (v97 嘗試加 turn[3]==0 共振失敗，0 trades 變化、PF 無變化，已 revert)
      (v100 加 A short_pct>=0 + C short_pct rising 失敗，trades -1223 但 PF 反降 0.01)
      (v101 單獨 C short_pct rising 失敗，trades -1108 但 PF 反降 0.01)
    """
    close = data.close
    sma = data.close_result.ma.sma
    turn = data.close_result.turn

    # v189: rule_ma 加 pre 模式 (鏡像 v188 buy) — 量配合時用「明日 sma 預測」
    # 提前通過 8<21 排列。飆跌股初期 sma8 還沒跌破 sma21，但若 status<=2 + 預測明日翻轉 → 准予進場做空
    volume_status_s = data.volume_result.volume_status
    vol_strong_s = volume_status_s <= 2
    pre_sma8_s = sma[8] + (close - _shift(close, 7)) / 8.0
    pre_sma21_s = sma[21] + (close - _shift(close, 20)) / 21.0
    ma_strict_short = (close < sma[8]) & (sma[8] < sma[21])
    ma_pre_short = (close < sma[8]) & vol_strong_s & (pre_sma8_s < pre_sma21_s)
    rule_ma = ma_strict_short | ma_pre_short
    # v171: sell 允許進場的市場狀態 = 任一時框 trend ∈ {0 中性, -1 空, -2 強空}.
    # 排除多頭 (+1/+2)、多頭衰竭 (+3)、空頭衰竭 (-3, 將反轉)。
    # (v172 試拿掉 medium scope 實測 portfolio PF 持平 1.5513，保留 medium 多元性)
    ms_init = data.market_state
    def _in_sell_zone(t):
        return (t == 0) | (t == -1) | (t == -2)
    rule_market = (
        _in_sell_zone(ms_init.short_trend)
        | _in_sell_zone(ms_init.medium_trend)
        | _in_sell_zone(ms_init.long_trend)
    )

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

    # 11. v175: short_pct gate 試 40 (v174 試 25 退步 -0.015, 反向收緊)
    rule_short_pct_gate = _short_pct_array(data) >= 40

    # 12. v130 port Go GS11：~nte (不在底背離 — 排除「死叉但動能轉強」的反彈段)
    rule_not_nte = ~data.macd.short.macd_convergence_nte

    # 13. v141: OBV short signal_down 2日 AND 今日不是 signal_up
    obv_short = data.obv.short
    rule_obv_bearish = (_last_n_any(obv_short.signal_down, 2)
                        & ~obv_short.signal_up)

    return (rule_ma & rule_turn & rule_break & rule_vol & rule_knot
            & rule_market & rule_not_double_down_hot & rule_osc
            & rule_medium_ma_bear
            & rule_short_pct_gate & rule_not_nte
            & rule_obv_bearish)


# ── BuyFleeSignal / SellFleeSignal (多翻空 / 空翻多) ────────────────────────


# Pct equivalents of original raw-score thresholds (v98 unit change).
# Raw score range is ±279.52 (stock-invariant), so pct = score / 2.7952.
# Original thresholds: ±75 → ±26.83; ±115 → ±41.14.
# v167: delta1 20 → 15. v166 (d=20) bait_flip cascade 新增 114 trades 平均
# +10.4% 淨報酬 (Portfolio PF +0.0114). 繼續推下限看是否還沒到頂.
_PCT_DELTA1_THRESHOLD = 15.0            # was 20 (v166) / 25 (v165) / 26.83 (v163)
_PCT_DELTA3_THRESHOLD = 115.0 / 2.7952  # ≈ 41.14 (~P0.5 of 3-day delta)


def _buy_flee_main(data: "StockData") -> BoolArray:
    """多翻空 main — 純 1 日急升 (rise1).

    v159: rise3 拿掉 (PF 0.85 累積形態趨勢尾段). rise1_only PF 3.05.
    v199a-g: rise_continuous mirror v197 全失敗（-0.0076 ~ -0.046）.
      不論長/短窗、ratio/threshold/搭配 stairstep gate/搭配 below_prev 皆 destructive.
      buy_flee 結構性對累積形態免疫 — 市場下跌是 event-style，慢累積 short_pct
      只是 lagging 反映過去跌幅，等到事件 rise1 才是 entry.
    """
    short_pct = _short_pct_array(data)
    prev1 = _shift(short_pct, 1)
    delta1 = short_pct - prev1
    return (delta1 >= _PCT_DELTA1_THRESHOLD) & (prev1 <= 0)


def _sell_flee_main(data: "StockData") -> BoolArray:
    """空翻多 main 子句 (v91 score-rise, 不含 bait_flip).

    多方訊號用 long_pct:
      rise1 = delta1>=26.83 & prev1<=0  (P0.5 級 1 日急升)
      rise3 = delta3>=80   & prev3<=0   (v163: 41.14→80 after sweep)
      v197: rise_continuous = 5天內3+天d1>0 + F加速(last2≥0.7×delta)
            + delta_from_min4>=55 + min_prev4<=10
            (補 rise1/rise3 漏抓的中速 V 形反轉，sweep peak +0.0052 vs v195)

    v163: rise3 delta3 41.14→80. dry-run sweep 顯示與 buy_flee 反向：
      sell_flee rise3 越極端 PF 越高 (d3≥80 PF 1.88 vs d3≥41 PF 1.38)，
      物理上是「3 日多方分數爆發性累積 = 真正多頭起點」，跟 buy_flee
      rise3 (越極端越像下跌透支) 完全相反。
    """
    long_pct = _long_pct_array(data)
    prev1 = _shift(long_pct, 1)
    prev3 = _shift(long_pct, 3)
    delta1 = long_pct - prev1
    delta3 = long_pct - prev3

    rise1 = (delta1 >= _PCT_DELTA1_THRESHOLD) & (prev1 <= 0)
    rise3 = (delta3 >= 80.0) & (prev3 <= 0)  # v163: was _PCT_DELTA3_THRESHOLD (41.14)
    # v197: rise_continuous — 中速 V 形反轉補捉路徑
    d1_y1 = _shift(delta1, 1)
    d1_y2 = _shift(delta1, 2)
    d1_y3 = _shift(delta1, 3)
    d1_y4 = _shift(delta1, 4)
    pos_count = ((delta1 > 0).astype(np.int8) + (d1_y1 > 0).astype(np.int8)
                 + (d1_y2 > 0).astype(np.int8) + (d1_y3 > 0).astype(np.int8)
                 + (d1_y4 > 0).astype(np.int8))
    cond_3of5 = pos_count >= 3
    prev1_lp = _shift(long_pct, 1)
    prev2_lp = _shift(long_pct, 2)
    prev3_lp = _shift(long_pct, 3)
    prev4_lp = _shift(long_pct, 4)
    min_prev4 = np.minimum(np.minimum(prev1_lp, prev2_lp),
                            np.minimum(prev3_lp, prev4_lp))
    delta_from_min4 = long_pct - min_prev4
    # F-accel: 後段集中度 ≥ 0.7（最近 2 天 d1 加總佔整段累積漲幅 ≥ 70%）
    last2_sum = delta1 + d1_y1
    cond_accel_F = last2_sum >= 0.7 * delta_from_min4
    rise_continuous = (cond_3of5 & cond_accel_F
                       & (delta_from_min4 >= 55.0) & (min_prev4 <= 10))
    return rise1 | rise3 | rise_continuous


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

    v127: main + post_strength gate (short_pct >= 30 確認進場時空方分數已偏空)
    main 內 rise1 已保證 short_pct >= 26.83 / rise3 >= 41.14 (with prev<=0)
    但 prev<=0 允許負值，實際 short_pct 進場時可能 < 30
    (v128 試 25 略輸 v127 +0.0008 雜訊內)
    v146: 加 ~long_knot 排除糾結 (鏡像 buy/sell pattern) - 失敗 PF -0.02
    v147: 改試 medium scope knot (SMA5/13/34) - 中時框糾結 持平
    v148: long+medium 並集排除 (兩 scope 任一糾結都排除)
    v152: gate 30 -> 35 (after ScoreBoard ~dead tuning, short_pct dist shifted lower)
          v151 (gate 25) / v153 (no gate) / v154 (dynamic) sweep:
          portfolio PF 1.488 / 1.498 / 1.459 / 1.469 → v152 win
    v155: stairstep gate — gate≥35 永遠過 OR (delta1≥25 & gate≥30) 也過。
          v152 的 gate=35 把 short_pct ∈ [30,35) 區間 main 真的優質事件砍掉了
          (dry-run d25_g30 差集 PF 1.86 / 勝率 50% / 94 trades). 階梯設計把
          有強翻轉動量的 [30,35) 段救回來，弱翻轉維持嚴 gate.
    """
    main = _buy_flee_main(data)
    short_pct = _short_pct_array(data)
    rule_post_strength = short_pct >= 30  # v160: gate 35→30 after v159 dropped rise3 (3D sweep PF 1.55 vs gate35 PF 1.44)
    rule_knot = ~(data.close_result.knot["long"].flag
                  | data.close_result.knot["medium"].flag)
    return main & rule_post_strength & rule_knot


def sell_flee_signal(data: "StockData") -> BoolArray:
    """空翻多訊號 — long_entry candidate.

    v129 = (main AND long_pct >= 30) OR bait_flip_up
    鏡像 v127 buy_flee 的 post_strength gate 僅套 main 部分
    bait_flip_up 為 gap-up 事件，當天 long_pct 未必反映，gate 不套

    bait_flip_up: 昨日 buy_flee 觸發 (誘空訊號) + 今日 gap_up (翻多打臉)
      → Go SellFlee subclause v36 的「假崩跌反彈再翻多」事件.
    v146: 加 ~long_knot 排除糾結 (鏡像 buy/sell pattern) - 失敗 PF -0.01
    v147: 改試 medium scope knot - 持平
    v148: long+medium 並集排除
    """
    main = _sell_flee_main(data)
    long_pct = _long_pct_array(data)
    # v197: post_strength gate 用 delta_from_min2（單日急升 → 2 日 V 形累積）
    prev1_lp = _shift(long_pct, 1)
    prev2_lp = _shift(long_pct, 2)
    min_prev2 = np.minimum(prev1_lp, prev2_lp)
    delta_from_min2 = long_pct - min_prev2
    rule_post_strength = (
        (long_pct >= 35)
        | ((delta_from_min2 >= 40) & (long_pct >= 30))
        | ((delta_from_min2 >= 50) & (long_pct >= 25))
    )
    # v195b: SB stairstep gate AND above_prev (站上前次洪量 high) — 強化既有 path
    # v195 OR 路徑 +4689 trades / -0.092 PF 失敗，改用 AND 收緊
    above_prev = data.volume_result.above_prev
    main_with_gate = main & rule_post_strength & above_prev

    prev_buy_flee_main = _shift(_buy_flee_main(data), 1)
    gap_up = _gap_up_today(data)
    bait_flip_up = prev_buy_flee_main & gap_up

    rule_knot = ~(data.close_result.knot["long"].flag
                  | data.close_result.knot["medium"].flag)
    return (main_with_gate | bait_flip_up) & rule_knot
