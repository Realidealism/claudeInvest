"""Pick (抄底) and Touch (摸頭) signal factories.

  pick  : long_entry  = PickCondition,  long_exit  = BuyFleeSignal
  touch : short_entry = TouchCondition, short_exit = SellFleeSignal

Defense (v43 selective flood, after probing all variants):
  Floor-ratchet on intraday H/L (long 13d / short 8d after v38) plus
  signal-specific flood rules where they help:
    pick  — flood_recent5 → LL8 (best long-side gain, +0.06 PF)
    touch — flood (single day) → HH3 (modest +0.01 PF)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

from analysis.chandelier import calculate_chandelier
from analysis.indicators import rolling_highest, rolling_lowest
from signal_backtest.signal import DefenseRule, SignalSet, SignalSpec
from signal_backtest.factories._conditions import (
    _long_pct_array,
    _short_pct_array,
    _shift,
    _transient_giveback_exit,
    pick_condition,
    touch_condition,
    buy_flee_signal,
    sell_flee_signal,
)

if TYPE_CHECKING:
    from backtest.data import StockData


def pick_signal(data: "StockData") -> SignalSpec:
    """抄底 (long-only)."""
    n = data.n
    not_dead_fish = ~data.money_result.dead  # money_level >= 3 (>= 9M turnover)
    long_entry = pick_condition(data) & not_dead_fish
    long_exit = buy_flee_signal(data)
    zero = np.zeros(n, dtype=np.bool_)

    flood = data.volume_result.flood
    flood_recent5 = rolling_highest(flood.astype(np.float32), 5) > 0
    # v56: 多方 stagnation 視窗 8d → 13d（給更多天數）
    close_hi13 = rolling_highest(data.close, 13)
    at_13d_high = data.close >= close_hi13
    any_high_in_13d = rolling_highest(at_13d_high.astype(np.float32), 13) > 0.5
    stagnant_long = ~any_high_in_13d
    # v202b: 鏡像 v200 sell_flee score-surge defense
    long_pct = _long_pct_array(data)
    lp_d1 = long_pct - _shift(long_pct, 1)
    vol_strong = data.volume_result.volume_status <= 2  # flood/big/high
    score_surge_with_vol = (lp_d1 > 15.0) & vol_strong
    # v227: Chandelier(21, 6.0) on pick — sweep 4-12 結論：mult=6 為 +0.011 峰值
    chand = calculate_chandelier(
        data.high.astype(np.float64), data.low.astype(np.float64),
        data.close.astype(np.float64), length=21, mult=6.0, use_close=True,
    )
    chand_long = chand.long_stop.astype(np.float32)
    chand_trigger = ~np.isnan(chand_long)
    # v202h: K 棒 exhaustion defense 加到 pick 退步 -0.022（pick 低 PF 誤殺成本高）, 不採用
    # v261: 鏡像 v253 — 進場後第 3 天，若 day1/2/3 high 都沒高於進場日 high → LL3
    entry_high_hyp_d3 = _shift(data.high, 3)
    no_high_d1_d3 = _shift(data.high, 2) <= entry_high_hyp_d3
    no_high_d2_d3 = _shift(data.high, 1) <= entry_high_hyp_d3
    no_high_d3_d3 = data.high <= entry_high_hyp_d3
    day3_no_high = no_high_d1_d3 & no_high_d2_d3 & no_high_d3_d3
    # v262: 鏡像 v254 — 進場後第 2 天，high 都未破 entry high AND 今日 L < 昨日 L → LL3
    entry_high_hyp_d2 = _shift(data.high, 2)
    no_high_d1_d2 = _shift(data.high, 1) <= entry_high_hyp_d2
    no_high_d2_d2 = data.high <= entry_high_hyp_d2
    l_down = data.low < _shift(data.low, 1)
    day2_no_high_l_down = no_high_d1_d2 & no_high_d2_d2 & l_down
    # v281: 鏡像 v280 buy defense — 昨日跳空+下影+量(peak90%) 今日跌破 → LL3 (進場3天內)
    prev_shadow_lo = _shift(data.candle_result.shadow.lower, 1)
    prev_vol_strong = _shift(vol_strong, 1)
    prev_vol = _shift(data.volume, 1)
    prev_vol_high5 = _shift(data.volume_result.high[5], 1)
    prev_vol_near_peak = prev_vol >= prev_vol_high5 * 0.9
    prev_low = _shift(data.low, 1)
    prev_jump = _shift(data.candle_result.jump, 1)
    fake_support = (prev_jump & prev_shadow_lo & prev_vol_strong
                    & prev_vol_near_peak
                    & (data.close < prev_low))
    long_defense = [
        DefenseRule(name="洪量後5日內8日低",
                    trigger=flood_recent5, source=rolling_lowest(data.low, 8)),
        DefenseRule(name="停滯13日無新高→8日低",
                    trigger=stagnant_long, source=rolling_lowest(data.low, 8)),
        DefenseRule(name="分數升>15+量強→8日低",
                    trigger=score_surge_with_vol, source=rolling_lowest(data.low, 8)),
        DefenseRule(name="Chandelier21x6",
                    trigger=chand_trigger, source=chand_long),
        DefenseRule(name="進場後3日皆未破進場日high→3日低",
                    trigger=day3_no_high, source=rolling_lowest(data.low, 3),
                    min_days_after_entry=3, max_days_after_entry=3),
        DefenseRule(name="進場後2日未破進場high+L<L[1]→3日低",
                    trigger=day2_no_high_l_down, source=rolling_lowest(data.low, 3),
                    min_days_after_entry=2, max_days_after_entry=2),
        DefenseRule(name="昨日跳空+下影+量今日跌破→3日低(進場3天內)",
                    trigger=fake_support, source=rolling_lowest(data.low, 3),
                    max_days_after_entry=3),
    ]
    # v352: 暫態 give-back 停損（近13日大漲後停滯且跌破 Chandelier21x4 → 提早鎖利）
    #   OR 進 long_exit（bar-level 訊號出場, 不進 ratchet/不凍結, 盤整解除即回鬆, 不砍 runner）
    long_exit = long_exit | _transient_giveback_exit(data)

    return SignalSpec(
        name="pick",
        signals=SignalSet(
            long_entry=long_entry,
            long_exit=long_exit,
            short_entry=zero,
            short_exit=zero,
        ),
        long_defense=long_defense,
    )


def touch_signal(data: "StockData") -> SignalSpec:
    """摸頭 (short-only)."""
    n = data.n
    not_dead_fish = ~data.money_result.dead  # money_level >= 3 (>= 9M turnover)
    short_entry = touch_condition(data) & not_dead_fish
    short_exit = sell_flee_signal(data)
    zero = np.zeros(n, dtype=np.bool_)

    flood = data.volume_result.flood
    # v56: touch source HH5 → HH8（更鬆給空頭反彈空間）
    close_lo13 = rolling_lowest(data.close, 13)
    at_13d_low = data.close <= close_lo13
    any_low_in_8d = rolling_highest(at_13d_low.astype(np.float32), 8) > 0.5
    stagnant_short = ~any_low_in_8d
    # v202c: 鏡像 v200 sell_flee score-surge defense（short side）
    # short_pct 升 = 市場急跌（對 short 部位有利），鎖利
    short_pct = _short_pct_array(data)
    sp_d1 = short_pct - _shift(short_pct, 1)
    vol_strong = data.volume_result.volume_status <= 2  # flood/big/high
    score_surge_short = (sp_d1 > 15.0) & vol_strong
    # v202j: K 棒 exhaustion (短側) — 近期低 + 大量 + 長上影 → HH3
    ll8 = rolling_lowest(data.low, 8)
    near_low = data.low <= ll8
    long_upper_shadow = data.candle_result.shadow.upper
    exhaustion_short = near_low & vol_strong & long_upper_shadow
    # v203k 鏡像: any over_lower (3|5|8) + vol_strong → HH2
    # (v204 sweep: HH8 mirror 完全 no-op 因短側 floor 已 8d, HH2 才能比 HH3 rules 更緊)
    ob = data.over_breakout
    any_over_low = ob.over_lower_3 | ob.over_lower_5 | ob.over_lower_8
    extreme_exhaustion_short = any_over_low & vol_strong
    # v228: Chandelier(21, 2.0) on touch — sweep 結論 touch 甜蜜點 mult=2 +0.026
    _chand_touch = calculate_chandelier(
        data.high.astype(np.float64), data.low.astype(np.float64),
        data.close.astype(np.float64), length=21, mult=2.0, use_close=True,
    )
    chand_short = _chand_touch.short_stop.astype(np.float32)
    chand_trigger_short = ~np.isnan(chand_short)
    # v253: 進場後第 3 天，若 day1/2/3 low 都沒低於進場日 low → HH3
    entry_low_hyp_d3 = _shift(data.low, 3)
    no_break_d1_d3 = _shift(data.low, 2) >= entry_low_hyp_d3
    no_break_d2_d3 = _shift(data.low, 1) >= entry_low_hyp_d3
    no_break_d3_d3 = data.low >= entry_low_hyp_d3
    day3_no_break = no_break_d1_d3 & no_break_d2_d3 & no_break_d3_d3
    # v254: 進場後第 2 天 — low 都未破 entry low AND 今日 H > 昨日 H → HH3
    #   抓「兩日內反轉動能 + 沒推進空頭」失敗形態
    entry_low_hyp_d2 = _shift(data.low, 2)
    no_break_d1_d2 = _shift(data.low, 1) >= entry_low_hyp_d2
    no_break_d2_d2 = data.low >= entry_low_hyp_d2
    h_up = data.high > _shift(data.high, 1)
    day2_no_break_h_up = no_break_d1_d2 & no_break_d2_d2 & h_up
    short_defense = [
        DefenseRule(name="洪量當日3日高",
                    trigger=flood, source=rolling_highest(data.high, 3)),
        DefenseRule(name="停滯8日無新低→8日高",
                    trigger=stagnant_short, source=rolling_highest(data.high, 8)),
        DefenseRule(name="分數升>15+量強→3日高",
                    trigger=score_surge_short, source=rolling_highest(data.high, 3)),
        DefenseRule(name="近期低+大量+長上影→3日高",
                    trigger=exhaustion_short, source=rolling_highest(data.high, 3)),
        DefenseRule(name="人/走/召跌+量強→2日高",
                    trigger=extreme_exhaustion_short, source=rolling_highest(data.high, 2)),
        DefenseRule(name="Chandelier21x2",
                    trigger=chand_trigger_short, source=chand_short),
        DefenseRule(name="進場後3日皆未破進場日low→3日高",
                    trigger=day3_no_break, source=rolling_highest(data.high, 3),
                    min_days_after_entry=3, max_days_after_entry=3),
        DefenseRule(name="進場後2日未破進場low+H>H[1]→3日高",
                    trigger=day2_no_break_h_up, source=rolling_highest(data.high, 3),
                    min_days_after_entry=2, max_days_after_entry=2),
    ]

    return SignalSpec(
        name="touch",
        signals=SignalSet(
            long_entry=zero,
            long_exit=zero,
            short_entry=short_entry,
            short_exit=short_exit,
        ),
        short_defense=short_defense,
        # v38: 做空訊號 floor ratchet 從 13d 拉緊到 8d，左尾風險改善
        short_floor_period=8,
    )
