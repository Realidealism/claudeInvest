"""Buy (波段多) and Sell (波段空) signal factories.

  buy  : long_entry  = BuyCondition,  long_exit  = BuyFleeSignal
  sell : short_entry = SellCondition, short_exit = SellFleeSignal
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
    buy_condition,
    sell_condition,
    buy_flee_signal,
    sell_flee_signal,
)

if TYPE_CHECKING:
    from backtest.data import StockData


def buy_signal(data: "StockData") -> SignalSpec:
    """波段多 (long-only)."""
    n = data.n
    not_dead_fish = ~data.money_result.dead  # money_level >= 3 (>= 9M turnover)
    long_entry = buy_condition(data) & not_dead_fish
    long_exit = buy_flee_signal(data)
    zero = np.zeros(n, dtype=np.bool_)

    # v43: 洪量規則對 buy 的所有變種都是負向（持倉 26d 太長被殺贏單），不加洪量規則
    # v56: 多方 stagnation 視窗 8d → 13d
    close_hi13 = rolling_highest(data.close, 13)
    at_13d_high = data.close >= close_hi13
    any_high_in_13d = rolling_highest(at_13d_high.astype(np.float32), 13) > 0.5
    stagnant_long = ~any_high_in_13d
    # v202a: 鏡像 v200 sell_flee score-surge defense
    long_pct = _long_pct_array(data)
    lp_d1 = long_pct - _shift(long_pct, 1)
    vol_strong = data.volume_result.volume_status <= 2  # flood/big/high
    score_surge_with_vol = (lp_d1 > 15.0) & vol_strong
    # v202h: K 棒特徵 exhaustion defense — 近期高 + 大量 + 長下影 → LL5
    hh8 = rolling_highest(data.high, 8)
    near_high = data.high >= hh8
    long_lower_shadow = data.candle_result.shadow.lower
    exhaustion = near_high & vol_strong & long_lower_shadow
    # v203k (D+LL8): any over_upper (3|5|8) + vol_strong → LL8
    ob = data.over_breakout
    any_over_up = ob.over_upper_3 | ob.over_upper_5 | ob.over_upper_8
    extreme_exhaustion = any_over_up & vol_strong
    # v229: Chandelier(21, 6.0) on buy — sweep 4.5/5/5.5/6/8 後選 6.0 (cost -0.0035 換最大獲利 +149)
    chand = calculate_chandelier(
        data.high.astype(np.float64), data.low.astype(np.float64),
        data.close.astype(np.float64), length=21, mult=6.0, use_close=True,
    )
    chand_long = chand.long_stop.astype(np.float32)
    chand_trigger = ~np.isnan(chand_long)
    # v280: 昨日跳空+下影+量(且為近5日最大量 90%+), 今日 close 跌破昨日 low → LL3
    #   物理：T-1 跳空向上 + 下影 + 量強 + 量為近 5 日峰值 90%+
    #         = 強開高 + 賣壓測試低點 + 量是真實 climax (climax/exhaustion top)
    #         T 收破昨日 low = 承接無效真崩
    #   max_days=5 限制：避免砍長持後期健康回測
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
        DefenseRule(name="停滯13日無新高→8日低",
                    trigger=stagnant_long, source=rolling_lowest(data.low, 8)),
        DefenseRule(name="分數升>15+量強→8日低",
                    trigger=score_surge_with_vol, source=rolling_lowest(data.low, 8)),
        DefenseRule(name="近期高+大量+長下影→5日低",
                    trigger=exhaustion, source=rolling_lowest(data.low, 5)),
        DefenseRule(name="人/走/召漲+量強→8日低",
                    trigger=extreme_exhaustion, source=rolling_lowest(data.low, 8)),
        DefenseRule(name="Chandelier21x6",
                    trigger=chand_trigger, source=chand_long),
        DefenseRule(name="昨日跳空+下影+量今日跌破→3日低(進場3天內)",
                    trigger=fake_support, source=rolling_lowest(data.low, 3),
                    max_days_after_entry=3),
    ]
    # v279 試驗封存（4 變體全 destructive，2026-05-26）：
    #   v1: surge_3x_vd5 + _hammer_lower_shadow → LL2 — 砍長尾贏家 (合一 846→230)
    #   v2: 同上 + 黑K — 中性無害但救不到 Top 10
    #   v3: OverHigh + vol_strong + _hammer + 黑K → LL2 — destructive，砍中型贏家
    #   v4: OverHigh + vol_strong + shadow.lower + 黑K → LL2 — 救 5521 +16.67ppts 但
    #       砍長尾 -1551 ppts (4128 中天 533→27 等)，淨 PF -0.035
    #   結論：buy 對「進場後快崩」型 (5521) 沒有安全的 K 棒/量能-based defense

    return SignalSpec(
        name="buy",
        signals=SignalSet(
            long_entry=long_entry,
            long_exit=long_exit,
            short_entry=zero,
            short_exit=zero,
        ),
        long_defense=long_defense,
    )


def sell_signal(data: "StockData") -> SignalSpec:
    """波段空 (short-only)."""
    n = data.n
    not_dead_fish = ~data.money_result.dead  # money_level >= 3 (>= 9M turnover)
    short_entry = sell_condition(data) & not_dead_fish
    short_exit = sell_flee_signal(data)
    zero = np.zeros(n, dtype=np.bool_)

    flood = data.volume_result.flood
    # v55: stagnation defense
    close_lo13 = rolling_lowest(data.close, 13)
    at_13d_low = data.close <= close_lo13
    any_low_in_8d = rolling_highest(at_13d_low.astype(np.float32), 8) > 0.5
    stagnant_short = ~any_low_in_8d
    # v201: 鏡像 v200 sell_flee score-surge defense（short side）
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
    # v228: Chandelier(21, 1.5) on sell — sweep 結論 sell 甜蜜點 mult=1.5 +0.021
    _chand_sell = calculate_chandelier(
        data.high.astype(np.float64), data.low.astype(np.float64),
        data.close.astype(np.float64), length=21, mult=1.5, use_close=True,
    )
    chand_short = _chand_sell.short_stop.astype(np.float32)
    chand_trigger_short = ~np.isnan(chand_short)
    short_defense = [
        DefenseRule(name="洪量當日3日高",
                    trigger=flood, source=rolling_highest(data.high, 3)),
        DefenseRule(name="停滯8日無新低→5日高",
                    trigger=stagnant_short, source=rolling_highest(data.high, 5)),
        DefenseRule(name="分數升>15+量強→3日高",
                    trigger=score_surge_short, source=rolling_highest(data.high, 3)),
        DefenseRule(name="近期低+大量+長上影→3日高",
                    trigger=exhaustion_short, source=rolling_highest(data.high, 3)),
        DefenseRule(name="人/走/召跌+量強→2日高",
                    trigger=extreme_exhaustion_short, source=rolling_highest(data.high, 2)),
        DefenseRule(name="Chandelier21x1.5",
                    trigger=chand_trigger_short, source=chand_short),
    ]
    # v283 試驗封存（dead rule, 2026-05-27）：
    #   鏡像 v280 buy fake-support 到 sell 短側 — squat + shadow.upper + vol_peak + close>prev_high → HH3
    #   結果：0 trade 變動 (含/不含進場 3 天窗口都一樣)
    #   原因：5 條件 AND 後 base rate 0.14%（48k bars 中 68 次），落在短持倉 sell trades 內近 0
    #   結構鏡像對稱但 base rate 不對稱 — 「跳空下 V 反 textbook」極罕見

    return SignalSpec(
        name="sell",
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
