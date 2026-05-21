"""Reversal signal factories — Flee系列.

  buy_flee  : short_entry = BuyFleeSignal,  short_exit = SellFleeSignal
              (多翻空進場做空，看到空翻多 = 趨勢再反轉就出場)
  sell_flee : long_entry  = SellFleeSignal, long_exit  = BuyFleeSignal
              (空翻多進場做多，看到多翻空 = 趨勢再反轉就出場)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

from analysis.indicators import rolling_highest, rolling_lowest
from signal_backtest.signal import DefenseRule, SignalSet, SignalSpec
from signal_backtest.factories._conditions import (
    _long_pct_array,
    _shift,
    buy_flee_signal,
    sell_flee_signal,
)

if TYPE_CHECKING:
    from backtest.data import StockData


def buy_flee_factory(data: "StockData") -> SignalSpec:
    """多翻空反轉 (short-only)."""
    n = data.n
    not_dead_fish = ~data.money_result.dead  # money_level >= 3 (>= 9M turnover)
    short_entry = buy_flee_signal(data) & not_dead_fish
    short_exit = sell_flee_signal(data)  # 出場：趨勢再反轉（空翻多）
    zero = np.zeros(n, dtype=np.bool_)

    # v43: 洪量規則對 buy_flee 邊際負（短側 floor 已 8d，flood 規則只是 redundant），不加
    # v56: buy_flee source HH5 → HH8
    close_lo13 = rolling_lowest(data.close, 13)
    at_13d_low = data.close <= close_lo13
    any_low_in_8d = rolling_highest(at_13d_low.astype(np.float32), 8) > 0.5
    stagnant_short = ~any_low_in_8d
    # v202j: K 棒 exhaustion (短側) — 近期低 + 大量 + 長上影 → HH5
    ll8 = rolling_lowest(data.low, 8)
    near_low = data.low <= ll8
    vol_strong = data.volume_result.volume_status <= 2
    long_upper_shadow = data.candle_result.shadow.upper
    exhaustion_short = near_low & vol_strong & long_upper_shadow
    short_defense = [
        DefenseRule(name="停滯8日無新低→8日高",
                    trigger=stagnant_short, source=rolling_highest(data.high, 8)),
        DefenseRule(name="近期低+大量+長上影→5日高",
                    trigger=exhaustion_short, source=rolling_highest(data.high, 5)),
    ]

    return SignalSpec(
        name="buy_flee",
        signals=SignalSet(
            long_entry=zero,
            long_exit=zero,
            short_entry=short_entry,
            short_exit=short_exit,
        ),
        short_defense=short_defense,
        # v38: 做空訊號 floor ratchet 從 13d 拉緊到 8d，左尾風險改善
        short_floor_period=8,
        # v198: init 5→3 (sweep peak; 2 退步、5 為原始 default)
        short_initial_period=3,
        # v201a: 進場日洪量 → init 防守用今日 H（強動能 → 緊停損）
        short_flood_tight_init=True,
    )


def sell_flee_factory(data: "StockData") -> SignalSpec:
    """空翻多反轉 (long-only)."""
    n = data.n
    not_dead_fish = ~data.money_result.dead  # money_level >= 3 (>= 9M turnover)
    long_entry = sell_flee_signal(data) & not_dead_fish
    long_exit = buy_flee_signal(data)  # 出場：趨勢再反轉（多翻空）
    zero = np.zeros(n, dtype=np.bool_)

    # v43: 洪量規則對 sell_flee 持續負向（持倉 20d 太長被殺贏單），不加
    # v56: 多方 stagnation 視窗 8d → 13d
    close_hi13 = rolling_highest(data.close, 13)
    at_13d_high = data.close >= close_hi13
    any_high_in_13d = rolling_highest(at_13d_high.astype(np.float32), 13) > 0.5
    stagnant_long = ~any_high_in_13d
    # v201m: long_pct 單日升 >15 + 量強(vol_status<=2, flood/big/high) → LL8
    # （後段持倉若分數爆升 + 量配合 = 短期高點風險，拉緊保護）
    # sweep d1∈{15,20,30,35} × vol≤{0,1,2,3} × source∈{LL3,LL5,LL8} → 此組合為峰值
    long_pct = _long_pct_array(data)
    lp_d1 = long_pct - _shift(long_pct, 1)
    vol_strong = data.volume_result.volume_status <= 2  # flood(0)/big(1)/high(2)
    score_surge_with_vol = (lp_d1 > 15.0) & vol_strong
    # v202h: K 棒特徵 exhaustion defense
    hh8 = rolling_highest(data.high, 8)
    near_high = data.high >= hh8
    long_lower_shadow = data.candle_result.shadow.lower
    exhaustion = near_high & vol_strong & long_lower_shadow
    long_defense = [
        DefenseRule(name="停滯13日無新高→8日低",
                    trigger=stagnant_long, source=rolling_lowest(data.low, 8)),
        DefenseRule(name="分數升>15+量強→8日低",
                    trigger=score_surge_with_vol, source=rolling_lowest(data.low, 8)),
        DefenseRule(name="近期高+大量+長下影→5日低",
                    trigger=exhaustion, source=rolling_lowest(data.low, 5)),
    ]

    return SignalSpec(
        name="sell_flee",
        signals=SignalSet(
            long_entry=long_entry,
            long_exit=long_exit,
            short_entry=zero,
            short_exit=zero,
        ),
        long_defense=long_defense,
        # v198: init 5→3 (sweep peak)
        long_initial_period=3,
        # v201a/b: long_flood_tight_init 對 sell_flee 退步（-0.0079）
        # 少數 catastrophic save (e.g. 4726 -26→-10) 不敵多數小回檔被誤殺
    )
