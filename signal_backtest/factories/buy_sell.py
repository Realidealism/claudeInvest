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
    _stance_long_held,
    buy_condition,
    sell_condition,
    buy_flee_signal,
    sell_flee_signal,
)

if TYPE_CHECKING:
    from backtest.data import StockData

# v361: tighten buy's long floor only once the thermometer's defensive latch has
# run this many sessions. Set to None to disable (same-window controlled runs).
STANCE_HELD_DEFENSE: int | None = 8


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
    # v202h: K 棒特徵 exhaustion defense — 近期高 + 大量 + 長下影 → LL8
    #   (sweep 29/30: 原 LL5 收緊淨負，LL8/LL13/整條刪除三者等價，同 blow-off top 封存結論)
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
        DefenseRule(name="近期高+大量+長下影→8日低",
                    trigger=exhaustion, source=rolling_lowest(data.low, 8)),
        DefenseRule(name="人/走/召漲+量強→8日低",
                    trigger=extreme_exhaustion, source=rolling_lowest(data.low, 8)),
        DefenseRule(name="Chandelier21x6",
                    trigger=chand_trigger, source=chand_long),
        DefenseRule(name="昨日跳空+下影+量今日跌破→3日低(進場3天內)",
                    trigger=fake_support, source=rolling_lowest(data.low, 3),
                    max_days_after_entry=3),
    ]
    # v361: latch age >= 8 sessions -> LL8 (sweep 36/37).
    #   The threshold is the edge, not the tightening: the same tightening with
    #   min_held=1 scores 1.8401 against a 1.8566 baseline, while gating it at
    #   7-8 sessions is worth +0.025. k over 4/5/6/7/8/10/15 is an inverted U
    #   peaking at 7-8 and decaying on both sides.
    if STANCE_HELD_DEFENSE is not None:
        long_defense.append(
            DefenseRule(name=f"溫度計守滿{STANCE_HELD_DEFENSE}日→8日低",
                        trigger=_stance_long_held(data, STANCE_HELD_DEFENSE),
                        source=rolling_lowest(data.low, 8)))
    # v288 試驗封存 (destructive, 2026-05-27):
    #   trigger: vs≤1 & high≥HH8 在 5 日內 → LL5
    #   Portfolio PF -0.0854 嚴重退步, 5314 世紀* +459→-16 (-475 ppts) 等災難
    #   失敗原因: 「flood/big @ HH8」其實是「健康突破」base rate，跟「distribution」同形態
    #   真正 distribution 需事後確認 (峰值出量 + 後續失敗創高), 非當下單純 K 棒可識別

    # v287/v288 試驗封存（紅瀑+UP波量 defense 全 destructive, 2026-05-27）：
    #   v287/v288 變體：新紅瀑首日 + UP波量 2x/3x + 紅瀑 length > 12波均 × 1.5 → tip1
    #   Portfolio PF 1.6840 → 1.6828 (-0.0012)，buy 僅 +28 trades trigger 太稀疏
    #   9 個 user 指定關鍵案例 (3026/6933/1519/5284/2457 等) 只動到 5284 且 -57.3 ppts
    #   結論：紅瀑/UP波量 distribution 形態無法 ex-ante 跟「健康放量」區分
    #   重要 case (5284 jpp-KY, 2457 飛宏) 紅瀑帶量是真實大幅 distribution 後反彈
    #   any tip1 source 拉高都會把後續反彈贏單早殺，與 long_defense 天花板一致

    # v289-v293 試驗封存（extreme_waterfall defense 全 destructive, 2026-05-27）：
    #   Primitive 新加 analysis/wave.py: extreme_waterfall = waterfall AND wl > wld12ma × 3.0
    #   Family 對照 (vs v281 baseline 1.6840):
    #     v289 (tip1, no gate)         : PF 1.6835, 賺賠 3.011, 太鬆基本無感
    #     v290 (up0, no gate)          : PF 1.6313, 賺賠 2.823, 太緊砍長尾
    #     v291 (up0 + UP波量2x)         : PF 1.6509, 賺賠 2.913, 過濾 TSMC 但仍砍 3026/5314
    #     v292 (up0 + 今日 flood/big)   : PF 1.6523, 賺賠 2.885, 砍更多
    #     v293 (mid0 + 今日 flood/big)  : PF 1.6675, 賺賠 2.952, family 最佳但仍 < baseline
    #   結論：buy trend-following 訊號的右尾贏家 (4128/5314/3026/3105/3701 級別 +200~+800%)
    #   來自於「不被中段切」，extreme_wf 本身是 trend momentum 訊號而非 distribution top；
    #   任何鎖利 source (tip1/mid0/up0) 在受害分類 type A (中段切) + type B (尾段切) 之間
    #   是不可調和的：鬆了 5314 不鎖到任何利、緊了 3026 又被早砍。
    #   primitive red_extreme_waterfall0 保留在 wave.py 供未來其他訊號 (buy_flee/touch) 用

    # v285/v286 試驗封存（波浪 defense 全 destructive, 2026-05-27）：
    #   v285 wave_d4_death → tip1：Portfolio PF -0.1339（歷史最大單版退步！）
    #     trigger 太頻繁 (短波死叉在 trend-up 健康回測常見), buy +5250 trades (+27%)
    #     切碎長尾大贏家 (5314 世紀* 459→25 -434 ppts), 救 6933 +10.92 ppts 不對稱
    #   v286 close_break_red_wf_down → red_wf_down_price：Portfolio PF -0.0363
    #     概念上稀，但新紅瀑形成 + 微觀跌破會誤觸發 (3026 禾伸堂 397→-5)
    #     buy +2727 trades, 切碎 350+% 級長尾贏家
    #   結論：波浪事件 + 緊 source 對 buy trend-following 結構性失敗
    #   buy 靠長持抓右尾大贏家，任何「波結構轉空」事件在大波段中頻發 = 切碎長尾

    # v284 試驗封存（2 變體全 destructive, 2026-05-27）：
    #   v1: 連續 3 日 vol_status≤2 → LL5 — 砍長尾 -475 ppts, 6933 沒救
    #   v2: 近 5 日 ≥3 日 vol_status≤2 → LL8 — 同樣 destructive -0.0048 PF
    #   原因：buy trend-up 段健康放量本就有「連續/頻繁大量」，rule fire 在 trend
    #   continuation 砍長尾贏家；6933 型急殺反而在進場後 vol 乾枯，LL ratchet 無效

    # v279 試驗封存（4 變體全 destructive，2026-05-26）：
    #   v1: surge_3x_vd5 + _hammer_lower_shadow → LL2 — 砍長尾贏家 (合一 846→230)
    #   v2: 同上 + 黑K — 中性無害但救不到 Top 10
    #   v3: OverHigh + vol_strong + _hammer + 黑K → LL2 — destructive，砍中型贏家
    #   v4: OverHigh + vol_strong + shadow.lower + 黑K → LL2 — 救 5521 +16.67ppts 但
    #       砍長尾 -1551 ppts (4128 中天 533→27 等)，淨 PF -0.035
    #   結論：buy 對「進場後快崩」型 (5521) 沒有安全的 K 棒/量能-based defense
    # v352 試驗封存：transient give-back exit 對 buy destructive（合池 -0.002~-0.028）——
    #   buy 巨尾(+1839%)太極端, mult=4 就踩到暫態線被砍, mult=5 保尾但仍淨負;
    #   buy give-back 是巨尾燃料, 動不得。pick/sell_flee 尾較溫和故淨正 (見 _transient_giveback_exit)。

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
