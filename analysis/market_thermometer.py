"""市場溫度計 — descriptive fragility gauge (綜合型 L1).

A 0-100 "how stretched is the market right now" reading, NOT a crash predictor
(see memory project_market_thermometer: simple gates don't time crashes; this only
describes current tension). Two components drive the 定位極端度 score, equal weight:

  外資期貨定位  foreign TX net open interest — hotter the more net-short foreigns
                are vs their own recent range (survived a fair false-positive test,
                1.47x standalone).
  融資水位      margin balance percentile — leverage backdrop (elevated at 6/7 tops).

P/C ratio and 微台散戶 were dropped (2026-07-21): both are extreme at BOTH tops and
bottoms (contrarian, no directional discrimination), so averaging them in only
diluted the score. The actionable signals live outside this gauge — 頂部過熱 uses
外資期貨 fresh-low, 攻防 uses OBV + 排列, 恐慌買進 uses 融資 + 深跌 + 快殺.
"""

from __future__ import annotations

from datetime import date, datetime

import numpy as np
import pandas as pd

from analysis.obv import calculate_obv, PERIOD_PARAMS

MARGIN_MA_WIN = 55  # 融資餘額金額 55日均線 ±2σ 乖離 (Bollinger z; deviation-from-trend, 不受長多水位長期偏高影響)
FUT_WIN = 78       # 外資期貨 net_oi percentile lookback. Unified with ALERT_LOW_WIN (2026-07-23)
                   # for one net_oi horizon: re-swept, 78 ties the prior 90 for crash separation
                   # (bottom-decile gate 1.54x vs 90's 1.52x, both 6/6) so aligning is free.
                   # tmp/_fut_window_sweep.py, _toplight_win_precision.py.
HI_WINDOW = 55     # 位階 context: index near its 55-day high
NEAR_HIGH_TOL = 2.0  # within 2% of the 55d high counts as 近高
# 攻防狀態: short-timeframe 多空頭排列 (market_breadth.short_trend, -2..+2). 攻擊 =
# 偏多以上 (>0); 防守 = 中性以下 (<=0). More responsive than a 20d MA (defensive ~49%
# of the time) but flips on short bounces.
ST_LABELS = {2: "強多", 1: "偏多", 0: "中性", -1: "偏空", -2: "強空"}
# 恐慌買進 (contrarian V-bottom, discrete): 深跌 + 融資短窗急殺(斷頭 flush). Validated
# 6/8 yrs positive BUT fails in grinding bears (2018/2022 negative, fired most) — a
# SHARP-CRASH-ONLY tool; caveat prominently. 深跌 must be after a fast washout.
PANIC_PFH = -8.0     # 距 55 日高 <= -8% (deep drawdown)
PANIC_MARGIN_WIN = 5  # 融資餘額金額 5-day drop window
PANIC_MARGIN_CHG = -2.0  # 融資 5 日跌 <= -2% (斷頭急殺)
PANIC_SPEED_WIN = 10  # 快殺 window (V-bottoms plunge fast; filters slow grinding bears)
PANIC_SPEED_CHG = -6.0  # 指數 10 日跌 <= -6% (fast washout; +9.3%/71% vs +6.9%/68%)
# 離散警戒燈 (discrete alarm, NOT a prediction): the validated crash gate. 近高 +
# 外資淨空創 78日新低 + 低P/C自滿. Precision 2.98x but ~71% false — a caution flag only.
ALERT_LOW_WIN = 78   # foreign net-OI fresh N-day net-short low. Swept 2026-07-23: 78 lifts
                     # 頂部過熱 precision 1.62→1.71x at the same 6/6 crash coverage, sitting back
                     # from the 86-day coverage cliff (86→5/6); gain is a uniform false-alarm
                     # cleanup across 2019/2021/2024 (not one event) and holds across 4 crash
                     # definitions. Stance unaffected (791→790, obv_weak dominates hot_wide).
                     # tmp/_toplight_win_precision.py, _stance_win55_probe.py.
# 融資過熱 (independent 2nd top flag): Bollinger z of 融資餘額/55MA成交金額. Normalizing 融資
# by turnover then de-trending gives 2.17x@+1.5σ (matches 外資期貨) with fixed persistence,
# and uniquely caught the 2025-03 -23% top that 外資期貨 missed. NOT OR-merged with fresh_low
# (that diluted clean years); shown as a separate flag. ~70% false, non-uniform, caution only.
# tmp/_overheat_mt_bollinger.py, _overheat_optimize.py.
MARGIN_OVERHEAT_Z = 1.5
# 頂部過熱·漲停 (3rd independent top flag): de-trended z of 漲停佔比 (limit-up share).
# Retail-froth overheat, orthogonal to 外資期貨/融資 (corr ~0). W=90/z≥1.0 sits on a
# broad sweep plateau (W55-120 × z1.0-1.5 all 1.5-1.9x, uniform 5-7/9 yrs) and is hot at
# 4/5 crash peaks (2020/2021/2024/2026-02; cold only at the 2022 slow bear). Modest
# ~1.5x lift, TIME-UNIFORM — cleaner than 融資過熱. tmp/_limitup_sweep.py.
LIMITUP_MA_WIN = 90
LIMITUP_OVERHEAT_Z = 1.0
# 漲停過熱當 top_vote 的 D 票時用更嚴的門檻 (顯示的漲停燈與 limitup_bear badge 仍用 1.0).
# 理由: z>=1.0 在 2026 的軋空行情變成常態開啟 (45% 的日子, 2019-2023 只有 9-16%), 讓 4-of-4
# 投票在這個 regime 退化. 收到 1.5 後票 lift 5.29→6.53、剔 2024 後 4.21→6.01、2026 亮燈率
# 11%→5%, 四個 episode (2021/2024/2026-02/2026-06) 一個沒少, 且 z=1.25/1.5/1.75 是 plateau
# 不是孤峰. Caveat: 緊化後的票在 2022-06 前近高命中 <4 天無法驗前半期 (stance 層前半期覆蓋
# 沒掉, 那段本來就靠趨勢腿). tmp/_voter_D_sweep.py, tmp/_voter_dropB.py.
TOP_VOTE_LIMITUP_Z = 1.5
# 頂部過熱·漲停 配合 空方排列 (high-conviction top): 漲停過熱 AND 排列翻空 (short_trend<0).
# 漲停 froth WHILE breadth has rolled over to bear = narrow-leader mania into a rolling top =
# distribution. Filters 漲停's 76% false down to ~41% precision (episode → 大回檔 ≥8%/40d)
# with NO loss of crash coverage (all of 2020/21/22/24/25). tmp/_limitup_x_shortarr.py.
# 加速惡化警示 (caution flag, NOT a prediction): 頂部過熱 present AND 排列尚未翻空
# (short_trend>=0) AND short 空頭排列(占比) 3日急升 ≥ +6pp. Fires in the lag window
# BEFORE stance flips 防守, at real tops (2021-04/2024-07/2025-02/2026-02) but ~half
# false (fwd20 median ≈ baseline) — a heads-up, not a signal. tmp/_warn_validate.py.
WARN_SD_CHG_WIN = 3
WARN_SD_CHG = 6.0
TOP_LOOKBACK = 3     # 攻防 entry: 過熱 within the last 3 trading days counts
STANCE_EXIT_DAYS = 3  # 攻防 exit: 排列 連續 N 天回到中性以上(short_trend>=0) 才解除防守
# 高點防守 (top-defense override): top_vote (4-orthogonal overheat >=3) 一亮就防守, 不等排列
# 翻空 — 這是「提早在高點防守」的來源. 進場記下當日 TAIEX 收盤=頂位, 用價格論點持有: 只在價格
# 仍在頂位之下(下跌中)才續守, 一旦漲回頂上(向上趨勢)或 early_reattack(底部)才轉攻. STANCE_TOP_MINHOLD
# 是頭幾天的抗抖動寬限. 採用驗證: 危險近高防守 12%→51% (drop-8%; -10/-12 更達 66/67%), 過熱頂
# (2024/26) 100%覆蓋且提早 17-18 天進防守, 非過熱型(COVID/慢熊)不受傷, mh=4-8 plateau, 逐年均勻
# (2021/24/25/26 皆升), long-on-attack 累報酬 +145%→+195% 而 maxDD 持平. tmp/_stance_adopt_check.py.
STANCE_TOP_MINHOLD = 5
# 高點防守「重掛閘門」: 頂位被價格漲過 (論點證偽) 後 STANCE_TOP_REENTRY_WIN 日內, 只要排列仍是
# 全面強多 (short_trend>=2) 就不准 top_vote 重新掛上防守. 沒有這道閘門時, 多頭噴出段會變成
# 棘輪——每次證偽只放行一天, 新的 top_vote 立刻在更高價位重掛 (2026-05~06 指數 +13.3% 卻有
# 36/40 天防守, 全部來自 top-mode 的 5 段連續重掛). 加閘門後 2026-05~06 防守 36→15 天, 而
# 危險近高防守 30%→28% (-10%/-12% 33%→31%/33%)、四個真頂覆蓋 (2021-04/2024-07/2026-02/
# 2026-06) 不變、attack-equity 累報酬 +195.9%→+195.8% 持平、maxDD -23.1% 不動. 全期只有
# 26 天翻攻 (2024:3 天穿插、2026:23 天). tmp/_stance_topdef_reentry.py, tmp/_stance_daydiff.py.
STANCE_TOP_REENTRY_WIN = 60
# 波段停滯防守 (swing mode, 第三種防守): 近 SWING_LOOK 日內票數 >= SWING_VOTES (過熱背景, 門檻低於
# TOP_VOTE_K 因為這層要的不是高信念頂) AND 指數跌破 SWING_HI_WIN 日高 SWING_DROP% (漲勢停下來)
# → 防守; 重新站上 SWING_HI_WIN 日高 → 攻擊. 給的是波段級別靈敏度: 上升途中一停就守、續創高就攻.
# 實測 (2019-07+, 與 TREND_EXIT_HI_WIN 一起): 防守 41.3%→40.2%, 危險近高 recall 28%→30%,
# attack-equity 累報酬 +195.8%→+252.7%, maxDD -23.1%→-22.4%, 真頂覆蓋 2021-04 80%→73%/其餘不變.
# SWING_LOOK 用 5 不用 3: 3 日版會漏掉「票在 4-5 天前、指數才開始停」的段落 (2019-07-26~30 放行後
# 5 日 -3~-4%), 且指定日命中/recall/覆蓋兩者相同, 故取 5.
# ★誠實 caveat — 這層過不了專案平常的採用門檻, 定位是「儀表靈敏度偏好」不是已證實的 edge:
#   (1) 報酬增益集中 2026/2022, 2019/2021/2024 反而各差 1~3pp;
#   (2) 120 個觸發日中 2024 佔 43、2026 佔 46 (74%), 2025 整年零觸發;
#   (3) 2020/2023 的觸發日事後反而優於非觸發日 (方向相反);
#   (4) SWING_DROP 敏感 (0.5→1.0 使用者指定日命中 10/14→6/14); SWING_LOOK 3/5/10 與票數>=2 是 plateau.
# tmp/_stance_swing_layer.py, tmp/_want_defense_features.py, tmp/_swing_look3.py.
SWING_LOOK = 5
SWING_VOTES = 2
SWING_DROP = 0.5
SWING_HI_WIN = 5
# 波段防守的續守上限: 它抓的是波段級停頓, 續跌本來就該交給趨勢腿, 沒必要一路守下去.
# 5/10 日稽核發現續守日是這層最弱的部分 (109 個續守日 fwd10 +3.11%, 比基準高 2.01pp = 守進反彈).
# 上限 5 天砍掉 54 個續守日而其他指標全同: 防守 40.0%→39.6%, 累報酬 +267.7%→+269.5%, 危險近高
# recall 30%、-10% 36%、六段崩盤/頂部覆蓋、maxDD -22.4% 一格未動, 使用者指定的 14 天仍 10 天防守.
# ★注意這只是砍白守的日子, 沒有解決方向問題 — 見 SWING_* 的 caveat 與下面這段.
# ★整層在 10 日尺度上方向是反的 (swing 防守日 fwd10 +2.38% vs 基準 +1.10%), 且**無法用參數修好**:
#   maxhold3 (+1.73% 但指定日剩 7/14、報酬 -45pp)、站回3日高 (+3.06%)、近高才進場 (+3.88%)、
#   連2紅轉攻 (+1.89%) 全部仍高於基準 — 因為「過熱背景下從 5 日高回檔 0.5%」抓到的是拉回, 而這個
#   樣本裡拉回之後 10 日多半是漲的. 它只在 5 日尺度勉強站得住 (+0.35% vs 基準 +0.60%). 保留的理由
#   是尾部: 關掉整層會讓危險近高 recall 30%→26%、-10% 36%→30%、累報酬 +268%→+223%.
#   tmp/_audit_510.py, _swing_hold_fix.py.
SWING_MAX_HOLD = 5
# 向下趨勢防守的價格出場: 指數站回 TREND_EXIT_HI_WIN 日高就轉攻, 不必死等排列回多方. 趨勢腿原本
# 只認 rec (排列回多方/連 3 天中性以上), 遇到「排列一路空、指數卻已收復」的背離段會整段空守
# (2026-02-02~02-11 排列 -2 不動, 指數卻從 31624 漲回 33606). 實測 (配 SWING_LOOK=5):
# 防守 44.2%→41.1%, 累報酬 +221.9%→+257.0%, maxDD -23.1%→-22.7%, 逐年 8 年中 7 年報酬改善
# = 時間均勻. ★代價落在真熊: 危險近高 recall 32%→31%, 2020 COVID 覆蓋 68%→68%、2022 慢熊 63%→62%;
# 其餘 episode (2021-04/2024-07/2025-04/2026-02/2026-06) 覆蓋不變.
# 窗掃描 8/10/12/13/15/20/25 (tmp/_trendexit_win_sweep.py): 覆蓋率從 12 起進入平原
# (COVID 68%/2022 62%/2025-04 64%), 報酬則在 10 有個峰 (+269.5%) 之後落在 246~257% 的平原.
# 選 13 (不選報酬峰的 10) 是使用者取捨: 用 -12.5pp 報酬換 recall 30%→31%、COVID 62%→68%、
# 2022 59%→62%, 防守日多 26 天.
# ★可用範圍是 9~20, 8 以下有懸崖: 2025-04 關稅段覆蓋 64%→27%、maxDD -22.4%→-24.2%、報酬 +226.8%.
#   機制: 2025-03-20 指數 22377 只是彈回 8 日內高點 (排列仍 -1、隔幾天翻回 -2), 窗 8 就判定「站回去」
#   而放行, 接著 7 個交易日站在攻擊邊直到 03-31 大跌才回防守; 窗 13 門檻高 2pp 從未觸發, 整段守住.
# ★注意拉長窗只是「多守幾天」不是「守得更準」: 防守日的 fwd10 中位在 6~20 全部都是 +0.82~0.90%、
#   負比 41~43%, 完全不隨窗變化. tmp/_trend_exit_hi.py, _trendexit_win_sweep.py.
TREND_EXIT_HI_WIN = 13
# 高點防守也吃同一條價格出場 (TOP_EXIT_ON_HI): 原本只認「漲回進場當日的頂位」, 但頂位在深回檔後
# 會變得遙不可及——2026-02-25 進場的頂位 35413, 指數跌到 31723 再彈回 34861 全程續守, 直到 04-10
# 才漲過頂位, 整段 V 型反彈 (+9.9%) 都站在防守邊. 指數站回 TREND_EXIT_HI_WIN 日高時, 「還在下跌中」
# 這個論點本來就已作廢, 不必等它漲回原始進場價. 實測完全免費: 危險近高 recall 30%、-10% 36%、七個
# 崩盤/頂部段覆蓋 (62/73/60/82/64/100/100%) 一格未動, 防守 40.2%→40.0%, 累報酬 +252.7%→+267.7%,
# maxDD -22.4% 不變. 窗 5 會傷覆蓋 (2021-04 73→67%、2026-06 100→94%), 20 較鈍, 故沿用 10.
# ★被否決 (勿重試): 「指數站在 10 日高上就不准任何腿進場」——報酬更好 (+289.5%) 但危險近高 recall
# 30%→21%、-10% 36%→21%、2026-02/06 真頂覆蓋 100%→92/94%, 保護力塌掉. tmp/_top_exit_hi2.py.
TOP_EXIT_ON_HI = True
# 站在 TREND_EXIT_HI_WIN 日高上就不准進場 (BLOCK_ENTRY_AT_HI): 出場已經認「站回 10 日高」, 這條讓
# 進場對稱——指數正在創 10 日新高時, 高點防守與趨勢腿都不新開防守 (波段層本來就要求跌破 5 日高,
# 不受影響). 效果: 2026-04-08 (排列仍 -2 但指數單日 +4.6%) 轉攻擊, 防守 40.0%→38.0%, 累報酬
# +267.7%→+289.5%, maxDD -22.4% 不變.
# ★實測後關閉 (2026-07-25): 累報酬雖然 +267.7%→+289.5%, 但判斷品質每一項都變差 —
#   危險近高防守 30%→21%、-10% 36%→21%, 2021-04 頂 73%→67%、2026-02 100%→92%、2026-06 100%→94%
#   (頂部當天指數還在創 10 日新高, 防守要隔一天才進場); 攻擊日的 fwd10 中位 +1.27%→+1.23%、正比
#   67%→65%, 之後 10 日踩到 -5% 的次數 55→63、-8% 的 5→9; 且 +22pp 報酬集中在 2024(+4.8pp)/
#   2026(+3.6pp), 2023 反而 -1.1pp = 不時間均勻. 放行的 36 天用 5/10 日評分只有 10 天變好、16 天
#   變差 (fwd10 中位 -1.68%, 正比僅 25%) — 賺的是頂部前最後幾根, 賠的是接下來兩週.
#   設 True 可重現該版本. tmp/_top_exit_hi2.py, _blockentry_eval510.py, _ab_head2head.py.
BLOCK_ENTRY_AT_HI = False
# 恐慌急殺早轉攻 (panic early re-attack): 防守中若「深跌 + 融資急殺 + 快殺 10日跌 <= STANCE_PANIC_EXIT_SPEED」
# 就提早轉攻, 接急殺 V 底反彈. 用比顯示的恐慌買進(PANIC_SPEED_CHG=-6%)更嚴的 -10%, 因為這是 *自動*
# 解除防守: -6% 會在 2022 慢熊自動接刀 (轉攻後 fwd20 -1%、防守涵蓋 60%->48%), -10% 只抓真急殺 V —
# 標準恐慌買進買訊號在 -10% 也更乾淨 (全體 fwd20 +5.4%->+11.0%、勝率 69%->100%、無 2022). 轉攻後
# STANCE_PANIC_HOLD 天抑制重新進場防止閃爍. tmp/_stance_panic_gate.py.
STANCE_PANIC_EXIT_SPEED = -10.0
STANCE_PANIC_HOLD = 5
# 攻防 早退轉攻 (2026-07-23): 外資期貨淨多創 EARLY_LONG_WIN 日新高 AND 空頭排列占比升速放緩
# (二階差<0, DECEL_WIN 窗). 讓防守在「排列尚未轉正」時提早解除——外資翻多到近期極端、同時空頭
# 恐慌的漲勢在收斂(力竭)時搶先轉攻. 受控對照 added fwd5 +2.16% vs 純等排列轉正 still +0.26%,
# 時間均勻(plateau w3-5×thr±5σ皆 +1.8~2.3%), 0% 反彈陷阱; 濾掉 2022 磨熊/2025 關稅崩兩次接刀且
# 保住 2020-03 COVID 底 +11.8%. 用二階差(升速)而非水位: V 底空頭占比仍在高檔, 只是漲不動了.
# tmp/_stance_early_exit_probe.py, _stance_decel_sweep.py.
EARLY_LONG_WIN = 60
DECEL_WIN = 5
# 早退後的「安全帶」(2026-07-23): 觸發後只要外資淨多還在其 60 日高點的 EARLY_BAND_FRAC 區間內
# (band = 高點 − frac×(高−低), 用區間比例故負值也成立), 就不准 obv_weak 把攻防重新拉回防守;
# 外資一縮手(net_oi 掉出 band)保護即解除、回歸正常進場. 修掉單日強制早退的閃爍(閃格 13→8)且
# thesis-gated 比盲數 K 天更早在真續崩時縮手. 5% 是閃格曲線膝點(更緊<5% 退回 11 閃格、報酬不變),
# 報酬持平(fwd5 +2.12%/0% 陷阱/5-7 正年). tmp/_stance_band_probe.py.
EARLY_BAND_FRAC = 0.05
# 大台期貨 OBV 弱勢 = ScoreBoard short-scope OBV in bearish state (trend<0), aligned with
# analysis.obv (short scope only). We use the persistent latched trend, NOT signal_down:
# the sparse down-cross event misses declines with no fresh cross (2026-07-09~16 reopened
# the stance gap), while trend<0 stays weak through the whole decline. The latched-trend
# form was rejected UPSTREAM only for cross-sectional scoring (cross-timeframe redundancy),
# which doesn't apply to a single market dial. tmp/_txf_obv_build_faithful.py.
# 高信念頂部 (top_vote): >=3 of 4 orthogonal overheat channels agree. Single AND-pairs
# overfit one event (2024), but a k-of-n VOTE is robust because each channel catches a
# different episode. Voters: A 外資期貨 fresh_low, B 外資選擇權避險偏空, D 漲停過熱, E 融資過熱
# (retail-froth was dropped — weakest voter, and 小台 froth degrades as retail migrates to
# 微台). >=3of4 lifts near-high 大回檔 rate 12.7%→~59% (4.6x), survives ex-2024 (3.6x), both
# halves positive, catches 2021/24/26; 243-config threshold sweep passes the robust gate.
# Caveat surfaced in UI: high-conviction LOW-recall, for ≥8-10% pullbacks (not deep crashes),
# blind to 2020 COVID (exogenous) and 2022 slow bear. tmp/_ensemble_test.py, _ensemble_sweep.py.
OPT_PCTL_WIN = 252       # 外資選擇權 bull(買權−賣權淨OI) 百分位窗
OPT_PCTL_MIN = 120       # min obs before a percentile is emitted (options start 2023-06)
OPT_BEARISH_PCTL = 0.33  # bull 落在 252日底 1/3 = 避險偏空 (B voter)
TOP_VOTE_K = 3           # 高信念頂部門檻: A/B/D/E 四個過熱訊號 >=K 同時亮
HISTORY_DAYS = 250 # sparkline length

def _regime(score: float, near_high: bool, fresh_low: bool) -> tuple[str, str, bool]:
    """(label, colour, is_danger). 頂部過熱 (the real top warning) is 外資期貨-driven:
    among near-high days, foreign net-OI (期) is the ONLY component that separates
    tops from ordinary highs (融資/P/C/散戶 are high at almost every high). So the
    red danger = 近高 AND 外資淨空創 78 日新低 (catches 5/6 tops incl. the recent
    wave; still ~82% false — top prediction is inherently hard). The composite score
    stays a descriptive 定位極端度. Below the high we make no bottom claim."""
    if fresh_low:
        return "頂部過熱", "#ef4444", True
    return "無頂部訊號", "#8a8a9a", False


def _roll_pctl(s: pd.Series, win: int) -> pd.Series:
    """Percentile rank (0-1) of each value within its trailing `win` window."""
    return s.rolling(win, min_periods=win).apply(lambda w: (w <= w.iloc[-1]).mean(), raw=False)


def compute_stance(short_trend, hot_wide, early_exit=None, reattack_safe=None,
                   top_vote=None, price=None, top_minhold=STANCE_TOP_MINHOLD,
                   panic_exit=None, panic_hold=STANCE_PANIC_HOLD,
                   top_reentry_win=None, swing=None, swing_exit=None,
                   trend_exit=None, block_entry_at_hi=False,
                   swing_max_hold=None) -> np.ndarray:
    """攻防 stateful hysteresis → per-day defensive[] boolean array. Two ways to defend:

    高點防守 (top mode, needs top_vote+price): top_vote (4-orthogonal overheat >=3) within
        TOP_LOOKBACK days → defend AT the high (bypasses 排列翻空), recording 頂位=price. Held
        by a price thesis: stays 防守 while price <= 頂位 (下跌中); exits 攻擊 when price rises
        back above 頂位, when price regains its TREND_EXIT_HI_WIN-day high (trend_exit — 深回檔
        後頂位遙不可及時, 站回 10 日高就算論點作廢), or on early_exit (底部). top_minhold days grace absorbs noise.
    重掛閘門 (top_reentry_win): 頂位被漲過而解除後的 top_reentry_win 日內, 排列仍全面強多
        (short_trend>=2) 就不准 top_vote 重新掛上防守 — 擋掉噴出段「越漲越防守」的棘輪.
    進場對稱條件 (block_entry_at_hi): 指數正在創 TREND_EXIT_HI_WIN 日新高時, 高點防守與趨勢腿都
        不新開防守 (出場已認同一條件).
    向下趨勢防守 (trend mode): 加寬過熱 (hot_wide within TOP_LOOKBACK) AND 排列翻空 (short_trend<0);
        exits 攻擊 on 排列回多方 (>0 單日 / 連續 STANCE_EXIT_DAYS 天 >=0), trend_exit (指數站回
        TREND_EXIT_HI_WIN 日高 — 排列還空但價格已收復的背離段), or early_exit.
    波段停滯防守 (swing mode, needs swing+swing_exit): 過熱背景下漲勢停下來 (跌破 5 日高) → 防守;
        重新站上 5 日高 (swing_exit) → 攻擊 (panic_exit/early_exit 也會解除). 優先序最低 (過熱頂與
        趨勢腿先判), 給的是波段級別靈敏度. ★非已證實 edge: 報酬增益集中 2026/2022、觸發日 74% 落在
        2024+2026 — 完整 caveat 見 SWING_* 常數.
    early_exit 後安全帶 (reattack_safe): 外資淨多仍在 60 日高 band 內時抑制 obv_weak 重新進場.
    恐慌急殺早轉攻 (panic_exit): 防守中若急殺 V (深跌+融資急殺+快殺<=-10%) → 立即轉攻接反彈,
        並 panic_hold 天抑制重新進場防閃爍. 優先於其他 exit.

    top_vote/price None → legacy trend-only stance (bit-identical to the pre-top-defense
    behaviour); panic_exit None → no panic re-attack; top_reentry_win None → no re-latch
    gate; swing None → no 波段停滯防守; swing_max_hold None → 波段防守不限天數; trend_exit None →
    趨勢腿只認排列回多方、高點防守只認頂位 (each None
    keeps the pre-feature behaviour bit-identical). Shared by the daily gauge and the
    intraday live-stance builder so a forming last bar produces the same latch as the close."""
    st = pd.Series(np.asarray(short_trend))
    hw = pd.Series(np.asarray(hot_wide))
    fl3 = (hw.rolling(TOP_LOOKBACK, min_periods=1).max() > 0).to_numpy()
    stv = st.to_numpy()
    rec3 = (st.rolling(STANCE_EXIT_DAYS, min_periods=STANCE_EXIT_DAYS).min() >= 0).fillna(False)
    rec = (rec3 | (st > 0)).to_numpy()
    ee = (np.zeros(len(st), dtype=bool) if early_exit is None
          else np.asarray(early_exit, dtype=bool))
    safe = (np.zeros(len(st), dtype=bool) if reattack_safe is None
            else np.asarray(reattack_safe, dtype=bool))
    pk = (np.zeros(len(st), dtype=bool) if panic_exit is None
          else np.asarray(panic_exit, dtype=bool))
    sw = (np.zeros(len(st), dtype=bool) if swing is None
          else np.asarray(swing, dtype=bool))
    swx = (np.zeros(len(st), dtype=bool) if swing_exit is None
           else np.asarray(swing_exit, dtype=bool))
    trx = (np.zeros(len(st), dtype=bool) if trend_exit is None
           else np.asarray(trend_exit, dtype=bool))
    at_hi = trx if block_entry_at_hi else np.zeros(len(st), dtype=bool)  # 創 10 日新高 → 不新開防守
    swing_held = 0   # 波段防守已守天數 (swing_max_hold 用)
    if top_vote is None or price is None:
        tv3 = np.zeros(len(st), dtype=bool)
        px = np.zeros(len(st))
    else:
        tv3 = (pd.Series(np.asarray(top_vote)).rolling(TOP_LOOKBACK, min_periods=1).max() > 0).to_numpy()
        px = np.asarray(price, dtype=float)
    out = np.zeros(len(st), dtype=bool)
    state = False
    supp = False   # 早退安全帶: 保護攻擊狀態不被 obv_weak 重新拉回防守
    mode = None    # 'top' (高點防守, 價格論點持有) | 'trend' (向下趨勢防守)
    entry = 0.0    # 頂位 = top mode 進場當日 price
    mh = 0         # top mode 抗抖動寬限剩餘天數
    pcool = 0      # 恐慌急殺轉攻後的重新進場抑制天數
    inval = -(10 ** 9)  # 頂位被漲過 (論點證偽) 的最後一天 index, 供重掛閘門用
    for i in range(len(st)):
        if supp and not safe[i]:
            supp = False              # 論點破 (net_oi 掉出 band) → 解除保護
        if pcool > 0:
            pcool -= 1
        if not state:
            if pcool > 0:
                pass                                      # 恐慌轉攻冷卻: 抑制重新進場
            elif tv3[i] and not at_hi[i] and not (top_reentry_win is not None
                                 and (i - inval) <= top_reentry_win and stv[i] >= 2):
                state = True; mode = "top"; entry = px[i]; mh = top_minhold  # 高點防守
            elif (not supp) and fl3[i] and stv[i] < 0 and not at_hi[i]:
                state = True; mode = "trend"                                 # 向下趨勢防守
            elif sw[i]:
                state = True; mode = "swing"; swing_held = 0                 # 波段停滯防守
        else:
            if pk[i]:
                state = False; mode = None; pcool = panic_hold  # 恐慌急殺 V → 早轉攻 (優先)
            elif ee[i]:
                state = False; mode = None; supp = True   # 底部 → 攻擊 + 啟動安全帶
            elif mode == "top":
                if mh > 0:
                    mh -= 1                                # 抗抖動寬限
                elif px[i] > entry or trx[i]:
                    state = False; mode = None            # 漲回頂上 / 站回 13 日高 → 攻擊
                    inval = i                             # 論點證偽 → 起算重掛閘門
            elif mode == "swing":
                swing_held += 1
                if swx[i] or (swing_max_hold is not None and swing_held >= swing_max_hold):
                    state = False; mode = None            # 站上 5 日高 / 續守到上限 → 攻擊
            elif rec[i] or trx[i]:
                state = False; mode = None                # 排列回多方 / 站回 13 日高 → 攻擊
        out[i] = state
    return out


def _early_reattack(m: pd.DataFrame) -> np.ndarray:
    """早退轉攻 trigger for compute_stance: 外資期貨淨多創 EARLY_LONG_WIN 日新高 AND 空頭
    排列占比升速放緩 (二階差 over DECEL_WIN < 0). Lets 防守 flip to 攻擊 before 排列 fully
    recovers, when foreign net-OI hits a fresh long extreme while the bear-breadth surge
    is losing steam. NaN (e.g. a forming intraday bar with no live sd_pct) -> False."""
    fresh_long = m["noi"] >= m["noi"].rolling(EARLY_LONG_WIN).max() - 1e-9
    decel = (m["sd_pct"] - 2 * m["sd_pct"].shift(DECEL_WIN)
             + m["sd_pct"].shift(2 * DECEL_WIN)) < 0
    return (fresh_long & decel).fillna(False).to_numpy()


def _reattack_safe(m: pd.DataFrame) -> np.ndarray:
    """早退安全帶: 外資淨多仍在其 EARLY_LONG_WIN 日高點的 EARLY_BAND_FRAC 區間內. Band 用
    60 日區間比例 (高 − frac×(高−低)) 而非 net_oi 的百分比, 故 net_oi 為負也成立. While true
    after an early reattack, obv_weak may not re-drag the stance into 防守. NaN -> False."""
    mx = m["noi"].rolling(EARLY_LONG_WIN).max()
    mn = m["noi"].rolling(EARLY_LONG_WIN).min()
    return (m["noi"] >= mx - EARLY_BAND_FRAC * (mx - mn)).fillna(False).to_numpy()


def _load_merged_frame(cur) -> pd.DataFrame:
    """Merged daily frame (margin / foreign net_oi / TAIEX / short 排列 total /
    大台期貨 OHLCV) on the common trading-date spine. Shared by the daily gauge
    and the intraday stance builder so both run the OBV + hysteresis over the
    identical history. 排列 uses short_trend_total (normal ∪ forming base) —
    matches the intraday breadth sidecar so the close boundary doesn't jump."""
    cur.execute("SELECT trade_date, margin_balance_value FROM tw.margin_summary ORDER BY trade_date")
    mg = pd.DataFrame(cur.fetchall())
    cur.execute("""SELECT trade_date, net_oi FROM tw.taifex_inst_futures
                   WHERE product='臺股期貨' AND investor='外資及陸資' ORDER BY trade_date""")
    fu = pd.DataFrame(cur.fetchall())
    cur.execute("""SELECT trade_date, close_price, turnover,
                          advance, advance_limit, decline, unchanged
                   FROM tw.index_prices WHERE index_id='TAIEX' ORDER BY trade_date""")
    ix = pd.DataFrame(cur.fetchall())
    cur.execute("""SELECT trade_date, short_trend_total, short_down_total, total_stocks
                   FROM tw.market_breadth ORDER BY trade_date""")
    br = pd.DataFrame(cur.fetchall())
    # 大台期貨 front-month OHLCV (per day = non-spread 一般 contract with MAX volume)
    cur.execute("""SELECT DISTINCT ON (trade_date) trade_date, contract_month,
                     open_price o, high_price h, low_price l, close_price c, volume v
                   FROM tw.taifex_futures_daily
                   WHERE contract='TX' AND session='一般' AND contract_month NOT LIKE '%%/%%'
                     AND volume IS NOT NULL AND close_price IS NOT NULL
                   ORDER BY trade_date, volume DESC""")
    tv = pd.DataFrame(cur.fetchall())
    # 外資臺指選擇權 net 買權/賣權未平倉 (B voter). bull = net_call_oi − net_put_oi;
    # low = foreign hedged/bearish. Live source is DB tw.taifex_inst_options (2023-06+),
    # verified bit-identical to the FinMind series the ensemble was validated on.
    cur.execute("""SELECT trade_date, call_put, net_oi FROM tw.taifex_inst_options
                   WHERE product LIKE '臺指%%' AND investor LIKE '外資%%' ORDER BY trade_date""")
    op = pd.DataFrame(cur.fetchall())
    mg["d"] = pd.to_datetime(mg["trade_date"]); mg["mgn"] = mg["margin_balance_value"].astype(float)
    fu["d"] = pd.to_datetime(fu["trade_date"]); fu["noi"] = fu["net_oi"].astype(float)
    ix["d"] = pd.to_datetime(ix["trade_date"]); ix["tx"] = ix["close_price"].astype(float)
    ix["to"] = ix["turnover"].astype(float).ffill()   # a few null-turnover days would else NaN the whole 55d window
    ix["pct_from_high"] = (ix["tx"] / ix["tx"].rolling(HI_WINDOW).max() - 1) * 100
    # 漲停佔比 (%): limit-up count as a share of traded stocks — speculative froth.
    # ffill the rare missing-count day (4 in the series) so it doesn't NaN the 90d z window.
    _traded = (ix["advance"].fillna(0) + ix["decline"].fillna(0) + ix["unchanged"].fillna(0)).astype(float)
    ix["lim"] = (ix["advance_limit"].astype(float) / _traded.replace(0, np.nan) * 100).ffill()
    br["d"] = pd.to_datetime(br["trade_date"]); br["st"] = br["short_trend_total"].astype(int)
    # short 空頭排列 占比 (total base): rising fast = deterioration accelerating
    br["sd_pct"] = (br["short_down_total"].astype(float)
                    / br["total_stocks"].replace(0, np.nan).astype(float) * 100)
    tv["d"] = pd.to_datetime(tv["trade_date"])
    for _k in ("o", "h", "l", "c", "v"):
        tv[_k] = tv[_k].astype(float)
    # limit_refer = prev close; on contract rollover use today's open (neutralize gap)
    tv = tv.sort_values("d").reset_index(drop=True)
    tv["roll"] = tv["contract_month"] != tv["contract_month"].shift(1)
    tv["ref"] = np.where(tv["roll"], tv["o"], tv["c"].shift(1))
    tv.loc[0, "ref"] = tv.loc[0, "c"]
    if not op.empty:
        op["d"] = pd.to_datetime(op["trade_date"])
        opv = op.pivot_table(index="d", columns="call_put", values="net_oi")
        opt = (opv.get("C", pd.Series(dtype=float)).astype(float)
               - opv.get("P", pd.Series(dtype=float)).astype(float)).rename("opt_bull").reset_index()
    else:
        opt = pd.DataFrame({"d": pd.Series(dtype="datetime64[ns]"), "opt_bull": pd.Series(dtype=float)})
    return (mg[["d", "mgn"]].merge(fu[["d", "noi"]], on="d")
            .merge(ix[["d", "pct_from_high", "tx", "to", "lim"]], on="d").merge(br[["d", "st", "sd_pct"]], on="d")
            .merge(tv[["d", "o", "h", "l", "c", "v", "ref"]], on="d")
            .merge(opt, on="d", how="left")   # LEFT: keep full history; opt_bull NaN pre-2023-06
            .sort_values("d").reset_index(drop=True))


def build_thermometer(cur, today: date | None = None) -> dict:
    m = _load_merged_frame(cur)
    # 大台期貨 OBV — aligned with ScoreBoard OBV machinery (analysis.obv), short scope only
    _obv = calculate_obv(m["c"].to_numpy(np.float32), m["ref"].to_numpy(np.float32),
                         m["h"].to_numpy(np.float32), m["l"].to_numpy(np.float32),
                         m["v"].to_numpy(np.float32), **PERIOD_PARAMS["short"])
    m["obv_weak"] = _obv.trend < 0   # short-scope OBV 空頭 latch = 量能轉弱 (持續狀態, 撐過整段下跌)
    m["fresh_low"] = m["noi"] <= m["noi"].rolling(ALERT_LOW_WIN).min() + 1e-9  # 頂部過熱 badge (窄, 2.17x)
    m["hot_wide"] = m["fresh_low"] | m["obv_weak"]   # 加寬弱勢 (外資期貨 OR OBV弱) for stance entry
    # ── 四正交過熱訊號 + 高信念頂部 (top_vote) — computed before 攻防 so it can drive 高點防守 ──
    _mt = m["mgn"] / m["to"].rolling(MARGIN_MA_WIN).mean()   # 融資/成交金額 (turnover-normalized)
    m["mt_z"] = (_mt - _mt.rolling(MARGIN_MA_WIN).mean()) / _mt.rolling(MARGIN_MA_WIN).std()
    m["margin_overheat"] = m["mt_z"] >= MARGIN_OVERHEAT_Z                                  # E voter
    _lz = (m["lim"] - m["lim"].rolling(LIMITUP_MA_WIN).mean()) / m["lim"].rolling(LIMITUP_MA_WIN).std()
    m["lim_z"] = _lz
    m["limitup_overheat"] = m["lim_z"] >= LIMITUP_OVERHEAT_Z            # 漲停燈 (顯示用, z>=1.0)
    m["limitup_vote"] = m["lim_z"] >= TOP_VOTE_LIMITUP_Z                # D voter (投票用, 更嚴)
    m["limitup_bear"] = m["limitup_overheat"] & (m["st"] < 0)   # 漲停過熱 + 排列翻空 (高信心頂 badge)
    # 外資選擇權避險偏空 (B voter): bull(買權−賣權淨OI) 落在 252日底 1/3. NaN pre-2023-06 -> False.
    m["opt_bull_pctl"] = m["opt_bull"].rolling(OPT_PCTL_WIN, min_periods=OPT_PCTL_MIN).apply(
        lambda w: np.mean(w[~np.isnan(w)] <= w[-1]) if (not np.isnan(w[-1]) and np.isfinite(w).sum() >= OPT_PCTL_MIN) else np.nan,
        raw=True)
    m["opt_bearish"] = (m["opt_bull_pctl"] <= OPT_BEARISH_PCTL).fillna(False)
    # 高信念頂部 (top_vote): >=TOP_VOTE_K of {A 外資期貨fresh_low, B 外資選擇權避險偏空,
    # D 漲停過熱, E 融資過熱}. Robust k-of-n vote over orthogonal channels (see const comment).
    m["top_vote_n"] = (m["fresh_low"].fillna(False).astype(int)
                       + m["opt_bearish"].astype(int)
                       + m["limitup_vote"].fillna(False).astype(int)
                       + m["margin_overheat"].fillna(False).astype(int))
    m["top_vote"] = m["top_vote_n"] >= TOP_VOTE_K
    m["mchg5"] = m["mgn"].pct_change(PANIC_MARGIN_WIN) * 100
    m["ret10"] = m["tx"].pct_change(PANIC_SPEED_WIN) * 100
    m["panic"] = ((m["pct_from_high"] <= PANIC_PFH) & (m["mchg5"] <= PANIC_MARGIN_CHG)
                  & (m["ret10"] <= PANIC_SPEED_CHG))   # 恐慌買進 (display marker, 快殺 -6%)
    # 恐慌急殺早轉攻 (stance only, 快殺 <= -10%): 比顯示的恐慌買進嚴, 自動轉攻才不在慢熊接刀
    m["panic_exit"] = ((m["pct_from_high"] <= PANIC_PFH) & (m["mchg5"] <= PANIC_MARGIN_CHG)
                       & (m["ret10"] <= STANCE_PANIC_EXIT_SPEED))
    # 波段停滯防守 (swing): 過熱背景 (近 SWING_LOOK 日票數 >= SWING_VOTES) + 漲勢停下來 (跌破 5 日高)
    _hi5 = m["tx"].rolling(SWING_HI_WIN, min_periods=1).max()
    m["swing"] = ((m["top_vote_n"] >= SWING_VOTES).rolling(SWING_LOOK, min_periods=1).max() > 0) \
        & (m["tx"] < _hi5 * (1 - SWING_DROP / 100))
    m["swing_exit"] = m["tx"] >= _hi5
    m["trend_exit"] = m["tx"] >= m["tx"].rolling(TREND_EXIT_HI_WIN, min_periods=1).max()
    # 攻防狀態 (stateful hysteresis): 高點防守 (top_vote 一亮就防守, 價格論點持有) + 向下趨勢防守
    # (加寬過熱 AND 排列翻空); 轉攻 = 漲回頂上(向上趨勢) / 排列回多 / early_reattack(底部) /
    # 恐慌急殺 V(panic_exit). 見 compute_stance.
    m["defensive"] = compute_stance(m["st"].to_numpy(), m["hot_wide"].to_numpy(),
                                    _early_reattack(m), _reattack_safe(m),
                                    top_vote=m["top_vote"].to_numpy(), price=m["tx"].to_numpy(),
                                    panic_exit=m["panic_exit"].to_numpy(),
                                    top_reentry_win=STANCE_TOP_REENTRY_WIN,
                                    swing=m["swing"].to_numpy(), swing_exit=m["swing_exit"].to_numpy(),
                                    trend_exit=m["trend_exit"].to_numpy(),
                                    block_entry_at_hi=BLOCK_ENTRY_AT_HI,
                                    swing_max_hold=SWING_MAX_HOLD)

    ma = m["mgn"].rolling(MARGIN_MA_WIN).mean()
    sd = m["mgn"].rolling(MARGIN_MA_WIN).std()
    m["margin_z"] = (m["mgn"] - ma) / sd                        # deviation from 55d trend in σ
    m["margin_hot"] = ((m["margin_z"] + 2) / 4).clip(0, 1) * 100  # −2σ→0, 均線→50, +2σ→100
    # (mt_z/margin_overheat, lim_z/limitup_overheat, limitup_bear, opt_bearish, top_vote are
    #  computed above — before 攻防 — so top_vote can drive the 高點防守 override.)
    # 加速惡化警示: 頂部過熱燈(近3日) AND 排列未翻空(st>=0) AND short空頭排列3日急升. Bridges the
    # stance lag (fires while still 攻擊, before 排列 flips). Caution flag, ~half false.
    m["top3"] = ((m["fresh_low"] | m["margin_overheat"]).rolling(TOP_LOOKBACK, min_periods=1).max() > 0)
    m["sd_chg"] = m["sd_pct"] - m["sd_pct"].shift(WARN_SD_CHG_WIN)
    m["warn"] = (m["top3"].to_numpy() & (m["st"].to_numpy() >= 0)
                 & (m["sd_chg"].to_numpy() >= WARN_SD_CHG))
    m["futures_hot"] = (1 - _roll_pctl(m["noi"], FUT_WIN)) * 100   # more net-short -> hotter
    # 定位極端度 = 外資期貨 + 融資 only. P/C 與微台散戶在頂/底皆極端(反指標無方向鑑別力),
    # 只會稀釋分數, 已移除 (see memory project_market_thermometer 2026-07-21).
    m["score"] = (m["margin_hot"] + m["futures_hot"]) / 2
    m = m.dropna(subset=["score"]).reset_index(drop=True)
    if m.empty:
        return {"as_of": None, "score": None, "bucket": None, "components": [], "history": []}

    last = m.iloc[-1]
    hist = m.tail(HISTORY_DAYS)
    components = [
        {"key": "futures", "name": "外資期貨定位", "hot": round(last["futures_hot"], 1),
         "detail": f"外資臺股期貨淨未平倉 {int(last['noi']):+,} 口（{FUT_WIN}日百分位越低越淨空＝越熱）"},
        {"key": "margin", "name": "融資水位", "hot": round(last["margin_hot"], 1),
         "detail": f"融資餘額金額 {last['mgn']/1e5:,.0f}億（距 {MARGIN_MA_WIN} 日均 {last['margin_z']:+.2f}σ；+2σ=過熱）"},
    ]
    def _r1(x):
        return None if pd.isna(x) else round(float(x), 1)

    history = []
    for r in hist.itertuples(index=False):
        lbl, col, _ = _regime(r.score, bool(r.pct_from_high >= -NEAR_HIGH_TOL), bool(r.fresh_low))
        history.append({"date": r.d.date().isoformat(), "score": round(r.score, 1),
                        "tx": round(r.tx), "label": lbl, "color": col,
                        "stance": "防守" if r.defensive else "攻擊",
                        "panic": bool(r.panic),
                        "m_alert": bool(r.margin_overheat),
                        "l_alert": bool(r.limitup_overheat),
                        "lb_alert": bool(r.limitup_bear),
                        "tv_alert": bool(r.top_vote),
                        "tv_n": int(r.top_vote_n),
                        "warn": bool(r.warn),
                        "swing": bool(r.swing),
                        "c": {"futures": _r1(r.futures_hot), "margin": _r1(r.margin_hot)}})

    pfh = last["pct_from_high"]
    a_near = bool(pfh >= -NEAR_HIGH_TOL)
    a_fresh = bool(last["fresh_low"])
    label, color, danger = _regime(last["score"], a_near, a_fresh)
    defensive = bool(last["defensive"])
    return {
        "as_of": last["d"].date().isoformat(),
        "score": round(last["score"], 1),
        "bucket": label,
        "bucket_color": color,
        "danger": danger,
        "stance": "防守" if defensive else "攻擊",
        "stance_color": "#ef4444" if defensive else "#22c55e",
        "stance_reason": (f"防守中（排列：{ST_LABELS.get(int(last['st']), '?')}）：高點防守（高信念頂部）／向下趨勢／"
                          f"波段停滯（過熱背景下跌破 {SWING_HI_WIN} 日高）；漲回頂位之上／站回 {SWING_HI_WIN} 日高／指數站回 {TREND_EXIT_HI_WIN} 日高／"
                          f"排列回多方／外資翻多且空頭力竭／恐慌急殺 V 任一則轉攻"
                          if defensive else f"攻擊中（排列：{ST_LABELS.get(int(last['st']), '?')}，中性以上）"),
        "swing": bool(last["swing"]),
        "near_high": a_near,
        "pct_from_high": round(float(pfh), 1),
        "hi_window": HI_WINDOW,
        "alert": a_fresh,
        "alert_conditions": [
            {"name": f"外資淨空創 {ALERT_LOW_WIN} 日新低（淨空最重）", "met": a_fresh},
        ],
        "margin_alert": bool(last["margin_overheat"]),
        "margin_alert_conditions": [
            {"name": f"融資/成交量 布林 z ≥ +{MARGIN_OVERHEAT_Z:.1f}σ（現 {last['mt_z']:+.1f}σ）",
             "met": bool(last["margin_overheat"])},
        ],
        "limitup_alert": bool(last["limitup_overheat"]),
        "limitup_alert_conditions": [
            {"name": f"漲停佔比 {LIMITUP_MA_WIN} 日乖離 z ≥ +{LIMITUP_OVERHEAT_Z:.1f}σ（現 {last['lim_z']:+.1f}σ、漲停 {last['lim']:.1f}%）",
             "met": bool(last["limitup_overheat"])},
        ],
        "limitup_bear_alert": bool(last["limitup_bear"]),
        "limitup_bear_conditions": [
            {"name": f"漲停過熱（漲停佔比 {LIMITUP_MA_WIN} 日乖離 z ≥ +{LIMITUP_OVERHEAT_Z:.1f}σ，現 {last['lim_z']:+.1f}σ）",
             "met": bool(last["limitup_overheat"])},
            {"name": f"排列翻空（short 排列：{ST_LABELS.get(int(last['st']), '?')}）",
             "met": bool(last["st"] < 0)},
        ],
        "top_vote": bool(last["top_vote"]),
        "top_vote_n": int(last["top_vote_n"]),
        "top_vote_k": TOP_VOTE_K,
        "top_vote_conditions": [
            {"name": f"外資期貨淨空創 {ALERT_LOW_WIN} 日新低", "met": bool(last["fresh_low"])},
            {"name": (f"外資選擇權避險偏空（買賣權淨OI {OPT_PCTL_WIN} 日百分位 ≤ {OPT_BEARISH_PCTL:.0%}，現 {last['opt_bull_pctl']:.0%}）"
                      if pd.notna(last["opt_bull_pctl"]) else "外資選擇權避險偏空（資料不足）"),
             "met": bool(last["opt_bearish"])},
            {"name": (f"漲停過熱（{LIMITUP_MA_WIN} 日乖離 z ≥ +{TOP_VOTE_LIMITUP_Z:.1f}σ，投票用門檻"
                      f"比漲停燈的 +{LIMITUP_OVERHEAT_Z:.1f}σ 嚴；現 {last['lim_z']:+.1f}σ）"),
             "met": bool(last["limitup_vote"])},
            {"name": f"融資過熱（{MARGIN_MA_WIN} 日 z ≥ +{MARGIN_OVERHEAT_Z:.1f}σ）", "met": bool(last["margin_overheat"])},
        ],
        "warn": bool(last["warn"]),
        "warn_conditions": [
            {"name": f"頂部過熱燈亮（近 {TOP_LOOKBACK} 日）", "met": bool(last["top3"])},
            {"name": f"排列尚未翻空（short 排列：{ST_LABELS.get(int(last['st']), '?')}）",
             "met": bool(last["st"] >= 0)},
            {"name": f"short 空頭排列 {WARN_SD_CHG_WIN} 日急升 ≥ +{WARN_SD_CHG:.0f}pp（現 {last['sd_chg']:+.1f}pp）",
             "met": bool(last["sd_chg"] >= WARN_SD_CHG)},
        ],
        "panic": bool(last["panic"]),
        "panic_conditions": [
            {"name": f"深跌（距 {HI_WINDOW} 日高 ≤ {PANIC_PFH:.0f}%）", "met": bool(last["pct_from_high"] <= PANIC_PFH)},
            {"name": f"融資 {PANIC_MARGIN_WIN} 日急殺（≤ {PANIC_MARGIN_CHG:.0f}%）", "met": bool(last["mchg5"] <= PANIC_MARGIN_CHG)},
            {"name": f"快殺（指數 {PANIC_SPEED_WIN} 日跌 ≤ {PANIC_SPEED_CHG:.0f}%）", "met": bool(last["ret10"] <= PANIC_SPEED_CHG)},
        ],
        "components": components,
        "history": history,
    }


def _intraday_short_trend_total() -> tuple[int, float, str] | None:
    """Live short-scope 排列 (total base) from the breadth sidecar that
    intraday_snapshot writes each pass. Returns (TREND_CODE, 空頭排列占比%, sidecar_date)
    or None. 空頭排列占比 feeds the 升速放緩 early-reattack test on the forming bar."""
    import json
    from pathlib import Path
    sidecar = Path(__file__).parent.parent / "data" / "breadth_intraday.json"
    if not sidecar.exists():
        return None
    try:
        with open(sidecar, encoding="utf-8") as f:
            ib = json.load(f)
    except Exception:
        return None
    total = ib.get("total") or ib.get("active")
    if not total:
        return None
    from analysis.market_breadth import classify_trend, TREND_CODE
    up = ib["short_up"] / total * 100
    dn = ib["short_down"] / total * 100
    return TREND_CODE[classify_trend(up, dn, 100 - up - dn)], dn, str(ib.get("trade_date"))


def _tx_forming_bar(volume_scale: float, now: datetime) -> tuple | None:
    """Today's forming 大台 bar (o,h,l,c,v) with h(t)-projected volume, taken as
    the last bar of tx_status.build_tx_data (which fetches the live cnyes TXF
    quote and scales its volume). Returns None if no live bar for today."""
    try:
        from analysis.tx_status import build_tx_data
        res = build_tx_data(intraday=True, volume_scale=volume_scale, now=now)
    except Exception:
        return None
    data = res[0] if isinstance(res, tuple) else res
    if data is None or len(data.dates) == 0 or data.dates[-1] != now.date():
        return None
    return (float(data.open[-1]), float(data.high[-1]), float(data.low[-1]),
            float(data.close[-1]), float(data.volume[-1]))


def build_intraday_stance(cur, volume_scale: float, now: datetime) -> dict | None:
    """Live 攻防 for the current intraday pass. Same history + hysteresis as the
    daily gauge (_load_merged_frame + compute_stance) but with a forming last bar:
    short_trend_total from the breadth sidecar, obv_weak from the daily TXF series
    with today's forming bar grafted on, fresh_low from the last available foreign
    net_oi (stale — only the OR side of entry; obv_weak carries it live)."""
    m = _load_merged_frame(cur)
    if m.empty:
        return None

    today = now.date()
    ts = pd.Timestamp(today)
    sc = _intraday_short_trend_total()
    forming = _tx_forming_bar(volume_scale, now)
    grafted = forming is not None and ts not in set(m["d"])   # 真的接了 forming bar 才算盤中
    if grafted:
        o, h, l, c, v = forming
        row = {col: np.nan for col in m.columns}
        row["d"] = ts
        row["o"], row["h"], row["l"], row["c"], row["v"] = o, h, l, c, v
        row["ref"] = m["c"].iloc[-1]                       # prev front-month close
        row["noi"] = m["noi"].iloc[-1]                     # stale (foreign net_oi is close-only)
        row["tx"] = m["tx"].iloc[-1]                        # stale TAIEX close for the 高點防守 price gate
        # opt_bull / mgn / lim stay NaN on the forming bar (close-only) → B/D/E voters False;
        # top_vote latches from recent closes via TOP_LOOKBACK, so 高點防守 holds live.
        if sc is not None and sc[2] == today.isoformat():
            row["st"] = sc[0]
            row["sd_pct"] = sc[1]                           # live 空頭排列占比 for 升速放緩
        else:
            row["st"] = int(m["st"].iloc[-1])              # sd_pct stays NaN -> no live early exit
        m = pd.concat([m, pd.DataFrame([row])], ignore_index=True)

    _obv = calculate_obv(m["c"].to_numpy(np.float32), m["ref"].to_numpy(np.float32),
                         m["h"].to_numpy(np.float32), m["l"].to_numpy(np.float32),
                         m["v"].to_numpy(np.float32), **PERIOD_PARAMS["short"])
    m["obv_weak"] = _obv.trend < 0
    m["fresh_low"] = m["noi"] <= m["noi"].rolling(ALERT_LOW_WIN).min() + 1e-9
    m["hot_wide"] = m["fresh_low"].fillna(False) | m["obv_weak"]
    # top_vote for 高點防守 (same voters as the daily gauge; forming bar's B/D/E are NaN→False,
    # so top_vote latches from recent closes and the daily gauge's 頂位 holds intraday).
    _mt = m["mgn"] / m["to"].rolling(MARGIN_MA_WIN).mean()
    m["margin_overheat"] = ((_mt - _mt.rolling(MARGIN_MA_WIN).mean()) / _mt.rolling(MARGIN_MA_WIN).std()) >= MARGIN_OVERHEAT_Z
    # D voter only (盤中不出漲停 badge), so this uses the stricter vote threshold
    m["limitup_vote"] = ((m["lim"] - m["lim"].rolling(LIMITUP_MA_WIN).mean()) / m["lim"].rolling(LIMITUP_MA_WIN).std()) >= TOP_VOTE_LIMITUP_Z
    _obp = m["opt_bull"].rolling(OPT_PCTL_WIN, min_periods=OPT_PCTL_MIN).apply(
        lambda w: np.mean(w[~np.isnan(w)] <= w[-1]) if (not np.isnan(w[-1]) and np.isfinite(w).sum() >= OPT_PCTL_MIN) else np.nan,
        raw=True)
    m["opt_bearish"] = (_obp <= OPT_BEARISH_PCTL).fillna(False)
    m["top_vote"] = (m["fresh_low"].fillna(False).astype(int) + m["opt_bearish"].astype(int)
                     + m["limitup_vote"].fillna(False).astype(int)
                     + m["margin_overheat"].fillna(False).astype(int)) >= TOP_VOTE_K
    # 恐慌急殺早轉攻 (close-only inputs; forming bar's mgn is NaN → panic_exit False, latches from closes)
    _mchg5 = m["mgn"].pct_change(PANIC_MARGIN_WIN) * 100
    _ret10 = m["tx"].pct_change(PANIC_SPEED_WIN) * 100
    m["panic_exit"] = ((m["pct_from_high"] <= PANIC_PFH) & (_mchg5 <= PANIC_MARGIN_CHG)
                       & (_ret10 <= STANCE_PANIC_EXIT_SPEED)).fillna(False)
    # 波段停滯防守 (同 daily). Forming bar 的 tx 是前一日收盤 (盤中拿不到即時指數), 它會讓 5 日高
    # 視窗少一個相異收盤 → 判定與收盤不一致 (實測 swing_exit 有 4% 的日子翻面). 故 forming bar 直接
    # 沿用前一收盤日的 swing/swing_exit, 讓盤中就是把收盤的 latch 帶著走.
    _tvn = (m["fresh_low"].fillna(False).astype(int) + m["opt_bearish"].astype(int)
            + m["limitup_vote"].fillna(False).astype(int) + m["margin_overheat"].fillna(False).astype(int))
    _hi5 = m["tx"].rolling(SWING_HI_WIN, min_periods=1).max()
    m["swing"] = ((_tvn >= SWING_VOTES).rolling(SWING_LOOK, min_periods=1).max() > 0) \
        & (m["tx"] < _hi5 * (1 - SWING_DROP / 100))
    m["swing_exit"] = m["tx"] >= _hi5
    m["trend_exit"] = m["tx"] >= m["tx"].rolling(TREND_EXIT_HI_WIN, min_periods=1).max()
    if grafted and len(m) >= 2:
        for _c in ("swing", "swing_exit", "trend_exit"):
            m.loc[m.index[-1], _c] = bool(m[_c].iloc[-2])
    defensive = compute_stance(m["st"].to_numpy(), m["hot_wide"].to_numpy(),
                               _early_reattack(m), _reattack_safe(m),
                               top_vote=m["top_vote"].to_numpy(), price=m["tx"].to_numpy(),
                               panic_exit=m["panic_exit"].to_numpy(),
                               top_reentry_win=STANCE_TOP_REENTRY_WIN,
                               swing=m["swing"].to_numpy(), swing_exit=m["swing_exit"].to_numpy(),
                               trend_exit=m["trend_exit"].to_numpy(),
                               block_entry_at_hi=BLOCK_ENTRY_AT_HI,
                               swing_max_hold=SWING_MAX_HOLD)

    last = m.iloc[-1]
    is_def = bool(defensive[-1])
    st_last = int(last["st"])
    return {
        "as_of": last["d"].date().isoformat(),
        "snapshot_time": now.isoformat(),
        "is_today": bool(last["d"].date() == today),
        "stance": "防守" if is_def else "攻擊",
        "stance_color": "#ef4444" if is_def else "#22c55e",
        "stance_reason": (f"防守中（排列：{ST_LABELS.get(st_last, '?')}）：高點防守（高信念頂部）／向下趨勢／"
                          f"波段停滯（過熱背景下跌破 {SWING_HI_WIN} 日高）；漲回頂位之上／站回 {SWING_HI_WIN} 日高／指數站回 {TREND_EXIT_HI_WIN} 日高／"
                          f"排列回多方／外資翻多且空頭力竭／恐慌急殺 V 任一則轉攻"
                          if is_def else f"攻擊中（排列：{ST_LABELS.get(st_last, '?')}，中性以上）"),
        "short_trend": st_last,
    }


if __name__ == "__main__":
    from db.connection import get_cursor
    with get_cursor(commit=False) as cur:
        t = build_thermometer(cur)
        print(f"as_of={t['as_of']} score={t['score']} bucket={t['bucket']}")
        for c in t["components"]:
            print(f"  {c['name']}: hot={c['hot']}  {c['detail']}")
        # sanity: was the gauge running hot before the 6 covered crash peaks?
        cur.execute("SELECT trade_date, margin_balance_value FROM tw.margin_summary ORDER BY trade_date")
        import pandas as pd
        mg = pd.DataFrame(cur.fetchall()); mg["d"] = pd.to_datetime(mg["trade_date"])
        cur.execute("""SELECT trade_date, net_oi FROM tw.taifex_inst_futures
                       WHERE product='臺股期貨' AND investor='外資及陸資' ORDER BY trade_date""")
        fu = pd.DataFrame(cur.fetchall()); fu["d"] = pd.to_datetime(fu["trade_date"])
        mg["mgn"] = mg["margin_balance_value"].astype(float); fu["noi"] = fu["net_oi"].astype(float)
        m = (mg[["d", "mgn"]].merge(fu[["d", "noi"]], on="d")
             .sort_values("d").reset_index(drop=True))
        m["mh"] = (((m["mgn"] - m["mgn"].rolling(MARGIN_MA_WIN).mean()) / m["mgn"].rolling(MARGIN_MA_WIN).std() + 2) / 4).clip(0, 1) * 100
        m["fh"] = (1 - _roll_pctl(m["noi"], FUT_WIN)) * 100
        m["s"] = (m["mh"] + m["fh"]) / 2
        m = m.set_index("d")
        print("\nsanity — 崩盤峰當日溫度分數 (vs 全樣本中位):")
        print(f"  全樣本 score 中位={m['s'].median():.0f} p75={m['s'].quantile(.75):.0f}")
        for p in ["2020-01-14", "2021-04-27", "2022-01-04", "2024-07-11", "2026-02-26", "2026-06-22"]:
            i = m.index.searchsorted(pd.Timestamp(p))
            if i < len(m):
                print(f"  {p}: score={m['s'].iloc[i]:.0f}")
