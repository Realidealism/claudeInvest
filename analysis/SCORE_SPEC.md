# ScoreBoard 技術評分給分規格

- 對應程式：`analysis/score.py`（`build_scoreboard()`）；整理基準 commit `991228cd`，2026-11-03
- cell 清單與分數總和由實際呼叫 `build_scoreboard()` 取得，不是從程式碼人工清點
- 改 `score.py` 或其上游指標時，請同步更新本檔；本檔與程式不一致時以程式為準

---

## 1. 結構

```
ScoreBoard「技術評分」
├── short  卡（14 cell／側）
├── medium 卡（26 cell／側）
└── long   卡（36 cell／側）
    每張卡分 long_items（做多側）與 short_items（做空側）
```

- 每側共 **76 個 cell**，滿分範圍 **±319.596**（short ±27.340、medium ±75.739、long ±216.517）。
- 每個 cell 都在兩側各註冊一次，做空側的分數是做多側的鏡像（正負號相反）。
- 卡片名稱（short／medium／long）只是分組，**不代表 cell 用的均線期數**。例如 medium 卡的「排列」cell 用的是 SMA 5/13/34，不是 21/34/55（見 §3.2）。
- cell 有兩種：
  - **bool**：條件成立時得 `points`
  - **連續**：`evaluate` 回傳一個值，夾在 [-cap, +cap]，直接當作分數

### 1.1 權重乘數

`實際分數 = 基準分 × WEIGHT_MULTIPLIERS[(類別, 卡)]`（score.py:53-72），表中查不到的組合乘數為 1.0。

乘數採 C dampened 版（2026-05-04）：
- `m_raw = clip(1 + 5×Δ, 0.3, 2.0)`，Δ 由 2017–2020 訓練期算出
- `m_final = (m_raw + 1) / 2`

| 類別 | short | medium | long |
|---|---|---|---|
| 扣抵 | —（已拔） | 1.185 | 1.099 |
| 排列 | — | 0.969 | 1.341 |
| 洪量 | 0.650 | 0.924 | 1.041 |
| MACD | 0.878 | （1.013 孤兒） | 1.0（不在表） |
| OBV | 0.815 | 1.033 | 0.938 |
| 波浪 | 0.912 | （1.036 孤兒） | 1.131 |
| 大盤 | — | 1.0（不在表） | （1.096 孤兒） |
| Donchian | — | — | 1.0（不在表） |

「孤兒」指乘數還留在表裡，但對應的 cell 已拔除，不會被用到。

---

## 2. Cell 總表（做多側分數；做空側全部正負相反）

### short 卡（14）

| 類別 | cell | 分數 |
|---|---|---|
| 洪量 | `站上1階洪`／`跌破1階洪` | +6.5／−6.5 |
| MACD | `short金叉_新`／`_續`、`short死叉_新`／`_續` | +4.390／+2.634／−4.390／−2.634 |
| OBV | `shortOBV升_新`／`_續`、`shortOBV降_新`／`_續` | +4.075／+2.445／−4.075／−2.445 |
| 波浪 | `浪D4金_新`／`_續`、`浪D4死_新`／`_續` | +4.560／+2.736／−4.560／−2.736 |

### medium 卡（26）

| 類別 | cell | 分數 |
|---|---|---|
| 扣抵 | `21MA漲扣`／`跌扣`、`34MA…`、`55MA…` | 各 ±5.925 |
| 扣抵 | `13MA模糊漲扣`／`跌扣`、`89MA模糊…` | 各 ±2.9625 |
| 排列 | `medium_sort_normal多排`／`空排` | ±9.690 |
| 排列 | `medium_forming多排`／`空排` | ±4.845 |
| 大盤 | `大盤medium_down_-10`、`_none_-5`、`_down_-5`、`_up_+5`、`_up_+10`、`_none_+5` | −10／−5／−5／+5／+10／+5 |
| 洪量 | `站上2階洪`／`跌破2階洪` | ±9.240 |
| OBV | `mediumOBV升_新`／`_續`、`降_新`／`_續` | +5.165／+3.099／−5.165／−3.099 |

### long 卡（36）

| 類別 | cell | 分數 |
|---|---|---|
| 扣抵 | `144MA漲扣`／`跌扣`、`233MA…`、`377MA…` | 各 ±16.485 |
| 扣抵 | `89MA模糊漲扣`／`跌扣` | ±16.485 |
| 排列 | `long_sort_normal多排`／`空排` | ±13.410 |
| 排列 | `long_forming多排`／`空排` | ±6.705 |
| 洪量 | `站上3階洪`／`跌破3階洪` | ±10.410 |
| MACD | `long金叉_新`／`_續`、`死叉_新`／`_續` | +5／+3／−5／−3 |
| OBV | `longOBV升_新`／`_續`、`降_新`／`_續` | +4.690／+2.814／−4.690／−2.814 |
| 波浪 | `浪D8金_新`／`_續`、`浪D8死_新`／`_續` | +5.655／+3.393／−5.655／−3.393 |
| Donchian | `Don233破高_新`／`_續`、`Don233破低_新`／`_續` | +5／+3／−5／−3 |
| 距離 | `距SMA55`、`距SMA89`、`距SMA144`、`距SMA233`、`距SMA377`（連續） | 各 [−10, +10] |
| 波動 | `低波動`（連續） | [−37.5, +37.5] |

---

## 3. 各 cell 觸發定義

### 3.0 事件型共用的「新／續」窗口（MACD、OBV、波浪、Donchian）

窗口常數在 score.py:875-878：`_MACD_FRESH_WINDOW=1`、`_MACD_CARRY_WINDOW=5`。

- **新**：事件發生在 [i−1, i]，也就是今天或昨天。基準分 ±5 × 乘數。
- **續**：事件發生在 [i−5, i]，且不符合「新」。基準分 ±3 × 乘數。
- 同一方向的「新」與「續」互斥。反向事件各自獨立計算，例如 3 天前死叉、今天金叉時，`金叉_新` 和 `死叉_續` 會同時觸發。

### 3.1 扣抵（`close_result.turn[p]`；analysis/close.py:316-353，參數在 constants.py:37-49）

- 值的意義：`turn[p][i]` 為 2＝漲扣、0＝跌扣、1＝中性。
- 判定方式（p ≥ 8）：
  - 先算 `hi = rolling_highest(close, bs)`、`lo = rolling_lowest(close, bs)`，窗口含當日。
  - 往前位移 `offset` 根：`close[i] > hi[i−offset]` 為 2，`close[i] < lo[i−offset]` 為 0，其餘為 1。
  - 兩邊都是嚴格比較，沒有容差。`i < offset+1` 時一律為 1。
- 參數表：

| p | offset | bs |
|---|---|---|
| 13 | 10 | 3 |
| 21 | 16 | 5 |
| 34 | 26 | 8 |
| 55 | 42 | 13 |
| 89 | 68 | 21 |
| 144 | 110 | 34 |
| 233 | 178 | 55 |
| 377 | 288 | 89 |

- 一般扣抵 cell：漲扣得 +pts，跌扣得 −pts。
- 模糊扣抵 cell（score.py:511-543）：
  - 該卡的核心期數全部為中性時才看，否則不觸發。
  - medium 卡的核心期數是 21/34/55，再看 13 與 89。
  - long 卡的核心期數是 144/233/377，再看 89。

### 3.2 排列（score.py:468-599）

**`sort_normal[scope]`**（close.py:193-207，參數 `SORT_NORMAL` 在 constants.py:22-26）
- medium＝SMA(5, 13, 34)，long＝SMA(8, 21, 55)。
- 多排：`m1 > m2 > m3`；空排：`m1 < m2 < m3`。只比均線之間的大小，不含收盤價；有相等時兩者都不成立。

**`sort_forming[scope]`**（close.py:210-276）
- `pre` 是 pre_sma（indicators.py:42-64），`preN[i] = mean(close[i−N+2 .. i])`，即最近 N−1 根（含今日）的均值。它等於「明日收盤＝這個均值」時明日的 SMA(N)，是對明日的預測。`cd` 是現行 SMA。GAP＝1.003。
- 多排的基本條件 `base_up`，四項全成立：
  - `pre1 > pre2×1.003`
  - `pre2 > pre3×1.003`
  - `pre1 > cd3`
  - `pre1[i] > pre1[i−1]`
- 多排成立條件：`base_up & (昨日 SMA 已多排 | volume_status < 3)`。volume_status 的 0／1／2 分別是洪／大量／量多。
- 空排：`pre1×1.003 < pre2`、`pre2×1.003 < pre3`、`pre1 < cd3`、`pre1[i] < pre1[i−1]`，再配上「昨日 SMA 已空排 | volume_status < 3」。GAP 乘在左邊，所以不是多排的精確鏡像。
- docstring 還寫了「排除已排列」這一條，但程式沒有實作，以程式為準。

### 3.3 大盤（score.py:654-731）

- 輸入：當日 `tw.market_breadth.medium_trend`，以及個股的 `sort_normal["medium"]` 狀態（up／down／none，none＝既非 up 也非 down）。
- 給分規則 `BREADTH_LONG_RULES`：

| trend 代碼 | 個股狀態 | 做多側分數 |
|---|---|---|
| {2} 強多 | down | −10 |
| {2} 強多 | none | −5 |
| {1, 3} 多 | down | −5 |
| {−1, −3} 空 | up | +5 |
| {−2} 強空 | up | +10 |
| {−2} 強空 | none | +5 |

- 意思是：大盤偏多時個股不跟漲扣分，大盤偏空時個股逆勢多排加分。
- 當天的 DB 列優先。DB 沒有當天資料時，改用盤中即時 breadth（`score._LIVE_BREADTH`）：盤中每一輪先算出即時趨勢代碼，再經 `set_live_breadth` 交給評分。即時代碼由 `market_breadth.live_trend_codes` 算出，用的是只算已排列的計數，口徑與 `medium_trend` 相同。兩者都沒有時，所有大盤 cell 都不觸發。晚上的正式路徑和回測從不設定即時代碼。
- trend 代碼的判定（market_breadth.py:36-48）：
  - 輸入是全市場 `sort_normal["medium"]` up／down／neutral 三者的占比。
  - neutral > 50% → 0。
  - up > down 時：`up > neutral > down` 為 2，否則 1。
  - down > up 時：`down > neutral > up` 為 −2，否則 −1。
  - up 與 down 相等 → 0。
  - 分母是非死魚檔數；母體為 `is_active AND market<>'ESB' AND security_type='STOCK'`。
- **±3（exhausting）在 DB 中不存在**：2026-11-03 查 `tw.market_breadth` 只有 {−2, −1, 0, 1, 2}，寫入路徑也不會產生 ±3。所以規則表裡的 3／−3 是死分支。

### 3.4 洪量（score.py:604-643；analysis/volume.py:420-476、583-686）

**洪量日**：以下五組條件同時成立。所有 `prev_*` 都是前一日的值。
- `all_big`：成交量同時大於三組量均線的最大值，三組為 short (3,5,8)、medium (8,13,21)、long (21,34,55)。
- 短頂且長頂：
  - 短頂：`vol > prev_high3×0.9`、`> prev_high5×0.85`、`> prev_high8×0.8`，三項全成立。
  - 長頂：`vol > prev_high21×0.9`、`prev_high34×0.8`、`prev_high55×0.7`，任一成立。
- 持續高：`vol > prev_high13×0.7`、`prev_high21×0.6`、`prev_high34×0.5`、`prev_high55×0.4`，四項全成立。
- `burst | vol > prev_high34`。burst 是以下兩者任一（volume.py:336-365）：
  - `vx_burst`：以下全部成立，其中 `vx = diff(vol)`，`vx_highN` 是 vx 的 N 日最高值。
    - 量增
    - `vx > prev_vr×0.3`
    - `vx == vx_high3`
    - `vx ≥ vx_high5×0.8`、`vx ≥ vx_high8×0.6`、`vx ≥ vx_high13×0.4` 三者任一
    - `vx ≥ vx_high21×0.3`
    - `vx ≥ vx_high34×0.2`
    - `vx ≥ vx_high55×0.1`
  - `v_short_burst`：量增，且 `vol > short_max + prev_vr×0.7`。
  - 「量增」是 `vol[i] ≥ vol[i−1]`；`short_max` 是 short 組量均線的最大值。
- 區間突破，以下任一成立：
  - `vol > prev_long_max + prev_vr×0.8`
  - `vol > prev_long_max + prev_vr×0.7` 且 `vol > prev_high8`
  - `vol > prev_long_max + prev_vr×0.6` 且 `vol > prev_high13`
  - 其中 `volume_range` 是 SMA34 的 55 日高低差。

**tier 參考價**（跳空感知）：
- 每個洪量日記一組 (hv, lv)。
- 用實體 `candle_top/bottom = max/min(open, close)` 判斷跳空：
  - `hv` 平常取當日 high；向下跳空時取前一日實體底。
  - `lv` 平常取當日 low；向上跳空時取前一日實體頂。
- 只保留最近 5 次洪量。`tier_k` 的上緣是最近 k 次 hv 的最大值，下緣是最近 k 次 lv 的最小值。

**站上／跌破**：
- short 卡用 tier 1、medium 卡用 tier 2、long 卡用 tier 3。
- 非洪量日，且之前已有至少 k 次洪量：站上＝`close >= 上緣`（含等號），跌破＝`close < 下緣`。
- 洪量日當天：tier 1 不觸發；tier 2 以上改用昨日的 tier 值比較。

### 3.5 MACD（analysis/macd.py:141-222）

- 卡片對應的 EMA 組：
  - short：`DIF = (EMA3+EMA8)/2 − EMA21`，`DEM = (EMA(DIF,3)+EMA(DIF,8))/2`。
  - long：`DIF = (EMA8+EMA21)/2 − EMA55`，`DEM = (EMA(DIF,8)+EMA(DIF,21))/2`。
- EMA 以第一筆為種子，`k = 2/(period+1)`。
- 金叉：`OSC = DIF − DEM` 由 ≤0 翻為 >0；死叉：由 >0 翻為 ≤0。OSC＝0 算在死叉側。

### 3.6 OBV（analysis/obv.py:53-273）

**計算流程**
1. OBV 逐日累加：`close > ref` 加成交量，`close < ref` 減成交量，相等不動。`ref` 取 `ref_price` 欄位（官方參考價），缺值時退回前一日收盤（backtest/data.py:340-354）。
2. 影子價：令 `obv_diff = OBV − SMA13(OBV)`，`shadow = obv_diff / std26(obv_diff) × std26(high−low)`。shadow > 0 時疊在 high 上，否則疊在 low 上；`std26(obv_diff) = 0`，或 `std26(high−low)` 為 NaN（前期資料不足）時，退回 close（obv.py:210）。
3. `macd = DEMA(EMA 平均後的影子價, dema_len) − EMA(close, slow_len)`。
4. 對 macd 做 slope_len 根線性回歸外推，得到投影值。
5. 階梯化（obv.py:246-255）：`a15 = Σ|src − prev階| / i`，從 index 0 起的全歷史累積平均，不是滾動窗口。`src > prev階 + a15` 或 `src < prev階 − a15` 時換成 src，否則維持上一階。

**事件**：階梯方向翻上的那天為 `signal_up`，翻下的那天為 `signal_down`。

**參數**

| 卡 | slow_len | 影子 EMA | DEMA | slope_len |
|---|---|---|---|---|
| short | 21 | 3/5 | 8 | 1 |
| medium | 55 | 8/13 | 21 | 2 |
| long | 144 | 21/34 | 55 | 3 |

### 3.7 波浪（analysis/wave.py:556-590）

- `dN_ma` 是最近 N 個波的 tip 價平均，包含當前波；當前波的 tip 隨每根 bar 更新。
- D4（short 卡）：
  - 金：`d2ma` 由 ≤`d4ma` 翻為 >`d4ma`；死為鏡像。
  - 需要波數 > 3。
- D8（long 卡）：
  - 金：`d4ma` 由 ≤`d8ma` 翻為 >`d8ma`；死為鏡像。
  - 需要波數 > 7。
- 本規格沒有展開波的切分與 tip 更新規則（`_process_up_wave`／`_process_down_wave`）。

### 3.8 Donchian（analysis/donchian.py:88-168；參數在 backtest/data.py:368）

- 上軌＝前 233 根（不含當日）的最高 high，下軌＝前 233 根的最低 low。
- 破高：`close > 上軌`；破低：`close < 下軌`，都是嚴格比較。
- 前 233 根不觸發。

### 3.9 距離（連續；score.py:1152-1268）

- `dev = close − SMA(N)`，其中 N ∈ {55, 89, 144, 233, 377}。
- `std = 母體標準差(dev[i−251 .. i])`，252 根，含當日。
- 分數：`clip(dev/std × 3, −10, +10)`。
- 以下情況給 0：`i < N+252`，或 `std ≤ 0`。

### 3.10 低波動（連續；score.py:1298-1371）

- 輸入：
  - `v`：個股 Parkinson-233 波動率，`sqrt(mean(ln(H/L)²)/(4 ln2))`，最近 233 根。有效 bar 數 < 116 時為 NaN（analysis/volatility.py:32-67）。
  - `mv`：`tw.market_vol.median_vol` 取當日或之前最近一日的值（由 analysis/build_market_vol.py 寫入）。它是跨股中位數，母體為 `is_active AND STOCK AND market IN (TWSE, TPEx)`，當日有效檔數 ≥ 100 才寫入。
- 公式：`z = −(ln(v/mv) − 0.0306) / 0.3929`，分數 `clip(z × 15, −37.5, +37.5)`。
  - 0.0306 與 0.3929 由 `_score_vol_calibrate.py` 校準。
  - 15 是 2026-07-11 使用者選定的值，不是最佳化結果。
- 以下情況給 0：`v` 非有限或 ≤ 0，`mv ≤ 0`，或日期早於 market_vol 起點。

---

## 4. 彙總、pct 與糾結救援

### 4.1 原始分

`_eval_side`（score.py:258-294）：
- bool cell 觸發時加 `points`。
- 連續 cell 加 `clip(evaluate, −cap, cap)`。
- 三張卡的 score、max、min 直接相加。

**分母固定**：
- `max_possible` 是所有正分 cell 的總和（連續 cell 取 +cap），`min_possible` 是所有負分 cell 的總和（連續 cell 取 −cap）。
- 互斥的 cell（新／續、漲扣／跌扣、6 個大盤狀態、模糊扣抵）也全部算進分母，所以 ±319.596 是理論值，實際到不了。
- 某個 cell 當天無法計算、只能回 0 時，分母照算。例如距 SMA377 在 i < 629 時固定回 0。

### 4.2 糾結救援（只套在合計上；score.py:243-245、297-347）

- 輸入：`close_result.knot[short|medium|long].flag`，定義見下方「knot flag」。
- 預設參數：
  - 比例：short 0.65、medium 0.60、long 0.55、dual 0.50、triple 0.45
  - `rescue_points` 5
  - `cap_rescue` True
- 依 short → medium → long 的順序，對每個時框跑階梯：
  - 當日糾結，且 `score < −ratio×|min|` → +5
  - 連 3 日糾結，且仍低於門檻 → 再 +5
  - 連 5 日糾結，且仍低於門檻 → 再 +5；若 `score > ratio×|max|` → −5
- 接著：
  - long 與 medium 同時糾結，且 `score < −0.50×|min|` → +5
  - 三個時框同時糾結，且 `score < −0.45×|min|` → +5
- 每一階都用更新後的分數判斷，所以會累加。
- 救援只改 score，不改 max／min。做空側也獨立套用一次。
- 門檻換算成原始分：short 階約 −207.7，triple 約 −143.8。也就是說，救援只對極端低分生效。

**knot flag**（close.py:374-490，參數 `KNOT_DEFS` 在 constants.py:60-64）
- `bias = (max(SMA組) − min(SMA組)) / min`。
- 門檻：`bound(th) = SMA_W(bias)[i−1] − th × std_W(bias)[i−1]`。
- 「連 d 日低於 bound(th)」記為 (th, d)：`bias[i], …, bias[i−d+1]` 全部 `< bound`，而且 d 日都與同一個 bound（取 i−1 日的值）比較。`i < 4` 時 flag 一律為 False。
- 均線組與 W：
  - short：SMA (3,5,8)，W＝55
  - medium：SMA (3,5,8,13,21)，W＝144
  - long：SMA (3…55)，W＝377
- 各時框的 flag：
  - short：(0.9,1d，用 ≤) 或 (0.8,1d) 或 (0.7,2d) 或 (0.6,3d) 或 (0.5,5d)
  - medium：[(0.9,1d) 或 (0.8,2d) 或 (0.7,3d)]，且 (0.6,5d)
  - long：[(0.9,1d) 或 (0.8,2d)]，且 (0.7,3d)，且 (0.6,5d)

### 4.3 pct（score.py:111-117）

```
pct = (score − min_possible) / (max_possible − min_possible) × 200 − 100
```

- 分母目前是 639.192，所以每 1 分原始分約等於 0.3129 pct，一次 +5 救援約等於 +1.56 pct。
- 兩側的 pct 分開取：做多用 `total.long.pct`（`_long_pct_array`），做空用 `total.short.pct`（`_short_pct_array`）。兩者約有 99.98% 是彼此的鏡像，只在糾結救援 cap 的邊緣情況會分歧（_conditions.py:405-410 的註解）。

---

## 5. 母體與消費端

- **score.py 本身不做母體過濾**，任何股票都能算分。
- 訊號工廠：
  - `signal_backtest/factories/_conditions.py:400-441` 逐 bar 呼叫，i 從 60 開始；前 60 根的 pct 為 0，例外也回 0。
  - 使用的檔案：buy_sell.py、flee.py、pick_touch.py。
- 每日／盤中排名：
  - `analysis/daily_snapshot.py` 與 `intraday_snapshot.py` 只納入 `money_level >= 4` 的股票。依程式註解，這是 8 日均成交額 ≥ 2,700 萬，比死魚門檻高一級。門檻可用環境變數 `DAILY_SNAPSHOT_MIN_LEVEL`／`INTRADAY_SNAPSHOT_MIN_LEVEL` 調整，預設都是 4。
  - `export/generate.py` 寫出 `scores_intraday.json`（多空各取 top-500），`telegram_bot/handlers/score.py` 讀取這個檔。
- pct 快取的 key 是 `board_fingerprint`（backtest/data.py:142-146），`SCORE_CACHE_VERSION="v3"`（score.py:401）。

---

## 6. 已拔除／不註冊的 cell

| cell | 狀態 | 位置 |
|---|---|---|
| 扣抵_short | 2026-04-28 拔除，05-13 重測後永久拒絕 | score.py:446-449 |
| MACD_medium | 2026-07-07 拔除；舊版 osc 方向 cell（`_add_macd_rules`）仍留在檔內，但沒有被呼叫 | score.py:737-863、900-903 |
| 大盤_long | 2026-07-07 拔除，`BREADTH_SCOPES` 只剩 medium | score.py:663-668 |
| 波浪_medium | 2026-05-13 拔除（D6） | score.py:1031-1034 |
| Donchian short／medium | 與其他 cell 共線，不註冊 | score.py:1094-1100 |
| 排列_short、`short_sort_normal空排` 不對稱版 | v298/v299 試驗，未採用 | score.py:482-486 |
| tip_breakout、sink_reversal | 已移除 | score.py:1011-1014 |

ablation 的結論與「勿重提」清單見 memory `project_score_ablation_findings`。

---

## 7. 程式內與本規格不一致的註解

- `build_scoreboard` 的 docstring（score.py:404-427）與實作不符，以實作為準：
  - 寫扣抵每條 ±5（long 卡實際是 15 × 乘數）。
  - 寫洪量每時框 ±15（實際是 10 × 乘數）。
  - 寫波浪只在 medium 卡，且含 tip_breakout。
  - 寫 Donchian 窗口為 18/47/123。
  - 寫距離 cell 是 medium SMA34 與 long SMA144。
- `_add_distance_rules` 的 docstring（score.py:1231）寫 4 個 cell（55/89/144/233），實際註冊 5 個（含 377）。
- 距離 cell 的區塊註解（score.py:1141-1150）也還寫 medium SMA34、long SMA144。
- `calc_sort_forming` 的 docstring 寫了條件 5「排除已排列」，程式沒有實作。

## 8. 未確認事項

- 低波動常數的校準母體註解寫「~dead universe」，但 `build_market_vol` 的母體沒有排除死魚，兩者是否一致尚未確認。
- 本規格沒有展開波浪的切波與 tip 更新規則（analysis/wave.py 的 `_process_up_wave`／`_process_down_wave`）。
