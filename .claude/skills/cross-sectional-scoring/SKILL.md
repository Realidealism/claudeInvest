---
name: cross-sectional-scoring
description: ScoreBoard cell ablation / weight tuning / 訊號形式選擇方法論。Use when adding/removing/upweighting/downweighting/testing cells in analysis/score.py, building or comparing tmp/score_panel.parquet, or evaluating ScoreBoard adoption decisions. 涵蓋 ablation workflow、universe (~dead) filter、regime split、weight-upgrade trap、trend vs event form 選擇。配合 memory project_score_ablation_findings.md / feedback_score_alive_universe.md 使用。
---

# ScoreBoard cell ablation 方法論（此專案版）

針對 [analysis/score.py](analysis/score.py) 的 ScoreBoard 系統。所有 ablation 用 `~dead_fish` 過濾（佔 panel 49.4%），由 [_score_panel.py](analysis/_score_panel.py) 建構的 ~3.4M panel rows，21+ cells。配合 memory [project_score_ablation_findings.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\project_score_ablation_findings.md) 永久封存的失敗清單避免重蹈。

## 快速路徑：純 weight 改動免 rebuild（改既有 cell 權重 / ablation）

若這次只改**既有 cell 的權重**（或做 ablation＝權重歸零），**不要** rebuild panel。cell 貢獻對權重線性，直接用既有 panel 做欄位算術，27 min → 秒級：

```bash
# scale = 新權重 / 舊權重（±5→±15 為 3.0；multiplier 1.341→2.0 為 ~1.49；ablation 為 0）
python -m analysis._score_reweight "排列_long=1.5,洪量_medium=0"
```

工具 [analysis/_score_reweight.py](analysis/_score_reweight.py) 直接輸出 baseline vs reweighted 的 H×regime decile spread 與採用判準，**預設 ~dead universe**（自動 merge `tmp/dead_fish_panel.parquet`；`--full-universe` 僅供參考不作採用依據）。精度：非 knot-rescue 列（實測 production panel 99.96%）與 rebuild bit-一致；0.04% knot-rescue 列為近似，與現行 `_score_ablation.py` 同級。**不改 score.py、不動 production panel**。多 cell 同時 reweight 直接列多個 `col=scale`（線性可加，不需 combo rebuild）。

⚠️ **距離_\* cells 是 continuous cap 型**（欄值 = clip(z×3, ±cap)，權重即 cap，非線性）：scale=0（ablation）與 **cap 調降**（scale<1，工具內部走 clip）精確可算；**cap 調升（scale>1）不可能從 panel 推得**（被截斷列已失去原始 z），工具會報錯——改 score.py 的 cap 後 rebuild。其餘類別皆 boolean 線性，任意 scale 可用。

⚠️ **僅限既有 cell 權重**。**新增從未算過的 cell** 或 **訊號形式轉換（trend↔event）** panel 沒有對應欄，`_score_reweight` 會報錯要你 rebuild——這類仍走下面完整流程。

## 標準 cell 改動工作流（新 cell / 換形式 / 需持久化採用結果時）

1. **備份 production panel**：`cp tmp/score_panel.parquet tmp/score_panel.bak_<label>`
2. **一次只改一件事** in [analysis/score.py](analysis/score.py)（cell weight / 新加 cell / 訊號形式轉換）。**永遠不要**同時改兩個獨立變數。
3. **重建 panel**：`python -m analysis._score_panel`（實測 33~65 min，full universe 1943 stocks × ~8.7 年；進度輸出走 stderr。**背景跑必須 detached（Start-Process）不帶 tool timeout**——60 分鐘 timeout 曾在 98% 處砍掉一次 rebuild）
4. **對照** vs backup：
   ```
   python -m analysis._score_compare_panels <baseline_path> "<label>" "<cell1,cell2,...>"
   ```
   工具：[analysis/_score_compare_panels.py](analysis/_score_compare_panels.py)
5. **決策**：全 H × regime ≥ 0 → 採用；任何 regime 明顯拖累 → 拒絕；mixed → tradeoff 評估。
6. **拒絕則從 backup 還原**：`cp tmp/score_panel.bak_<label> tmp/score_panel.parquet` 並 revert score.py。
7. **採用後**：清掉 TEMP 註解、更新 [project_score_ablation_findings.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\project_score_ablation_findings.md)。

多 cell 組合採用（例如 cell A 升權 + cell B 降權）：先單獨測各自，最後做 **combo rebuild** 驗證 naive sum 是否成立。

## Universe：永遠用 ~dead 過濾

> **規則**：評分系統應用範圍是過濾過的活耀股（~dead_fish），不是全宇宙。詳見 [feedback_score_alive_universe.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\feedback_score_alive_universe.md)

實作：
- Dead-fish panel：`tmp/dead_fish_panel.parquet`（建構工具 [analysis/_build_dead_fish_panel.py](analysis/_build_dead_fish_panel.py)，~1 min；重建只在加新股或週期性更新時做）
- 在 ablation 端過濾（實作為 left-merge + `fillna(False)` 後濾 `~is_dead_fish`——dead panel 缺的日期視為活股，**不是** inner join，inner 會整批丟掉 dead panel 未涵蓋的日期），**不要**在 score_panel build 階段過濾（保留 universe 選擇彈性）
- dead panel 落後 score panel 的期間會整段被當活股 → 兩者其中一個 rebuild 後，檢查另一個的 max date 是否跟上
- 範本：[analysis/_score_obv_ablation_dead.py](analysis/_score_obv_ablation_dead.py)、[analysis/_score_all_ablation_dead.py](analysis/_score_all_ablation_dead.py)（全 cell 一次跑）
- 死魚雜訊會偽造 bear regime hedge — bear edge 在 ~dead 下消失的 cell，原本只是死魚下漂 + signal 同向的偽相關

## Regime split — 預設拆解（2026-07-07 改制：breadth 日級分桶）

forward horizons {5, 20, 60} × regimes {full, bull_L, bear_L, neutral_L, bull_M, bear_M, neutral_M}。regime 來自 `tw.market_breadth` 日級 trend 標籤（`_score_ablation.regime_from_breadth()`：bull={+1,+2,+3}、bear={-1,-2,-3}、neutral={0}，L=long_trend、M=medium_trend）。**採用判準只看 full + bull_L + bear_L**；M 桶與 neutral 標 [ref] 供參考。舊制年份切（BEAR_YEARS={2021,2022}）已廢——2021 實為多頭年（bull_L 136/244 日），年份切曾製造「bear 巨賺」假象差點誤採用排列拔除。

採用 tier：
- **Clean win**：全 H × (full/bull_L/bear_L) ≥ 0。例：2026-05-13 扣抵_long ±5→±15（舊制數字 H=60 full +0.253）。
- **Tradeoff**：full 正、某 regime 微負。算 gain/loss 比；> 2 偏採用；< 1.5 拒絕或找中間 weight。
- **Drag**：任何 horizon × regime 持續負。拒絕。

**逐年 Δ 表必看（與 regime 格同等地位）**：三個現役工具已標配輸出。增益集中單一年份、或最近兩個完整年轉負 → 不採用，不管 regime 格多漂亮（排列拔除案例：九格近全綠，逐年拆解揭穿增益全在 2021+薄樣本 2026、2024/2025 皆負）。

## ★ 偏態閘門：decile spread 是平均數，會獎勵「加載變異數」（2026-07-11 新增，必跑）

**現象（實測）**：在 ~dead panel 上，把當日截面按波動率切五分位，對「同日同 total_long decile 同儕」的超額——**平均數單調上升**（vol60/fwd_60：Q1 -1.33 → Q5 +0.97）但**中位數與勝率完全反向**（中位數 Q1 +0.87 → Q5 -2.05；贏過同儕比例 Q1 54.4% → Q5 45.1%），每格 n≈35 萬。典型高波動股其實輸給同分同儕，平均數靠少數暴衝贏家撐起。

**含義**：decile spread（歷來每一個採用決策的唯一依據）是**平均數**，因此任何偏向高波動股的 cell 都能靠右尾把它做好看，代價是勝率下降＋最大虧損加深——**直接違反三大目標前兩項**。現行 board 已被此偏差滲透：Spearman(total_long, vol) = +0.226（94.5% 交易日為正），D10 落在波動率第 62 百分位、D1 在第 37；**H=20 的 median decile spread 是 -0.057%（負的）**，即該尺度的 mean spread 幾乎全部是右尾。

**閘門（新 cell / weight 改動一律適用）**：mean spread 之外，**必須同時回報**
1. **median decile spread**（D10 中位數 − D1 中位數）
2. **D10 勝率**（top decile 中贏過當日全市場中位數的比例）
3. **D10 最慘 5%**（top decile 報酬的 5% 分位，尾部風險）

判準：
- **mean 為正、但 median 反向或勝率 < 50%** → **偏態假象，不採用**（不管 mean spread 多漂亮）
- mean 微降、但 median / 勝率 / 尾部三者同時改善 → **可採用**，但必須在 memory 明記「這是一筆用 mean spread 換勝率與 maxL 的交易」並附 tradeoff 表交使用者裁決（不得自行拍板）
- 三個新指標的計算範本：`analysis/_score_skew_audit.py`（逐 cell 的 mean/median/勝率對照）與 `analysis/_score_vol_neutral_sim.py`（board 層級 tradeoff 曲線），皆為凍結 panel 欄位算術、零 rebuild

**cell 性格體檢（已跑，2026-07-11）**：載波動率最重的正是史上兩大改良——扣抵_long（rho +0.362）與 距離 cells（p233 +0.342 / p377 +0.331 / p144 +0.310）；它們的多方 cohort 平均數是中位數的 4~6 倍、勝率僅 50.6~50.9%。**最誠實的 cell 是洪量家族**（洪量_long 平均 +2.37 / 中位數 +1.34 / 勝率 53.5%，且唯一負向載波動率 rho -0.128）。紅旗：**大盤_medium 多方 cohort 中位數 -0.10、勝率 49.6%**（純右尾）；OBV 三 scope 的空方 cohort 平均數全為正（符號反向）。註：cohort 性格 ≠ 邊際貢獻，此表**不構成拔除依據**，只說明「那個 Δ 是用什麼買來的」。

## Rebuild 漂移底線（2026-07-07 null rebuild 實測，判讀 rebuild 對照的前提）

程式碼零改動、只隔兩個月資料的 null rebuild，row-aligned 對照出的「純漂移」：**64.8% rows 的 total 變了**；Δspread 底線 H5/H20/H60 full = -0.013/-0.027/-0.020，bear_L 到 -0.077，**逐年格可達 ±0.27**（2020 H60 -0.269）。含義：
- **cell 權重/拔除決策一律用 `_score_reweight`（凍結 panel 欄位算術，精確、零漂移）**，這也是專案歷來 ablation 的正統方法
- rebuild 對照（兩個不同時間建的 panel 相比）混入漂移，|Δ| 低於上述底線的格子不可信；「reweight 值 vs rebuild 對照值」可差 3~5 倍（排列_long 實測 +0.086 vs +0.017），差異是漂移不是工具錯
- rebuild 的正當用途：新 cell / 訊號形式轉換（panel 無對應欄）、以及定期重錨 baseline。重錨後所有舊 baseline 數字失效，需重跑 reweight 取新基準

## Weight-upgrade 陷阱（三度驗證）

**單 cell Δ 是現權重的邊際貢獻，不是加碼斜率**。已優化過的 cell 升權會 self-drag。

歷史紀錄（皆 ~dead universe）：

| Cell | 動作 | 結果 |
|---|---|---|
| 扣抵_long | ±5 → ±15 | ✅ H=60 full +0.253（原 weight 確實 undersized）|
| 排列_long | ±10 → ±15 | ❌ 單 cell Δ +0.071 → +0.040、bear -0.042 |
| 距離_p233 | ±10 → ±15 | ❌ 單 cell Δ +0.059 → +0.054、bear -0.028 |

升權前 checklist：
- Cell 單 cell Δ 大（H=60 full > +0.1pp）相對現點數貢獻 → 可能 under-weighted
- 同類別其他 cells 在更高 weight → 此 cell 是 laggard
- 之前沒跑過 sweep → 假設已接近 sweet spot
- 不確定就**測最小步**（±10→±12）

## Trend (latched) vs Event form

**橫截面排序系統一律優先 event form**。Trend (latched) 失敗已三度確認（永久封存）：

| 嘗試 | universe | H=60 full Δ | 結果 |
|---|---|---|---|
| OBV trend ±15 第一次 | 全宇宙 | -0.306 | 拒絕 |
| OBV trend ±15 (heavy-tier + slope ASC) | 全宇宙 | -0.350 | 仍拒絕 |
| OBV trend ±15 (heavy-tier + slope ASC) | ~dead | **-0.399** | 過濾後更糟，永久封存 |

Event form 標準設計：±5 fresh (0-1d 窗口) / ±3 carry (2-5d 窗口) / 6d 後 0。

若 latched 形式 ablation 失敗，**先用 event form 重試再放棄該指標**。許多 trend 被拒絕的指標在 event 形式下成功（OBV、MACD、wave_trend）。**event 成功 ≠ trend 可用** — 兩者不同訊號，不可互推。

## Combo verification

兩個獨立 cell Δ 的 naive sum 是估算起點，**必須** combo rebuild 驗證。

實例（2026-05-13）：
- 扣抵_long ±15 單跑：+0.253 H=60 full
- 洪量 ±10 單跑：+0.140 H=60 full
- naive sum：+0.393
- **組合實測**：+0.319（naive 的 81%）
- H=60 bear naive sum +0.029、實測 +0.041（141%，positive interaction）

## 已採用 production state（cell/weight 最後改動 2026-05-13；2026-07-07 複查仍為當前——期間 score.py 的 v195/v205/v208/v303 皆為 signal-factory DefenseRule，非 ScoreBoard cell/weight）

| 變更 | 採用日 | H=60 full Δ |
|---|---|---:|
| 拔 OBV/波浪/短週期 latched | 2026-04-28 | +0.515 |
| MACD/OBV/波浪 event 形式加回 | 2026-05-02/03 | -0.098（換 H=20 空頭副效益）|
| C dampened weights + Donchian Fib 233 | 2026-05-04 | +0.057 |
| 4 距離 cells (p55/89/144/233) | 2026-05-04 | +0.346 |
| **扣抵_long ±15 + 洪量 ±10**（~dead 評估）| 2026-05-13 | **+0.319** |
| 拔 MACD_medium + 大盤_long（breadth regime 複審，凍結算術 combo）| 2026-07-07 | +0.035（H5 +0.012 / H20 +0.018，九格全正）|

累計 H=60 full +1.14pp / +37% 相對原始 baseline。

## 永久封存路線（不要再試）

| 路線 | 原因 |
|---|---|
| trend (latched) 形式 cell（任何指標） | 三度失敗，cross-sectional 反指標 |
| 排列_long ±15 | bear regime 結構性受傷 |
| 距離_p233 cap > 10 | 已過 sweet spot |
| 扣抵_long ±20+ | edge 量級會再退（2026-04-28 sweep 結論）|
| obv_ma_len Fibonacci 對齊（short 8/21、long 21/55） | short scope 退化 |
| 三類別_short（扣抵/排列/大盤 short scope）| 2-3 次重測都失敗 |
| Donchian short/medium | subset 共線結構性問題 |

## 工具索引

| 用途 | 腳本 |
|---|---|
| 純 weight 改動免 rebuild（欄位算術，秒級，預設 ~dead）| [analysis/_score_reweight.py](analysis/_score_reweight.py) — `"col=scale,..."`（scale=新/舊權重，ablation=0；距離 cap 只可降不可升）|
| 建 score panel（新 cell / 換形式才需）| [analysis/_score_panel.py](analysis/_score_panel.py)（~27 min）|
| 建 dead_fish 過濾 panel | [analysis/_build_dead_fish_panel.py](analysis/_build_dead_fish_panel.py)（~1 min）|
| 兩 panel 對照 | [analysis/_score_compare_panels.py](analysis/_score_compare_panels.py) |
| 全 cell 雙宇宙 ablation（含排列/大盤等無專屬模組的類別）| [analysis/_score_all_ablation_dead.py](analysis/_score_all_ablation_dead.py)（~dead）/ [analysis/_score_ablation.py](analysis/_score_ablation.py)（全宇宙）|
| OBV cell ablation | [analysis/_score_obv_ablation_dead.py](analysis/_score_obv_ablation_dead.py) / [analysis/_score_obv_ablation.py](analysis/_score_obv_ablation.py) |
| 類別專用 ablation（僅這幾類有專屬模組）| `analysis/_score_<category>_ablation.py`（macd / wave / donchian_lucas / knot）；排列(sort)、大盤(breadth) 無專屬模組，走上面兩支通用 ablation |

## Anti-patterns

- **只看 mean decile spread**，跳過 median / 勝率 / 尾部（偏態閘門）——mean 是右尾可以偽造的
- **只看 H=60 full**，跳過 bear regime 檢查
- **跳過 ~dead filter** 想「看更多 data」
- **同失敗模式換 cosmetic 參數重試**（trend form 換不同 magnitude）
- **單 cell Δ 平平就升權「給它一個機會」**
- **多 cell 採用只信 naive sum**
- **不更新 memory** — 失敗清單救未來的時間
