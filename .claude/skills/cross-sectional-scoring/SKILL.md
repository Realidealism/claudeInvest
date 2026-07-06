---
name: cross-sectional-scoring
description: ScoreBoard cell ablation / weight tuning / 訊號形式選擇方法論。Use when adding/removing/upweighting/downweighting/testing cells in analysis/score.py, building or comparing tmp/score_panel.parquet, or evaluating ScoreBoard adoption decisions. 涵蓋 ablation workflow、universe (~dead) filter、regime split、weight-upgrade trap、trend vs event form 選擇。配合 memory project_score_ablation_findings.md / feedback_score_alive_universe.md 使用。
---

# ScoreBoard cell ablation 方法論（此專案版）

針對 [analysis/score.py](analysis/score.py) 的 ScoreBoard 系統。所有 ablation 用 `~dead_fish` 過濾（佔 panel 49.4%），由 [_score_panel.py](analysis/_score_panel.py) 建構的 ~3.4M panel rows，21+ cells。配合 memory [project_score_ablation_findings.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\project_score_ablation_findings.md) 永久封存的失敗清單避免重蹈。

## 標準 cell 改動工作流

1. **備份 production panel**：`cp tmp/score_panel.parquet tmp/score_panel.bak_<label>`
2. **一次只改一件事** in [analysis/score.py](analysis/score.py)（cell weight / 新加 cell / 訊號形式轉換）。**永遠不要**同時改兩個獨立變數。
3. **重建 panel**：`python -m analysis._score_panel`（~27 min，full universe 1937 stocks × ~8.7 年）
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
- 在 ablation 端用 `inner join` 過濾，**不要**在 score_panel build 階段過濾（保留 universe 選擇彈性）
- 範本：[analysis/_score_obv_ablation_dead.py](analysis/_score_obv_ablation_dead.py)、[analysis/_score_all_ablation_dead.py](analysis/_score_all_ablation_dead.py)（全 cell 一次跑）
- 死魚雜訊會偽造 bear regime hedge — bear edge 在 ~dead 下消失的 cell，原本只是死魚下漂 + signal 同向的偽相關

## Regime split — 預設拆解

forward horizons {5, 20, 60} × regimes {full, bull, bear}。每 metric 九格。

採用 tier：
- **Clean win**：全 H × regime ≥ 0。直接採用。例：2026-05-13 扣抵_long ±5→±15 (H=60 full +0.253 / bear +0.089)。
- **Tradeoff**：full 正、某 regime 微負。算 gain/loss 比；> 2 偏採用；< 1.5 拒絕或找中間 weight。例：洪量 ±15→±10 (gain/loss = 2.33 vs ±7 的 1.86)。
- **Drag**：任何 horizon × regime 持續負。拒絕。例：排列_long ±10→±15 H=60 bear -0.042。

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

## 已採用 production state（截至 2026-05-13）

| 變更 | 採用日 | H=60 full Δ |
|---|---|---:|
| 拔 OBV/波浪/短週期 latched | 2026-04-28 | +0.515 |
| MACD/OBV/波浪 event 形式加回 | 2026-05-02/03 | -0.098（換 H=20 空頭副效益）|
| C dampened weights + Donchian Fib 233 | 2026-05-04 | +0.057 |
| 4 距離 cells (p55/89/144/233) | 2026-05-04 | +0.346 |
| **扣抵_long ±15 + 洪量 ±10**（~dead 評估）| 2026-05-13 | **+0.319** |

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
| 建 score panel | [analysis/_score_panel.py](analysis/_score_panel.py)（~27 min）|
| 建 dead_fish 過濾 panel | [analysis/_build_dead_fish_panel.py](analysis/_build_dead_fish_panel.py)（~1 min）|
| 兩 panel 對照 | [analysis/_score_compare_panels.py](analysis/_score_compare_panels.py) |
| 全 cell 雙宇宙 ablation | [analysis/_score_all_ablation_dead.py](analysis/_score_all_ablation_dead.py) |
| OBV cell ablation | [analysis/_score_obv_ablation_dead.py](analysis/_score_obv_ablation_dead.py) |
| 類別專用 ablation | `analysis/_score_<category>_ablation.py`（MACD/wave/sort/breadth/donchian_lucas）|

## Anti-patterns

- **只看 H=60 full**，跳過 bear regime 檢查
- **跳過 ~dead filter** 想「看更多 data」
- **同失敗模式換 cosmetic 參數重試**（trend form 換不同 magnitude）
- **單 cell Δ 平平就升權「給它一個機會」**
- **多 cell 採用只信 naive sum**
- **不更新 memory** — 失敗清單救未來的時間
