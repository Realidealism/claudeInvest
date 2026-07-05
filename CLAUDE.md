# Claude Code Rules

## Language
- 所有回應使用繁體中文
- Code comments must be written in English
- Commit messages must be written in English

## 路由：何時讀哪個檔（讀了再動手，不要憑印象）
| 觸發情境 | 先讀 |
|---|---|
| 要派 subagent / 選模型 / 預計讀 >3 檔或 >500 行 / 長輸出指令 | `.claude/rules/01_dispatch.md` |
| 卡關想重試、不確定「算不算完成」、猶豫要不要問使用者、提案涉及訊號/防守/出場邏輯 | `.claude/rules/02_judgment.md` |
| 要寫派工 prompt（搜尋/實作/重構/研究/審查） | `.claude/rules/03_templates.md` |
| 要修改 CLAUDE.md、rules 檔、memory 制度 | `.claude/rules/04_maintenance.md` |
| 新 session 接手大型/跨對話任務 | `.claude/rules/05_letter.md` |
| 使用者說「回測」或要改 signal_backtest/factories | skill `signal-factory` |
| ScoreBoard cell ablation / 權重調整 | skill `cross-sectional-scoring` |

這些規則的成因見 `.claude/rules/00_diagnosis.md`。

## Task Continuity
- 僅在預期任務較大、可能跨對話時，才將計畫和關鍵進度記錄到記憶系統（memory/）
- 任務完成後應清理相關的進度記錄，避免記憶膨脹

## When In Doubt, Ask
- 實作前應主動列出關鍵假設，若假設不確定則先與使用者確認再動手
- 遇到不懂或沒把握的部分，明確說出不確定，不要假裝理解後硬做
- 若發現需求不合理或有更好的替代方案，應主動提出並說明理由（附 tradeoff：利弊、成本、風險），而非照單全收
- 遇到不確定的技術決策（套件選擇、架構方向、實作方式）時，先詢問使用者再繼續
- 若找不到明確的 API 端點或資料來源，先詢問使用者是否有已知來源；自行搜尋三次未果就停止並詢問
- 「該問 vs 自己決定」的具體判準見 `.claude/rules/02_judgment.md` Rubric 3

## Suggestion Batching
- 當使用者要求修正或改良方案時，若項目超過三個，一次只先提出前三個，剩餘項目等當前批次處理完後再接續提出
- 例外：若項目之間有互相影響或依賴關係（例如 A 的實作會影響 B 的設計），可一次提出超過三個，以便整體評估
- 此規則僅適用於修正建議，不適用於任務規劃的步驟列舉

## Response Style
### 規劃階段
- 主動列出完整計畫與步驟，仔細詢問需求細節、邊界條件與預期行為
- 計畫需與使用者確認後再開始實作

### 執行階段
- 直接給結果，不要前言、不要總結
- 使用工具後，只回報結果，不描述過程；除非使用者主動問，否則不解釋正在做什麼
- 程式碼和資料維持完整精確，只壓縮自然語言

## Minimal & Surgical Changes
- 只修改與任務直接相關的程式碼，不順手「改善」周邊的程式碼、註解或格式
- 不重構沒壞的東西，遵循既有風格
- 若自己的修改導致某些 import / 變數 / 函式變成無用，應一併清除；但不主動刪除原本就存在的 dead code
- 不加沒被要求的功能、不建不必要的抽象層、不為假設性的未來需求預做設計
- 一次性 / 拋棄式程式不要做抽象，直接寫死即可
- 不為不可能發生的情境加錯誤處理
- 若寫出的程式碼明顯可以更精簡，應主動簡化；既有實作明顯臃腫時可整段重寫成更短版本（不限於小幅修改）

## Goal-Driven Execution
- 收到模糊任務時，先將其轉換為可驗證的具體目標再動手；計畫中每個步驟附驗證方式
- 驗證標準越明確，越能獨立完成；標準不明確時應先向使用者確認
- 「算不算真的完成」的判準見 `.claude/rules/02_judgment.md` Rubric 2

## Execution Modes
- 預設：依 Goal-Driven Execution 規則（具體目標 + 驗證方式）
- 「TDD 模式」：使用者說「TDD」或「TDD 跑」時，將需求轉為失敗測試 → 寫程式讓它通過 → 必要時重構，直到測試全綠才停
- 「探索模式」：使用者說「探索」或「先看看」時，允許先寫一次性 script 觀察數據／印中間結果，不需事先定義完整驗證標準；產出的 script 視為拋棄式，不做抽象
- 「回測模式」：使用者說「回測」時，依 skill `signal-factory` 執行（6 訊號 + 歸檔 + SQLite 記錄 + diff 解讀，不主動 commit）

## Commands
- 提示使用者執行的指令必須是完整、可直接複製貼上執行的成品，不可省略參數或用 placeholder
