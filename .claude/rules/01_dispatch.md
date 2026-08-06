# 01 模型調度守則

主對話（你）的角色是**指揮官**：拆解任務、派工、整合結論、與使用者對話。大量閱讀與機械工作交給 subagent。理由見 [00_diagnosis.md](00_diagnosis.md) 問題 2。

## §0 本環境實際可用的資源（2026-07-25 覆查；工具介面若已改版，以當下 tool schema 為準）

- **Agent tool `subagent_type`**：
  - `Explore`——唯讀搜索，最常派。適合「掃 repo / 找定義 / 摸清慣例」
  - `Plan`——出實作計畫用
  - `general-purpose`——多步驟執行（會改檔、跑指令、查網頁）
  - `claude`——萬用；`claude-code-guide`——查 Claude Code / Claude API 用法
- **Agent tool `model` 參數**：`haiku` / `sonnet` / `opus` / `fable`。`fable` 在 enum 內但可用性**未驗證**，規則不得依賴它；要用先派一個極小 agent 試。**Agent tool 沒有 effort 參數**，不要試著傳。
- **主模型即 opus 時（2026-07 起的常態）**：「升 opus / 派 opus 第二意見」不再是升級，是平調。本檔所有原寫「派 opus」的步驟（§3 設計/審查、§5 升級、§6 第二意見）一律解讀為 **fresh-context 對抗審查**：同級模型 + 不知道前情 + [03_templates.md](03_templates.md) T5 的「找問題不是背書」立場。價值在第二隻眼睛沒有沉沒成本，不在模型層級——所以這些步驟不因「反正同級」而省略。
- **SendMessage**：可延續已派過的 agent（保留它的 context）繼續追問，不必重派新 agent 重讀一遍。
- **多 agent 編排**：本 harness **沒有 Workflow tool**（2026-07-25 查證：已載入與 deferred 工具清單皆無）。需要大規模並行審查時走使用者觸發的 `/code-review ultra`——它是使用者觸發且計費的，**我不可自行啟動**。
- **run_in_background**（Bash / Agent 都有）：回測等長時間指令用它，指令直接寫本體、**不要加 `&` 或 `nohup`**（雙重背景會秒噴假完成通知，見 [02_judgment.md](02_judgment.md) Rubric 6）。Agent 預設就是背景執行。
- **Monitor**：等待長指令/背景工作的**官方機制**，盯**產出檔實際變更**（mtime、大小）或程序狀態，取代 sleep 輪詢與「等 agent 自己通知」（後者失效過多次，見 [02_judgment.md](02_judgment.md) Rubric 6）。
- **ScheduleWakeup / Cron\***：定時回訪用。只在使用者明示要求週期性任務時用（ScheduleWakeup 僅在 `/loop` 自訂節奏模式下有意義）。
- **只有主對話有的工具**：`ScheduleWakeup`、`EnterPlanMode`/`ExitPlanMode`、`AskUserQuestion`、`Monitor`。subagent 查不到它們是正常的——**別把 subagent 回報的「查無此工具」當成工具不存在**（2026-07-25 驗收 agent 就這樣誤報過兩次）。
- **isolation: `worktree`**（Agent tool 參數）：高風險批次改檔讓 agent 在獨立 git worktree 動手，不污染主工作區。**注意不保護 repo 外的共用產出路徑**（如 tmp/score_panel.parquet），那類仍須照 skill 先備份。
- **TodoWrite**：多步驟長任務追進度用；單點修改不必開。
- **EnterPlanMode / ExitPlanMode**：非瑣碎任務的計畫確認走這對工具當批准閘（比純文字確認硬），與 CLAUDE.md「Grill-Me 規劃協議」併用——逼問樹解完後才進 ExitPlanMode。

## §1 指揮官不下場（硬規則）

以下情況一律派 subagent，主對話只接收結論。**授權欄**依 CLAUDE.md「Subagent 授權」分類——「自動」＝不必問使用者直接派（覆蓋 harness 的「除非使用者要求否則不要 spawn」預設）；「先問」＝說明要派什麼、做什麼，得到同意才派：

| 情況 | 派 | 授權 |
|---|---|---|
| 預計讀 >3 個檔案，或單檔 >500 行，或要「掃一遍」某目錄/慣例/命名 | `Explore`（唯讀） | 自動 |
| 驗收 read-back（§6） | `Explore`（唯讀） | 自動 |
| 回測、build、長輸出指令 | `run_in_background` 或輸出重導到檔案，主對話只讀 tail/摘要 | 自動 |
| 查網頁、讀外部文件、做研究 | `general-purpose`（回報須附來源 URL） | 先問 |
| 批次機械改檔（同一 pattern，檔數或改動點**任一 ≥4** 即派） | 自己先改 1 處確立 pattern，再派 agent 套其餘 | 先問 |

分界線是**唯讀 vs 會動手**：`Explore` 與背景長指令屬前者，`general-purpose`（改檔、跑指令、對外查）屬後者。

**可以自己來的例外**：已知確切檔案+行號的單點讀寫；≤3 檔且 <4 個改動點的小改；與使用者的討論；讀 subagent 回報。另：「先問」被使用者否決時 → 主對話自己做，但先聲明 context 風險（長輸出一律落檔只讀摘要）。

「派工感覺比較慢」不構成例外。省下的等待時間，遠小於主 context 被灌爆後「忘記任務目標、摘要後細節失真」的代價。

## §2 派工三件套（每個派工 prompt 必含，模板見 [03_templates.md](03_templates.md)）

1. **目標與動機**：要什麼＋為什麼要。動機讓 agent 在你沒料到的邊界情況做對取捨。
2. **驗收條件**：可機械判定的完成標準。能列舉就列舉（「找出所有呼叫點」→「回報每個呼叫點的 檔案:行號，我預期至少在 signals/ 和 telegram_bot/ 各有一處」）。
3. **回報格式**：指定欄位與長度上限（「條列，每條 ≤2 行，總長 ≤30 行」）。

沒寫驗收條件的派工，agent 會自己定義成功，然後你得到一份不能用的回報。

## §3 模型選擇表

| 任務類型 | model | 備註 |
|---|---|---|
| 精確 pattern 批次套用、格式轉換、機械清單整理 | `haiku` | 錯了損失小 |
| 搜索、探索、一般實作、驗收 read-back | `sonnet`（預設） | 不確定就選這個 |
| 架構設計、難 debug、對抗審查、第二意見、品味判斷 | `opus` | 判斷密度高的才用 |

省 token 不是把 opus 級任務塞給 haiku 的理由——返工比較貴。反過來，機械任務派 opus 也是浪費。主模型已是 opus 時，最後一列的重點從「換更強的模型」變成「換一雙 fresh 的眼睛」（見 §0）。

## §4 回報合約（寫進每個派工 prompt）

- subagent 只回：**結論、關鍵證據（檔案:行號）、明確標注的不確定處**
- 長產物（完整報告、大 diff、數據表）寫進檔案（scratchpad 或指定路徑），回報只傳路徑
- 禁止在回報中貼整個檔案內容
- 找不到就說找不到、做不到就說做不到＋卡在哪，禁止腦補結果
- 收到回報後：agent 的回報是**證詞不是事實**——關鍵結論（尤其要動手改東西之前）抽查 1–2 個檔案:行號驗證

## §5 升降級路徑

- `haiku` 錯 1 次 → 同任務直接升 `sonnet` 重派。不給 haiku 第二次機會。
- `sonnet` 在**同一個子任務**連錯 2 次 → 升 `opus`，prompt **必附完整失敗軌跡**：做了什麼、錯誤訊息原文、已排除的假設。不附軌跡的升級 = 讓對方從零重跑，浪費且可能犯一樣的錯。
- `opus` 也連錯 2 次 → 停。帶軌跡回報使用者（併用 [02_judgment.md](02_judgment.md) Rubric 3/4 判斷是問人還是換路）。主模型本身即 opus 時，這一階仍須**實際派出** fresh-context `opus` subagent（不是「反正我就是 opus 所以跳過」），它再連錯 2 次才算此階不通。
- 解出模式後的批次套用 → **降回** `haiku`/`sonnet`。已知解法的重複執行不需要貴模型。
- **同一種做法最多重試 2 輪**。第 3 次動手前必須三選一：換方法、升級模型、問使用者。

## §6 驗證不自驗

做的人不驗自己的成果。驗收派 **fresh-context agent**（新派的、prompt 裡不含實作過程、不含「我認為已完成」之類的引導語）：

- **檔案/文件類**：read-back——只給路徑與驗收條件清單，讓它讀檔逐條回答是否達標（派 `Explore`，唯讀自動授權）
- **程式碼類**：跑測試或實跑，回報必須貼輸出證據。「看程式碼邏輯是對的」不算通過。回測類另須過 [02_judgment.md](02_judgment.md) Rubric 5 底線清單。另可用 skill `/code-review`（本次 working diff）與 `/security-review`（安全面向）補一輪；`/code-review ultra` 是使用者觸發且計費，**只能建議使用者跑，不可自行啟動**。
- **等待長驗證跑完**：用 `Monitor` 盯產出檔變更，不要 sleep 輪詢、不要等 agent 自己回報（見 §0）
- **高風險判斷**（採不採用某版本、刪東西、對外發送、動 production）：走 [02_judgment.md](02_judgment.md) 誠實條款的升級階梯（fresh-context 對抗審查 → 多樣本 fresh agent 評審 → 使用者裁決），不在此重述
- 驗收不過 → 修完再驗，直到過。驗收 agent 說「大致沒問題但 X 怪怪的」→ X 就是下一個要處理的事，不是可以忽略的雜訊。
