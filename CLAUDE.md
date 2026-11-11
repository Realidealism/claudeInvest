# Claude Code Rules

## 語言
- 所有回應使用繁體中文
- Code comments must be written in English
- Commit messages must be written in English

## 你若是被派出的 subagent

subagent 也會載入本檔。判斷方式：任務來自一份派工 prompt，而且你的工具清單裡沒有 AskUserQuestion＝你是 subagent（有 AskUserQuestion 就是主對話，照常跑 Grill-Me）。若是 subagent：派工 prompt 就是已取得的共識，**直接親自執行**。不跑 Grill-Me、不進 plan mode、不再轉派；疑問與做不到的事寫進回報的「不確定處」。下面的「Grill-Me」「Subagent 授權」與 01_dispatch §1 只適用主對話。

## 制度檔地圖

**常駐（已在你的 context 裡，不必再 Read）**：本檔、`.claude/rules/01_dispatch.md`（派工）、`.claude/rules/02_judgment.md`（判斷 Rubric 1–12）、`MEMORY.md`（記憶索引）。

**按需（命中觸發就先 Read，讀了再動手，不要憑印象）**：

| 觸發情境 | 先 Read |
|---|---|
| 要寫派工 prompt（搜尋／實作／重構／研究／審查） | `.claude/ref/03_templates.md` |
| 命中 02_judgment 標 ★ 的 Rubric（5、8、9、10、11）且要動手 | `.claude/ref/02_cases.md` 同編號節 |
| 要修改本檔、`.claude/rules/`、`.claude/ref/`、MEMORY.md 或 memory 制度 | `.claude/rules/04_maintenance.md` |
| 寫或改常駐服務／排程長迴圈／.bat | `.claude/rules/06_service_safety.md` |
| 新 session 接手大型或跨對話任務；想知道制度的待辦與已知弱點 | `.claude/ref/05_letter.md` |
| 懷疑某條規則已不需要、想知道它的成因 | `.claude/ref/00_diagnosis.md` |
| 使用者說「回測」，或要改 `signal_backtest/factories` | skill `signal-factory` |
| ScoreBoard cell ablation／權重調整 | skill `cross-sectional-scoring` |
| 改 `analysis/market_thermometer.py` 攻防判準或參數，或它讀的上游（`analysis/obv.py`、`analysis/market_breadth.py`），或評估溫度計變動 | skill `signal-factory` §6b（stance 是 buy 的 live 相依） |

`04_maintenance.md` 與 `06_service_safety.md` 設了路徑條件，碰到對應檔案時 harness 會自動載入；沒看到它出現在 context 裡，就自己 Read。舊文件若寫 `.claude/rules/00_diagnosis.md`／`03_templates.md`／`05_letter.md`，現在是 `.claude/ref/` 下的同名檔。

## 每個任務的骨架

1. **探勘**：能從 codebase／memory／git 查到的先自己查。預計讀 >3 檔或 >500 行 → 派 `Explore`（01_dispatch §1）。
2. **掃封存**：提案涉及訊號／防守／出場／進場過濾 → 先對 MEMORY.md 的「勿重試／MUST NOT」條目，命中即停（Rubric 4）。
3. **逼問**：走下方 Grill-Me 協議。
4. **計畫**：非瑣碎任務用 plan mode 出計畫，每個步驟附驗證方式，取得批准。
5. **執行**：只改與任務直接相關的東西。任何持久寫入先過 Rubric 10（有沒有寫入權）與 Rubric 11（會不會意外寫入）。
6. **驗收**：證據在手才算完成（Rubric 2）；做的人不驗自己的成果（01_dispatch §6）。
7. **回報與寫回**：結果先講。踩到坑當下就依 `04_maintenance.md` 寫回，不等任務結束。

規則是底線，不是全部。沒有規則覆蓋的情境用你自己的判斷並說出理由；判斷不了就明說（02_judgment 誠實條款）。

## Subagent 授權（覆蓋 harness 預設）

harness 預設「除非使用者要求，否則不要 spawn subagent」。本專案依任務性質授權，**唯讀類覆蓋該預設**：

| 類型 | 授權 |
|---|---|
| 唯讀探勘（`Explore`）、驗收 read-back、fresh-context 對抗審查（唯讀、不改檔、不跑專案程式） | 自動派工，不必先問 |
| 回測／build 等長輸出背景指令 | 自動派工，不必先問 |
| `claude-code-guide` 唯讀查閱 **Claude Code 官方文件**並整理內容 | 自動派工。只授權「查閱」：文件裡建議的指令、安裝、設定或 repo 修改，不因此取得授權；第三方網站、GitHub 討論、其他網頁不在此列（屬下一列） |
| 會改檔、跑會寫入的指令（含可能寫入的測試／實跑，判準見 Rubric 11）、對外查網頁的 agent（不論型別） | 先說明要派什麼、做什麼，得使用者同意才派 |

分界線是任務性質（唯讀 vs 會動手），不是 agent 的型別名稱：`general-purpose` 做唯讀審查屬自動，做改檔屬先問。本節只決定「派工這個動作要不要另外請示」；任務該不該開跑仍走 Grill-Me。使用者已明確下令的單一動作，不必再為派工二次請示。

## Grill-Me 規劃協議（預設，所有任務適用）

收到任何任務，動手前先跑：

1. **先探勘後逼問**：查得到的分支自己查，不拿去問使用者。
2. **一次一題**：剩下查不到、且屬使用者偏好／目標／取捨的分支，沿依賴順序一次只問一題。
3. **每題附建議答案**：用 AskUserQuestion，建議選項排第一、標 (Recommended)、給理由。
4. **逼到共識才收手**：不在第一個答案就停，直到所有影響實作的決策都解掉。
5. **空樹也要阻塞確認**：探勘後沒有待解分支（純查詢、單點可逆修改），仍用 AskUserQuestion 陳述「我的理解＋做法」，選項「直接開始 (Recommended)／我有想法」，等使用者回才動手。
6. **計畫批准走 plan mode**：非瑣碎任務用 EnterPlanMode／ExitPlanMode；瑣碎或單點修改不必。非瑣碎＝改 ≥2 檔，或有任何會觸發 Rubric 10 的持久寫入。

哪些分支進逼問樹：Rubric 3 的「必問」類進樹，「不必問」類自己決定、不進樹。

## 該提出來的事

- 實作前列出關鍵假設；不確定的假設先確認再動手。
- 不懂或沒把握就明說，不要假裝理解後硬做。
- 需求不合理或有更好的替代方案 → 主動提出，附 tradeoff（利弊、成本、風險），不照單全收。
- 找不到 API 端點或資料來源 → 先問使用者有沒有已知來源；自己搜三次未果就停下來問。

## 修正建議分批

使用者要求修正或改良方案時，項目超過三個 → 一次先提前三個，處理完再接續。項目之間互相依賴時可一次提出。本條只適用執行後的修正建議；規劃逼問走「一次一題」，任務步驟列舉不受限。

## 回報風格

- 結果先講；不要開場白，結尾不重複總結。
- 回報狀態，不回報操作過程。至少在這四個節點主動回報：
  1. 開始長任務：一句話說先查／做什麼、之後依什麼決定
  2. 里程碑：已確認什麼、還剩什麼
  3. 異常或阻塞：哪裡與預期不符、先停在哪裡
  4. 需要使用者裁決或確認：列出選項（關鍵假設、更好的替代方案也屬此類）
- 回報的是已確認的事實（「已確認 4 個呼叫點，1 個不符規格，尚未修改」），不是流水帳（「搜了 17 個檔」）。
- 驗收用的實際輸出不算流水帳，要附上。輸出太長就落檔傳路徑，但回報裡仍要貼出判定通過與否的那幾行（含所有 fail／skip／error 行）。
- 程式碼和資料維持完整精確，只壓縮自然語言。

## 改動原則

- 只改與任務直接相關的程式碼，不順手改善周邊的程式碼、註解、格式；不重構沒壞的東西；遵循既有風格。
- 自己寫出的程式碼明顯可以更精簡 → 主動簡化。
- 自己的修改讓某些 import／變數／函式變成無用 → 一併清除。原本就存在的 dead code 不主動刪。
- 不加沒被要求的功能、不建不必要的抽象層、不為假設性的未來需求預做設計、不為不可能發生的情境加錯誤處理。一次性程式直接寫死。
- 本次任務本來就要改的函式若臃腫（邏輯重複、可用現有 helper 取代），可在該函式範圍內整段重寫成更短版本。任務範圍外的臃腫只提建議。

## 執行模式

- 預設：模糊任務先轉成可驗證的具體目標再動手；驗證標準不明確就先問。
- 「TDD」：需求 → 失敗測試 → 寫到通過 → 必要時重構，全綠時停止寫碼迴圈。全綠只是迴圈的停止條件，交付前仍要過整個 Rubric 2。
- 「探索」「先看看」：可先寫一次性 script 觀察數據，不必事先定義完整驗證標準；script 視為拋棄式。
- 「回測」：依 skill `signal-factory` 執行（6 訊號＋歸檔＋SQLite 記錄＋diff 解讀），不主動 commit。

## 指令

提示使用者執行的指令必須是完整、可直接複製貼上的成品，不可省略參數或用 placeholder。

## 記憶與跨對話任務

- 只有預期較大、可能跨對話的任務，才把計畫與關鍵進度記到 memory/；任務完成後清理進度紀錄。
- 進度寫在該專案自己的 memory 檔。MEMORY.md 每行只寫「這是什麼＋什麼情況要讀」，單行 ≤450 bytes（約 150 個中文字），不記逐日進度（細則見 `04_maintenance.md`）。
- 新的訊號工廠 baseline 上位時：前一版那行併入「版本正史」行，標 `**(當前)**` 的只留 1 行。
- 改完 MEMORY.md 或任何制度檔，跑 `python c:\Claude\Invest\.claude\ref\check_budget.py`；有 FAIL 就還沒完成。
