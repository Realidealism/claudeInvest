# 00 快速診斷：本環境三大失效模式與修法

> 2026-07-05 由 Fable 5 制度化 session 盤點產出。供其他 rules 檔引用，保留「為什麼要有這些規則」的依據。修法當日已全部落地。

## 問題 1（最漏 token）：MEMORY.md 歷史 baseline 膨脹

> **路徑**：本專案 rules 檔中的 `memory/` 與 `MEMORY.md` 一律指 `C:\Users\Real\.claude\projects\c--Claude-Invest\memory\`（**不在 repo 內**，repo 根目錄沒有這兩者）。找不到就等於失敗封存清單失效，見問題 3。

**證據**：2026-07-05 盤點時 MEMORY.md 14.8KB / ~70 行，其中 29 行是 v152–v352 標「(歷史)」的 baseline 條目。MEMORY.md 每個 session 開頭全量載入，等於每次白付數千 tokens 讀已被取代的版本紀錄。版本正史本來就在 SQLite（`data/signal_versions.db`，用 `python -m signal_backtest._versions list` 查）。

**根因**：每採用一版就新增一行索引，舊版的行從不收攏。

**修法（制度）**：
- 已執行：歷史 baseline 行壓成 1 行合併索引（個別 `project_v*_baseline.md` 檔留在 memory/ 不刪，需要細節時再讀）。
- 之後每次新 baseline 寫入 MEMORY.md 時，**必須同時**把前一版的那行併入歷史合併索引行。MEMORY.md 任何時刻只允許 1 行「(當前)」baseline。
- 失敗封存類條目（「勿重試/別重提/MUST NOT」）不在壓縮之列——那些是防重蹈用的，必須逐條留在索引。

## 問題 2（最容易失焦）：主對話下場做大量讀取

**證據**：本專案的回測輸出動輒數百行、受害股報告很長、掃 repo 要讀十數檔。這些內容進主對話 context 後：(a) 稀釋掉早前的任務目標與使用者約束，模型開始忘記自己在幹嘛；(b) 加速 context 用盡觸發自動摘要，摘要後細節失真、之前的結論變得不可靠。

**根因**：對模型來說「自己直接讀」比「派工再等回報」感覺比較快，於是跳過委派——這個直覺是錯的（代價見 [01_dispatch.md](01_dispatch.md) §1）。

**修法**：[01_dispatch.md](01_dispatch.md) 的硬門檻——預計讀 >3 檔或 >500 行一律派 Explore；長指令輸出落檔傳路徑，主對話只讀摘要。門檻是硬規則不是建議。

## 問題 3（最容易出錯）：重試已封存的死路 ＋ 自己驗自己

**證據**：memory/ 中至少 8 條「勿重試/別重提/MUST NOT」條目（momentum_override 連 11 版全負、flee force-exit 復活、防守貼上揚大均線、blow-off top defense、扣抵 state trailing stop、breadth weakening…），每條背後都是數小時回測白跑。它們被寫下來，正說明「換個 session 就忘、又提一次」反覆發生過。另外「看程式碼覺得對」式的自我驗證在本環境特別危險——cp950 編碼、PowerShell 5.1、.bat 中文陷阱都是實際炸過的雷，邏輯上對的東西在這台機器上會壞。

**修法**：
- 動手改訊號/防守/出場邏輯前，先掃 MEMORY.md 的失敗封存條目；命中即停（動作見 [02_judgment.md](02_judgment.md) Rubric 4）。
- 驗收一律走 [01_dispatch.md](01_dispatch.md) §6「驗證不自驗」：檔案用 fresh agent read-back、程式碼用實跑/測試輸出、高風險判斷加第二意見。
