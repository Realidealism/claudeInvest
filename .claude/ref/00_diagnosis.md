# 00 診斷：這套制度為什麼長這樣

> 按需讀取檔。想知道某條規則的成因、或懷疑某條規則已不需要時才讀。
> v1：2026-07-05（Fable 5）。v2：2026-11-11（Fable 5.2），依四個月實際使用後的退化證據重寫。

名詞（全制度共用）：

- **常駐檔**：每個 session 開頭自動載入的檔＝`CLAUDE.md`、`.claude/rules/` 下沒有 `paths:` frontmatter 的 `.md`（現為 01、02）、`MEMORY.md`。
- **路徑條件檔**：`.claude/rules/` 下有 `paths:` frontmatter 的 `.md`（現為 04、06），模型讀寫符合路徑的檔案時才載入。
- **細節檔**：`.claude/ref/*.md`，不自動載入，要用時才 Read。
- **memory/**、**MEMORY.md**：一律指 `C:\Users\Real\.claude\projects\c--Claude-Invest\memory\`（不在 repo 內，repo 根目錄沒有這兩者）。

## v2 診斷（2026-11-11 實測）

### 問題 1（最漏 token）：常駐載入量四個月長成三倍，而且量錯了單位

**證據**：`.claude/rules/` 下的 `.md` 全部自動載入，CLAUDE.md 的「何時讀哪個檔」路由表實際上沒有省任何 token。實測 CLAUDE.md 8.9KB＋rules 七檔 73.8KB＋MEMORY.md 30.3KB＝113KB，每個 session 開場全付。`02_judgment.md` 一檔 41KB（07-05 初版約 15KB）；它的 26 份備份四個月間從 15KB 長到 40KB，中間只有兩次小幅回縮。v1 的精簡門檻用**行數**（rules 單檔 250 行、MEMORY.md 60 行）；02_judgment 只有 224 行所以從沒觸發，但它的單行可以長到數 KB。

**根因**：(a) 以為路由表等於按需載入；(b) 每次踩坑追加一段，「一行格式」寫成一整段；(c) 門檻以行計，行可以無限長。

**修法**：
- 兩層：常駐只留「觸發→動作」核心（`rules/01_dispatch.md`、`rules/02_judgment.md`），事故敘述、長表格、模板、維護協議搬到 `.claude/ref/`。
- 預算改用位元組，用腳本量，不用目測：`python c:\Claude\Invest\.claude\ref\check_budget.py`（預算數字在 [04_maintenance.md](../rules/04_maintenance.md)）。
- 新教訓一律先進細節檔的踩坑紀錄，不直接加進常駐檔。

### 問題 2（最容易失焦）：規則本身變成噪音

**證據**：v1 的問題 2 是「主對話讀太多檔」。那條仍成立，但四個月後多了一個更近的來源：常駐規則裡混著大量單一事故的敘述（具名公司、commit hash、日期、狀態碼）。要找「我現在該做什麼」得先讀完事故史。MEMORY.md 同病：86 行裡有 16 行超過 400 bytes，最長一行 2.4KB，內容是某個進行中專案逐日的進度——索引行變成了內容本體，而 CLAUDE.md 明文規定進度紀錄完成後要清。

**根因**：寫的人（當下的 session）記得事故全貌，覺得每個細節都重要；讀的人（之後的 session）只需要觸發條件與動作。

**修法**：
- 常駐核心每條規則只寫：觸發、動作、一個正例一個反例（各一行）。事故原文在 `ref/02_cases.md` 同編號節。
- MEMORY.md 每行只寫「這是什麼＋什麼情況要讀」，單行上限 450 bytes；進行中專案的進度寫在該專案自己的 memory 檔，索引行不記逐日狀態。
- v1 修法不變：預計讀 >3 檔或 >500 行派 `Explore`，長輸出落檔只讀摘要（`rules/01_dispatch.md` §1）。

### 問題 3（最容易出錯）：制度與 harness 現實脫節，而且同型錯誤重複發生

**證據（脫節）**：v1 的 01_dispatch §0 寫「Agent tool 沒有 effort 參數，不要試著傳」——2026-11-11 的 tool schema 已有 `effort`（low／medium／high／xhigh／max）。`TodoWrite` 已不在工具清單。`Monitor`、`SendMessage`、`EnterPlanMode` 變成 deferred tool，要先用 `ToolSearch` 載入 schema 才能呼叫。多處規則寫死「主模型即 opus」。

**證據（重複）**：踩坑紀錄 9 條中，3 條是同一個形狀（審查「修一個、冒一個」不收斂：10-11、10-13、10-21），2 條是授權邊界（10-13 順序、10-20 連線），2 條是驗收器設計（11-05、11-07）。對應的 Rubric 4／10／2 當時都已存在——規則埋在 41KB 裡，等於沒被讀到。

**根因**：(a) 把某一天的工具清單寫成永久事實；(b) 教訓只被追加在檔尾，沒有折回它所屬的 Rubric 條文。

**修法**：
- §0 的工具清單標快照日期，並寫明「與當下 tool schema 不一致時以 schema 為準，並依 04_maintenance 修正」。模型層級用相對寫法（「與主模型同級或更高」），不寫死型號。
- 同型踩坑累積到 2 條就折進對應 Rubric 的核心條文（v2 已把上述 7 條折進 Rubric 2／4／10／11）。
- v1 修法不變：動訊號／防守／出場邏輯前先掃 MEMORY.md 失敗封存（Rubric 4）；驗收不自驗（01_dispatch §6）。
- 靠自律的規則對弱模型最不可靠。能由 harness 強制的閘門（hook）比文字規則穩；v2 寫了提案但**未啟用**，見 [05_letter.md](05_letter.md)「待使用者裁決」。

## v1 診斷（2026-07-05）仍然有效的部分

### MEMORY.md 的 baseline 行

每採用一版訊號工廠 baseline 就新增一行、舊版的行從不收攏，曾讓 MEMORY.md 有 29 行歷史 baseline。規則：

- MEMORY.md 任何時刻只允許 **1 行**標「(當前)」的 baseline。新 baseline 寫入時，**必須同時**把前一版那行併入「訊號工廠版本正史＋歷史 baseline 索引」那一行。個別 `project_v*_baseline.md` 檔留在 memory/ 不刪。
- 版本正史在 SQLite：`python -m signal_backtest._versions list`（`data/signal_versions.db`）。
- 失敗封存條目（「勿重試／別重提／MUST NOT」）不在壓縮之列，必須逐條留在索引，因為它們是防重蹈用的。

### 重試已封存的死路、自己驗自己

memory/ 裡每條「勿重試／MUST NOT」背後都是數小時白跑的回測（momentum_override 連 11 版全負、flee force-exit 復活、防守貼上揚大均線…）。它們被寫下來，正說明「換個 session 就忘、又提一次」反覆發生過。另外「看程式碼覺得對」在這台機器上特別危險：cp950 編碼、PowerShell 5.1、.bat 中文陷阱都是實際炸過的雷。

### 主對話下場讀取

回測輸出動輒數百行、掃 repo 要讀十數檔。這些內容進主對話後會稀釋任務目標，並加速觸發自動摘要，摘要後細節失真。「自己直接讀比較快」的直覺是錯的。

## v2 驗收紀錄（2026-11-11）

用來回答「這套配置的機制到底有沒有實際驗過」。每項標明是實測還是只有文件依據。

| 項目 | 結果 | 依據 |
|---|---|---|
| `paths:` 條件載入機制 | **生效**。五種寫法都驗過：`*.bat`、`**/*.bat`、單一檔名、`目錄/檔名`、`目錄/**` | 實測：五個內容全新的臨時探針檔，fresh agent 逐一 Read 對應檔案，每次只附帶載入對應的那一個探針（探針已刪） |
| `04_maintenance.md` 隨制度檔載入 | **生效** | 實測：fresh agent Read `.claude/ref/` 下的檔之後，新版 04 以 system-reminder 附帶出現 |
| `06_service_safety.md` 隨服務檔載入 | **本 session 未能驗到** | 實測兩次都沒有附帶出現。推測原因：它的本文與 session 開場已載入的舊版完全相同，被 harness 去重。pattern 寫法與上面通過的探針同型。**新 session 第一次碰 `.bat` 或 `intraday_snapshot.py` 時確認一次**；沒出現就自己 Read，並在踩坑紀錄記一行 |
| `.claude/ref/` 不自動載入 | **本 session 無法驗** | 只有官方文件依據（文件只列 CLAUDE.md 與 `.claude/rules/` 為自動載入來源）。本 session 派出的 agent 拿到的是 session 開場的規則快照，看不出差別。**新 session 開場確認：context 裡不應出現「03 派工 Prompt 模板」「05 給未來 session 的信」的內文** |
| subagent 載入什麼 | `general-purpose` 會載入 CLAUDE.md、常駐 rules、MEMORY.md，而且是 **session 開場時的版本** | 實測：兩個 fresh agent 回報 context 裡有重整前的舊版規則全文與 MEMORY.md。這與官方文件「subagent 不載入 auto memory」的說法不一致，以實測為準 |
| MEMORY.md 25KB／200 行截斷 | **未確認** | 官方文件有此上限；但本 session 開場時 MEMORY.md 為 30KB，context 裡仍看得到最後一行。預算仍照 25KB 保守設定 |
| `check_budget.py` 會不會漏報 | 能辨識失敗 | 實測：對刻意弄壞的複本（常駐檔超標、rules/ 多出檔案、拿掉 frontmatter、打錯一個 glob、超長的新踩坑條目）各自在預期的檢查項上 FAIL，exit code 1 |
| MEMORY.md 壓縮沒有遺失內容 | 通過 | 腳本逐行比對＋fresh agent 複核：被改短的 19 行，原文都逐字存在於各自連結的 memory 檔；封存關鍵字行數 16 → 16 |
| 對抗審查 | 5 項會導致錯誤行為、11 項會誤導、12 項可改進；已處理 | fresh-context opus。清單原文在 `tmp/institution_v2/review_findings.md`（tmp 不進版控） |
