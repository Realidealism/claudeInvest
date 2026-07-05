# 03 派工 Prompt 模板

配合 [01_dispatch.md](01_dispatch.md) 使用。`【】`是填空。每型附一個本專案情境的填寫範例（範例中的檔名/情境是示意，派工時以當下實況為準）。

通用規則：
- 每個 prompt 都要有：背景與動機 / 目標 / 範圍（含「不要做」）/ 驗收條件 / 回報格式
- agent 看不到你的對話歷史——所有它需要的 context 都要寫進 prompt，路徑寫絕對路徑
- 回報格式永遠包含「不確定處明確標注」與「長產物落檔傳路徑」

---

## T1 搜尋／探勘（subagent_type: Explore，model: sonnet）

```
背景：【為什麼要找這個，找到後要拿來做什麼】
目標：【要找什麼——定義、呼叫點、慣例、資料流】
範圍：搜 【目錄/glob】；不需要讀 【排除範圍，如 build/、dist/、tests fixtures】
驗收：【怎樣算找全——如「所有 import 此模組的檔案」「我預期 X 和 Y 目錄至少各一處，若沒有請明說」】
回報：條列「檔案:行號 — 一句話說明」，每條 ≤2 行；找不到就回報「未找到」＋已搜過的 pattern 清單；總長 ≤40 行
```

**範例**：
> 背景：要把 buy 訊號的 Chandelier 防守參數改成 per-signal，需要先知道現在防守價在哪些地方被計算與消費。
> 目標：找出 `pos_defense_price` 的所有寫入點與讀取點。
> 範圍：搜 c:\Claude\Invest\signal_backtest\ 與 c:\Claude\Invest\signals\；不需要讀 tmp/、logs/。
> 驗收：每個寫入/讀取點都列出；我預期 factories/ 內至少有寫入點，若某訊號檔完全沒有請明說。
> 回報：兩節「寫入點」「讀取點」，各條列 檔案:行號 — 一句話；總長 ≤40 行。

## T2 實作（subagent_type: general-purpose，model: sonnet）

```
背景與動機：【要解什麼問題、為什麼現在做】
目標：【具體改什麼，一句話】
作法：【已確立的方案；有參考實作就給 檔案:行號】
範圍：只改 【檔案清單】；不要動 【明確排除——周邊重構、格式、其他訊號】
驗收：【可機械判定——測試指令與預期結果 / 實跑指令與預期輸出】
回報：改了哪些檔（檔案:行號 + 一句話）、驗收指令的實際輸出原文、不確定處；diff 大就落檔傳路徑
```

**範例**：
> 背景與動機：sell_flee 的 give-back 出場要加停滯天數參數，目前寫死 5 日，要 sweep 用。
> 目標：把 `_transient_giveback_exit` 的停滯窗口抽成參數 `stall_win`，預設 5，行為不變。
> 作法：參考 signal_backtest/factories/_conditions.py 內該 helper 現有簽名，加 keyword 參數即可。
> 範圍：只改 _conditions.py 與其兩個呼叫點（pick、sell_flee）；不要動其他訊號、不要改任何門檻值。
> 驗收：`python -m signal_backtest.run --signal sell_flee`（照 skill signal-factory 的跑法）結果與改前 bit-identical。
> 回報：改動點 檔案:行號、驗收輸出中的 PF/交易數原文、任何不確定處。

## T3 重構（subagent_type: general-purpose，model: sonnet；設計拿不準先派 Plan）

```
背景與動機：【現在的痛點；為什麼值得動】
目標：【重構後的形狀】
不變量：【行為必須完全不變的部分＋怎麼證明——測試、bit-identical 對照、輸出 diff】
範圍：【檔案清單】；不要順手改 【格式、命名、無關 dead code】
驗收：【不變量的證明指令＋預期結果】
回報：結構前後對照（簡述）、驗收輸出原文、被迫做的取捨
```

**範例**：
> 背景與動機：六個 factory 檔各自複製了 K 棒 helper，改一處要同步六處，已經漏改過。
> 目標：抽到 _conditions.py 共用，六檔改 import。
> 不變量：六訊號回測結果 bit-identical（這是硬證明，不是「邏輯相同」）。
> 範圍：factories/ 六檔 + _conditions.py；不要動門檻值、不要改既有命名風格。
> 驗收：重構前先跑一次存檔，重構後再跑，diff 為空。
> 回報：搬了哪些 helper（清單）、diff 結果原文、若有無法統一的差異逐條列出。

## T4 研究（subagent_type: general-purpose，model: sonnet；結論要做高風險決策時升 opus）

```
背景：【要回答什麼決策；答案會怎麼被使用】
問題：【具體問句，可多個】
來源優先序:【官方文件 > 原始碼 > issue/論壇；本專案內部問題則 memory/ 與 repo 優先】
驗收：每個結論附來源（URL 或 檔案:行號）；查不到的明說「查無」，不得推測補位
回報：每問題一節：結論一句話 + 證據 + 信心（高/中/低與原因）；總長 ≤50 行，細節落檔傳路徑
```

**範例**：
> 背景：想把 intraday sweeper 從輪詢改成 websocket，需先確認 Shioaji SDK 支援度，答案決定要不要立案。
> 問題：(1) 目前安裝的 shioaji 版本是否支援 tick websocket 訂閱上限 200 檔以上？(2) 斷線重連是 SDK 內建還是要自己寫？
> 來源優先序：官方文件與 SDK 原始碼（site-packages 內）優先，部落格文章僅作線索。
> 驗收：每個結論附文件 URL 或原始碼 檔案:行號；查不到就寫「查無」。
> 回報：兩節，各為 結論+證據+信心；總長 ≤50 行。

## T5 審查（subagent_type: general-purpose，model: opus；fresh context——prompt 不含實作過程與「我覺得沒問題」）

```
背景：【被審物是什麼、要用在哪、風險等級】
被審物：【檔案路徑清單 或 diff 範圍（如 git diff main）】
審查面向：【正確性 / 規則互打 / 路徑與名稱真偽 / 邊界情況 /【任務特定面向】】
立場：你的任務是找出問題，不是確認沒問題。每個面向至少主動嘗試反駁一次。
驗收：每個 finding 附 位置 + 為什麼是問題 + 建議修法；逐面向回報「查過、發現 N 項」，不可整體一句「沒問題」帶過
回報：依嚴重度排序（會壞 > 會誤導 > 可改進）；沒有 finding 的面向要說明查了什麼所以排除
```

**範例**：
> 背景：.claude/rules/ 下六個制度檔剛寫完，未來所有 session 都會依它行動，錯誤會被長期放大。
> 被審物：c:\Claude\Invest\.claude\rules\00–05 六檔 + c:\Claude\Invest\CLAUDE.md。
> 審查面向：(1) 規則互相打架 (2) 檔案路徑/工具名/模型名是否真實存在 (3) 弱模型會誤讀的模糊語句 (4) 路由表是否有斷鏈。
> 立場：找問題，不是背書。每檔至少提出一個「這句會被怎麼誤讀」的嘗試。
> 驗收：finding 附 檔案:行號 + 誤讀方式 + 修法。
> 回報：依嚴重度排序；每個面向都要有「查了什麼」的交代。
