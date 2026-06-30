# 專案：微台期貨當沖系統（群益 SKCOM）

## 角色
協助開發小型微台期貨當沖系統的工程師。語言用繁體中文，程式碼註解用英文。

## 不可違反的規則
- 券商存取一律經 `broker/` 的 `BrokerAdapter`；上層禁止繞過、直接 import 任何券商 SDK（capital_skcom / 元件）
- 回測與實盤共用 `strategy/ risk/ position/`，只抽換 `broker/` 與資料來源
- 策略只吃「已收盤 K 棒」（on_bar_close）；禁止 look-ahead
- 任何下單必經 `risk_manager`；到強制平倉時點只准平倉
- 行情即時走事件回報（COM 事件經 pump 進 EventBus）；禁止輪詢
- 成本模型用「每口固定手續費 + 交易稅」，不可用股票百分比費率
- 未通過當前 Phase 的 pytest 不得進入下一個 Phase

## 程式風格
- pydantic 管設定；型別註記齊全
- 所有交易訊號 / 下單 / 回報寫結構化 log
- 新功能要能寫測試再實作

## 環境
- 核心 / 回測用一般 64-bit Python 3.11+（測試在此跑）
- Live 群益 SKCOM 用 64-bit Python + comtypes（已註冊 x64 SKCOM.dll，登入/收 tick/抓歷史 K 實測 OK）

## 目前進度（pytest 50 passed）
**離線 + live 資料 + 紙上交易 + 韌性 全部打通。** 整條管線 tick→1m/5m 聚合→策略→風控→部位→broker，回測與即時路徑對拍一致，含成本模型（每口手續費 20 + 交易稅 0.00002）。

**進入點**
- 回測單檔/多檔：`run_backtest_csv.py "reports\tm_*.csv" [lookback] [atr_mult] [atr_period]`
- 參數掃描：`run_sweep.py "reports\tm_*.csv"`（lookback×atr_mult 格）
- 抓歷史 K：`tools\fetch_kbars.py TM0000 YYYYMMDD YYYYMMDD 5 out.csv`（讀 .env）
- 即時：`run_live.py`（observe）/ `--paper`（真實 tick + SimBroker 模擬成交）/ `--trade --real`（正式環境真單，max_lots=1）

**策略**：`engulfing`（吞噬+相對低/高），lookback=20 最佳但 PF 僅 ~1.15（偏弱、逆勢摸頂底）。改良 deferred；可考慮 B2 順勢回檔過濾。`ma_cross` 為更早的佔位策略。

**live 關鍵事實**
- 微台近月群益代碼 = `TM0000`（大台 TX00 / 小台 MTX00）；報價 decimal=2（tick ×100，新版歷史 K 已處理小數）
- COM 是 STA：事件只在「建立物件的那條執行緒」觸發 → 抓資料用 inline pump、live 用 `serve()` 單執行緒 pump（含斷線重連、退避、重訂閱）
- 群益**測試環境登入失敗 1097**（帳號未開）→ 模擬單走 `--paper`（本地 SimBroker 成交），非群益 UAT
- 歷史 K **深度有限**（僅近數月，2024 抓不到）；單次查詢 ≤1 個月
- 成交寫檔：`reports\paper_trades.csv` / `live_trades.csv`

**待辦 / TODO(verify)**
- `OnNewData` 成交回報欄位索引（`_RPT_PRICE=15`/`_RPT_QTY=24`）、`serve()` 斷線碼（3002/3022/3033）、`get_kbars` K 行格式、`list_positions` 未完成 — 全需「真實成交/真實斷線」時用 log 核對
- 下一步：非假日盤中跑 `--paper` 驗證即時迴路；之後策略改良
