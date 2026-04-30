# hermit-stock — 台股贏勢股篩選系統

基本面+動能 winning-stock 篩選 + 分析 + 回測，with lookahead-bias 防護。

## 最佳策略（實證）

```
Gate F6+F7+F8 + Top-10 + score floor=3
```

- 必過 gate：F6（FCF 健康）、F7（月營收動能）、F8（季營收動能）
- 從通過 gate 的池中按 8 規則總分挑前 10 檔
- 等權持有，月營收 / 季報公告次日 rebalance（一年約 12-16 次）

### 期望績效（誠實估計）

| 場景 | Cumret 7.75y | 年化 | Sharpe |
|---|---|---|---|
| Paper（無流動性過濾）| 5317% | 70% | 1.75 |
| 1000 萬流動性閾值 | 1610% | 46% | 1.33 |
| **OOS + 1000 萬閾值（實戰預期）** | **~30-40% 年化** | — | **~1.0-1.2** |

OOS Sharpe = 1.35（2021-2024 純驗證），策略**通過 IS/OOS 穩健性測試**。

---

## 安裝（首次）

```bash
cd c:/Claude/Invest/hermit_stock
uv sync                # 建 .venv + 安裝相依
```

`.env` 需在父目錄 `c:/Claude/Invest/.env`，含：
```
DB_HOST=localhost
DB_PORT=5433
DB_NAME=invest
DB_USER=postgres
DB_PASSWORD=...
FINMIND_TOKEN=...      # 配息資料 backfill 用
```

---

## 日常工作流

### 每月例行（每月 11–15 號月營收公告後）

#### 1. 確認 DB 資料已更新
父專案的 scrapers 應該每天/每月例行跑：
```bash
# 父專案
python daily_update.py            # 每日股價
python -m scrapers.revenue        # 每月 11 號月營收
python -m scrapers.financials     # 每季報截止後
```

#### 2. 補抓新除權息（每月一次足矣）
```bash
cd c:/Claude/Invest/hermit_stock
uv run hermit-stock backfill-dividends
```
會自動 skip 已有資料的 ticker，只抓最新一個月新增的配息 / 減資事件。

#### 3. 跑當月 Top-20 篩選
```bash
uv run hermit-stock screen \
    --as-of 2026-04-30 \
    --min-score 6 \
    --output picks_2026_04.xlsx
```

#### 4. 對 Top-K 跑深度分析（含 forward EPS）
```bash
uv run hermit-stock analyze 2308 \
    --as-of 2026-04-30 \
    --forward-eps \
    --output reports/2308.md
```

或批次跑前 20 檔（使用 `scripts/batch_analyze_top20.py`，自行修改 AS_OF）：
```bash
uv run python scripts/batch_analyze_top20.py
```

---

### 解讀報告

每份分析報告（[reports/analyzer.py](src/hermit_stock/reports/analyzer.py)）有 7 區段：

1. **Step 1-3 macro**：當前 regime（Bull / Neutral / Bear）+ 5 個總經訊號
2. **Step 4-5 產業面**：跳過（Phase 6 不做）
3. **Step 6 八條規則**：F1-F8 逐條結果（✓/✗/?）
4. **Step 7 估值**：自動選 PE/PB/PS、5Y 區間位置、目標價、決策
5. **附錄**：核心 KPI + 最近 8 季原始資料

#### 決策怎麼讀

| 決策 | 條件 | 意義 |
|---|---|---|
| BUY | upside ≥ +20% | 目前股價低於 5Y 歷史平均估值 + 20% 安全邊際 |
| HOLD | 0% < upside < +20% | 估值合理區 |
| SELL | upside ≤ 0% | 已高於歷史平均估值 |

**注意**：「SELL」不等於要立刻砍倉——基本面好的公司即使估值偏高也可能繼續漲。把 SELL 當成「**現價沒有買進的安全邊際**」即可。

#### Forward EPS 模式
加 `--forward-eps` 會用月營收動能外推未來 12 個月 EPS，給高成長股「估值折扣」。在 paper 上 trailing 看起來 SELL 的成長股，forward 可能變 BUY。

---

## 進階用法

### 自訂回測（測試策略變體）

```bash
# 試試只用 F7 (動能單選) 看歷史表現
uv run hermit-stock backtest \
    --start 2017-01-01 --end 2024-09-30 \
    --gate-rules "" \
    --min-score-floor 1 \
    --top-k 10 \
    -o backtest_only_F7

# 加流動性過濾（中型部位適用）
uv run hermit-stock backtest \
    --start 2017-01-01 --end 2024-09-30 \
    --gate-rules F6,F7,F8 \
    --min-avg-turnover 50000000 \
    -o backtest_5000W

# 含 macro filter（Bear 時減持）
uv run hermit-stock backtest \
    --start 2017-01-01 --end 2024-09-30 \
    --gate-rules F6,F7,F8 \
    --macro-filter \
    -o backtest_macro

# 17 變體 ablation（找哪條規則最重要）
uv run hermit-stock backtest \
    --start 2017-01-01 --end 2024-09-30 \
    --gate-rules F6,F7,F8 \
    --ablation \
    -o backtest_ablation
```

### 改寫 batch script 跑指定 ticker
編輯 `scripts/batch_analyze_top20.py` 的 `AS_OF`。

---

## 關鍵注意事項

### 1. 估值錨：5Y 歷史 trailing PE
`>+2σ` 的股票會被判 SELL，但**對成長股可能誤判**。Forward EPS 模式可緩解。

### 2. F6 在科技股可能誤判
F6（FCF 健康）：「不可連 3 年負值」。資本支出大的成長期公司（半導體、設備）可能被錯殺。

### 3. F3（毛利率連續上升）是已知拖累
ablation 顯示 drop_F3 反而表現更好。但目前仍保留 F3 在 score（不是 gate）裡。

### 4. 流動性的真實衝擊
帳面 5317% 是 paper 上限。**實際個人交易，年化期望 30-40%、Sharpe ~1.0-1.2**。

### 5. 資料延遲
- 月營收 → 次月 10 號公布、11 號可用
- 季報 → 截止日（5/15、8/14、11/14、3/31）
- 系統用法定截止日為 publish_date（保守，零 lookahead）

---

## 模組結構

```
hermit_stock/
├── data/
│   ├── adapters/db_adapter.py    # PG 適配器（含 cash_flow A→Q 派生）
│   ├── adjusted_price.py          # 還原價計算
│   ├── publish_date.py            # 法定公告日推算
│   └── as_of.py                   # lookahead-bias 唯一過濾入口
├── indicators/                     # 6 大類技術指標
├── scoring/rules.py               # F1-F8 + Thresholds
├── valuation/
│   ├── multiples.py               # 每日 PE/PB/PS
│   ├── bands.py                   # 5Y rolling 統計區間
│   ├── selector.py                # 自動 PE/PB/PS 選擇
│   ├── methods.py                 # 目標價 + 決策
│   └── forward_eps.py             # 月營收動能 forward 外推
├── macro/                          # 5 訊號 + Bull/Neutral/Bear
├── backtest/                       # engine + ablation + metrics
├── reports/                        # analyzer + screener + Excel/PNG
├── scrapers/                       # FinMind backfill
└── cli.py                         # analyze / screen / backtest / backfill-dividends

scripts/
├── batch_analyze_top20.py         # 一次跑前 20 檔深度分析
├── compare_gate_variants.py       # 比較 gate 變體
├── compare_robustness.py          # 倖存者偏差 + 流動性敏感度
└── robustness_validation.py       # IS/OOS 分割 + sensitivity grid
```

---

## 常見問題

### Q：每月跑一次就夠嗎？
A：對。策略的 rebalance 訊號是每月 10 號月營收 + 季報截止日次日。

### Q：Top-20 變動很大怎麼辦？
A：每月 NEW 進入比例約 20-30%。這是策略本性——動能會輪動。長期持有所有 Top-20 比擇時切換更穩定。

### Q：要追蹤 macro regime 嗎？
A：Macro filter 經實證對策略沒幫助。但 analyze 報告的 macro 區段仍是有用的市場狀態參考。

### Q：可以加自己的規則嗎？
A：[scoring/rules.py](src/hermit_stock/scoring/rules.py) 加 `f9_xxx` 函式 + 加進 `evaluate_all`。記得補測試。

---

## 測試

```bash
uv run pytest -q                    # 131 tests
uv run ruff check .                 # 排版
uv run black --check .              # 風格
uv run mypy src                     # 型別
```

---

*最後更新：commit 5da6d66*
