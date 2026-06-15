# DATA_MAP — 集保大戶選股模型資料對照 (M0)

唯讀連線：`chip_model.db_access.get_ro_cursor()`，沿用 `config.settings.DB_CONFIG`
（離散 `DB_*` 環境變數，非 `DATABASE_URL`），連線即 `set_session(readonly=True)`。

模型只用以下三張表（皆在 `tw` schema）。

---

## 1. `tw.shareholder_distribution` — 集保戶股權分散表（主資料）

- **用途**：TDCC 每週五快照、週六或次週公布的股權分散級距。模型追蹤對象。
- **主鍵**：`(stock_id, data_date)`。
- **代號格式**：`stock_id VARCHAR(10)`，無 padding、無空白（scraper `.strip()` 過）。
  含普通股、ETF、含字母尾碼之特殊證券（如 `00403A`）。
- **日期格式**：`data_date DATE`。
- **資料範圍**：`2025-04-25` ~ `2026-06-12`，共 **59 個快照週**（約 14 個月）。
- **週連續性**：間隔多為 7 天（46 次），少數 6/8 天（公布日順移），**有 1 次 13 天斷檔**
  （農曆年假跳一週）。`consec_up` 以「快照列」為步進，該斷檔視為一步。
- **欄位**：17 級距 × 3 欄 = `t{1..17}_{holders,shares,pct}`。
  - 級距 1 = 1–999 股（零股散戶）；級距 15 = 1,000,001 股以上 = **1000 張以上（千張大戶，追蹤目標）**。
  - **級距 16 = 差異數調整（reconciliation），非「全為 0」**（修正原 SPEC §4 的誤解）。
  - 級距 17 = 合計（計算比例的分母）。
- **千張比例**：直接用 `t15_pct`（NUMERIC(6,2)，官方占比%）。

### 級距加總 invariant（修正原 SPEC §5 的「<0.01%」前提）
`sum(t1..t16_shares)`（**含 t16**）對 `t17_shares` 的相對誤差，在 **普通股 universe**（n=26,587）：

| 統計量 | 值 |
|---|---|
| median | 0（完全吻合） |
| 落在 0.01% 內比例 | 98.9% |
| 99 百分位 | 0.0116% |
| 99.9 百分位 | 0.13% |
| 最大 | 0.76%（5291@2026-04-10） |

- 非普通股（ETF 等）誤差可達 **40%**（如 `00403A` 0.47%、其他更高）。
- 模型直接用官方 `t15_pct` / `t17_shares`，此殘差僅作資料健全性閘門，不影響選股正確性。

---

## 2. `tw.daily_prices` — 日線股價（回測用）

- **用途**：回測進場/出場價來源。
- **主鍵**：`(stock_id, trade_date)`（欄名為 **`trade_date`**，非 `date`）。
- **資料範圍**：`2016-01-04` ~ `2026-06-12`，2543 個交易日，**遠長於集保（瓶頸在集保側）**。
- **模型用欄位**：`close_price`（另有 OHLC、量、三大法人、融資券等大量欄位，本模型未用）。

---

## 3. `tw.stocks` — 證券主檔（普通股 universe + 基準對照）

- **用途**：§5 過濾非普通股。
- **欄位**：`stock_id, name, market, industry, listed_date, is_active,
  security_type, delisted_date, ...`。
- **`security_type` 全部相異值**：`STOCK`(2459) / `EQUITY_ETF`(205) / `BOND_ETF`(136)。
- **普通股篩選條件**：`security_type='STOCK' AND delisted_date IS NULL`
  （取代用 `00xx` 前綴硬猜 ETF/權證）。

## 4. `tw.index_prices` — 指數日線（回測基準）

- **基準**：`index_id='TAIEX'`（加權指數，`tw.indices` 列出 `TAIEX` / `TPEx`）。
- **欄位**：`(index_id, trade_date, close_price, ...)`。

---

## M0 人工審查重點
1. 集保僅 ~59 週 → 回測樣本偏小，結論信心度低（報告已標註）。
2. 級距 16 = 差異數調整（非全 0）；加總 invariant 為「近似」非「<0.01% 硬性」。
3. 普通股 universe 以 `tw.stocks.security_type='STOCK'` 為準。
