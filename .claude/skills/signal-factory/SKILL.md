---
name: signal-factory
description: 6 訊號工廠（pick/touch/buy/sell/buy_flee/sell_flee）改良、回測、版本管理協議。Use when editing signal_backtest/factories/*.py, tuning v## thresholds, discussing PF/win-rate/portfolio evaluation, or when user says「回測」「跑一下」「看效果」. 涵蓋 7 步回測流程、6 訊號 invariants、合池 PF 評估、force-exit 連動、metric/命名慣例（LL/HH vs bs.close）、K 棒 helpers、Go port protocol、bug-fix re-sweep、sweep 方法論、受害股報告、永久封存失敗清單。
---

# 訊號工廠改良協議

針對 `signal_backtest/factories/` 6 訊號工廠的改良方法論。包含固定回測流程、不可違反的 invariants、評估標準與已知失敗模式索引。

## 1. 標準回測流程（用戶說「回測」立即執行 1-7 步）

觸發詞：「回測」「跑一下」「看效果」「測一下」明確要求 → 啟動。**MUST NOT** 自動觸發於每次 code edit。

```bash
# Step 1: 查目前最大版本號
python -m signal_backtest._versions list  # → 抓最大 v{N}，下一版 v{N+1}

# Step 2: slug 命名：用戶改動描述抓 1-2 關鍵詞 kebab-case
#         例「加洪量 defense」→ flood-defense-short

# Step 3: 8 訊號合跑（背景，等通知）
python -m signal_backtest --signals pick,touch,buy,sell,buy_flee,sell_flee,unified_long,unified_short \
    --output tmp/sb_compare --cache --workers 16
# MUST 跑 8 個（6 基本 + 2 unified）
# MUST 加 --cache 跟 --workers 16（細節見 §1b）

# Step 4: 歸檔
cp -r tmp/sb_compare/* tmp/sb_versions/v{N}_{slug}/

# Step 5: 寫入 SQLite
python -m signal_backtest._versions add v{N} "用戶描述" --scan-dir tmp/sb_versions/v{N}_{slug}/

# Step 6: diff 上一版
python -m signal_backtest._versions diff v{N-1} v{N}
# 逐筆層 diff（五節：概覽/年度歸因/受害股A/受害股B/集中時段，年度佔比>50% 自動標警）
python -m signal_backtest._versions tdiff v{N-1} v{N} --all-signals

# Step 7: 完整報告（MUST 包含全部 7 項，不可省略）：
#  (a) per-signal 表：python -m signal_backtest._compare tmp/sb_versions/v{N}_{slug}/
#      MUST 額外列「賺賠比 (贏均 / |輸均|)」欄位 — PF 旁邊。結構診斷用，揭露 scalp vs trend 形態
#  (b) Unified 完整資訊（_compare 不含，從 unified_long/short trades.parquet 自算，扣 0.4% 成本）
#      MUST 同樣列賺賠比
#  (c) vs 前版 diff（trade Δ / 勝率 Δ / 淨均 Δ / PF 淨 Δ / 最大虧 Δ / 回撤淨 Δ / 賺賠比 Δ）
#  (d) Portfolio PF 合池（6 訊號 trades 串接後 net PF — 通常是「總體指標」）+ Portfolio 賺賠比
#  (e) 三大目標對照（見 §3）
#  (f) 受害股列表（見 §1a）— A. 同 trade 變差 Top 10  +  B. 新進場大虧 Top 10
#  (g) 關聯記憶 + 明確結論（值不值得保留？下一步方向？）
```

完成 1-7 後 **MUST NOT 自動 commit / push / 跳下個改動** — 回到等指示狀態。

### 1a. 受害股列表（Step 7f）

任何訊號條件改動 MUST 列出「**受害股**」— 讓用戶看見改動付出的代價，不只看平均指標升降。

**2026-07-06 起首選工具**：`python -m signal_backtest._versions tdiff v{N-1} v{N} --signal X`（[signal_backtest/_trade_diff.py](signal_backtest/_trade_diff.py)）已內建 A/B 兩面向 + 年度歸因 + 集中時段，直接跑指令即可，不必再手寫下方 inline script。下方樣板保留作 fallback（tdiff 壞掉或需客製欄位時用）。

兩個面向：

**A. 既有 trade 變差**（同 stock+entry_date 兩版都進，新版淨報酬退步）：
```python
for s in changed_signals:
    v_old = pd.read_parquet(f'.../{prev_v}/{s}/trades.parquet')
    v_new = pd.read_parquet(f'.../{this_v}/{s}/trades.parquet')
    # ... canonical column rename ...
    m = v_old.merge(v_new, on=['code','entry_date'], suffixes=('_old','_new'))
    m['diff'] = m['net_new'] - m['net_old']
    worse = m[m['diff'] < -5].nsmallest(10, 'diff')   # 砍 5% 以上
    # 列出 code/name/entry_date/net_old/net_new/diff/exit_reason
```

**B. 新進場大虧**（新版獨有的 trade，淨報酬 < -10%）：
```python
v_old_keys = set(v_old[['code','entry_date']].apply(tuple, axis=1))
v_new_keys = v_new[['code','entry_date']].apply(tuple, axis=1)
new_trades = v_new[~v_new_keys.isin(v_old_keys)]
big_losses = new_trades[new_trades.net < -10].nsmallest(10, 'net')
# 列出 code/name/entry_date/net/exit_reason
```

**為什麼必列**：
- 平均指標（PF / 勝率 / 賺賠比）看不到「單一極端 case」對 maxL 的影響
- 受害股集中時段 → 可能反映「特定市況條件下訊號失效」（如 2021/4-5 航海王崩段、2025-2026 V 反段）
- 觀察受害股可發現「需要加什麼防守」的線索（例如 2530 華建提示了 metric mismatch bug）
- 用戶可以從受害股清單質疑改動是否真的「淨利」（即使整體 PF 升，也許犧牲了不該犧牲的部位）

**樣板格式**：
```
=== A. 同 trade 變差 Top 10 ===
signal  code  name  entry_date  net_old  net_new  diff  exit_reason_new

=== B. 新進場大虧 Top 10 ===
signal  code  name  entry_date  days  net  exit_reason

=== 集中時段觀察 ===
(若 5+ 筆同月份集中，明確指出，如「2021/4-5 集中：航海王崩跌段」)
```

詳見 [feedback_signal_version_workflow.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\feedback_signal_version_workflow.md)。

### 1b. Cache 與 workers 規則

| 項目 | 規則 |
|---|---|
| `--cache` | 必加。cache key = stock_id + DB 日期 + ScoreBoard fingerprint |
| ScoreBoard 自動 invalidate | 配置改變（cells/weights/knot）via `board_fingerprint()` 自動 |
| cell 邏輯改但 name/points 不變 | 要手動 bump `SCORE_CACHE_VERSION` |
| 舊 fingerprint 殘留 cache | 每 process 首次 `load_stock_data` 時自動清理 |
| **dataclass schema 改變** | `VolumeResult / BSResult / CloseResult` 加減欄位 → **MUST `rm -rf data/stock_cache/`**，否則 AttributeError |
| `--workers 16` | physical cores 上限（避開 hyperthreading 遞減）|
| 速度實測（Win 16C/24T） | w=1 27:46 / w=16 ScoreBoard 不 cache 3:02 / w=16 + SB cold 3:55 / **w=16 + SB warm 27s（61.7x）** |

### 單訊號 ad-hoc 測試（非正式 7 步流程）

用戶有時只想快測一個訊號（debug、條件 sanity check）：

```bash
# MUST 至少跑 3 個：目標訊號 + unified_long + unified_short
python -m signal_backtest --signals {target},unified_long,unified_short \
    --output tmp/sb_adhoc --cache --workers 16
```

理由：force-exit 連動架構（§5）讓單訊號改動會影響 unified 退場時點，**MUST** 看 unified 結果才能判斷淨效應。沒附 unified 的單訊號表只是局部診斷，**MUST NOT** 拿來做採用決策。

單訊號 ad-hoc 測試 **MUST NOT** 寫入 `signal_versions.db`（避免污染版本軸）— 等決定後再走完整 1-7 步。

## 2. Hard constraints — MUST / MUST NOT

| 規則 | 來源 |
|---|---|
| **MUST NOT** 刪除任何訊號或新增第 7 個訊號（6 訊號固定） | [feedback_signal_factory_invariants.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\feedback_signal_factory_invariants.md) |
| **MUST** 在 ~dead 過濾下重設計訊號（`~data.money_result.dead`, money_level >= 3） | [project_signals_v94_high_volume.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\project_signals_v94_high_volume.md) |
| **MUST** 用 `.pct` (-100~+100) 而非 raw `.score`；helper 是 `_long_pct_array` / `_short_pct_array` | [feedback_score_use_pct.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\feedback_score_use_pct.md) |
| **MUST** 做多用 `_long_pct_array`、做空用 `_short_pct_array`（雖 99.98% 鏡像對偶仍分開） | [feedback_score_pct_side_split.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\feedback_score_pct_side_split.md) |
| **MUST** 方向側用中文「多」/「空」（或「做多」/「做空」），避開英文 long / short（會跟時框 short/medium/long ScoreCard 詞彙混淆）；表格欄位偏好短形「多」「空」 | [feedback_long_short_chinese.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\feedback_long_short_chinese.md) |
| **MUST** 任何單訊號回測（含 `--signal X` ad-hoc 測試）報告必須同時跑/顯示 unified_long + unified_short 結果，因 force-exit 連動讓單訊號改動會影響合 entry mode（見 §5） | feedback_signal_version_workflow.md + §5 |
| **MUST** trailing stop 用 `freeze=True`，不隨新訊號移動 | [feedback_trailing_stop_freeze.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\feedback_trailing_stop_freeze.md) |
| **MUST NOT** 主動 commit；用戶看完 diff 確認再 commit | feedback_signal_version_workflow.md |

## 3. 評估標準（MUST 全部達標才視為改良）

[feedback_signal_improvement_goals.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\feedback_signal_improvement_goals.md) 三大目標：

1. **提高勝率**
2. **降低最大虧損、提升最大獲利**
3. **提升交易質量、降低交易次數**（質量＝**賺賠比** + PF）

「交易數↑ 但 PF 不升」**MUST** 視為負向。
「PF 不變但賺賠比下降」**MUST** 視為**結構性退步**（訊號變脆弱，對 cost / 黑天鵝更敏感）。

## 3a. PF vs 賺賠比 — 兩個維度分工

| 指標 | 公式 | 揭露 | 何時用 |
|---|---|---|---|
| **PF** | Σ正報酬 / \|Σ負報酬\| | 邊際 / 是否賺錢 | 「這訊號還有 edge 嗎？」 |
| **賺賠比** | 平均贏均 / \|平均輸均\| | 結構 / 一筆贏 vs 一筆輸 | 「訊號結構脆弱還是 robust？」 |

恆等式：`PF = 賺賠比 × (勝率 / (1-勝率))`

**同 PF 不同賺賠比**：賺賠比高的更 robust（trend-following 形態），低的脆弱（scalpy，cost 一漲就崩）。
**同訊號 tune 方向**：賺賠比 < 1.7 而 PF < 1 → 改 stop loss / trailing stop 結構，不是調 score gate 閾值。

## 4. 合池 PF 才是真實評估（pick/touch/buy/sell）

[feedback_signal_portfolio_eval.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\feedback_signal_portfolio_eval.md)：

- 單一訊號 PF 改良但合池 PF 退步 = 稀釋現象 = 失敗
- **MUST** 把 6 訊號 trades 串接後算 net PF 當總體指標
- Per-signal Δ 只是診斷，不是決策依據

## 5. Flee 訊號 force-exit 設計與評估

`buy_flee` / `sell_flee` 是整個 signal-factory 最特殊的訊號類別 — 4 個子段集中討論。

### 5a. 雙重角色架構

[project_signal_force_exit_architecture.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\project_signal_force_exit_architecture.md)：

`buy_flee` / `sell_flee` 同時是：
- **自身的 entry signal**（觸發後做空 / 做多）
- **pick / buy / touch / sell 的強制出場 trigger**

**MUST** 意識到動 buy_flee/sell_flee 條件會連動其他 4 訊號的退場時點 → 隔離測試一個訊號改動是不可能的，永遠看合池影響。

（用戶 insight：「flee 本來就是要做為提早離場做準備，不然只是另一個做多做空」）

### 5b. Flee 改良的評估規則

**MUST** 額外滿足：

1. **觸發頻率必須維持足夠密度服務 force-exit 角色** — flee trades 大幅下降意味著「離場警報變稀」，即便自身 PF 升、合池 PF 升，也**MUST** 視為功能退化警訊
2. **MUST NOT** 把 flee 當成第 7 個 entry-only 訊號優化 — 違反 §2 invariant（6 訊號固定，flee 不可變成普通做空/做多）
3. flee 報告 MUST 額外包含：
   - flee trade count Δ（不只 PF）
   - 對 pick/touch/buy/sell 的「訊號出場」比例 Δ（trades.parquet 的 `出場原因` 欄位算）
   - 若「訊號出場」比例 < 1%，警示用戶 flee 已實質失去 force-exit 角色

### 5c. 合池 PF 對 flee 是表面指標

「flee 自身 PF 升 + 合池 PF 升」可能來自「flee 變稀導致自身 trade set 集中在高 PF 事件」，**而非真實 edge 改善**。

範例：v152 → v167 flee 觸發從 buy_flee 844 / sell_flee 2724 → 210 / 823，合池 PF 看似升，但 flee 已退化成「低頻純 entry 訊號」。

→ 真正 flee 改良 MUST 同時看：合池 PF + flee trade count + 其他訊號「訊號出場」比例（§5b 第 3 點）

### 5d. Force-exit 角色弱化警訊量化

v167 當時實測 exit_reason 分布（snapshot，可能已過時，audit 跑 `trades['出場原因'].value_counts(normalize=True)`）：

| 訊號 | 訊號出場 % | 防守價 % |
|---|---:|---:|
| pick | 0.0% | 99.2% |
| touch | 0.2% | 98.5% |
| buy | 0.0% | 96.2% |
| sell | 0.1% | 98.7% |

訊號出場 < 1% 表示 flee 已實質失去 force-exit 角色（v167 退化來源：v152→v167 一連串 flee 進場條件收嚴 — rise3 拿掉、階梯 gate、rise3 d80 等）。

**改良方向修正**：

1. 想要 flee 服務 force-exit → MUST 讓 flee 觸發更密（放寬進場條件、加新觸發 path），即使稀釋 flee 自身 PF
2. 想要其他訊號提早停損停利，除了讓 flee 變密，可改：
   - `long_floor_period` / `short_floor_period`（trailing stop 視窗）
   - 加新 `DefenseRule`（事件型防守，不限 flee 訊號）
   - 改 pick/touch/buy/sell 的 `long_exit` / `short_exit` 來源（多訊號 OR）
3. **MUST NOT** 把 v167「flee 觸發極低 → 自身 PF 升」當成功優化 — 那是把 flee 推向 entry-only，背離設計意圖

## 6. ScoreBoard pct 整合

| 規則 | 細節 |
|---|---|
| Gate 條件用閾值 | 例如 `_short_pct_array(data) >= 30` 當 post-strength filter |
| ScoreBoard 改動會讓 pct 分佈漂移 | max/min_possible 改變 → 同樣閾值 filter 出不同股票集合 |
| 既有 v## 規則 **MUST** 在 ScoreBoard 大改後重 tune 閾值 | 例：v127-v148 的 `short_pct >= 30` 在 ScoreBoard tuning 後可能完全失效 |

## 6b. 溫度計 stance 耦合（v359 起，MUST）

**耦合事實**：`signal_backtest/factories/_conditions.py` 的 `_stance_block_dates()` / `_stance_long_held_dates()` **live import** `analysis.market_thermometer.daily_stance_frame()`，不是凍結快照。改溫度計任何影響 `defensive` / `mode` 的判準、投票、參數 → 消費點的日集合當場改變，回測結果跟著變，**沒有任何警告**。

**消費點（2026-08-06 實查；每次動手前 MUST 重新 grep，這是程式碼事實不是架構保證）**：

| # | 消費點 | 位置 | 來源 date set |
|---|---|---|---|
| 1 | buy 進場 `rule_not_defensive`（v359） | `_conditions.py` | `_stance_block_dates()` |
| 2 | buy 防守 `STANCE_HELD_DEFENSE=8`（v361） | `buy_sell.py` | `_stance_long_held_dates(8)` |
| 3 | sell 進場 `SELL_STANCE_GATE=(3,25)`（v362） | `_conditions.py` | `_stance_long_held_dates(3)` |

→ **會變的是 buy 與 sell**；pick / touch / buy_flee / sell_flee 不消費 stance，應 bit-identical。（v363 另改了溫度計內部的 A 票投票形式，不新增消費點但同時動到 `top_vote` 與 swing 層兩個下游。）

**觸發**：動 `analysis/market_thermometer.py`（或它讀的上游欄位，如 `analysis/obv.py`、`analysis/market_breadth.py`）任何會改到 stance 的東西；或評估「要不要採用某個溫度計攻防改動」。**本節優先於 §1 的「回測」字面觸發**：改完溫度計後即使使用者說「回測」，仍先走以下三段式，過了第 3 段才轉入 §1 完整 7 步。

**動作（三段式，逐段過才進下一段）**：

1. **差異日（零回測，秒級）**：算改前/改後 blocked date set（`defensive AND mode in ("top","trend")`，T+1 位移）的對稱差 + 逐年分布。
   - 差異日 = 0 → 對工廠零影響，只評溫度計自身，免回測。
   - 差異日 <20 天或集中單一年 → 單事件，依 rules/02_judgment Rubric 5 第 6 點不足以支撐採用。
   - **MUST 用新 process 算**：`_stance_block_dates()` / `_stance_long_held_dates()` 都是 `lru_cache`，同一 process 內改參數不會重算。三組 date set 各自算差異日（上表三個消費點）。
2. **工廠受控對照**：開跑前 MUST 先 `grep -rn "_stance_block\|_stance_long_held" signal_backtest/factories/` 重新確認消費點清單（上表是查核當日的事實，沒有測試把關，不可只信本文件）。
   - **MUST 雙跑同資料窗**：baseline 用「同一份程式把開關關掉（設 None）重跑」，**MUST NOT 拿上一版的歸檔當 baseline**——DB 每日更新，跨日期的 diff 欄不可讀（v363 實例：`diff v362 v363` 讓不消費 stance 的 pick/touch 也出現 Δ，其 overfit 警示全是污染產物）。
   - **MUST 序列執行，不可同時跑兩版**：回測 worker 會在啟動時重新 import `market_thermometer`，中途改常數會讓不同 worker 讀到不同值。順序＝設常數 → 算差異日 → 跑回測 → 換常數 → 再跑。
   - 跑滿 8 訊號（省不了多少時間，且四個非消費訊號正好當 sanity）：`pick,touch,buy,sell,buy_flee,sell_flee,unified_long,unified_short`。合池＝6 訊號串接。**四個非消費訊號若不 bit-identical ＝耦合外洩，先查 bug 再談效果**。
   - 對照 baseline 的 合池 PF / 賺賠比 / 勝率 / maxL / maxG + 受害股（§1a）+ 前後半 + 逐年。
3. **雙帳呈報，使用者裁決**：溫度計側（每單位曝險年化、recall/DD **同窗**，不可用累報酬）與工廠側（三大目標，§3）**任一為負 → 既不自行採用也不自行否決**，附雙側數字交使用者取捨。雙側皆正且使用者裁定採用 → 才走 §1 完整 7 步開新版本號歸檔，SQLite 結論註明「上游＝溫度計改動」。

**反向**：重掃 buy 的 gate/門檻時，stance 是活的相依——溫度計若在兩次 sweep 之間改過，舊 sweep 結論失效（同 §6 ScoreBoard pct 漂移的道理）。

## 7. Metric / 命名慣例（強 anti-bug 規則）

涉及「N 日新低/新高」必須先區分要的是「日內」還是「收盤」極值 — 選錯 metric 會造成 entry/defense 條件鬆/緊偏差，最大左尾損失常源自此類 bug（v265 / v271 案例）。

| 命名 | 公式 | 何時用 |
|---|---|---|
| `LL_N` (日內 low rolling) | `rolling_lowest(data.low, N)` | 「最近 N 日低點」「new N-day low」「intraday extreme」|
| `HH_N` (日內 high rolling) | `rolling_highest(data.high, N)` | 「最近 N 日高點」「new N-day high」「intraday extreme」|
| `data.close_result.bs.close_s[N]` | `rolling_lowest(data.close, N)` | 「N 日最低**收盤**」（明確收盤-based）|
| `data.close_result.bs.close_b[N]` | `rolling_highest(data.close, N)` | 「N 日最高**收盤**」（明確收盤-based）|

**MUST**：寫條件時若 LHS 是 `data.low / data.high`（日內），RHS **MUST** 也用 `rolling_lowest(low)` / `rolling_highest(high)`，**NOT** `bs.close_s` / `bs.close_b`。混用是 metric mismatch bug（v265 案例 2530 華建 -16.75% 大虧根源）。

**MUST**：寫條件時若 LHS 是 `data.close`（收盤），RHS 用 `bs.close_s` / `bs.close_b` 才是 apples-to-apples。

**MUST**：local variable / 註解命名要透露 metric 來源：
- ✓ `close_b = data.close_result.bs.close_b`（看得出收盤）
- ✗ `bs_high = data.close_result.bs.close_b`（混淆「高」字面理解成日內 high）
- ✓ `prev_ll8 = _shift(rolling_lowest(data.low, 8), 1)`
- ✗ `prev_bs_low_8 = _shift(data.close_result.bs.close_s[8], 1)`（含義不明）

### 7a. 命名 vs 行為一致性 audit（每幾版檢一次）

掃 3 類 drift：
1. **`DefenseRule.name` 中的「N日高/低」vs source 實際 `rolling_*(_, N)`** — 應一致
2. **註解 `→ HH<N>` / `→ LL<N>` vs source 實際** — v203k 案例：3 處「→ HH8」實際為 HH2，drift v204 sweep 後沒同步
3. **註解 `v###` 提及的 sweep 邏輯是否還在** — 可能規則早被改但註解殘留

audit 自動化思路（提示）：parse DefenseRule name 的「(\d+)日(高|低)」+ source 的 `rolling_*\(_, (\d+)\)`，比對 N 值差異。

## 8. K 棒形態 helpers

`analysis/candle.py` 與 `signal_backtest/factories/_conditions.py` 有多種 K 棒形態判斷，**MUST** 區分使用：

| Helper | 定義 | 抓的形態 |
|---|---|---|
| `data.candle_result.shadow.upper` | `upper_wick > (HL.ma - HL.std*0.1) × 0.5` 絕對長度判定 | 「顯著上影」（不排除大實體）|
| `data.candle_result.shadow.lower` | mirror | 「顯著下影」|
| `_hammer_upper_shadow(data)` | `upper_wick/HL ≥ 0.5 AND body/HL ≤ 0.3` | **真吐血線** (climax top)|
| `_hammer_lower_shadow(data)` | `lower_wick/HL ≥ 0.5 AND body/HL ≤ 0.3` | **真錘子線** (climax bottom)|

**MUST NOT** 自己手刻 `(high - close) / hl ≥ X` 或 `(close - low) / hl ≥ X` 當「長影線」判定：
- 對大陽線 `close > open`，`(close - low)` ≈ 整根 candle 高度 → ratio 永遠接近 1，**錯把大陽當錘子線**（v265 bug 來源）
- 對大陰線同理，錯把大陰當吐血線

要「真實長影 + 小實體」用 `_hammer_*`；要「相對 rolling HL 顯著影」用 `data.candle_result.shadow.*`。

## 9. Go 原碼比對 protocol（port-faithfulness）

Port 自 Go `C:\Github\Invest\internal\calculatetrade2\CalculateTrade2.go`。**Go 註解可能誤導，MUST 看 calculation 不看 struct 註解**：

```bash
# 找某 Go 變數的真實計算
grep -n "VAR_NAME\[DayCount\] = Calculate" C:/Github/Invest/internal/calculatetrade2/CalculateTrade2.go
```

Known Go 欄位類別（命名規律）：

| Go 欄位 | 計算 | Python 對應 |
|---|---|---|
| `CD<N>B` | `Calculate.HighestFloat32(Close, N, ...)` | `data.close_result.bs.close_b[N]` |
| `CD<N>S` | `Calculate.LowestFloat32(Close, N, ...)` | `data.close_result.bs.close_s[N]` |
| `HD<N>B` | `Calculate.HighestFloat32(High, N, ...)` | `rolling_highest(data.high, N)` |
| `HD<N>S` | `Calculate.LowestFloat32(High, N, ...)` | `rolling_lowest(data.high, N)`（少見）|
| `LD<N>B` | `Calculate.HighestFloat32(Low, N, ...)` | `rolling_highest(data.low, N)`（少見）|
| `LD<N>S` | `Calculate.LowestFloat32(Low, N, ...)` | `rolling_lowest(data.low, N)` |

**踩過坑案例**：
- `CD2B` Go 註解 `//高價2` 看似「高價」實際用 Close → 對應 `close_b` not 日內 high
- `LD55S` 是 Go LowestFloat32(**Low**, 55)，Python v110 ratchet 誤用 `bs.close_s[55]` 是 port bug（v273 fix）

**MUST**：port 新 Go 規則時 grep 該變數真實 calculation，**MUST NOT** 信 struct 註解。

## 10. Bug-fix 後 re-sweep 義務

修了會影響 entry/defense 條件行為的 bug 後（特別是 metric mismatch、helper 誤用等），原本在 buggy 環境 sweep 出來的閾值可能不再最佳。**MUST**：

1. 列出受影響的 path（哪些 rules 在 buggy 環境 tune）
2. 對這些 path 重 sweep
3. **不必對未受影響的 path 全部重 tune**（避免 curve-fitting 累積）

案例：v271 修了 blow-off metric mismatch (bs.close_s/b → rolling intraday) → v274-v278 重 sweep pick blow-off lp 閾值，發現 `lp ≥ 5` 比舊 `lp ≥ 0` 好 +0.036 pick PF（在 buggy 環境 sweep 的 lp ≥ 0 不再最佳）。其他 4 訊號的 tier gates（v234/v238/v263/v264）未涉及 bug，不重 sweep。

## 11. Sweep 方法論

### 11-0. 參數提案卡（MUST，先於任何 sweep 或閾值提案）

**觸發**：向使用者提出「調某閾值 / 掃某區間 / 換某窗口」的任何提案，或動手寫 sweep spec 的 GRID 之前。
**動作**：提案訊息 MUST 含以下四問的答案，缺一不得開跑。答不出來的那一問，就是還沒想清楚的地方。

1. **為何是這個參數** — 機制假設，或觀察到的具體失效模式（哪些交易壞在這裡）。「試試看」「順便一起掃」不是理由。
2. **為何是這個值域** — 每個端點的來源：現行 baseline ±N / 本序列上已知的分布 / 市場慣例。**沿用他處參數 MUST 標「借用自 X，本序列未驗」，且不得當中心點**——借來的參數不算參數，挑選理由會隨舊用途一起失效（RS window 123 案例，02_judgment Rubric 5 第 6 點）。
3. **為何是這個步長與點數** — 要能分辨 plateau vs 孤峰。步長太粗看不出 plateau，太細只是噪音。
4. **預期方向與可證偽點** — 「若假設成立，應看到 X」。跑完 MUST 對照這句話；方向反了要明說「假設證偽」，不得事後改口成「本來就只是想看看」。

範例（RS 窗口）：
- ✗「掃 rs_win = 20 / 60 / 123」
- ✓「(1) 假設：高分股跌破自身 RS 低點＝籌碼鬆動早期訊號，需要一個回看窗口定義『低點』。(2) 值域 20–233：下限月線級、上限半年；123 是從 distance cell 借來的，本序列未驗，故納入但不當中心點。(3) 步長取費氏 20/34/55/89/144/233，看有無 plateau。(4) 若假設成立，D9 應在**多個相鄰窗口**一致為負且每格 n ≥ 3000；只在單一窗口出現極值 → 薄樣本假象，棄。」

引用任何 sweep / cohort / regime 表的格子做決策前，**MUST 先看該格的 n**；表格產出一律連 n 一起印。

### 11a. 多變數 sweep grid 模板

要同時測 2 個閾值改動時，**MUST** 用 4-plan grid 隔離效應：

| Plan | X | Y | 目的 |
|---|---|---|---|
| A | 改 | 改 | 兩個都改的合計效果 |
| B | 不動 | 改 | 只改 Y 的純效應 |
| C | 改 | 不動 | 只改 X 的純效應 |
| D | 改（極端值） | 改 | 額外探邊界 |

對比 A-B-C 可分辨「X 與 Y 是加成、抵銷還是中性」。比一次改兩個然後猜哪個生效安全多了。

### 11b. Proxy vs real backtest

10 日前向收益 proxy（offline forward-return）有用但有限：
- ✓ **相對排序可信**（哪個閾值較佳）
- ✗ **絕對 PF 不可信**（系統性低估，因為沒考慮 defense rules / force exits / 動態出場）
- ✗ 若訊號搭配 strong defense（如 v264 touch tier gate），proxy 預測可能跟 real 偏差大（touch sp 閾值案例：proxy 說 sp ≥ -5/-10 較佳，real 卻是 sp ≥ 0 與 sp ≥ -5 持平甚至退步）

**MUST**：用 proxy 找 top 2-3 候選 → 用 real backtest 驗證 → 不可只看 proxy 就採用。

### 11b-1. Proxy 過關 ≠ 能用：三道硬關卡（2026-07-20 起，籌碼/量能探索特化）

**觸發**：任何 offline / forward-return proxy 顯示候選 edge。**動作**：採用或付 rebuild 前，**三關全過才算數**（少一關即不採用）。理由：2026-07 券資比/量能探索反覆發生「proxy 亮燈、下一關就死」——券資比/dtc/climax-量都過了 raw proxy，各死在其中一關；股東會軋空三關全過才是真的。

1. **vs 同日同分數桶同儕**：算超額時扣掉「同一天、同 total_long decile」的同儕均值。超額 ≈0 → 訊號只是換句話說「低分/高分」，**冗餘於 ScoreBoard**，丟掉。（券資比、dtc、融資背離都死在這：對全市場有超額、對同分數桶歸零。）
2. **真 trades 轉移測試**（§11b real backtest 驗證的強化版）：proxy 在**它自己的 cohort**上量的，不代表轉移到工廠實際進場——**MUST** 拿 v358 真成交 post-hoc 把該條件套到 pick/buy/sell_flee/sell 的實際 entry 上看效應是否還在。（climax-量在 Donchian proxy 兩關全過，一到真工廠進場就消失、逐年還反向。）
3. **時間均勻**（§Rubric 5 的 proxy 版）：逐年、且增益非集中單一年/單一事件（2020 崩盤、2025 關稅崩是慣犯）。用「同年高−低」抵市場 beta 再看。

**負向結論也要交待**：判定「無 edge」時 MUST 附真實失敗案例（具名交易/受害股 + 失敗機制），不能只給 aggregate——通用規則見 rules/02_judgment.md Rubric 2。

### 11b-2. 兩個 close-based 回測資料陷阱（2026-07-20 起）

- **無還原價**：專案引擎 `build_stock_data` 用**原始 close_price**（連 v358 亦然），**沒有還原價序列**。任何 close-based proxy/回測 **MUST 剔除窗口內單日 >±40% 的移動**（台股單日限 ±10% → >40% 必是未還原分割/減資）。範例：先前誤判為「-92.7% 內行放空崩塌」的那筆其實是 5314 世紀分割假象，非真虧；剔後 W=6 股東會軋空結果不變＝edge 非假象膨脹。
- **存量 vs 流量 + 分母污染**：探籌碼變數 **MUST 先分存量（餘額/水位）vs 流量（當日量/事件）**——2026-07 探索中融券**餘額**七輪全死、融券**賣出流量**才有料。比值型變數要查**分母污染**：券資比＝融券/融資的暴衝多來自**融資**塌陷（崩跌股融資被斷頭），不是融券變多；乾淨版用隔離分子的指標（days-to-cover＝融券/量）。

### 11c. 統一 sweep runner 與封存（2026-07-06 起）

跑參數掃描一律用 `signal_backtest/sweep.py`，不再手寫 ad-hoc 迴圈腳本：

```bash
# 掃之前 MUST 先查此參數區間是否已掃過（命中相同區間且結論負向 → 停，除非使用者明示重開）
python -m signal_backtest._versions sweeps list --signal {signal}

# spec 檔（拋棄式，放 tmp/）合約：NAME、SIGNAL（必填）、GRID（GRID[0] MUST 為當前 baseline 參數）、
# build(data, *params) -> dict(side/entry/exit_/defense_rules/...)，選填 PARAM_DESC
python -m signal_backtest.sweep tmp/sweep_spec_{topic}.py --workers 16 --cache
# 試跑不留紀錄加 --no-archive；正式掃描跑完自動寫入 sweeps 表

# 解讀完 MUST 回填一行結論（plateau 位置 / 採用與否 / 時間均勻性）
python -m signal_backtest._versions sweeps conclude {id} "結論一句話"
```

- 彙總表內建 前半PF/後半PF（entry_date 中點日切）與 `⚠時間不均` 標記——Rubric「sweep 單調 ≠ 可採用，必須時間均勻」的機械化，逐點自動檢查
- 落檔 `tmp/sweeps/{NAME}/`（summary.csv + 各點 trades parquet）
- 封存的意義：跨 session 防重掃已死參數區間（momentum_override 連做 11 版的教訓，下沉到參數粒度）

## 12. 永久封存失敗模式（MUST NOT 重試）

每條一行 + memory 指針，避免重蹈：

- **Go 槓桿值狀態機**規則移植：Go 有 state machine，我們是 SMA 硬條件，部分規則永遠不觸發 → [project_go_rule_porting_pitfalls.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\project_go_rule_porting_pitfalls.md)
- **扣抵 state 當 trailing stop trigger**：v44-v47 全失敗，pick 進場特別衝突；需 event-based 而非 state-based → [project_turn_defense_failed.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\project_turn_defense_failed.md)
- **blow-off top 出場/防守對多方（封存完全）**：凍結型 v48-v53 六變種 + 暫態型 v353 皆失敗；根因=紅棒+洪量是 runner 燃料，賣強勢棒必砍右尾（maxG 841→244）；多方早出場只准賣走弱（give-back 型）→ [project_blow_off_top_defense_failed.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\project_blow_off_top_defense_failed.md)
- **MA 共振 filter 鏡像對稱**：sell+buy_flee 大成功 +0.043，但 v60 touch / v61 sell_flee 鏡像 destructive 失敗 → [project_ma_confluence_asymmetry.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\project_ma_confluence_asymmetry.md)
- **buy_flee 改 score-based**：v85-v95 多次驗證 pattern 已優，改 score 必崩 PF 0.94-2.17 < baseline 2.98 → [project_signals_v94_high_volume.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\project_signals_v94_high_volume.md)
- **bait_flip 鏡像**：bait_flip_up 對 sell_flee 有效 (+0.17)，bait_flip_down 對 buy_flee 失敗 (-1.45)。Go 原版只給 sell_flee 加 v36 子句正確 → 同上
- **Flee knot scope 單獨**：v146/v147 單 scope (long_knot or medium_knot) 都傷；v148 long+medium 並集 +0.0064 採用 → [project_flee_knot_scope_union.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\project_flee_knot_scope_union.md)
- **OBV trend (latched)**：三度測試（含 ~dead universe）全失敗，event 形式才採用 → [project_obv_multi_period.md](C:\Users\Real\.claude\projects\c--Claude-Invest\memory\project_obv_multi_period.md)

## 13. Tool 索引

| 用途 | 路徑 |
|---|---|
| pick / touch 訊號 | [signal_backtest/factories/pick_touch.py](signal_backtest/factories/pick_touch.py) |
| buy / sell 訊號 | [signal_backtest/factories/buy_sell.py](signal_backtest/factories/buy_sell.py) |
| buy_flee / sell_flee 訊號 | [signal_backtest/factories/flee.py](signal_backtest/factories/flee.py) |
| unified_long / unified_short | [signal_backtest/factories/unified.py](signal_backtest/factories/unified.py) |
| 共享規則 helpers | [signal_backtest/factories/_conditions.py](signal_backtest/factories/_conditions.py) |
| 版本管理 CLI | [signal_backtest/_versions.py](signal_backtest/_versions.py) |
| 6 訊號聚合表 | [signal_backtest/_compare.py](signal_backtest/_compare.py) |
| 版本歷史 DB | `data/signal_versions.db`（SQLite）|
| 改良 / 拒絕完整記憶 | `~/.claude/projects/c--Claude-Invest/memory/project_signal_*.md` |
| 受害股分析 + per-trade diff (§1a) | `python -m signal_backtest._versions tdiff vA vB --signal X`（或 `--all-signals`）— [signal_backtest/_trade_diff.py](signal_backtest/_trade_diff.py)，§1a inline 樣板僅作 fallback |
| 統一 sweep runner (§11c) | `python -m signal_backtest.sweep <spec.py> --workers 16 --cache` — [signal_backtest/sweep.py](signal_backtest/sweep.py)，spec 合約 NAME/SIGNAL/GRID/build，GRID[0]=baseline，前半/後半 PF 內建 |
| sweep 結果封存 (§11c) | `python -m signal_backtest._versions sweeps list/show/conclude/delete` — runner 跑完自動寫入，結論用 conclude 回填 |

## Anti-patterns（MUST NOT）

### 評估 / 決策
- **MUST NOT** 用 raw `.score` — **MUST** 用 `.pct`（§2）
- **MUST NOT** 看單一訊號 PF 升就採用 — **MUST** 看合池 PF（§4）
- **MUST NOT** 只看 PF 不看賺賠比 — 同 PF 結構差異大；賺賠比 < 1.7 的訊號 cost 一漲就崩（§3a）
- **MUST NOT** 只看平均指標決策 — 必列受害股，揭露「改動付出的代價」與「集中時段失效模式」（§1a）
- **MUST NOT** 跳過任何 §1 第 7 步報告項目（per-signal、unified、diff、合池、三大目標、**受害股**、關聯記憶、結論）

### 改動 / 設計
- **MUST NOT** 對既有失敗路線換 cosmetic 變體重試（trend form、扣抵 state、blow-off 等）（§12）
- **MUST NOT** 直接複製 Go 規則：Go state machine ≠ 我們 SMA 條件；先 dry-run（§9）
- **MUST NOT** 信 Go struct 註解（如 CD2B 寫「高價2」實際用 Close）— port 時 grep calculation site（§9）
- **MUST NOT** 把 `data.low / data.high`（日內）跟 `bs.close_s / bs.close_b`（收盤）混比 — metric mismatch 是 entry 條件鬆/緊偏差的常見 bug（§7）
- **MUST NOT** 自己手刻 `(high - close) / hl` 當「長上影線」— 對大陰線會錯把整根 candle 當影線；用 `_hammer_upper_shadow` 或 `data.candle_result.shadow.upper`（§8）
- **MUST NOT** 提出參數改動或開 sweep 而未附提案卡四問（為何這個參數 / 為何這個值域 / 為何這個步長 / 預期方向與可證偽點）（§11-0）
- **MUST NOT** 直接沿用他處（別的序列、別的子系統）的最佳參數當本序列的中心點 — 借來的參數不算參數，MUST 在本序列重掃（§11-0）
- **MUST NOT** 改溫度計攻防（stance）而不算差異日、不跑工廠受控對照 — stance 是 buy 的 live 相依，改了工廠就變（§6b）
- **MUST NOT** 同時改 2+ 閾值然後猜哪個有效 — 用 4-plan A/B/C/D grid 隔離（§11a）
- **MUST NOT** 只看 proxy（offline forward-return）就採用閾值改動 — 系統性低估 real PF，**MUST** real backtest 驗證（§11b）

### 流程 / 維運
- **MUST NOT** 改 dataclass schema 後忘記 `rm -rf data/stock_cache/` — 舊 pickle 反序列化 AttributeError（§1b）
- **MUST NOT** 把 flee 訊號當成普通做多/做空訊號優化（§5）— flee 本質是 force-exit trigger，自身 PF 升但觸發崩 = 設計失敗，不是優化成功
- **MUST NOT** 自動 commit / push / 跳下個改動（§1）
