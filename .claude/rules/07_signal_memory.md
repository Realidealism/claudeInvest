---
paths:
  - "signal_backtest/**"
  - "backtest/__init__.py"
  - "backtest/__main__.py"
  - "backtest/adjust.py"
  - "backtest/chart.py"
  - "backtest/data.py"
  - "backtest/engine.py"
  - "backtest/market_breadth_compare.py"
  - "backtest/market_breadth_trend.py"
  - "backtest/plotly_chart.py"
  - "backtest/report.py"
  - "backtest/runner.py"
  - "backtest/signals.py"
  - "backtest/strategy.py"
  - "backtest/trade.py"
  - "analysis/score.py"
  - "analysis/_score*.py"
  - "analysis/SCORE_SPEC.md"
  - "analysis/_build_dead_fish_panel.py"
  - "analysis/market_thermometer.py"
  - "analysis/market_state.py"
  - "analysis/market_percentage.py"
  - "analysis/market_breadth.py"
  - "analysis/obv.py"
  - "analysis/wave.py"
  - "analysis/volume.py"
  - "analysis/volatility.py"
  - "analysis/build_market_vol.py"
  - "analysis/indicators.py"
  - "analysis/close.py"
  - "analysis/macd.py"
  - "analysis/donchian.py"
  - "analysis/candle.py"
  - "analysis/chandelier.py"
  - "analysis/constants.py"
  - "analysis/over_breakout.py"
  - "analysis/live_reconcile.py"
  - "charts/signals.py"
  - "tests/test_stance_*.py"
  - "tests/test_score_*.py"
  - "tests/test_engine_*.py"
  - "tests/test_unified_*.py"
  - "tests/test_sweep_baseline.py"
---

# 07 訊號工廠記憶提取

你正在讀或改訊號工廠、評分或攻防（溫度計）相關的檔案。這個主題的失敗封存與研究結論不在開場的 MEMORY.md，而在主題索引：

`C:/Users/Real/.claude/projects/c--Claude-Invest/memory/INDEX_signal_factory.md`

**觸發 → 動作**：本 session 還沒讀過這份索引 → 現在先 Read **全文**（不可只搜關鍵字或只讀片段），再提出或實作任何訊號、防守、出場、進場過濾、評分、溫度計攻防的改動。已讀過就不必重讀。

- 讀不到（檔案不存在、路徑失效、內容是空的或明顯不完整）＝未讀：停止這類提案與修改，向使用者回報讀取失敗與所用的路徑。
- 命中禁止條目：停，先回報該條目與對應的 memory 檔名（02_judgment Rubric 4）。
- 不得改提語意相近的方案來繞過封存；認為不同就說明差異，由使用者決定是否重開。
- 各條強度不同，照條目寫的範圍執行，不擴大也不縮小；「未測」「不採用」「未封存」都不等於已核准可試。

例：要改 `signal_backtest/factories/flee.py` 的 knot 條件 → 先讀索引，看到 flee knot 那條記「別再單獨試 long_knot／medium_knot」，就不提單一 scope 的變體。

本檔只在 Read／Edit／Write 碰到上列路徑時載入。討論階段、用 Bash 直接跑回測、`tmp/` 下的 sweep 規格、Explore／Plan 型 subagent 都不會觸發：那些情況靠 MEMORY.md 的指標行與 CLAUDE.md 骨架第 2 步。
