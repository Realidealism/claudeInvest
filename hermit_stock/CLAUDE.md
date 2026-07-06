# hermit_stock 子專案

## 定位與狀態
- 基本面＋動能贏勢股篩選＋回測子專案,方法與實證績效見 [README.md](README.md)
- 開發凍結於 2026-04-30(commit c63166c);daily snapshot 已接入父專案 `daily_update.py` 管線,屬「停止開發、仍在生產」狀態——改動前先確認不影響父專案每日排程

## 環境(與父專案不同,勿混用)
- 依賴用 uv 管理:`cd c:/Claude/Invest/hermit_stock` 後 `uv sync`;執行一律 `uv run <script>`
- `.venv` 內沒有 pip,不要用父專案的 system Python 對它裝套件或直接跑
- `.env` 共用父目錄 `c:/Claude/Invest/.env`(DB 連線 + FINMIND_TOKEN)

## 慣例
- `backtest_out*/`、`top20_reports*/`、`*.log` 為可再生產物,已在本目錄 .gitignore,不要 commit
- 現行最佳策略:Gate F6+F7+F8 + Top-10 + score floor=3(實證數字見 README)
