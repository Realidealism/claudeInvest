"""`/help` — full command reference for the Telegram bot."""

from __future__ import annotations

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes

from telegram_bot.auth import restricted


_HELP_TEXT = """[投資管家指令]

⌨️ 鍵盤按鈕
  🌡️ 市場概況     大盤寬度與多空趨勢
  📊 追蹤清單     列出 watchlist
  📈 進出場建議   今日全市場 6 訊號

🔍 查詢
  /market        市場面概況（三尺度寬度 + 多空策略 + 趨勢變化）
  /score <股號>   單股完整資訊（評分 + 訊號 + ETF + 處置）
  /signals       今日 6 訊號全市場
  /watch         追蹤清單
  /status        系統狀態
  /ping          健康檢查
  /help          本說明

⚙️ 追蹤管理
  /watch add <股號>     加入
  /watch remove <股號>  移除

📡 自動推送
  • 12:50 cron 完成 → 追蹤清單訊號 hit
  • 盤中每 5 分鐘 → 追蹤清單投影爆量（≥2.5×）
  • 失敗 → [排程] tag 推送"""


@restricted
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(_HELP_TEXT)


def register(application) -> None:
    application.add_handler(CommandHandler("help", cmd_help))
