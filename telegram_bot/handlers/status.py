"""Health-check handlers: /start, /ping, /status."""

from __future__ import annotations

import platform
from datetime import datetime, timezone, timedelta

from telegram import KeyboardButton, ReplyKeyboardMarkup, Update
from telegram.ext import CommandHandler, ContextTypes

from telegram_bot.auth import restricted

_TPE = timezone(timedelta(hours=8))

BTN_MARKET = "🌡️ 市場概況"
BTN_WATCH = "📊 追蹤清單"
BTN_SIGNALS = "📈 進出場建議"
BTN_WATCH_ADD = "➕ 加入"
BTN_WATCH_REMOVE = "➖ 移除"

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_MARKET), KeyboardButton(BTN_WATCH)],
        [KeyboardButton(BTN_SIGNALS)],
        [KeyboardButton(BTN_WATCH_ADD), KeyboardButton(BTN_WATCH_REMOVE)],
    ],
    resize_keyboard=True,
    is_persistent=True,
)


@restricted
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "投資管家上線。\n"
        "鍵盤按鈕：🌡️ 市場概況 / 📊 追蹤清單 / 📈 進出場建議\n"
        "管理追蹤：➕ 加入 / ➖ 移除（批次中可按訊息上的 ✅ 完成）\n"
        "打 /help 看完整指令說明。",
        reply_markup=MAIN_KEYBOARD,
    )


@restricted
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("pong")


@restricted
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    now = datetime.now(_TPE).strftime("%Y-%m-%d %H:%M:%S")
    msg = (
        f"目前時間：{now} (台北)\n"
        f"主機：{platform.node()}\n"
        f"Python：{platform.python_version()}"
    )
    await update.message.reply_text(msg)


def register(application) -> None:
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("ping", cmd_ping))
    application.add_handler(CommandHandler("status", cmd_status))
