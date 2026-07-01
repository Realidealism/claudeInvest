"""/chip — 集保大戶選股週報 (chip_model weekly picks, on demand)."""

from __future__ import annotations

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes

from telegram_bot.auth import restricted
from telegram_bot.chip_report import build_chip_report


@restricted
async def cmd_chip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    report = build_chip_report(full=True)
    await update.message.reply_text(report or "尚無集保選股資料。")


def register(application) -> None:
    application.add_handler(CommandHandler("chip", cmd_chip))
