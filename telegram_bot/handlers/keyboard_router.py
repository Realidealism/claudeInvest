"""Map persistent ReplyKeyboard button texts to existing command handlers.

The three buttons defined in `handlers/status.MAIN_KEYBOARD` send plain text
(not slash commands), so we register one Regex MessageHandler per button
that delegates to the corresponding cmd_* coroutine.

Registration order matters: register AFTER the slash CommandHandlers so
those still match `/market` / `/watch` / `/signals` typed manually.
"""

from __future__ import annotations

import re

from telegram.ext import MessageHandler, filters

from telegram_bot.handlers.market import cmd_market
from telegram_bot.handlers.signals import cmd_signals
from telegram_bot.handlers.status import (
    BTN_MARKET,
    BTN_SIGNALS,
    BTN_WATCH,
    BTN_WATCH_ADD,
    BTN_WATCH_REMOVE,
)
from telegram_bot.handlers.watch import (
    cmd_watch_add_button,
    cmd_watch_remove_button,
    cmd_watch_score,
)


def register(application) -> None:
    application.add_handler(
        MessageHandler(filters.Regex(rf"^{re.escape(BTN_MARKET)}$"), cmd_market)
    )
    application.add_handler(
        MessageHandler(filters.Regex(rf"^{re.escape(BTN_WATCH)}$"), cmd_watch_score)
    )
    application.add_handler(
        MessageHandler(filters.Regex(rf"^{re.escape(BTN_SIGNALS)}$"), cmd_signals)
    )
    application.add_handler(
        MessageHandler(
            filters.Regex(rf"^{re.escape(BTN_WATCH_ADD)}$"), cmd_watch_add_button
        )
    )
    application.add_handler(
        MessageHandler(
            filters.Regex(rf"^{re.escape(BTN_WATCH_REMOVE)}$"), cmd_watch_remove_button
        )
    )
