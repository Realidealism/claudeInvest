"""Whitelist guard for command handlers.

Decorate any handler with @restricted to silently ignore messages from
chat_ids not in TELEGRAM_ALLOWED_CHAT_IDS.
"""

from __future__ import annotations

import logging
from functools import wraps
from typing import Awaitable, Callable

from telegram import Update
from telegram.ext import ContextTypes

from telegram_bot.config import require_chat_ids

logger = logging.getLogger(__name__)

Handler = Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[None]]


def restricted(handler: Handler) -> Handler:
    @wraps(handler)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        user = update.effective_user
        if chat is None or chat.id not in require_chat_ids():
            logger.warning(
                "rejected message from chat_id=%s user=%s",
                chat.id if chat else None,
                user.username if user else None,
            )
            return
        await handler(update, context)

    return wrapper
