"""Telegram bot configuration — reads from config.settings (which loads .env)."""

from config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_ALLOWED_CHAT_IDS


class TelegramConfigError(RuntimeError):
    """Raised when bot is invoked without required env vars."""


def require_token() -> str:
    if not TELEGRAM_BOT_TOKEN:
        raise TelegramConfigError(
            "TELEGRAM_BOT_TOKEN is not set in .env. "
            "Create a bot via @BotFather and copy the token."
        )
    return TELEGRAM_BOT_TOKEN


def require_chat_ids() -> tuple[int, ...]:
    if not TELEGRAM_ALLOWED_CHAT_IDS:
        raise TelegramConfigError(
            "TELEGRAM_ALLOWED_CHAT_IDS is not set in .env. "
            "Send any message to your bot, then visit "
            "https://api.telegram.org/bot<TOKEN>/getUpdates to find your chat_id."
        )
    return TELEGRAM_ALLOWED_CHAT_IDS


def primary_chat_id() -> int:
    """The first chat_id in the whitelist — used as the default push target."""
    return require_chat_ids()[0]
