"""PTB Application bootstrap — long-polling daemon entry."""

from __future__ import annotations

import logging
import logging.handlers
import os
from pathlib import Path

from telegram import BotCommand
from telegram.ext import Application, ApplicationBuilder

from telegram_bot.config import require_token
from telegram_bot.handlers import chip as chip_handlers
from telegram_bot.handlers import help as help_handlers
from telegram_bot.handlers import keyboard_router as keyboard_handlers
from telegram_bot.handlers import market as market_handlers
from telegram_bot.handlers import score as score_handlers
from telegram_bot.handlers import signals as signals_handlers
from telegram_bot.handlers import social as social_handlers
from telegram_bot.handlers import status as status_handlers
from telegram_bot.handlers import watch as watch_handlers
from telegram_bot.watchers import afterclose_brief as afterclose_brief_watcher
from telegram_bot.watchers import defense_proximity_alert as defense_proximity_watcher
from telegram_bot.watchers import intraday_volume_alert as volume_alert_watcher
from telegram_bot.watchers import morning_brief as morning_brief_watcher
from telegram_bot.watchers import social_monitor as social_monitor_watcher
from telegram_bot.watchers import stance_alert as stance_alert_watcher

logger = logging.getLogger(__name__)

_LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
_LOG_FILE = _LOG_DIR / "telegram_bot.log"


def _configure_logging() -> None:
    _LOG_DIR.mkdir(exist_ok=True)
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_h = logging.handlers.RotatingFileHandler(
        _LOG_FILE, maxBytes=2_000_000, backupCount=3, encoding="utf-8"
    )
    file_h.setFormatter(fmt)
    stream_h = logging.StreamHandler()
    stream_h.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Avoid duplicate handlers on hot-reload
    root.handlers = [file_h, stream_h]

    # PTB / httpx are chatty at INFO
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram.ext").setLevel(logging.INFO)


async def _post_init(app: Application) -> None:
    """Register the bot command menu (the blue "Menu" button + `/` list)."""
    await app.bot.set_my_commands(
        [
            BotCommand("start", "啟動／顯示按鈕鍵盤"),
            BotCommand("market", "市場概況"),
            BotCommand("watch", "追蹤清單"),
            BotCommand("signals", "進出場建議"),
            BotCommand("chip", "集保大戶選股週報"),
            BotCommand("help", "完整指令說明"),
            BotCommand("status", "服務狀態"),
        ]
    )


def build_application() -> Application:
    token = require_token()
    app = ApplicationBuilder().token(token).post_init(_post_init).build()
    status_handlers.register(app)
    help_handlers.register(app)
    score_handlers.register(app)
    watch_handlers.register(app)
    market_handlers.register(app)
    signals_handlers.register(app)
    social_handlers.register(app)
    chip_handlers.register(app)
    keyboard_handlers.register(app)  # MUST register after slash CommandHandlers
    volume_alert_watcher.register(app)
    defense_proximity_watcher.register(app)
    morning_brief_watcher.register(app)
    afterclose_brief_watcher.register(app)
    stance_alert_watcher.register(app)
    # social_monitor_watcher.register(app)  # disabled — re-enable by uncommenting
    return app


def run() -> None:
    _configure_logging()
    logger.info("starting telegram_bot pid=%s cwd=%s", os.getpid(), os.getcwd())
    app = build_application()
    # run_polling blocks until SIGINT / SIGTERM — nssm will deliver these
    # when stopping the service.
    app.run_polling(drop_pending_updates=True)
