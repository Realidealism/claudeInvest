"""Telegram remote notification + control for the Invest project.

Public entry points:
    from telegram_bot.notify import send_sync, send_async

    # CLI: outbound one-shot (used by intraday_cron.bat etc.)
    python -m telegram_bot.notify "message text"

    # CLI: long-polling daemon (registered as nssm Windows service)
    python -m telegram_bot
"""
