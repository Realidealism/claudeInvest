"""Outbound notification API.

`send_sync` is fire-and-forget over the Bot API HTTPS endpoint and does NOT
require the long-polling daemon to be running. Any script (bat file, scraper,
cron job, hermit batch) can push a message by importing and calling it.

CLI usage (used by intraday_cron.bat etc.):
    python -m telegram_bot.notify "message text"
    echo error | python -m telegram_bot.notify --stdin
    python -m telegram_bot.notify --tag SCRAPE "ETF holdings failed"
"""

from __future__ import annotations

import argparse
import logging
import sys

import requests

from telegram_bot.config import (
    TelegramConfigError,
    primary_chat_id,
    require_token,
)

logger = logging.getLogger(__name__)

_API_BASE = "https://api.telegram.org"
_TIMEOUT_S = 10


def send_sync(
    text: str,
    *,
    chat_id: int | None = None,
    parse_mode: str | None = None,
    silent: bool = False,
) -> bool:
    """Send a message to Telegram. Returns True on success, False on any failure.

    Designed to never raise — callers (scrapers, cron) should not crash because
    notification failed. Set silent=True to skip the recipient's push sound.
    """
    try:
        token = require_token()
        target = chat_id if chat_id is not None else primary_chat_id()
    except TelegramConfigError as exc:
        logger.warning("telegram notify skipped: %s", exc)
        return False

    payload: dict[str, object] = {
        "chat_id": target,
        "text": text,
        "disable_notification": silent,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode

    try:
        r = requests.post(
            f"{_API_BASE}/bot{token}/sendMessage",
            json=payload,
            timeout=_TIMEOUT_S,
        )
        if r.status_code != 200:
            logger.warning(
                "telegram sendMessage non-200: %s %s", r.status_code, r.text[:200]
            )
            return False
        return True
    except requests.RequestException as exc:
        logger.warning("telegram sendMessage failed: %s", exc)
        return False


async def send_async(text: str, *, chat_id: int | None = None) -> bool:
    """Async variant for use inside the PTB Application event loop.

    Falls back to requests for simplicity — PTB's Bot has its own send method
    if we ever need richer features (attachments, inline keyboards), but for
    plain text this is enough.
    """
    return send_sync(text, chat_id=chat_id)


def _cli() -> int:
    p = argparse.ArgumentParser(
        prog="python -m telegram_bot.notify",
        description="Send a one-shot Telegram message via the configured bot.",
    )
    p.add_argument("text", nargs="?", help="Message text (omit when using --stdin).")
    p.add_argument(
        "--stdin",
        action="store_true",
        help="Read message body from stdin instead of positional arg.",
    )
    p.add_argument(
        "--tag",
        default=None,
        help="Optional [TAG] prefix prepended to the message.",
    )
    p.add_argument(
        "--silent",
        action="store_true",
        help="Send without the recipient's push notification sound.",
    )
    args = p.parse_args()

    if args.stdin:
        body = sys.stdin.read().rstrip()
    elif args.text is not None:
        body = args.text
    else:
        p.error("provide a positional message or --stdin")
        return 2

    if not body:
        print("telegram_bot.notify: empty message, nothing to send", file=sys.stderr)
        return 1

    msg = f"[{args.tag}] {body}" if args.tag else body
    ok = send_sync(msg, silent=args.silent)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(_cli())
