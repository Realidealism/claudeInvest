"""Fire-and-forget Telegram notifications for paper/live trade events.

Reads FUT_DAYTRADE_TG_BOT_TOKEN / FUT_DAYTRADE_TG_CHAT_ID from the environment;
disabled (no-op) if either is missing. Send failures are swallowed so a flaky
network never disrupts the trading loop. Messages are Traditional Chinese.
"""
from __future__ import annotations

import json
import os
import urllib.request

_API = "https://api.telegram.org/bot{token}/sendMessage"


class TelegramNotifier:
    def __init__(self, token: str | None = None, chat_id: str | None = None) -> None:
        self.token = token or os.environ.get("FUT_DAYTRADE_TG_BOT_TOKEN", "")
        self.chat_id = chat_id or os.environ.get("FUT_DAYTRADE_TG_CHAT_ID", "")
        self.enabled = bool(self.token and self.chat_id)

    def send(self, text: str) -> None:
        if not self.enabled:
            return
        try:
            data = json.dumps({"chat_id": self.chat_id, "text": text}).encode("utf-8")
            req = urllib.request.Request(
                _API.format(token=self.token), data=data,
                headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=5)
        except Exception as e:  # never break the trading loop on a notify failure
            print(f"[tg] send failed: {e}")
