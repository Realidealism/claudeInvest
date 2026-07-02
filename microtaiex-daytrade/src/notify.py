"""Telegram notifications + on-demand status polling for paper/live.

Reads FUT_DAYTRADE_TG_BOT_TOKEN / FUT_DAYTRADE_TG_CHAT_ID from the environment;
disabled (no-op) if either is missing. Send failures are swallowed so a flaky
network never disrupts the trading loop. Messages are Traditional Chinese.

send() is fire-and-forget. poll_commands() long-polls getUpdates so a persistent
reply-keyboard button (STATUS_KEYBOARD) lets the user request a status snapshot
on demand — call it from the live periodic() hook (same thread as send()).
"""
from __future__ import annotations

import json
import os
import urllib.request

_API = "https://api.telegram.org/bot{token}/{method}"

# Persistent reply keyboard: tapping the button sends its text as a message,
# which poll_commands() picks up. is_persistent keeps it docked in the client.
STATUS_KEYBOARD = {
    "keyboard": [[{"text": "🔍 狀態"}]],
    "resize_keyboard": True,
    "is_persistent": True,
}


class TelegramNotifier:
    def __init__(self, token: str | None = None, chat_id: str | None = None) -> None:
        self.token = token or os.environ.get("FUT_DAYTRADE_TG_BOT_TOKEN", "")
        self.chat_id = chat_id or os.environ.get("FUT_DAYTRADE_TG_CHAT_ID", "")
        self.enabled = bool(self.token and self.chat_id)
        self._offset: int | None = None   # getUpdates cursor (None = not drained yet)

    def _post(self, method: str, payload: dict, timeout: float = 5.0):
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            _API.format(token=self.token, method=method), data=data,
            headers={"Content-Type": "application/json"})
        return json.loads(urllib.request.urlopen(req, timeout=timeout).read())

    def send(self, text: str, reply_markup: dict | None = None) -> None:
        if not self.enabled:
            return
        try:
            payload = {"chat_id": self.chat_id, "text": text}
            if reply_markup is not None:
                payload["reply_markup"] = reply_markup
            self._post("sendMessage", payload)
        except Exception as e:  # never break the trading loop on a notify failure
            print(f"[tg] send failed: {e}")

    def poll_commands(self) -> list[str]:
        """Return message texts received since the last poll. The first call
        drains any backlog and returns [] (so a stale tap before restart is not
        replayed). Errors are swallowed and yield []."""
        if not self.enabled:
            return []
        try:
            payload: dict = {"timeout": 0}
            if self._offset is not None:
                payload["offset"] = self._offset
            resp = self._post("getUpdates", payload)
            first = self._offset is None
            cmds: list[str] = []
            for u in resp.get("result", []):
                self._offset = u["update_id"] + 1
                msg = u.get("message") or u.get("edited_message") or {}
                text = (msg.get("text") or "").strip()
                if text:
                    cmds.append(text)
            return [] if first else cmds
        except Exception as e:
            print(f"[tg] poll failed: {e}")
            return []
