"""Minimal FinMind HTTP client with rate-limit + retry, shared by scrapers.

The parent project's scrapers/financials.py uses the same FinMind v4 endpoint
with a 2 s/req rate limit ("free tier ~600 req/hour"). Empirical probes show
the free tier actually tolerates ~3.4 req/s without issue, but we stay
conservative at 0.4 s/req to leave headroom for parallel backfills.
"""

from __future__ import annotations

import time
from typing import Any

import requests  # type: ignore[import-untyped]

FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
DEFAULT_INTERVAL = 0.4
# When 402 hits, we sleep RATE_LIMIT_SLEEP and retry once. If still 402, we
# raise — the caller will mark this ticker as failed and move on. The hourly
# cap should expire by the next ticker, so we don't keep burning wall-clock.
RATE_LIMIT_SLEEP = 300.0  # 5 minutes
MAX_RETRIES = 2
BACKOFF_BASE = 30.0  # for non-rate-limit errors


class FinMindError(RuntimeError):
    pass


class FinMindClient:
    def __init__(self, *, interval: float = DEFAULT_INTERVAL, token: str | None = None) -> None:
        self.interval = interval
        self.token = token
        self._last_req = 0.0
        # Shared cooldown: when one request gets 402, subsequent requests
        # block until this timestamp before hitting the wire. This prevents
        # the per-ticker 5-min wait penalty when the hourly cap is exhausted.
        self._cooldown_until = 0.0

    def _throttle(self) -> None:
        now = time.time()
        if now < self._cooldown_until:
            time.sleep(self._cooldown_until - now)
            now = time.time()
        elapsed = now - self._last_req
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        self._last_req = time.time()

    def _set_cooldown(self, seconds: float) -> None:
        self._cooldown_until = time.time() + seconds

    def fetch(
        self,
        dataset: str,
        *,
        data_id: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, str] = {"dataset": dataset}
        if data_id is not None:
            params["data_id"] = data_id
        if start_date is not None:
            params["start_date"] = start_date
        if end_date is not None:
            params["end_date"] = end_date
        if self.token:
            params["token"] = self.token

        for attempt in range(MAX_RETRIES):
            self._throttle()
            try:
                r = requests.get(FINMIND_URL, params=params, timeout=30)
            except requests.exceptions.RequestException as e:
                if attempt == MAX_RETRIES - 1:
                    raise FinMindError(f"network: {e}") from e
                time.sleep(BACKOFF_BASE)
                continue
            if r.status_code == 402:
                # Hourly cap. Set a shared cooldown so subsequent requests
                # don't each pay the wait individually.
                if attempt == 0:
                    self._set_cooldown(RATE_LIMIT_SLEEP)
                    # _throttle() at top of next iter will block on cooldown
                    continue
                raise FinMindError("rate-limited (402) after sleep")
            if r.status_code != 200:
                raise FinMindError(f"http {r.status_code}: {r.text[:200]}")
            payload = r.json()
            if payload.get("msg") != "success":
                msg = payload.get("msg", "")
                if "free" in msg.lower() or "level" in msg.lower():
                    raise FinMindError(f"tier error: {msg}")
                # "no data" is success-with-empty for some endpoints
                return []
            return list(payload.get("data") or [])
        raise FinMindError("max retries exhausted")
