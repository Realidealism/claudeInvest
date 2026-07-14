"""ASCII-only entry point for intraday_cron.bat to push Chinese alerts.

CMD on Windows mangles non-ASCII chars inside .bat compound blocks
(`if (...)`) regardless of chcp, so the bat passes an event key + numeric
extras and this module composes the Chinese message in Python.

CLI:
    python -m telegram_bot.cron_alert snapshot_failed 255
    python -m telegram_bot.cron_alert git_push_failed
"""

from __future__ import annotations

import sys

from telegram_bot.notify import send_sync

_TAG = "排程"


def _msg_snapshot_failed(extra: str) -> str:
    exit_code = extra or "?"
    return f"盤中快照失敗（exit {exit_code}）"


def _msg_git_push_failed(_extra: str) -> str:
    return "盤中快照後 git push 失敗"


def _msg_backfill_financials_failed(extra: str) -> str:
    exit_code = extra or "?"
    return f"財報 backfill 失敗（exit {exit_code}）"


def _msg_backfill_hermit_failed(extra: str) -> str:
    exit_code = extra or "?"
    return f"hermit 快照 backfill 失敗（exit {exit_code}）"


def _msg_microtaiex_report_failed(extra: str) -> str:
    session = {"day": "日盤", "night": "夜盤"}.get(extra, extra or "?")
    return f"微台當沖{session}結算推播失敗"


_EVENTS = {
    "snapshot_failed": _msg_snapshot_failed,
    "git_push_failed": _msg_git_push_failed,
    "backfill_financials_failed": _msg_backfill_financials_failed,
    "backfill_hermit_failed": _msg_backfill_hermit_failed,
    "microtaiex_report_failed": _msg_microtaiex_report_failed,
}


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if not args:
        print("usage: python -m telegram_bot.cron_alert <event> [extra]", file=sys.stderr)
        print("events:", ", ".join(_EVENTS), file=sys.stderr)
        return 2

    event = args[0]
    extra = args[1] if len(args) > 1 else ""
    composer = _EVENTS.get(event)
    if composer is None:
        print(f"unknown event '{event}'", file=sys.stderr)
        return 2

    body = composer(extra)
    text = f"[{_TAG}] {body}"
    ok = send_sync(text)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
