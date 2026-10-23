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


def _msg_snapshot_exceptions(extra: str) -> str:
    # Called in-process by the intraday daemon with the summary line of
    # analysis.snapshot_errors (it may be Chinese; no .bat in between).
    return f"盤中快照逐檔例外，這一輪沒有寫入：{extra or '?'}"


def _msg_backfill_financials_failed(extra: str) -> str:
    exit_code = extra or "?"
    return f"財報 backfill 失敗（exit {exit_code}）"


def _msg_financials_sweep_failed(extra: str) -> str:
    exit_code = extra or "?"
    return f"季報更新失敗（exit {exit_code}）"


def _msg_backfill_hermit_failed(extra: str) -> str:
    exit_code = extra or "?"
    return f"hermit 快照 backfill 失敗（exit {exit_code}）"


def _msg_microtaiex_report_failed(extra: str) -> str:
    session = {"day": "日盤", "night": "夜盤"}.get(extra, extra or "?")
    return f"微台當沖{session}結算推播失敗"


def _msg_market_closed_detected(extra: str) -> str:
    # extra = "<quotes>|<latest trade>" from intraday.sweeper.
    count, _, latest = (extra or "?|?").partition("|")
    return (f"今日判定休市：開盤後 {count} 檔報價沒有任何今天的成交"
            f"（最新一筆 {latest or '?'}），盤中寫入、快照與告警暫停")


def _msg_market_closure_undetermined(extra: str) -> str:
    # extra = "<HH:MM>|<quotes>|<timestamped>" from intraday.sweeper.
    at, quotes, stamped = ((extra or "").split("|") + ["?", "?", "?"])[:3]
    return (f"{at} 仍無法判定今天是否開盤（{quotes} 檔報價，{stamped} 檔有成交時間），"
            f"盤中照常寫入；請確認玉山報價的成交時間欄位")


def _msg_market_closed_revoked(extra: str) -> str:
    return f"撤銷休市判定：{extra or '?'} 出現今天的成交，盤中恢復寫入"


def _msg_live_reconcile_failed(extra: str) -> str:
    exit_code = extra or "?"
    return f"月對帳失敗（exit {exit_code}），見 logs\\live_reconcile.log"


_EVENTS = {
    "snapshot_failed": _msg_snapshot_failed,
    "git_push_failed": _msg_git_push_failed,
    "snapshot_exceptions": _msg_snapshot_exceptions,
    "backfill_financials_failed": _msg_backfill_financials_failed,
    "backfill_hermit_failed": _msg_backfill_hermit_failed,
    "microtaiex_report_failed": _msg_microtaiex_report_failed,
    "financials_sweep_failed": _msg_financials_sweep_failed,
    "live_reconcile_failed": _msg_live_reconcile_failed,
    "market_closed_detected": _msg_market_closed_detected,
    "market_closed_revoked": _msg_market_closed_revoked,
    "market_closure_undetermined": _msg_market_closure_undetermined,
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
