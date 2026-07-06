"""Data-freshness / heartbeat monitor for the invest pipeline.

Catches the *silent* failures the existing per-scraper Telegram alerts miss:

  daily    — verify the most recent trading day's daily pipeline is fully
             consistent across the snapshot tables. Healthy -> a silent OK
             ping (dead-man's switch); stale -> an audible alert naming the
             lagging tables. Run pre-open (~08:30 weekday): by then the
             previous evening's daily_update must have finished, so this is
             insensitive to whatever time daily_update actually runs.

  intraday — liveness probe for the intraday sweeper daemon. NSSM restarts
             it on a crash but not on a hang; this alerts when
             tw.intraday_quotes stops advancing mid-session. Run every
             ~10 min during the session (self-gates on in_session()).

Messages are composed in Python (no CMD CP950 mangling) and sent via
notify.send_sync, which is fire-and-forget and does not need the bot daemon.

CLI:
    python -m telegram_bot.health_check daily
    python -m telegram_bot.health_check intraday
    python -m telegram_bot.health_check --self-test
"""

from __future__ import annotations

import os
import sys
from datetime import date

from db.connection import get_cursor
from intraday.session import in_session, now_tpe
from telegram_bot.notify import send_sync

_TAG = "健康檢查"

# (label, table, date column) checked by the daily probe. Every table here
# must have advanced to the latest trading day (per tw.index_prices/TAIEX)
# once the previous evening's daily_update pipeline has finished.
_DAILY_TABLES = [
    ("日線收盤", "tw.daily_prices", "trade_date"),
    ("市場廣度", "tw.market_breadth", "trade_date"),
    ("評分快照", "tw.score_snapshot", "snapshot_date"),
    ("訊號快照", "tw.signal_snapshot", "snapshot_date"),
]

# Intraday: alert when tw.intraday_quotes has not advanced for this long.
_INTRADAY_STALE_MINUTES = 5

# Sentinel file remembering "already alerted today" so the every-10-min
# intraday probe fires at most one alert per trading day.
_SENTINEL = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "logs",
    ".health_intraday_alerted",
)


def _scalar(sql: str, params: tuple = ()):
    """Run a read-only single-column query and return its value (or None)."""
    with get_cursor(commit=False) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        if not row:
            return None
        return next(iter(row.values()))


def _latest_trading_day():
    return _scalar(
        "SELECT MAX(trade_date) AS d FROM tw.index_prices WHERE index_id = 'TAIEX'"
    )


def run_daily() -> int:
    latest = _latest_trading_day()
    if latest is None:
        send_sync(f"[{_TAG}] 找不到任何交易日資料（index_prices 為空）")
        return 1

    stale = []
    for label, table, col in _DAILY_TABLES:
        d = _scalar(f"SELECT MAX({col}) AS d FROM {table}")
        if d is None or d < latest:
            seen = d.strftime("%m-%d") if d else "無"
            stale.append(f"{label} 停在 {seen}")

    want = latest.strftime("%m-%d")
    if stale:
        body = "盤後 pipeline 異常（應更新至 {}）：\n- {}".format(
            want, "\n- ".join(stale)
        )
        send_sync(f"[{_TAG}] {body}")
        return 1

    send_sync(f"[{_TAG}] ✓ 系統正常（資料更新至 {want}）", silent=True)
    return 0


def run_intraday() -> int:
    now = now_tpe()
    if not in_session(now):
        return 0

    today = now.date()
    traded = _scalar("SELECT MAX(trade_date) AS d FROM tw.intraday_quotes")
    if traded != today:
        # No rows for today yet: either a holiday or the sweeper has not
        # produced its first tick. The first-tick case is a startup problem,
        # not a mid-session hang, so stay silent here.
        return 0

    last_upd = _scalar("SELECT MAX(updated_at) AS t FROM tw.intraday_quotes")
    if last_upd is None:
        return 0

    age_min = (now - last_upd).total_seconds() / 60.0
    if age_min > _INTRADAY_STALE_MINUTES:
        if not _already_alerted(today):
            send_sync(
                f"[{_TAG}] 盤中 sweeper 停擺：intraday_quotes 已 {int(age_min)} 分鐘未更新"
            )
            _mark_alerted(today)
        return 1

    # Healthy again -> re-arm the once-per-day alert.
    _clear_alerted()
    return 0


def run_self_test() -> int:
    ok = send_sync(f"[{_TAG}] 自我測試：投遞鏈路正常")
    return 0 if ok else 1


def _already_alerted(today: date) -> bool:
    try:
        with open(_SENTINEL, encoding="ascii") as f:
            return f.read().strip() == today.isoformat()
    except OSError:
        return False


def _mark_alerted(today: date) -> None:
    try:
        os.makedirs(os.path.dirname(_SENTINEL), exist_ok=True)
        with open(_SENTINEL, "w", encoding="ascii") as f:
            f.write(today.isoformat())
    except OSError:
        pass


def _clear_alerted() -> None:
    try:
        os.remove(_SENTINEL)
    except OSError:
        pass


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    mode = args[0] if args else ""
    if mode == "daily":
        return run_daily()
    if mode == "intraday":
        return run_intraday()
    if mode == "--self-test":
        return run_self_test()
    print(
        "usage: python -m telegram_bot.health_check {daily|intraday|--self-test}",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
