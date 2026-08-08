"""Data-freshness / heartbeat monitor for the invest pipeline.

Catches the *silent* failures the existing per-scraper Telegram alerts miss:

  daily    — verify the most recent trading day's daily pipeline is fully
             consistent across the snapshot tables. Healthy -> a silent OK
             ping (dead-man's switch); stale -> an audible alert naming the
             lagging tables. Run pre-open (~08:30 weekday): by then the
             previous evening's daily_update must have finished, so this is
             insensitive to whatever time daily_update actually runs.

  intraday — liveness probe for the intraday daemons. NSSM restarts them
             on a crash but not on a hang; this alerts when
             tw.intraday_quotes (sweeper) or tw.score_snapshot_intraday
             (snapshot daemon) stops advancing mid-session. Run every
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
from intraday.session import in_session, in_snapshot_session, now_tpe
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

# Column groups of tw.daily_prices that each come from one scraper leg, checked
# per market. MAX(trade_date) advancing says nothing about these: when one leg
# dies the rows are still written by the other legs, so the day looks complete
# while a whole column is NULL for one market (TWSE institutional 2026-07-02,
# SBL 2026-06-10, price limits 2026-06-30, after-hours 2026-05-12 all failed
# this way and went unnoticed for weeks).
_COLUMN_GROUPS = [
    ("法人", "foreign_net"),
    ("融資券", "margin_balance"),
    ("借券", "sbl_balance"),
    ("當沖", "dt_volume"),
    ("零股", "ol_price"),
    ("盤後", "ah_price"),
    ("漲跌停", "limit_up"),
    ("參考價", "ref_price"),
]

# Fraction of the trailing-median coverage below which a group counts as dead.
# Calibrated over 2024-2026 (628 trading days, 28 firings): 25 are unambiguous
# leg deaths -- 0 or 1 row against a median in the hundreds, with nothing in
# between -- so the exact ratio barely matters for those. The remaining 3 are
# real market behaviour: after-hours participation collapses on a limit-down
# day (2025-04-07) and drifts near the line when the session thins out. That
# is roughly one false alarm a year, accepted in exchange for catching a dead
# leg the day it happens.
_COLUMN_MIN_RATIO = 0.7
_COLUMN_BASELINE_DAYS = 20

# Intraday: alert when tw.intraday_quotes has not advanced for this long.
_INTRADAY_STALE_MINUTES = 5

# Snapshot daemon runs back-to-back passes (roughly one per minute); allow a
# generous margin for slow passes before calling it hung.
_SNAPSHOT_STALE_MINUTES = 15

_LOGS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "logs",
)

# Sentinel files remembering "already alerted today" so the every-10-min
# intraday probe fires at most one alert per trading day per component.
_SENTINEL = os.path.join(_LOGS_DIR, ".health_intraday_alerted")
_SENTINEL_SNAPSHOT = os.path.join(_LOGS_DIR, ".health_intraday_snapshot_alerted")


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


def _dead_column_groups(latest: date) -> list[str]:
    """Report column groups whose coverage collapsed on the latest trading day.

    Compares each (market, group) against its own median over the preceding
    _COLUMN_BASELINE_DAYS trading days, so a group that is legitimately sparse
    (after-hours) is judged against its own level rather than a global one.
    Groups whose baseline is 0 are skipped: they predate the data (TWSE
    intraday odd-lot starts 2020-10-26) or genuinely carry nothing.
    """
    counts = ", ".join(
        f"count(p.{col}) AS c_{col}" for _, col in _COLUMN_GROUPS
    )
    with get_cursor(commit=False) as cur:
        cur.execute(
            f"""
            SELECT p.trade_date AS d, s.market AS m, {counts}
            FROM tw.daily_prices p JOIN tw.stocks s USING (stock_id)
            WHERE s.market IN ('TWSE', 'TPEx')
              AND p.trade_date > %s - INTERVAL '60 days'
              AND p.trade_date <= %s
            GROUP BY 1, 2
            ORDER BY 1
            """,
            (latest, latest),
        )
        rows = cur.fetchall()

    dead = []
    for market in ("TWSE", "TPEx"):
        series = [r for r in rows if r["m"] == market]
        today = [r for r in series if r["d"] == latest]
        if not today:
            dead.append(f"{market} 日線當日無資料")
            continue
        baseline = [r for r in series if r["d"] < latest][-_COLUMN_BASELINE_DAYS:]
        if not baseline:
            continue
        for label, col in _COLUMN_GROUPS:
            prior = sorted(r[f"c_{col}"] for r in baseline)
            median = prior[len(prior) // 2]
            actual = today[0][f"c_{col}"]
            if median > 0 and actual < median * _COLUMN_MIN_RATIO:
                dead.append(
                    f"{market} {label} 僅 {actual} 檔有值（近{len(prior)}日中位 {median}）"
                )
    return dead


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

    stale.extend(_dead_column_groups(latest))

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
    rc = _check_sweeper(now, today)
    if in_snapshot_session(now):
        # The snapshot daemon stops mid-session passes at 13:30 and idles
        # until the post-close final pass, so only probe it before 13:30.
        rc = max(rc, _check_snapshot(now, today))
    return rc


def _check_sweeper(now, today: date) -> int:
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


def _check_snapshot(now, today: date) -> int:
    snapped = _scalar(
        "SELECT MAX(snapshot_date) AS d FROM tw.score_snapshot_intraday"
    )
    if snapped != today:
        # No rows for today yet: holiday, or the daemon is still in its
        # h(t)-warm-up window and has not written a first pass. Both are
        # startup conditions, not a mid-session hang, so stay silent here.
        return 0

    last_pass = _scalar(
        "SELECT MAX(snapshot_time) AS t FROM tw.score_snapshot_intraday"
    )
    if last_pass is None:
        return 0

    age_min = (now - last_pass).total_seconds() / 60.0
    if age_min > _SNAPSHOT_STALE_MINUTES:
        if not _already_alerted(today, _SENTINEL_SNAPSHOT):
            send_sync(
                f"[{_TAG}] 盤中 snapshot daemon 停擺：score_snapshot_intraday "
                f"已 {int(age_min)} 分鐘未更新"
            )
            _mark_alerted(today, _SENTINEL_SNAPSHOT)
        return 1

    _clear_alerted(_SENTINEL_SNAPSHOT)
    return 0


def run_self_test() -> int:
    ok = send_sync(f"[{_TAG}] 自我測試：投遞鏈路正常")
    return 0 if ok else 1


def _already_alerted(today: date, sentinel: str = _SENTINEL) -> bool:
    try:
        with open(sentinel, encoding="ascii") as f:
            return f.read().strip() == today.isoformat()
    except OSError:
        return False


def _mark_alerted(today: date, sentinel: str = _SENTINEL) -> None:
    try:
        os.makedirs(os.path.dirname(sentinel), exist_ok=True)
        with open(sentinel, "w", encoding="ascii") as f:
            f.write(today.isoformat())
    except OSError:
        pass


def _clear_alerted(sentinel: str = _SENTINEL) -> None:
    try:
        os.remove(sentinel)
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
