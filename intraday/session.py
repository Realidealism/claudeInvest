"""Trading-session time helpers (TPE timezone).

Restored from the compiled bytecode ``intraday/__pycache__/session.cpython-312.pyc``
(the source file was lost from disk while remaining untracked in git). Behaviour
validated byte-for-byte against that bytecode across a full-week datetime grid.
"""
from __future__ import annotations

from datetime import datetime, time as dtime, timedelta, timezone

TPE_TZ = timezone(timedelta(hours=8))

SESSION_OPEN = dtime(9, 0)
SESSION_CLOSE = dtime(13, 30)
SWEEPER_CUTOFF = dtime(13, 50)
POST_CLOSE_WINDOW_END = dtime(14, 0)


def now_tpe() -> datetime:
    return datetime.now(TPE_TZ)


def _is_weekday(now: datetime) -> bool:
    return now.weekday() < 5


def in_session(now: datetime) -> bool:
    """Sweeper-side: 09:00 ≤ now ≤ 13:50 on a weekday.

    The 20-minute tail past the close keeps the sweeper running so it can
    capture the closing-auction 13:30 bucket which sometimes arrives late.
    """
    if not _is_weekday(now):
        return False
    t = now.time()
    return SESSION_OPEN <= t <= SWEEPER_CUTOFF


def in_snapshot_session(now: datetime) -> bool:
    """Snapshot-side: 09:00 ≤ now < 13:30 on a weekday.

    The snapshot loop stops issuing mid-session passes after 13:30; the
    daemon then enters the post-close window and waits for the sweep's
    13:30 bucket before running one final close-aligned pass.
    """
    if not _is_weekday(now):
        return False
    t = now.time()
    return SESSION_OPEN <= t < SESSION_CLOSE


def in_post_close_window(now: datetime) -> bool:
    """13:30 ≤ now < 14:00 on a weekday."""
    if not _is_weekday(now):
        return False
    t = now.time()
    return SESSION_CLOSE <= t < POST_CLOSE_WINDOW_END


def seconds_until_next_open(now: datetime) -> float:
    """Seconds from ``now`` to the next trading day's 09:00 TPE.

    On a weekday before 09:00, that's today; otherwise it's the next
    Mon–Fri at 09:00.
    """
    today_open = now.replace(hour=SESSION_OPEN.hour, minute=SESSION_OPEN.minute,
                             second=0, microsecond=0)
    if now < today_open and _is_weekday(now):
        return (today_open - now).total_seconds()
    d = now.date() + timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    next_open = datetime.combine(d, SESSION_OPEN, tzinfo=TPE_TZ)
    return (next_open - now).total_seconds()
