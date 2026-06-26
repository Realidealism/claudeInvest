"""Publish watchlist hits from the latest intraday signal snapshot.

Designed to run on its own Task Scheduler cadence (every ~15 min during
the trading session) — independent of the back-to-back snapshot daemon
that keeps tw.signal_snapshot_intraday fresh.

Workflow per invocation:
  1. Look up the most recent (snapshot_date, snapshot_time) tuple in
     tw.signal_snapshot_intraday.
  2. Intersect with the TW watchlist.
  3. UPSERT each (trade_date, stock_id, signal_type) into
     tw.intraday_push_state so first_seen_at is pinned the first time we
     observe each fire. Rows that already exist are left alone.
  4. Select rows for snapshot_date with pushed_at IS NULL — these are the
     watchlist fires we haven't yet messaged today.
  5. If non-empty, send one consolidated Telegram message and stamp
     pushed_at = NOW() on every row we just sent.

Result: each (stock_id, signal_type) pair is delivered exactly once per
trading day, regardless of how often the snapshot daemon refires it.

CLI:
    python -m telegram_bot.push_intraday_signals
    python -m telegram_bot.push_intraday_signals --always   # send even when no pending hits
    python -m telegram_bot.push_intraday_signals --dry-run  # no send, no pushed_at stamp
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone

from db.connection import get_cursor
from intraday.watchlist import load_tw_watchlist
from telegram_bot.notify import send_sync

_TPE_TZ = timezone(timedelta(hours=8))

# Display order matches the user's 6-signal mental model
# (entry signals first, then exit signals).
_SIGNAL_ORDER = ("pick", "touch", "buy", "sell", "buy_flee", "sell_flee")


_SIGNAL_LABEL = {
    "pick": "撿便宜",
    "touch": "摸頭",
    "buy": "做多",
    "sell": "做空",
    "buy_flee": "多單逃命",
    "sell_flee": "空單逃命",
}


def _format_hh_mm(snap_time) -> str:
    if snap_time is None:
        return ""
    try:
        return snap_time.astimezone(_TPE_TZ).strftime("%H:%M")
    except (AttributeError, ValueError):
        return str(snap_time)


def _format_hit(entry: dict) -> str:
    ticker = entry["stock_id"]
    name = entry.get("name", "") or ""
    market = entry.get("market", "") or ""
    return f"  {ticker} {name}（{market}）"


def _latest_snapshot(cur) -> tuple:
    cur.execute("""
        SELECT snapshot_date, snapshot_time
        FROM tw.signal_snapshot_intraday
        ORDER BY snapshot_date DESC, snapshot_time DESC
        LIMIT 1
    """)
    row = cur.fetchone()
    if row is None:
        return None, None
    return row["snapshot_date"], row["snapshot_time"]


def _current_watchlist_fires(cur, snap_date, snap_time, watchlist: set[str]) -> list[dict]:
    """All (signal, stock_id) observations for the latest snapshot that
    intersect the watchlist, with stock display fields."""
    if not watchlist:
        return []
    cur.execute("""
        SELECT s.signal, s.stock_id, st.name, st.market
        FROM tw.signal_snapshot_intraday s
        JOIN tw.stocks st ON st.stock_id = s.stock_id
        WHERE s.snapshot_date = %s
          AND s.snapshot_time = %s
          AND s.stock_id = ANY(%s)
        ORDER BY s.signal, s.stock_id
    """, (snap_date, snap_time, list(watchlist)))
    return list(cur.fetchall())


def _mark_first_seen(cur, snap_date, fires: list[dict]) -> None:
    """Idempotent UPSERT — first occurrence wins, repeats are no-ops."""
    if not fires:
        return
    cur.executemany(
        """
        INSERT INTO tw.intraday_push_state
            (trade_date, stock_id, signal_type, first_seen_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (trade_date, stock_id, signal_type) DO NOTHING
        """,
        [(snap_date, r["stock_id"], r["signal"]) for r in fires],
    )


def _pending_fires(cur, snap_date, fires: list[dict]) -> list[dict]:
    """Filter ``fires`` down to rows whose intraday_push_state.pushed_at
    is still NULL — i.e. haven't been sent today yet.

    Relies on ``_mark_first_seen`` having already inserted every fire so
    a single query against intraday_push_state returns the pending set
    for the day; we then JOIN back in Python to recover display fields."""
    if not fires:
        return []
    cur.execute(
        """
        SELECT stock_id, signal_type
        FROM tw.intraday_push_state
        WHERE trade_date = %s AND pushed_at IS NULL
        """,
        (snap_date,),
    )
    pending = {(r["stock_id"], r["signal_type"]) for r in cur.fetchall()}
    return [r for r in fires if (r["stock_id"], r["signal"]) in pending]


def _stamp_pushed(cur, snap_date, fires: list[dict]) -> None:
    if not fires:
        return
    cur.executemany(
        """
        UPDATE tw.intraday_push_state
        SET pushed_at = NOW()
        WHERE trade_date = %s AND stock_id = %s AND signal_type = %s
        """,
        [(snap_date, r["stock_id"], r["signal"]) for r in fires],
    )


def build_message(snap_date, snap_time, fires: list[dict],
                  *, include_empty: bool) -> str | None:
    """Build the consolidated Telegram body for the pending fires.

    Returns None when there's nothing to send (no pending hits) and the
    caller didn't ask for an explicit empty-state ping."""
    grouped: dict[str, list[dict]] = {sig: [] for sig in _SIGNAL_ORDER}
    for r in fires:
        sig = r["signal"]
        if sig in grouped:
            grouped[sig].append(r)

    sections: list[str] = []
    total = 0
    for sig in _SIGNAL_ORDER:
        bucket = grouped[sig]
        if not bucket:
            continue
        total += len(bucket)
        label = _SIGNAL_LABEL.get(sig, sig)
        body = "\n".join(_format_hit(e) for e in bucket)
        sections.append(f"{label}（{len(bucket)} 檔）\n{body}")

    if total == 0 and not include_empty:
        return None

    header = f"[追蹤清單訊號] {snap_date}"
    pretty_time = _format_hh_mm(snap_time)
    if pretty_time:
        header += f"  快照於 {pretty_time}"

    if total == 0:
        return f"{header}\n\n目前無新觸發訊號。"
    return f"{header}\n\n" + "\n\n".join(sections)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m telegram_bot.push_intraday_signals",
        description="Push pending watchlist signal hits from the latest intraday snapshot.",
    )
    p.add_argument(
        "--always",
        action="store_true",
        help="Send a message even when there are no pending watchlist hits.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Build the message but do not send it or stamp pushed_at.",
    )
    args = p.parse_args(argv)

    watchlist = set(load_tw_watchlist(include_etf=True))
    if not watchlist:
        print("push_intraday_signals: watchlist is empty, nothing to compare",
              file=sys.stderr)
        return 0

    with get_cursor() as cur:
        snap_date, snap_time = _latest_snapshot(cur)
        if snap_date is None:
            print("push_intraday_signals: no intraday snapshot found")
            return 0

        fires = _current_watchlist_fires(cur, snap_date, snap_time, watchlist)
        _mark_first_seen(cur, snap_date, fires)
        pending = _pending_fires(cur, snap_date, fires)

        msg = build_message(snap_date, snap_time, pending,
                            include_empty=args.always)
        if msg is None:
            if not fires:
                print(f"push_intraday_signals: no watchlist fire on snapshot "
                      f"{snap_date} {snap_time:%H:%M}")
            else:
                print(f"push_intraday_signals: {len(fires)} watchlist fire(s) "
                      f"already pushed earlier today, nothing new to send")
            return 0

        if args.dry_run:
            print("push_intraday_signals: dry-run, would have sent:")
            print(msg)
            return 0

        ok = send_sync(msg)
        if not ok:
            print("push_intraday_signals: send_sync failed", file=sys.stderr)
            return 2

        _stamp_pushed(cur, snap_date, pending)
        print(f"push_intraday_signals: sent {len(pending)} new fire(s) "
              f"({len(msg)} chars)")
        return 0


if __name__ == "__main__":
    sys.exit(main())
