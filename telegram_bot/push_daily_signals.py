"""Push the full-market daily signal digest after the post-close pipeline.

Companion to push_intraday_signals but for the *daily* snapshot: reads
tw.signal_snapshot for the latest trade date (full market, no watchlist
filter), sorts each signal bucket by turnover, shows at most _TOP_N stocks
per signal and folds the rest into a "…另 N 檔" tail line. Days with no
fires send nothing — the [每日更新] summary already acts as the daily
heartbeat.

Guards:
  * Only pushes when the requested date IS the newest snapshot_date, so
    historical backfill runs of daily_update stay silent.
  * A logs/ sentinel remembers the last pushed date, so re-running
    daily_update for the same date doesn't double-push.
  * If the message would exceed Telegram's 4096-char limit (possible on
    crash days when one signal fires broadly), the per-signal cap is
    halved until it fits.

CLI:
    python -m telegram_bot.push_daily_signals
    python -m telegram_bot.push_daily_signals --dry-run   # print, no send/stamp
    python -m telegram_bot.push_daily_signals --force     # ignore sentinel
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date

from db.connection import get_cursor
from telegram_bot.notify import send_sync
from telegram_bot.push_intraday_signals import _SIGNAL_LABEL, _SIGNAL_ORDER

_TOP_N = 30

# Telegram hard limit is 4096; leave headroom for the header.
_MAX_CHARS = 4000

_SENTINEL = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "logs",
    ".daily_signals_pushed",
)


def _latest_snapshot_date(cur) -> date | None:
    cur.execute("SELECT MAX(snapshot_date) AS d FROM tw.signal_snapshot")
    row = cur.fetchone()
    return row["d"] if row else None


def _fires(cur, snap_date: date) -> list[dict]:
    cur.execute(
        """
        SELECT s.signal, s.stock_id, st.name, st.market, s.turnover
        FROM tw.signal_snapshot s
        JOIN tw.stocks st ON st.stock_id = s.stock_id
        WHERE s.snapshot_date = %s
        ORDER BY s.signal, s.turnover DESC NULLS LAST, s.stock_id
        """,
        (snap_date,),
    )
    return list(cur.fetchall())


def _format_line(entry: dict) -> str:
    ticker = entry["stock_id"]
    name = entry.get("name", "") or ""
    market = entry.get("market", "") or ""
    turnover = entry.get("turnover")
    line = f"  {ticker} {name}（{market}）"
    if turnover is not None:
        line += f" {float(turnover) / 1e8:.1f}億"
    return line


def build_message(snap_date: date, fires: list[dict]) -> str | None:
    """One consolidated body, or None when the day has no fires."""
    if not fires:
        return None

    grouped: dict[str, list[dict]] = {sig: [] for sig in _SIGNAL_ORDER}
    for r in fires:
        if r["signal"] in grouped:
            grouped[r["signal"]].append(r)

    header = f"[日線訊號] {snap_date}"
    top_n = _TOP_N
    while True:
        sections: list[str] = []
        for sig in _SIGNAL_ORDER:
            bucket = grouped[sig]
            if not bucket:
                continue
            label = _SIGNAL_LABEL.get(sig, sig)
            shown = bucket[:top_n]
            lines = [_format_line(e) for e in shown]
            if len(bucket) > top_n:
                lines.append(f"  …另 {len(bucket) - top_n} 檔")
            sections.append(f"{label}（{len(bucket)} 檔）\n" + "\n".join(lines))
        msg = f"{header}\n\n" + "\n\n".join(sections)
        if len(msg) <= _MAX_CHARS or top_n <= 5:
            return msg
        top_n //= 2


def _already_pushed(snap_date: date) -> bool:
    try:
        with open(_SENTINEL, encoding="ascii") as f:
            return f.read().strip() == snap_date.isoformat()
    except OSError:
        return False


def _mark_pushed(snap_date: date) -> None:
    try:
        os.makedirs(os.path.dirname(_SENTINEL), exist_ok=True)
        with open(_SENTINEL, "w", encoding="ascii") as f:
            f.write(snap_date.isoformat())
    except OSError:
        pass


def push_daily_signals(target_date: date | None = None, *,
                       dry_run: bool = False, force: bool = False) -> int:
    """Send the digest for ``target_date`` (default: newest snapshot).

    Returns 0 on success or a legitimate no-op, 2 when the send failed.
    """
    with get_cursor(commit=False) as cur:
        latest = _latest_snapshot_date(cur)
        if latest is None:
            print("push_daily_signals: tw.signal_snapshot is empty")
            return 0
        snap_date = target_date or latest
        if snap_date != latest:
            print(f"push_daily_signals: {snap_date} is not the newest "
                  f"snapshot ({latest}), skipping (backfill run)")
            return 0
        fires = _fires(cur, snap_date)

    msg = build_message(snap_date, fires)
    if msg is None:
        print(f"push_daily_signals: no fires on {snap_date}, nothing to send")
        return 0

    if not force and _already_pushed(snap_date):
        print(f"push_daily_signals: {snap_date} already pushed, skipping")
        return 0

    if dry_run:
        print("push_daily_signals: dry-run, would have sent:")
        print(msg)
        return 0

    if not send_sync(msg):
        print("push_daily_signals: send_sync failed", file=sys.stderr)
        return 2

    _mark_pushed(snap_date)
    print(f"push_daily_signals: sent {len(fires)} fire(s) ({len(msg)} chars)")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m telegram_bot.push_daily_signals",
        description="Push the full-market daily signal digest to Telegram.",
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Build and print the message without sending.")
    p.add_argument("--force", action="store_true",
                   help="Send even if this date was already pushed.")
    args = p.parse_args(argv)
    return push_daily_signals(dry_run=args.dry_run, force=args.force)


if __name__ == "__main__":
    sys.exit(main())
