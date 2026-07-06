"""Intraday snapshot daemon — back-to-back ScoreBoard + 6 signal passes.

Long-running daemon that drives tw.score_snapshot_intraday /
tw.signal_snapshot_intraday / tw.open_positions_intraday throughout the
trading session. Each pass:

  1. Computes the market-wide volume scale = 1 / h(t) at the current TPE
     moment, using the h(t) curve built from tw.intraday_value_profile.
  2. Per-stock parallel evaluation: load history + today's forming bar
     (volume scaled to projected full-day), compute ScoreBoard pcts at
     4 bars, evaluate the 6 signal-factory conditions on the latest bar.
  3. Persists full alive-universe long/short ranks + signal fires + open
     positions to the *_intraday tables.
  4. Refreshes the 3 intraday JSON exports.

Loop schedule
-------------
  09:00–13:30   back-to-back; sleep 30s on HCurveNotReadyError so the
                first 15 min while the sweep warms up don't crash the
                daemon.
  13:30–14:00   post-close window; waits for the sweep to confirm the
                13:30 bucket, then runs one final close-aligned pass and
                marks today done.
  outside       sleeps until the next weekday's 09:00.

Git push is NOT performed here — it is driven by intraday_publish.bat on
its own Task Scheduler cadence so the back-to-back snapshot loop doesn't
pollute git history with 30+ commits per session. The one exception is
the daily post-close final pass: it pushes once, immediately after it
runs, so the complete-close data reaches Vercel at ~13:45 without
waiting for the next publish tick.

Telegram watchlist-signal pushes DO run here, right after every pass, so
a fire reaches the user within one pass (~1-3 min) instead of the next
publish tick (worst case ~10 min). tw.intraday_push_state dedup plus its
advisory lock make this safe alongside the publish cadence, which stays
in place as a backstop.

Usage:
  intraday_snapshot.exe              # daemon: loop across trading days
  intraday_snapshot.exe --once       # run one pass and exit (smoke test)
  intraday_snapshot.exe --force-once # run one pass ignoring h(t) guards
"""

from __future__ import annotations

import signal
import sys
import threading
import time
import traceback

# Line-buffer stdout/stderr so NSSM-redirected log files get every print
# immediately instead of waiting for a 4 KB block to fill (the daemon
# spends most of its time sleeping and never hits that block size).
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except (AttributeError, ValueError):
    pass


# Last wall-clock epoch at which we polled the TAIFEX MIS VIX endpoint.
# The snapshot loop fires back-to-back (often every few seconds) so we
# throttle the external HTTP call; export_intraday still rewrites
# vix.json every pass from the DB row this scrape leaves behind.
_LAST_VIX_SCRAPE_AT: float = 0.0
_VIX_SCRAPE_INTERVAL_S: float = 60.0


def _maybe_scrape_vix() -> None:
    """Poll TAIFEX MIS getQuoteListVIX at most once per minute, swallowing
    any failure so a flaky upstream never crashes the snapshot loop."""
    global _LAST_VIX_SCRAPE_AT
    now = time.time()
    if now - _LAST_VIX_SCRAPE_AT < _VIX_SCRAPE_INTERVAL_S:
        return
    _LAST_VIX_SCRAPE_AT = now
    try:
        from datetime import date as _date
        from scrapers.vix_tw_intraday import scrape_date as scrape_vix
        scrape_vix(_date.today())
    except Exception as e:
        print(f"[LOOP] VIX intraday scrape failed (non-fatal): {e}")


def _run_pass(*, final_pass: bool = False) -> int:
    """Run a single snapshot pass + JSON export. Returns evaluated stock count."""
    from analysis.intraday_snapshot import run as run_snapshot
    from export.generate import export_intraday

    # Refresh the latest TWVIX before export so vix.json reflects the
    # current poll. Throttled to once/min internally.
    _maybe_scrape_vix()

    summary = run_snapshot(final_pass=final_pass)
    print()
    export_intraday()
    print()
    return summary["stocks_evaluated"]


def _push_final() -> None:
    """Push the just-written complete-close JSON to Vercel immediately after
    the daily final pass, instead of waiting for the next intraday_publish
    tick. Non-fatal: a failure only logs + alerts and lets the regular
    publisher cadence retry the same files."""
    from intraday_git_push import push as push_intraday
    try:
        rc = push_intraday()
        if rc != 0:
            raise RuntimeError(f"push returned {rc}")
    except Exception:
        print("[LOOP] [WARN] final-pass push failed (non-fatal):")
        traceback.print_exc()
        try:
            import subprocess
            subprocess.run([sys.executable, "-m", "telegram_bot.cron_alert",
                            "git_push_failed"], check=False)
        except Exception:
            pass


def _push_signals() -> None:
    """Push any new watchlist signal fires right after a pass instead of
    waiting up to 10 minutes for the next intraday_publish tick. Exactly-once
    delivery is guaranteed by tw.intraday_push_state (+ advisory lock), so
    running this every pass alongside the publish cadence is safe. Non-fatal:
    on failure the regular publish tick delivers the same fires."""
    try:
        from telegram_bot.push_intraday_signals import main as push_signals
        rc = push_signals([])
        if rc != 0:
            print(f"[LOOP] [WARN] signal push returned {rc} (non-fatal)")
    except Exception:
        print("[LOOP] [WARN] signal push failed (non-fatal):")
        traceback.print_exc()


def _run_loop(stop_event: threading.Event) -> None:
    """Drive back-to-back snapshot passes across trading sessions."""
    from datetime import time as dtime
    from intraday.estimate import HCurveNotReadyError
    from intraday.session import (
        in_post_close_window,
        in_snapshot_session,
        now_tpe,
        seconds_until_next_open,
    )
    from intraday.store import post_close_bucket_count

    # >=3 post-close buckets ('13:35', '13:40', '13:45') means the sweeper
    # has run at least 3 iterations after the 13:30 closing auction match.
    # That gives E.Sun ~1 minute to propagate the auction match for every
    # stock (including thinly-traded / halted ones that settle slowly).
    POST_CLOSE_GATE = 3
    # Hard fallback: if the strict gate hasn't been satisfied by this
    # wall-clock time but at least one post-close sweep has landed, run
    # the final pass anyway with a degraded warning. Beyond this we'd be
    # too close to the 14:00 post-close window boundary.
    FALLBACK_TIME = dtime(13, 55)

    final_done_for: object = None

    while not stop_event.is_set():
        now = now_tpe()
        today = now.date()

        # Outside the trading window AND today's final pass is already
        # done (or it's not a trading day) → sleep until tomorrow 09:00.
        if not in_snapshot_session(now) and not in_post_close_window(now):
            final_done_for = None
            sleep_for = min(seconds_until_next_open(now), 300.0)
            print(f"[LOOP] outside session, sleeping {sleep_for:.0f}s "
                  f"(now {now:%Y-%m-%d %H:%M:%S} TPE)")
            if stop_event.wait(sleep_for):
                break
            continue

        # Post-close: wait for >=3 post-close sweep buckets, then run final
        # pass once. Degraded fallback at 13:55 to avoid missing today's
        # snapshot entirely if sweep is slow.
        if in_post_close_window(now):
            if final_done_for == today:
                # Already ran today's final pass — fall through to the
                # next-open sleep on the next tick.
                if stop_event.wait(30):
                    break
                continue
            n = post_close_bucket_count(today)
            if n >= POST_CLOSE_GATE:
                print(f"[LOOP] post-close: {n} post-close sweep bucket(s) "
                      f"confirmed → final pass")
                degraded = False
            elif now.time() >= FALLBACK_TIME and n >= 1:
                print(f"[LOOP] [WARN] post-close fallback at "
                      f"{now:%H:%M:%S} TPE with only {n} bucket(s) "
                      f"(<{POST_CLOSE_GATE}); firing final pass anyway")
                degraded = True
            else:
                print(f"[LOOP] post-close: waiting for {POST_CLOSE_GATE} "
                      f"post-close bucket(s), have {n} "
                      f"({now:%H:%M:%S} TPE)")
                if stop_event.wait(15):
                    break
                continue
            try:
                _run_pass(final_pass=True)
                final_done_for = today
                tag = " (degraded)" if degraded else ""
                print(f"[LOOP] final pass for {today} complete{tag}")
                # Deliver close-aligned fires + push the complete-close data
                # now rather than on the next publish tick.
                _push_signals()
                _push_final()
            except Exception:
                print("[LOOP] [ERROR] final pass failed:")
                traceback.print_exc()
                # Retry the final pass after a delay rather than letting
                # today slip without a close-aligned snapshot.
                if stop_event.wait(30):
                    break
            continue

        # In-session: back-to-back passes.
        t0 = time.time()
        try:
            _run_pass(final_pass=False)
        except HCurveNotReadyError as e:
            print(f"[LOOP] h(t) not ready ({e}); retrying in 30s")
            if stop_event.wait(30):
                break
            continue
        except Exception:
            print("[LOOP] [ERROR] snapshot pass crashed:")
            traceback.print_exc()
            if stop_event.wait(10):
                break
            continue
        elapsed = time.time() - t0
        print(f"[LOOP] pass done in {elapsed:.1f}s")
        _push_signals()
        # Yield briefly so signal handlers and stop_event can interrupt
        # before the next heavy pass kicks off.
        if stop_event.wait(1.0):
            break

    print("[LOOP] stopped")


def main(argv: list[str]) -> int:
    once = "--once" in argv
    force_once = "--force-once" in argv

    from db.connection import init_db
    print("Initializing database schema ...")
    init_db()
    print()

    if once or force_once:
        # Smoke-test path: run one pass and exit. --force-once bypasses
        # the today-buckets guard so it works pre-09:00 against yesterday's
        # h(t) curve (estimate.get_h_curve returns a valid curve as long
        # as historical data is present).
        from intraday.estimate import HCurveNotReadyError
        try:
            n = _run_pass(final_pass=force_once)
            print(f"[MAIN] one-shot done. {n} stocks evaluated.")
            return 0
        except HCurveNotReadyError as e:
            print(f"[MAIN] [ERROR] h(t) not ready: {e}", file=sys.stderr)
            return 1

    stop_event = threading.Event()

    def _handle_sig(signum, _frame):
        print(f"\n[MAIN] signal {signum} received, shutting down ...")
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_sig)
    try:
        signal.signal(signal.SIGTERM, _handle_sig)
    except (AttributeError, ValueError):
        # SIGTERM isn't always available on Windows
        pass

    try:
        _run_loop(stop_event)
    except KeyboardInterrupt:
        stop_event.set()

    print("[MAIN] done.")
    return 0


if __name__ == "__main__":
    # Required when packaged as a Windows exe — multiprocessing workers
    # spawn by re-running the executable, and without freeze_support they'd
    # re-execute __main__ recursively (each one re-runs init_db, deadlocking
    # on AccessExclusiveLock). No-op in unfrozen Python.
    from multiprocessing import freeze_support
    freeze_support()

    try:
        sys.exit(main(sys.argv[1:]))
    except Exception:
        print("[MAIN] [ERROR] unhandled exception:")
        traceback.print_exc()
        sys.exit(1)
