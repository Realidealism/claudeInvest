"""Intraday (12:50) one-shot snapshot CLI.

Runs once per trading day after the morning session has built enough
intraday_value_profile samples that the h(t) curve is reasonably stable.

Steps:
  1. Apply DB migrations (including 046 for the intraday tables).
  2. Compute the market-wide volume scale = 1 / h(t) at the current TPE moment.
  3. Per-stock parallel evaluation: load history + today's forming bar
     (volume scaled to projected full-day), compute ScoreBoard pcts at
     4 bars, evaluate the 6 signal-factory conditions on the latest bar.
  4. Persist top-100 long/short ranks + signal fires to the *_intraday
     tables, anchored to the most recent close for the 變動 column.
  5. Refresh frontend/public/data/scores_intraday.json and operations_intraday.json.

Deployment:
  Schedule via Windows Task Scheduler at 12:50 TPE on weekdays.
  Requires the long-running intraday_sweep_update.exe to have already
  populated tw.intraday_quotes + tw.intraday_value_profile during the
  morning session.
"""

from __future__ import annotations

import sys
import traceback


def main(argv: list[str]) -> int:
    from db.connection import init_db
    from analysis.intraday_snapshot import run as run_snapshot
    from export.generate import export_intraday

    print("Initializing database schema ...")
    init_db()
    print()

    summary = run_snapshot()
    print()

    export_intraday()
    print()

    print(f"[MAIN] done. {summary['stocks_evaluated']} stocks "
          f"({summary['score_long']} long / {summary['score_short']} short).")
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
