"""Intraday REST sweeper-only updater for the h(t) curve data feed.

A trimmed-down variant of intraday_update.py that:
  - Writes tw.intraday_quotes + tw.intraday_value_profile
  - Uses E.Sun REST only (no SinoPac failover, no WebSocket, no signals)
  - Skips the pre-market ref_price bootstrap — the h(t) curve only needs
    market-wide total_value, which E.Sun REST returns directly.
  - Sweeps every 20s until the 13:30 bucket lands (or 13:50 safety cutoff),
    then sleeps until the next trading day's 09:00 and resumes automatically.

Usage:
  intraday_sweep_update.exe             # run forever, loop across trading days
  intraday_sweep_update.exe --force     # skip trading-hours gate (smoke test)

Deployment:
  Place .env next to the exe with DB_* and ESUN_CONFIG_INI set. On first run
  the SDK login prompts for the .p12 cert + account password (cached in
  Windows keyring afterwards).
"""

import signal
import sys
import threading
import traceback

from db.connection import init_db
from intraday import sweeper
from intraday.sdk_loader import load_sdk


def main(argv: list[str]) -> int:
    force = "--force" in argv

    print("Initializing database schema ...")
    init_db()
    print()

    sdk = load_sdk()
    print()

    stop_event = threading.Event()

    def _handle_sig(signum, frame):
        print(f"\n[MAIN] signal {signum} received, shutting down ...")
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_sig)
    try:
        signal.signal(signal.SIGTERM, _handle_sig)
    except (AttributeError, ValueError):
        # SIGTERM isn't always available on Windows
        pass

    t_sweep = threading.Thread(
        target=sweeper.run,
        kwargs={
            "stop_event": stop_event,
            "sdk": sdk,
            "interval_sec": 20,
            "force": force,
            "sinopac_api": None,
            "sinopac_contracts": None,
        },
        name="sweeper",
        daemon=True,
    )
    t_sweep.start()

    try:
        while not stop_event.is_set():
            stop_event.wait(1.0)
    except KeyboardInterrupt:
        stop_event.set()

    print("[MAIN] waiting for sweeper to finish ...")
    t_sweep.join(timeout=10.0)
    if t_sweep.is_alive():
        print("[MAIN] sweeper did not exit within 10s")

    print("[MAIN] done.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except Exception:
        print("[MAIN] [ERROR] unhandled exception:")
        traceback.print_exc()
        sys.exit(1)
