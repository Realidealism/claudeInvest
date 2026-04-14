"""Intraday real-time update pipeline (E.Sun / esun_marketdata).

Usage:
  python intraday_update.py                 # run until Ctrl+C
  python intraday_update.py --force         # skip trading-hours gate (smoke test)
  python intraday_update.py --no-signals    # data layer only
  python intraday_update.py --no-ws         # REST sweeper only
  python intraday_update.py --no-pre        # skip SinoPac pre-market refresh
  python intraday_update.py --no-failover   # disable SinoPac REST fallback

Startup sequence:
  1. Run DB migrations (idempotent)
  2. If today's SinoPac reference prices aren't in tw.intraday_quotes yet,
     log in to Shioaji and pull them (one call, full market). This is the
     authoritative source for ref_price / limit_up / limit_down.
  3. Build + log in to the esun_marketdata SDK (interactive on first run — the
     .p12 cert password and account password are cached in keyring afterwards)
  4. Optionally keep the Shioaji session alive for REST failover — if E.Sun
     fails 3 times in a row, the sweeper switches to SinoPac snapshots.
  5. Spawn worker threads, each sharing the same authenticated SDK:
       [SWEEP]  REST sweeper            — full TSE+OTC snapshot every 20s
       [WS]     WebSocket watcher       — watchlist ticks (trades + books)
       [SIGNAL] Signal engine (Phase B) — poll latest snapshot, fire rules

Ctrl+C (SIGINT) triggers a clean shutdown.
"""

import signal
import sys
import threading
import traceback
from datetime import date

from db.connection import get_cursor, init_db
from intraday import sweeper, watcher, signals
from intraday.sdk_loader import load_sdk


def _has_reference_today() -> bool:
    """Return True if tw.intraday_quotes already has a sinopac_pre row for today."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT 1
            FROM tw.intraday_quotes
            WHERE trade_date = %s
              AND ref_price  IS NOT NULL
            LIMIT 1
            """,
            (date.today(),),
        )
        return cur.fetchone() is not None


def _bootstrap_reference_prices() -> None:
    """Run pre_market_update.main() if today's ref prices aren't loaded yet."""
    if _has_reference_today():
        print("[MAIN] SinoPac reference prices already present for today, skipping")
        return

    print("[MAIN] SinoPac reference prices missing for today, running pre-market refresh ...")
    import pre_market_update
    rc = pre_market_update.main()
    if rc != 0:
        print(f"[MAIN] [WARN] pre-market refresh exited with code {rc} — continuing anyway")


def _prepare_sinopac_failover():
    """Log in to Shioaji and pre-filter contracts for failover snapshot use.

    Returns (api, contracts_dict) or (None, None) on failure.
    The caller keeps `api` alive for the sweeper thread.
    """
    try:
        # Import order: sinopac_reference uses utils.classifier, must be
        # imported before sinopac_loader which loads shioaji and pollutes
        # sys.modules['utils'].
        from intraday.sinopac_reference import fetch_reference_rows  # noqa: F401
        from intraday.sinopac_loader import load_api
        from utils.classifier import classify_tw_security

        api = load_api()

        # Pre-filter to stocks+ETFs (same logic as sinopac_reference)
        tse = [c for c in api.Contracts.Stocks.TSE
               if classify_tw_security(getattr(c, "code", "")) is not None
               and float(getattr(c, "reference", 0) or 0) > 0]
        otc = [c for c in api.Contracts.Stocks.OTC
               if classify_tw_security(getattr(c, "code", "")) is not None
               and float(getattr(c, "reference", 0) or 0) > 0]

        contracts = {"TSE": tse, "OTC": otc}
        print(f"[MAIN] SinoPac failover ready: TSE={len(tse)} OTC={len(otc)}")
        return api, contracts
    except Exception:
        print("[MAIN] [WARN] SinoPac failover setup failed — sweeper will run without fallback:")
        traceback.print_exc()
        return None, None


def main(argv: list[str]):
    force        = "--force" in argv
    run_signals  = "--no-signals" not in argv
    run_ws       = "--no-ws" not in argv
    run_pre      = "--no-pre" not in argv
    run_failover = "--no-failover" not in argv

    print("Initializing database schema ...")
    init_db()
    print()

    if run_pre:
        try:
            _bootstrap_reference_prices()
        except Exception:
            print("[MAIN] [WARN] pre-market bootstrap raised — continuing without it:")
            traceback.print_exc()
        print()

    # Log in BEFORE spawning workers so the keyring prompt (if any) runs in
    # the main thread and every worker sees a fully authenticated SDK.
    sdk = load_sdk()
    print()

    # Optionally keep the Shioaji session alive for REST failover.
    # If E.Sun was already used for pre-market, the session from
    # _bootstrap_reference_prices is closed; we open a fresh one here.
    sinopac_api = None
    sinopac_contracts = None
    if run_failover:
        sinopac_api, sinopac_contracts = _prepare_sinopac_failover()
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

    threads: list[threading.Thread] = []

    t_sweep = threading.Thread(
        target=sweeper.run,
        kwargs={
            "stop_event": stop_event, "sdk": sdk,
            "interval_sec": 20, "force": force,
            "sinopac_api": sinopac_api,
            "sinopac_contracts": sinopac_contracts,
        },
        name="sweeper",
        daemon=True,
    )
    threads.append(t_sweep)

    if run_ws:
        t_ws = threading.Thread(
            target=watcher.run,
            kwargs={"stop_event": stop_event, "sdk": sdk},
            name="watcher",
            daemon=True,
        )
        threads.append(t_ws)

    if run_signals:
        t_sig = threading.Thread(
            target=signals.run,
            kwargs={"stop_event": stop_event, "interval_sec": 30},
            name="signals",
            daemon=True,
        )
        threads.append(t_sig)

    for t in threads:
        t.start()

    try:
        while not stop_event.is_set():
            stop_event.wait(1.0)
    except KeyboardInterrupt:
        stop_event.set()

    print("[MAIN] waiting for threads to finish ...")
    for t in threads:
        t.join(timeout=10.0)
        if t.is_alive():
            print(f"[MAIN] thread {t.name} did not exit within 10s")

    # Clean up SinoPac session if it was opened for failover
    if sinopac_api is not None:
        try:
            from intraday.sinopac_loader import logout_api
            logout_api(sinopac_api)
        except Exception:
            pass

    print("[MAIN] done.")


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except Exception:
        print("[MAIN] [ERROR] unhandled exception:")
        traceback.print_exc()
        sys.exit(1)
