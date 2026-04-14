"""REST sweeper — the coarse layer of the intraday pipeline.

Every `interval_sec` seconds, fetches a full TSE + OTC snapshot via
E.Sun REST (primary) and upserts into tw.intraday_quotes with
source='rest_sweep'.

Failover: when E.Sun fails `_MAX_CONSECUTIVE_FAILURES` times in a row, the
sweeper automatically switches to SinoPac Shioaji snapshots as a backup
data source. It reverts to E.Sun once that succeeds again.

Runs in its own thread. Graceful shutdown via a threading.Event.
"""

import threading
import traceback
from datetime import datetime, time as dtime, timedelta, timezone

from intraday import esun_rest, sinopac_snapshot, store


_TPE_TZ = timezone(timedelta(hours=8))

# TWSE regular session runs 09:00–13:30 local. Give a 5-min buffer so the
# closing auction snapshot still makes it in.
_SESSION_OPEN  = dtime(hour=9,  minute=0)
_SESSION_CLOSE = dtime(hour=13, minute=35)

_MAX_CONSECUTIVE_FAILURES = 3


def _now_tpe() -> datetime:
    return datetime.now(_TPE_TZ)


def _in_session(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    t = now.time()
    return _SESSION_OPEN <= t <= _SESSION_CLOSE


def _seconds_until_next_open(now: datetime) -> float:
    today_open = now.replace(
        hour=_SESSION_OPEN.hour, minute=_SESSION_OPEN.minute, second=0, microsecond=0
    )
    if now < today_open and now.weekday() < 5:
        return (today_open - now).total_seconds()

    d = now.date() + timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    next_open = datetime.combine(d, _SESSION_OPEN, tzinfo=_TPE_TZ)
    return (next_open - now).total_seconds()


def run(stop_event: threading.Event, sdk, interval_sec: int = 20, force: bool = False,
        sinopac_api=None, sinopac_contracts: dict | None = None):
    """Main sweeper loop.

    stop_event:          set to request shutdown
    sdk:                 logged-in esun_marketdata SDK instance
    interval_sec:        seconds between full TSE+OTC sweeps (default 20s)
    force:               skip the trading-hours gate — useful for off-hours smoke tests
    sinopac_api:         optional logged-in Shioaji instance for failover
    sinopac_contracts:   optional {'TSE': [...], 'OTC': [...]} pre-filtered contract lists
    """
    failover_ready = sinopac_api is not None and sinopac_contracts is not None
    consecutive_failures = 0
    using_fallback = False

    print(f"[SWEEP] starting, interval={interval_sec}s, force={force}, "
          f"failover={'ready' if failover_ready else 'disabled'}")

    while not stop_event.is_set():
        now = _now_tpe()

        if not force and not _in_session(now):
            sleep_for = min(_seconds_until_next_open(now), 300.0)
            print(f"[SWEEP] outside session, sleeping {sleep_for:.0f}s")
            if stop_event.wait(sleep_for):
                break
            continue

        today = now.date()

        # Primary path: E.Sun REST
        if not using_fallback:
            try:
                tse = esun_rest.fetch_snapshot_quotes(sdk, "TSE")
                otc = esun_rest.fetch_snapshot_quotes(sdk, "OTC")

                n_tse = store.upsert_quotes(tse, market="TSE", trade_date=today)
                n_otc = store.upsert_quotes(otc, market="OTC", trade_date=today)
                print(f"[SWEEP] {now:%H:%M:%S} TSE={n_tse} OTC={n_otc}")

                if consecutive_failures > 0:
                    print(f"[SWEEP] E.Sun recovered after {consecutive_failures} failure(s)")
                consecutive_failures = 0

            except Exception:
                consecutive_failures += 1
                print(f"[SWEEP] [ERROR] E.Sun sweep failed ({consecutive_failures}/{_MAX_CONSECUTIVE_FAILURES}):")
                traceback.print_exc()

                if failover_ready and consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                    using_fallback = True
                    print(f"[SWEEP] switching to SinoPac fallback after {consecutive_failures} consecutive failures")

        # Fallback path: SinoPac Shioaji
        if using_fallback:
            try:
                tse = sinopac_snapshot.fetch_snapshot_quotes(sinopac_api, sinopac_contracts["TSE"])
                otc = sinopac_snapshot.fetch_snapshot_quotes(sinopac_api, sinopac_contracts["OTC"])

                n_tse = store.upsert_quotes(tse, market="TSE", trade_date=today)
                n_otc = store.upsert_quotes(otc, market="OTC", trade_date=today)
                print(f"[SWEEP] {now:%H:%M:%S} TSE={n_tse} OTC={n_otc} (SinoPac fallback)")

            except Exception:
                print("[SWEEP] [ERROR] SinoPac fallback also failed:")
                traceback.print_exc()

            # Periodically try E.Sun again (every 5 cycles)
            consecutive_failures += 1
            if consecutive_failures % (_MAX_CONSECUTIVE_FAILURES + 5) == 0:
                print("[SWEEP] retrying E.Sun primary ...")
                try:
                    esun_rest.fetch_snapshot_quotes(sdk, "TSE")
                    # If it worked, switch back
                    using_fallback = False
                    consecutive_failures = 0
                    print("[SWEEP] E.Sun recovered, switching back to primary")
                except Exception:
                    pass  # Stay on fallback

        if stop_event.wait(interval_sec):
            break

    print("[SWEEP] stopping")
