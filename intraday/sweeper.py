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

# TWSE regular session runs 09:00–13:30 local. Keep sweeping until 13:50 as a
# safety cutoff in case the closing-auction snapshot is slow to land; the main
# loop exits early as soon as the 13:30 bucket is confirmed in the DB.
_SESSION_OPEN       = dtime(hour=9,  minute=0)
_SESSION_CLOSE      = dtime(hour=13, minute=50)
_CLOSE_BUCKET_TIME  = dtime(hour=13, minute=30)

_MAX_CONSECUTIVE_FAILURES = 3

# E.Sun sdk_token is fixed at login() time and silently goes stale across the
# day boundary — the API then returns 200 with data=[] instead of 401, so we
# both relogin on date change and after N consecutive empty responses.
_MAX_CONSECUTIVE_EMPTY = 3

# 5-minute bucket boundaries for the value profile curve
_BUCKET_MINUTES = 5


def _try_relogin(sdk, reason: str) -> bool:
    """Refresh the E.Sun SDK token. Returns True on success."""
    print(f"[SWEEP] relogin: {reason}")
    try:
        sdk.login()
        print("[SWEEP] relogin ok")
        return True
    except Exception:
        print("[SWEEP] [ERROR] relogin failed:")
        traceback.print_exc()
        return False


def _now_tpe() -> datetime:
    return datetime.now(_TPE_TZ)


def _in_session(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    t = now.time()
    return _SESSION_OPEN <= t <= _SESSION_CLOSE


def _time_bucket(now: datetime) -> str:
    """Round down to the nearest 5-minute bucket, e.g. '09:05', '10:30'."""
    m = now.minute - (now.minute % _BUCKET_MINUTES) + _BUCKET_MINUTES
    h = now.hour
    if m >= 60:
        h += 1
        m -= 60
    return f"{h:02d}:{m:02d}"


def _record_value_profile(records: list[dict], trade_date, now: datetime):
    """Sum total_value across all snapshot records and write to profile table."""
    total = sum(r.get("total_value") or 0 for r in records)
    if total <= 0:
        return
    bucket = _time_bucket(now)
    store.upsert_value_profile(trade_date, bucket, total)


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
    consecutive_empty = 0
    using_fallback = False
    # Caller has just authenticated the SDK; treat today as the login date.
    last_login_date = _now_tpe().date()

    print(f"[SWEEP] starting, interval={interval_sec}s, force={force}, "
          f"failover={'ready' if failover_ready else 'disabled'}")

    # Tracks the date for which today's session has been marked complete
    # (either the 13:30 bucket landed, or the 13:50 safety cutoff fired).
    session_done_date = None

    while not stop_event.is_set():
        now = _now_tpe()
        today = now.date()
        session_done = (session_done_date == today)

        # Day rollover — refresh the SDK token before today's first sweep.
        if today != last_login_date:
            if _try_relogin(sdk, f"date changed {last_login_date} -> {today}"):
                last_login_date = today
                consecutive_empty = 0

        if not force and (not _in_session(now) or session_done):
            # If we drifted past 13:50 without the 13:30 bucket, warn once
            # and mark today as done so the next-open sleep path kicks in.
            if (session_done_date != today
                    and now.weekday() < 5
                    and now.time() > _SESSION_CLOSE):
                if not store.has_close_bucket(today):
                    print(f"[SWEEP] [WARN] 13:30 bucket not received by 13:50 for {today}")
                session_done_date = today

            sleep_for = min(_seconds_until_next_open(now), 300.0)
            print(f"[SWEEP] outside session, sleeping {sleep_for:.0f}s")
            if stop_event.wait(sleep_for):
                break
            continue

        # Primary path: E.Sun REST
        if not using_fallback:
            try:
                tse = esun_rest.fetch_snapshot_quotes(sdk, "TSE")
                otc = esun_rest.fetch_snapshot_quotes(sdk, "OTC")

                n_tse = store.upsert_quotes(tse, market="TSE", trade_date=today)
                n_otc = store.upsert_quotes(otc, market="OTC", trade_date=today)
                _record_value_profile(tse + otc, today, now)
                print(f"[SWEEP] {now:%H:%M:%S} TSE={n_tse} OTC={n_otc}")

                # Empty 200-OK responses are the silent-stale-token symptom.
                # Force a relogin once the count crosses the threshold.
                if not tse and not otc:
                    consecutive_empty += 1
                    print(f"[SWEEP] [WARN] empty snapshot ({consecutive_empty}/{_MAX_CONSECUTIVE_EMPTY})")
                    if consecutive_empty >= _MAX_CONSECUTIVE_EMPTY:
                        if _try_relogin(sdk, f"{consecutive_empty} consecutive empty snapshots"):
                            last_login_date = today
                        consecutive_empty = 0
                else:
                    consecutive_empty = 0

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
                _record_value_profile(tse + otc, today, now)
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

        # Early session end: once the 13:30 bucket is written, today's work
        # for the h(t) curve is complete — mark done and let the next loop
        # tick fall through to the sleep-until-next-open branch.
        if now.time() >= _CLOSE_BUCKET_TIME and store.has_close_bucket(today):
            print(f"[SWEEP] 13:30 bucket confirmed for {today} — ending today's session")
            session_done_date = today

        if stop_event.wait(interval_sec):
            break

    print("[SWEEP] stopping")
