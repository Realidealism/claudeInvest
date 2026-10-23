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
from datetime import datetime, time as dtime

from intraday import esun_rest, session as _session, sinopac_snapshot, store


_TPE_TZ = _session.TPE_TZ

# TWSE regular session runs 09:00–13:30 local. Keep sweeping until 13:50 as a
# safety cutoff in case the closing-auction snapshot is slow to land; the main
# loop exits early as soon as the 13:30 bucket is confirmed in the DB.
_SESSION_OPEN       = _session.SESSION_OPEN
_SESSION_CLOSE      = _session.SWEEPER_CUTOFF
_CLOSE_BUCKET_TIME  = _session.SESSION_CLOSE

_MAX_CONSECUTIVE_FAILURES = 3

# E.Sun sdk_token is fixed at login() time and silently goes stale across the
# day boundary — the API then returns 200 with data=[] instead of 401, so we
# both relogin on date change and after N consecutive empty responses.
_MAX_CONSECUTIVE_EMPTY = 3

# 5-minute bucket boundaries for the value profile curve
_BUCKET_MINUTES = 5

# Closure detection: no trade stamped today by the first bucket boundary, on a
# sweep carrying at least this many timestamped quotes (a full sweep is ~2200),
# means the exchange did not open.
_CLOSURE_DECISION_TIME = dtime(9, 5)
_CLOSURE_MIN_RECORDS = 1000
# Trades stamped today needed to call the day open (or withdraw a closure), so
# a stray stamp on a closed day cannot reopen it; a real open has thousands.
_OPEN_MIN_TRADES = 20
# Still undecided on a non-empty sweep by then: the timestamps themselves are
# unusable, so alert and write as before detection existed rather than lose a
# trading day to a broken field.
_UNDECIDED_GIVE_UP_TIME = dtime(9, 10)

# Indices recorded alongside each bucket: (symbol, value_col, close_col, pct_col).
# IX0001 = 發行量加權股價指數 (TWSE), IX0043 = 櫃買指數 (TPEx). Each quote carries
# its exchange's own cumulative turnover, which is the published figure — unlike
# our summed market_total_value.
_INDEX_SYMBOLS = (
    ("IX0001", "twse_trade_value", "taiex_close", "taiex_change_pct"),
    ("IX0043", "tpex_trade_value", "tpex_close", "tpex_change_pct"),
)


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
    return _session.now_tpe()


def _in_session(now: datetime) -> bool:
    return _session.in_session(now)


def _time_bucket(now: datetime) -> str:
    """Round down to the nearest 5-minute bucket, e.g. '09:05', '10:30'."""
    m = now.minute - (now.minute % _BUCKET_MINUTES) + _BUCKET_MINUTES
    h = now.hour
    if m >= 60:
        h += 1
        m -= 60
    return f"{h:02d}:{m:02d}"


def _fetch_index_snapshot(sdk) -> dict:
    """Authoritative index levels + exchange turnover for the current bucket.

    Best-effort: an index that can't be read is simply left out, so the
    profile row still lands with whatever we did get.
    """
    snap: dict = {}
    for symbol, value_key, close_key, pct_key in _INDEX_SYMBOLS:
        quote = esun_rest.fetch_index_quote(sdk, symbol)
        if not quote:
            continue
        snap[value_key] = quote["trade_value"]
        snap[close_key] = quote["close"]
        snap[pct_key] = quote["change_pct"]
    return snap


def _record_value_profile(records: list[dict], trade_date, now: datetime, sdk=None):
    """Sum total_value across all snapshot records and write to profile table."""
    total = sum(r.get("total_value") or 0 for r in records)
    if total <= 0:
        return
    bucket = _time_bucket(now)
    index_snapshot = None
    if sdk is not None:
        try:
            index_snapshot = _fetch_index_snapshot(sdk)
        except Exception:
            # The h(t) curve is the load-bearing output here; index columns
            # are a bonus and must never cost us the row.
            print("[SWEEP] [WARN] index snapshot failed:")
            traceback.print_exc()
    store.upsert_value_profile(trade_date, bucket, total, index_snapshot)


def _finalize_day_or_purge(trade_date):
    """End-of-day integrity check: keep the day only if every 5-min bucket
    from 09:05 to 13:30 is present. Partial days get purged so the h(t)
    curve EMA stays uncontaminated.
    """
    is_complete, missing = store.check_day_completeness(trade_date)
    if is_complete:
        print(f"[SWEEP] {trade_date} bucket coverage complete")
        return
    sample = ", ".join(missing[:5]) + ("..." if len(missing) > 5 else "")
    deleted = store.purge_day_profile(trade_date)
    print(f"[SWEEP] [WARN] {trade_date} incomplete — missing {len(missing)} buckets "
          f"({sample}); purged {deleted} rows from intraday_value_profile")


def classify_session(records: list[dict], now: datetime) -> str:
    """'open', 'closed' or 'undecided' for today, from one snapshot sweep.

    On a weekday closure (typhoon, holiday) E.Sun keeps serving the previous
    session's quotes, so every last_trade_at is older than today. Trades
    stamped today (at least _OPEN_MIN_TRADES) mean the market is open. 'closed' needs a full-looking
    sweep after the first bucket with no trade from today; anything thinner
    (empty response, fallback rows without timestamps) is a source problem,
    not a closure, and stays 'undecided'.
    """
    today = now.date()
    stamps = [r["last_trade_at"] for r in records if r.get("last_trade_at")]
    n_today = sum(1 for t in stamps if t.astimezone(_TPE_TZ).date() == today)
    if n_today >= _OPEN_MIN_TRADES:
        return "open"
    if (n_today == 0 and now.time() >= _CLOSURE_DECISION_TIME
            and len(stamps) >= _CLOSURE_MIN_RECORDS):
        return "closed"
    return "undecided"


def _closure_alert(event: str, extra: str) -> None:
    """In-process cron_alert call (the sweeper runs as a PyInstaller exe, so a
    '-m' subprocess would start another sweeper). Non-fatal."""
    try:
        from telegram_bot.cron_alert import main as cron_alert_main
        rc = cron_alert_main([event, extra])
        if rc != 0:
            print(f"[SWEEP] [WARN] {event} alert returned {rc}")
    except Exception:
        print(f"[SWEEP] [WARN] {event} alert failed:")
        traceback.print_exc()


def _advance_verdict(verdict: str, records: list[dict], now: datetime) -> str:
    """Move today's verdict forward with one sweep. 'open' is final; a sweep
    showing today's trades overturns 'closed' and 'fallback'; 'fallback'
    (write without a verdict) is where an undecided day ends up after
    _UNDECIDED_GIVE_UP_TIME; an inconclusive sweep never downgrades a verdict.

    The closure row is a convenience for the other consumers: a DB error
    writing or withdrawing it is logged and the in-memory verdict stands."""
    new = classify_session(records, now)
    today = now.date()
    if new == "open":
        if verdict == "closed":
            try:
                deleted = store.clear_detected_closure(today)
            except Exception:
                deleted = "?"
                traceback.print_exc()
            print(f"[SWEEP] [WARN] trades seen after closure verdict for {today}; "
                  f"withdrawn ({deleted} row)")
            _closure_alert("market_closed_revoked", f"{now:%H:%M}")
        else:
            print(f"[SWEEP] {today} open: trades stamped today")
        return "open"
    stamps = [r["last_trade_at"] for r in records if r.get("last_trade_at")]
    if new == "closed" and verdict == "undecided":
        latest = max(stamps).astimezone(_TPE_TZ)
        evidence = (f"{now:%H:%M} sweep: {len(stamps)} quotes, none traded today; "
                    f"latest trade {latest:%Y-%m-%d %H:%M}")
        try:
            store.mark_detected_closure(today, evidence)
        except Exception:
            print("[SWEEP] [WARN] closure row not written:")
            traceback.print_exc()
        print(f"[SWEEP] [WARN] {today} judged closed — {evidence}")
        _closure_alert("market_closed_detected", f"{len(stamps)}|{latest:%Y-%m-%d %H:%M}")
        return "closed"
    if verdict == "undecided" and records and now.time() >= _UNDECIDED_GIVE_UP_TIME:
        print(f"[SWEEP] [WARN] {today} still undecided at {now:%H:%M} "
              f"({len(records)} quotes, {len(stamps)} timestamped); writing anyway")
        _closure_alert("market_closure_undetermined",
                       f"{now:%H:%M}|{len(records)}|{len(stamps)}")
        return "fallback"
    return verdict


def _seconds_until_next_open(now: datetime) -> float:
    return _session.seconds_until_next_open(now)


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
    # Whether today traded at all: 'undecided', 'open', 'closed' or 'fallback'.
    # Nothing is written while it is 'undecided' or 'closed'.
    verdict_date = None
    verdict = "undecided"

    while not stop_event.is_set():
        now = _now_tpe()
        today = now.date()
        if not force and verdict_date != today and _in_session(now):
            verdict_date, verdict = today, "undecided"
            try:
                source = store.market_closure_source(today)
            except Exception:
                # Detection below still runs; only the calendar shortcut is lost.
                print("[SWEEP] [WARN] closure lookup failed; detecting from quotes:")
                traceback.print_exc()
                source = None
            if source == "intraday_detected":
                # Restarted after today's own verdict: keep watching for trades.
                verdict = "closed"
            elif source is not None:
                print(f"[SWEEP] {today} closed per tw.market_holidays ({source})")
                session_done_date = today
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
                if verdict_date == today and verdict == "closed":
                    print(f"[SWEEP] {today} closed; nothing to finalize")
                else:
                    if not store.has_close_bucket(today):
                        print(f"[SWEEP] [WARN] 13:30 bucket not received by 13:50 for {today}")
                    _finalize_day_or_purge(today)
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

                if not force and verdict != "open":
                    verdict = _advance_verdict(verdict, tse + otc, now)
                if force or verdict in ("open", "fallback"):
                    n_tse = store.upsert_quotes(tse, market="TSE", trade_date=today)
                    n_otc = store.upsert_quotes(otc, market="OTC", trade_date=today)
                    _record_value_profile(tse + otc, today, now, sdk)
                    print(f"[SWEEP] {now:%H:%M:%S} TSE={n_tse} OTC={n_otc}")
                else:
                    print(f"[SWEEP] {now:%H:%M:%S} {verdict}, not written "
                          f"(TSE={len(tse)} OTC={len(otc)})")

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

                # SinoPac rows carry no trade timestamp, so they cannot open
                # the day; they still land unless E.Sun already judged it
                # closed, as they did before closure detection existed.
                if not force and verdict == "closed":
                    print(f"[SWEEP] {now:%H:%M:%S} closed, not written (SinoPac fallback)")
                else:
                    n_tse = store.upsert_quotes(tse, market="TSE", trade_date=today)
                    n_otc = store.upsert_quotes(otc, market="OTC", trade_date=today)
                    # sdk is deliberately omitted: we only reach the fallback after
                    # E.Sun failed repeatedly, so index lookups against it would just
                    # burn a timeout per sweep. The profile row still lands; the
                    # brief degrades to ratios-only for those buckets.
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
                except Exception as e:
                    print(f"[SWEEP] E.Sun still failing, staying on fallback: {e}")

        # Early session end: once the 13:30 bucket is written, today's work
        # for the h(t) curve is complete — mark done and let the next loop
        # tick fall through to the sleep-until-next-open branch.
        # Keep sweeping until the 13:50 cutoff (handled in the outside-session
        # branch above). The early-end optimization was removed so that the
        # snapshot daemon can wait for >=3 post-close buckets (13:35/13:40/13:45)
        # before its final pass — this gives E.Sun time to propagate the
        # closing-auction match for slowly-settling stocks.

        if stop_event.wait(interval_sec):
            break

    print("[SWEEP] stopping")
