"""A weekday the exchange does not open (typhoon, holiday) is detected from the
sweep itself -- E.Sun keeps serving the previous session's quotes -- and every
intraday consumer stands down. 2026-10-23 (typhoon) ran the whole intraday
line on 10-22 quotes because only the weekday was checked."""
import contextlib
from datetime import datetime, timedelta, timezone

import pytest

from intraday import sweeper
from telegram_bot import health_check
from telegram_bot.watchers import (
    defense_proximity_alert,
    intraday_volume_alert,
    market_volume_brief,
    stance_alert,
)

TPE = timezone(timedelta(hours=8))
FRI = datetime(2026, 10, 23, 10, 0, tzinfo=TPE)
STALE = datetime(2026, 10, 22, 13, 30, tzinfo=TPE)


def _quotes(n, at):
    return [{"stock_id": f"{1000 + i}", "last_trade_at": at} for i in range(n)]


# --- classify_session -------------------------------------------------------

def test_twenty_trades_stamped_today_mean_open():
    recs = _quotes(2000, STALE) + _quotes(20, FRI.replace(hour=9, minute=0, second=3))
    assert sweeper.classify_session(recs, FRI.replace(hour=9, minute=0, second=20)) == "open"


def test_a_few_stray_stamps_neither_open_nor_close_the_day():
    recs = _quotes(2200, STALE) + _quotes(19, FRI)
    assert sweeper.classify_session(recs, FRI) == "undecided"


def test_full_sweep_with_only_old_trades_after_0905_means_closed():
    assert sweeper.classify_session(_quotes(2229, STALE), FRI) == "closed"


def test_no_verdict_before_the_first_bucket():
    assert sweeper.classify_session(_quotes(2229, STALE), FRI.replace(hour=9, minute=4)) == "undecided"


def test_thin_or_empty_sweep_is_a_source_problem_not_a_closure():
    assert sweeper.classify_session([], FRI) == "undecided"
    assert sweeper.classify_session(_quotes(999, STALE), FRI) == "undecided"


def test_rows_without_timestamps_cannot_close_the_day():
    # SinoPac fallback rows carry last_trade_at=None.
    assert sweeper.classify_session(_quotes(2229, None), FRI) == "undecided"


def test_utc_stamps_are_judged_on_the_taipei_date():
    # 2026-10-23 01:00 UTC is 09:00 TPE on the 23rd.
    at = datetime(2026, 10, 23, 1, 0, tzinfo=timezone.utc)
    assert sweeper.classify_session(_quotes(20, at), FRI) == "open"


# --- verdict transitions ----------------------------------------------------

@pytest.fixture
def calls(monkeypatch):
    log = []
    monkeypatch.setattr(sweeper.store, "mark_detected_closure",
                        lambda d, ev: log.append(("mark", d, ev)))
    monkeypatch.setattr(sweeper.store, "clear_detected_closure",
                        lambda d: log.append(("clear", d)) or 1)
    monkeypatch.setattr(sweeper, "_closure_alert",
                        lambda event, extra: log.append(("alert", event, extra)))
    return log


def test_closure_is_recorded_and_alerted_once(calls):
    v = sweeper._advance_verdict("undecided", _quotes(2229, STALE), FRI)
    assert v == "closed"
    assert [c[0] for c in calls] == ["mark", "alert"]
    assert calls[0][1] == FRI.date()
    assert "2229 quotes" in calls[0][2] and "2026-10-22 13:30" in calls[0][2]
    assert calls[1][1:] == ("market_closed_detected", "2229|2026-10-22 13:30")
    # A second stale sweep changes nothing.
    assert sweeper._advance_verdict(v, _quotes(2229, STALE), FRI) == "closed"
    assert len(calls) == 2


def test_trades_after_a_closure_verdict_withdraw_it(calls):
    v = sweeper._advance_verdict("closed", _quotes(20, FRI), FRI)
    assert v == "open"
    assert calls == [("clear", FRI.date()), ("alert", "market_closed_revoked", "10:00")]


def test_inconclusive_sweep_never_downgrades(calls):
    assert sweeper._advance_verdict("closed", [], FRI) == "closed"
    assert sweeper._advance_verdict("undecided", [], FRI) == "undecided"
    assert calls == []


# --- the sweep loop writes nothing until the day is open --------------------

class _OneTick:
    def is_set(self):
        return False

    def wait(self, _seconds):
        return True          # stop after the first sweep


def _run_one_sweep(monkeypatch, quotes, closure_source=None):
    writes = []
    monkeypatch.setattr(sweeper, "_now_tpe", lambda: FRI)
    monkeypatch.setattr(sweeper.esun_rest, "fetch_snapshot_quotes", lambda sdk, m: quotes[m])
    monkeypatch.setattr(sweeper.store, "market_closure_source", lambda d: closure_source)
    monkeypatch.setattr(sweeper.store, "upsert_quotes",
                        lambda recs, market, trade_date: writes.append(("quotes", market)) or len(recs))
    monkeypatch.setattr(sweeper, "_record_value_profile",
                        lambda recs, d, now, sdk=None: writes.append(("profile",)))
    monkeypatch.setattr(sweeper.store, "mark_detected_closure", lambda d, ev: writes.append(("mark",)))
    monkeypatch.setattr(sweeper.store, "clear_detected_closure", lambda d: writes.append(("clear",)) or 1)
    monkeypatch.setattr(sweeper, "_closure_alert", lambda e, x: writes.append(("alert", e)))
    sweeper.run(_OneTick(), sdk=object())
    return writes


def test_a_closed_day_writes_no_quotes(monkeypatch):
    writes = _run_one_sweep(monkeypatch, {"TSE": _quotes(1234, STALE), "OTC": _quotes(995, STALE)})
    assert writes == [("mark",), ("alert", "market_closed_detected")]


def test_an_open_day_writes_as_before(monkeypatch):
    writes = _run_one_sweep(monkeypatch, {"TSE": _quotes(1234, FRI), "OTC": _quotes(995, FRI)})
    assert writes == [("quotes", "TSE"), ("quotes", "OTC"), ("profile",)]


def test_a_calendar_closure_skips_the_sweep(monkeypatch):
    fetched = []
    monkeypatch.setattr(sweeper.esun_rest, "fetch_snapshot_quotes",
                        lambda sdk, m: fetched.append(m) or [])
    monkeypatch.setattr(sweeper, "_now_tpe", lambda: FRI)
    monkeypatch.setattr(sweeper.store, "market_closure_source", lambda d: "twse_calendar")
    monkeypatch.setattr(sweeper, "_finalize_day_or_purge", lambda d: None)
    sweeper.run(_OneTick(), sdk=object())
    assert fetched == []


def test_a_restart_after_detection_keeps_watching_without_writing(monkeypatch):
    writes = _run_one_sweep(monkeypatch, {"TSE": _quotes(1234, STALE), "OTC": _quotes(995, STALE)},
                            closure_source="intraday_detected")
    assert writes == []


# --- consumers stand down ---------------------------------------------------

@pytest.mark.parametrize("mod, kwargs, next_step", [
    (intraday_volume_alert, {"respect_gates": True}, "_purge_old_cooldown"),
    (defense_proximity_alert, {"respect_gates": True}, "_purge_old_cooldown"),
    (stance_alert, {"respect_gates": True}, "_live_stance"),
    (market_volume_brief, {"respect_gates": True}, "compute_projection"),
])
def test_watchers_skip_a_closed_day(monkeypatch, mod, kwargs, next_step):
    monkeypatch.setattr(mod, "_now_tpe", lambda: FRI)
    monkeypatch.setattr(mod, "is_market_closed", lambda d: d == FRI.date())

    def past_the_gate(*a, **kw):
        raise AssertionError("ran past the closure gate")
    monkeypatch.setattr(mod, next_step, past_the_gate)
    _, status = mod._run_check(**kwargs)
    assert "market closed today" in status


def test_health_check_intraday_is_silent_on_a_closed_day(monkeypatch):
    monkeypatch.setattr(health_check, "now_tpe", lambda: FRI)
    monkeypatch.setattr(health_check, "is_market_closed", lambda d: True)

    def boom(*a):
        raise AssertionError("probed the sweeper on a closed day")
    monkeypatch.setattr(health_check, "_check_sweeper", boom)
    assert health_check.run_intraday() == 0


def test_snapshot_loop_runs_no_pass_on_a_closed_day(monkeypatch, capsys):
    import intraday_snapshot
    monkeypatch.setattr("intraday.session.now_tpe", lambda: FRI)
    monkeypatch.setattr("intraday.store.is_market_closed", lambda d: True)

    def boom(**kw):
        raise AssertionError("snapshot pass ran on a closed day")
    monkeypatch.setattr(intraday_snapshot, "_run_pass", boom)
    # capsys swaps out stderr, which faulthandler's watchdog needs a fileno for.
    monkeypatch.setattr(intraday_snapshot, "_deadline", lambda s: contextlib.nullcontext())
    intraday_snapshot._run_loop(_OneTick())
    assert "market closed today" in capsys.readouterr().out


# --- review follow-ups ------------------------------------------------------

def test_undecided_after_0910_alerts_and_falls_back_to_writing(calls):
    # Quotes arrive but carry no usable timestamps (field renamed, say).
    v = sweeper._advance_verdict("undecided", _quotes(2229, None), FRI.replace(hour=9, minute=10))
    assert v == "fallback"
    assert calls == [("alert", "market_closure_undetermined", "09:10|2229|0")]
    # Real trades later still settle the day as open, without another alert.
    assert sweeper._advance_verdict(v, _quotes(20, FRI), FRI) == "open"
    assert len(calls) == 1


def test_undecided_before_0910_or_on_empty_sweeps_keeps_waiting(calls):
    assert sweeper._advance_verdict("undecided", _quotes(2229, None),
                                    FRI.replace(hour=9, minute=9)) == "undecided"
    assert sweeper._advance_verdict("undecided", [], FRI) == "undecided"
    assert calls == []


def test_fallback_day_writes(monkeypatch):
    monkeypatch.setattr(sweeper, "_now_tpe", lambda: FRI)   # 10:00, past 09:10
    writes = _run_one_sweep(monkeypatch, {"TSE": _quotes(1234, None), "OTC": _quotes(995, None)})
    assert writes == [("alert", "market_closure_undetermined"),
                      ("quotes", "TSE"), ("quotes", "OTC"), ("profile",)]


def test_failed_closure_row_write_still_stops_writing_and_alerts(monkeypatch):
    def down(d, ev):
        raise RuntimeError("db down")
    alerts = []
    monkeypatch.setattr(sweeper.store, "mark_detected_closure", down)
    monkeypatch.setattr(sweeper, "_closure_alert", lambda e, x: alerts.append(e))
    assert sweeper._advance_verdict("undecided", _quotes(2229, STALE), FRI) == "closed"
    assert alerts == ["market_closed_detected"]


def test_closure_lookup_failure_falls_back_to_detection(monkeypatch):
    def down(d):
        raise RuntimeError("db down")
    writes = _run_one_sweep(monkeypatch, {"TSE": _quotes(1234, STALE), "OTC": _quotes(995, STALE)})
    assert writes == [("mark",), ("alert", "market_closed_detected")]
    monkeypatch.setattr(sweeper.store, "market_closure_source", down)
    writes2 = []
    monkeypatch.setattr(sweeper.store, "upsert_quotes",
                        lambda recs, market, trade_date: writes2.append(market) or len(recs))
    monkeypatch.setattr(sweeper.esun_rest, "fetch_snapshot_quotes",
                        lambda sdk, m: _quotes(10, FRI) if m == "TSE" else _quotes(10, FRI))
    sweeper.run(_OneTick(), sdk=object())        # lookup raised; quotes decide: open
    assert writes2 == ["TSE", "OTC"]


def test_a_detected_closure_is_not_finalized_as_an_incomplete_day(monkeypatch):
    finalized = []
    after_close = FRI.replace(hour=13, minute=55)
    monkeypatch.setattr(sweeper, "_now_tpe", lambda: next(clock))
    monkeypatch.setattr(sweeper.esun_rest, "fetch_snapshot_quotes",
                        lambda sdk, m: _quotes(1200, STALE))
    monkeypatch.setattr(sweeper.store, "market_closure_source", lambda d: None)
    monkeypatch.setattr(sweeper.store, "mark_detected_closure", lambda d, ev: None)
    monkeypatch.setattr(sweeper, "_closure_alert", lambda e, x: None)
    monkeypatch.setattr(sweeper, "_finalize_day_or_purge", lambda d: finalized.append(d))
    monkeypatch.setattr(sweeper.store, "has_close_bucket", lambda d: False)

    class _TwoTicks:
        n = 0

        def is_set(self):
            return False

        def wait(self, _seconds):
            self.n += 1
            return self.n >= 2

    # _now_tpe is also read once before the loop for the login date.
    clock = iter([FRI, FRI, after_close])
    sweeper.run(_TwoTicks(), sdk=object())
    assert finalized == []


@pytest.mark.parametrize("event, extra, needle", [
    ("market_closed_detected", "2229|2026-10-22 13:30", "今日判定休市"),
    ("market_closed_revoked", "10:00", "撤銷休市判定"),
    ("market_closure_undetermined", "09:10|2229|0", "仍無法判定"),
])
def test_closure_alert_messages(monkeypatch, event, extra, needle):
    from telegram_bot import cron_alert
    sent = []
    monkeypatch.setattr(cron_alert, "send_sync", lambda text: sent.append(text) or True)
    assert cron_alert.main([event, extra]) == 0
    assert needle in sent[0]


def test_daily_update_says_when_it_skips_on_a_detected_closure(monkeypatch):
    import daily_update
    from datetime import date
    sent = []
    monkeypatch.setattr(daily_update, "send_sync", lambda text: sent.append(text))
    monkeypatch.setattr(daily_update, "_closure_source", lambda d: "intraday_detected")
    daily_update._note_detected_closure(date(2026, 10, 23))
    assert len(sent) == 1
    assert "依盤中偵測判定休市" in sent[0] and "source = 'intraday_detected'" in sent[0]
    sent.clear()
    monkeypatch.setattr(daily_update, "_closure_source", lambda d: "twse_calendar")
    daily_update._note_detected_closure(date(2026, 10, 23))
    assert sent == []
