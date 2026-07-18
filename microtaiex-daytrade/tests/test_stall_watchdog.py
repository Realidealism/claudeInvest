"""serve() silent-stall watchdog: prolonged tick silence during an open session
that had already been delivering ticks must trip a restart, but off-hours silence,
fresh feeds, and whole non-trading days (no tick ever) must not."""
import time

from broker.capital_skcom import CapitalSKCOMAdapter


def _adapter():
    a = CapitalSKCOMAdapter("id", "pw", "F02-1234567", skcom_dll_path="SKCOM.dll")
    a._mark_alive()  # arm _last_alive to "now"
    return a


def _seen_tick(a, ago: float) -> None:
    """Simulate a real tick delivered ``ago`` seconds in the past."""
    a._session_had_tick = True
    a._last_tick_at = time.monotonic() - ago


def test_stall_during_session_trips():
    a = _adapter()
    _seen_tick(a, 120.0)  # feed was alive, then 120s of silence
    assert a._stalled(time.monotonic(), lambda: True, stall_timeout=90.0) is True


def test_fresh_feed_does_not_trip():
    a = _adapter()
    _seen_tick(a, 1.0)  # tick just arrived
    assert a._stalled(time.monotonic(), lambda: True, stall_timeout=90.0) is False


def test_no_tick_yet_does_not_trip():
    # Whole non-trading day (weekend/holiday): session_of reads in-window but no
    # tick has ever arrived -> must NOT restart-loop even after long silence.
    a = _adapter()
    assert a._session_had_tick is False
    assert a._stalled(time.monotonic(), lambda: True, stall_timeout=90.0) is False


def test_silence_off_session_does_not_trip():
    a = _adapter()
    _seen_tick(a, 600.0)  # long silence, but market closed
    assert a._stalled(time.monotonic(), lambda: False, stall_timeout=90.0) is False


def test_disabled_when_no_gate():
    a = _adapter()
    _seen_tick(a, 600.0)
    assert a._stalled(time.monotonic(), None, stall_timeout=90.0) is False


def test_disabled_when_timeout_nonpositive():
    a = _adapter()
    _seen_tick(a, 600.0)
    assert a._stalled(time.monotonic(), lambda: True, stall_timeout=0.0) is False
