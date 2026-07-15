"""serve() silent-stall watchdog: prolonged tick silence during an open session
must trip a restart, but off-hours silence and fresh feeds must not."""
import time

from broker.capital_skcom import CapitalSKCOMAdapter


def _adapter():
    a = CapitalSKCOMAdapter("id", "pw", "F02-1234567", skcom_dll_path="SKCOM.dll")
    a._mark_alive()  # arm _last_alive to "now"
    return a


def test_stall_during_session_trips():
    a = _adapter()
    a._last_alive = time.monotonic() - 120.0  # 120s of silence
    assert a._stalled(time.monotonic(), lambda: True, stall_timeout=90.0) is True


def test_fresh_feed_does_not_trip():
    a = _adapter()  # just marked alive
    assert a._stalled(time.monotonic(), lambda: True, stall_timeout=90.0) is False


def test_silence_off_session_does_not_trip():
    a = _adapter()
    a._last_alive = time.monotonic() - 600.0  # long silence, but market closed
    assert a._stalled(time.monotonic(), lambda: False, stall_timeout=90.0) is False


def test_disabled_when_no_gate():
    a = _adapter()
    a._last_alive = time.monotonic() - 600.0
    assert a._stalled(time.monotonic(), None, stall_timeout=90.0) is False


def test_disabled_when_timeout_nonpositive():
    a = _adapter()
    a._last_alive = time.monotonic() - 600.0
    assert a._stalled(time.monotonic(), lambda: True, stall_timeout=0.0) is False
