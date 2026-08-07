"""Tests for utils.console.pause_before_exit.

The bare input() this replaces failed in two different ways: EOFError when
stdin was redirected, and -- far worse -- an indefinite block under Task
Scheduler, whose allocated console stdin neither raises nor delivers. That
one wedged daily_update.exe for days, one zombie per nightly run, holding a
lock that stopped the exe being rebuilt.

Both entry points call this at the very end, so a regression here does not
show up as a failure; it shows up as a process that never exits.
"""
import builtins
import time

from utils.console import pause_before_exit


def test_returns_immediately_when_stdin_is_closed(monkeypatch):
    """The redirected-stdin case: EOFError must not escape."""
    def boom(*args):
        raise EOFError("stdin is not a terminal")

    monkeypatch.setattr(builtins, "input", boom)
    pause_before_exit(timeout=5)


def test_returns_immediately_on_oserror(monkeypatch):
    """A detached console raises OSError rather than EOFError."""
    def boom(*args):
        raise OSError("handle is invalid")

    monkeypatch.setattr(builtins, "input", boom)
    pause_before_exit(timeout=5)


def test_gives_up_when_input_never_returns(monkeypatch):
    """The Task Scheduler case, and the reason this helper exists.

    stdin neither raises nor delivers, so the call must be abandoned rather
    than waited on.
    """
    def never(*args):
        time.sleep(300)

    monkeypatch.setattr(builtins, "input", never)
    started = time.perf_counter()
    pause_before_exit(timeout=0.5)
    assert time.perf_counter() - started < 5


def test_returns_as_soon_as_input_does(monkeypatch):
    """A watching user pressing Enter must not wait out the timeout."""
    monkeypatch.setattr(builtins, "input", lambda *a: "")
    started = time.perf_counter()
    pause_before_exit(timeout=30)
    assert time.perf_counter() - started < 5


def test_waiting_thread_does_not_keep_the_process_alive():
    """It must be a daemon, or a hung prompt still blocks interpreter exit."""
    import threading
    before = {t for t in threading.enumerate()}
    import builtins as b
    orig = b.input
    b.input = lambda *a: time.sleep(300)
    try:
        pause_before_exit(timeout=0.2)
        leaked = [t for t in threading.enumerate()
                  if t not in before and not t.daemon]
        assert not leaked, f"non-daemon threads left behind: {leaked}"
    finally:
        b.input = orig
