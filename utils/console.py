"""Console helpers for scripts that are both double-clicked and scheduled."""

import threading

PAUSE_TIMEOUT_SEC = 30


def pause_before_exit(timeout: float = PAUSE_TIMEOUT_SEC) -> None:
    """Let a double-click user read the output, but never block a scheduler.

    A bare input() is unsafe here in two different ways. From another process
    or a redirected shell it raises EOFError, which takes the exit code with
    it. Under Task Scheduler it is worse: the allocated console's stdin
    neither raises nor ever receives input, so the call blocks forever -- that
    left daily_update.exe wedged for days, one zombie process per nightly run,
    holding a lock on the exe that stopped it being rebuilt.

    Waiting on a daemon thread bounds both cases: the prompt still appears for
    whoever is watching, and the process exits regardless.
    """
    def _wait():
        try:
            input("\nPress Enter to exit...")
        except (EOFError, OSError):
            pass

    t = threading.Thread(target=_wait, daemon=True)
    t.start()
    t.join(timeout=timeout)
