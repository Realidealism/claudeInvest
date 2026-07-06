"""Cross-process mutex serializing git publishes to origin/main.

daily_update, intraday_git_push and ftse_txf_update all publish
frontend/public/data through throwaway worktrees on the same .git.
Concurrent runs can collide on git's internal ref locks (fetch/worktree
add/prune), which — unlike a rejected push — is not covered by the
publishers' retry loops. Wrapping each publish critical section in this
lock serializes them.

Implementation: atomic O_CREAT|O_EXCL lock file under logs/. A holder
that crashed without cleanup is detected by mtime staleness (a publish
takes seconds; anything older than _STALE_SECONDS is a corpse) and the
lock is broken. The tiny window where two stale-breakers race each other
is backstopped by the publishers' existing push-reject retry.
"""

from __future__ import annotations

import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path

_STALE_SECONDS = 300.0
_POLL_SECONDS = 2.0


def _lock_path() -> str:
    # PyInstaller exe: __file__ points to the temp _MEI dir; the repo is
    # the exe's parent.parent (mirrors daily_update._git_push_frontend).
    if getattr(sys, "frozen", False):
        repo = Path(sys.executable).parent.parent
    else:
        repo = Path(__file__).resolve().parent
    return str(repo / "logs" / ".publish_main.lock")


@contextmanager
def publish_lock(timeout: float = 360.0):
    """Acquire the publish mutex, waiting up to ``timeout`` seconds.

    Raises TimeoutError when the lock stays held (and non-stale) for the
    whole wait — callers should surface that loudly, not swallow it.
    """
    lock = _lock_path()
    os.makedirs(os.path.dirname(lock), exist_ok=True)
    deadline = time.monotonic() + timeout
    while True:
        try:
            fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, f"{os.getpid()}\n".encode("ascii"))
            os.close(fd)
            break
        except FileExistsError:
            try:
                stale = time.time() - os.path.getmtime(lock) > _STALE_SECONDS
            except OSError:
                continue  # holder released between open and getmtime
            if stale:
                try:
                    os.remove(lock)
                except OSError:
                    pass
                continue
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"publish lock held for over {timeout:.0f}s: {lock}"
                )
            time.sleep(_POLL_SECONDS)
    try:
        yield
    finally:
        try:
            os.remove(lock)
        except OSError:
            pass
