"""One row per snapshot run in tw.snapshot_runs: which build wrote the
snapshot rows of a date (daily) or of a pass (intraday).

The snapshots import the backtest factories, so the build commit is the
version that produced them; the rows themselves carry no version.
"""
from __future__ import annotations

import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

from db.connection import get_cursor

REPO = Path(__file__).resolve().parents[1]


def build_commit() -> str:
    """The commit the running code was built from, "+dirty" when uncommitted
    sources went in; "unknown" when neither the stamp nor git can say.

    Only an exe reads the stamp: one left in the tree by an interrupted build
    would otherwise name that build for every run from source."""
    if getattr(sys, "frozen", False):
        try:
            import _build_stamp  # written by tools/build_info.py stamp; bundled in the exes
            return _build_stamp.COMMIT
        except ImportError:
            return "unknown"
    try:
        head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO,
                              capture_output=True, text=True, timeout=10)
        status = subprocess.run(["git", "status", "--porcelain", "--", "*.py", "*.spec"],
                                cwd=REPO, capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    if head.returncode != 0 or status.returncode != 0 or not head.stdout.strip():
        return "unknown"
    return head.stdout.strip() + ("+dirty" if status.stdout.strip() else "")


def save_run(kind: str, snapshot_date: date, snapshot_time: datetime | None,
         history_days: int | None, stocks_loaded: int, stocks_scored: int) -> None:
    """Record the run that just wrote the snapshot rows. A rerun of the same
    date (daily) or pass (intraday) replaces its row, as it replaces the rows."""
    with get_cursor() as cur:
        cur.execute(
            "DELETE FROM tw.snapshot_runs WHERE kind = %s AND snapshot_date = %s "
            "AND snapshot_time IS NOT DISTINCT FROM %s",
            (kind, snapshot_date, snapshot_time),
        )
        cur.execute(
            "INSERT INTO tw.snapshot_runs (kind, snapshot_date, snapshot_time, build_commit, "
            "history_days, stocks_loaded, stocks_scored) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (kind, snapshot_date, snapshot_time, build_commit(), history_days,
             stocks_loaded, stocks_scored),
        )
