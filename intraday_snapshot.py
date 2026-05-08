"""Intraday (12:50) one-shot snapshot CLI.

Runs once per trading day after the morning session has built enough
intraday_value_profile samples that the h(t) curve is reasonably stable.

Steps:
  1. Apply DB migrations (including 046 for the intraday tables).
  2. Compute the market-wide volume scale = 1 / h(t) at the current TPE moment.
  3. Per-stock parallel evaluation: load history + today's forming bar
     (volume scaled to projected full-day), compute ScoreBoard pcts at
     4 bars, evaluate the 6 signal-factory conditions on the latest bar.
  4. Persist top-100 long/short ranks + signal fires to the *_intraday
     tables, anchored to the most recent close for the 變動 column.
  5. Refresh frontend/public/data/scores_intraday.json and operations_intraday.json.
  6. Auto-commit and push the 3 intraday JSONs so Vercel redeploys.

Deployment:
  Schedule via Windows Task Scheduler at 12:50 TPE on weekdays.
  Requires the long-running intraday_sweep_update.exe to have already
  populated tw.intraday_quotes + tw.intraday_value_profile during the
  morning session.
"""

from __future__ import annotations

import subprocess
import sys
import traceback
from pathlib import Path


_INTRADAY_JSONS = [
    "frontend/public/data/scores_intraday.json",
    "frontend/public/data/operations_intraday.json",
    "frontend/public/data/positions_intraday.json",
]


def _repo_root() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent.parent
    return Path(__file__).parent


def _commit_and_push_intraday(repo_root: Path) -> None:
    """Commit and push the 3 intraday JSONs so Vercel redeploys.

    Only stages the explicit intraday paths — never touches other working-tree
    changes (e.g. signal-factory research in progress). Any git failure is
    logged but does not fail the snapshot: the JSONs are already on disk and
    can be pushed manually."""

    def _run(cmd: list[str]) -> subprocess.CompletedProcess:
        return subprocess.run(cmd, cwd=repo_root, capture_output=True, text=True)

    diff = _run(["git", "diff", "--quiet", "HEAD", "--", *_INTRADAY_JSONS])
    if diff.returncode == 0:
        print("[PUSH] no intraday changes vs HEAD — skipping commit.")
        return

    add = _run(["git", "add", *_INTRADAY_JSONS])
    if add.returncode != 0:
        print(f"[PUSH] [ERROR] git add failed: {add.stderr.strip()}")
        return

    commit = _run(["git", "commit", "-m", "Update intraday data"])
    if commit.returncode != 0:
        msg = commit.stderr.strip() or commit.stdout.strip()
        print(f"[PUSH] [ERROR] git commit failed: {msg}")
        return

    push = _run(["git", "push"])
    if push.returncode != 0:
        print(f"[PUSH] [ERROR] git push failed: {push.stderr.strip()}")
        print("[PUSH] commit is local — push manually with: git push")
        return

    print("[PUSH] intraday data committed and pushed to origin.")


def main(argv: list[str]) -> int:
    from db.connection import init_db
    from analysis.intraday_snapshot import run as run_snapshot
    from export.generate import export_intraday

    print("Initializing database schema ...")
    init_db()
    print()

    summary = run_snapshot()
    print()

    export_intraday()
    print()

    _commit_and_push_intraday(_repo_root())
    print()

    print(f"[MAIN] done. {summary['stocks_evaluated']} stocks "
          f"({summary['score_long']} long / {summary['score_short']} short).")
    return 0


if __name__ == "__main__":
    # Required when packaged as a Windows exe — multiprocessing workers
    # spawn by re-running the executable, and without freeze_support they'd
    # re-execute __main__ recursively (each one re-runs init_db, deadlocking
    # on AccessExclusiveLock). No-op in unfrozen Python.
    from multiprocessing import freeze_support
    freeze_support()

    try:
        sys.exit(main(sys.argv[1:]))
    except Exception:
        print("[MAIN] [ERROR] unhandled exception:")
        traceback.print_exc()
        sys.exit(1)
