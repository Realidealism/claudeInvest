"""Worktree-based git push for the intraday publish step.

Pushes a fixed allowlist of intraday JSONs to origin/main through a throwaway
detached worktree pinned at origin/main — the same mechanism
daily_update._git_push_frontend uses. This never commits to local main, so
local main can't diverge from origin (daily / ftse pushes also go through
worktrees and never advance local main), which eliminates the
push-reject → rebase → untracked-file-collision failure mode that the old
direct-push path hit.

On the freshly-checked-out origin/main worktree we overwrite the allowlisted
files with our local copies and commit — so our fresh intraday data wins over
whatever the EOD push left for the overlapping files (breadth / vix /
fear_greed / yield_curve), matching the old `-X theirs` intent.

Exit codes: 0 = pushed or nothing to push, 2 = failed after retries.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent
DATA = REPO / "frontend" / "public" / "data"
REL = "frontend/public/data"
TARGET = "main"
WT = REPO.parent / "_invest_publish_intraday"
FILES = (
    "scores_intraday.json",
    "operations_intraday.json",
    "positions_intraday.json",
    "breadth.json",
    "vix.json",
    "yield_curve.json",
    "fear_greed.json",
)


def _git(*args, cwd, check=True):
    return subprocess.run(["git", *args], cwd=cwd, check=check)


def push() -> int:
    for attempt in range(2):
        if WT.exists():  # clean a leftover from a previously crashed run
            subprocess.run(["git", "worktree", "remove", "--force", str(WT)],
                           cwd=REPO, check=False)
            subprocess.run(["git", "worktree", "prune"], cwd=REPO, check=False)
        _git("fetch", "origin", TARGET, cwd=REPO)
        _git("worktree", "add", "--force", "--detach", str(WT),
             f"origin/{TARGET}", cwd=REPO)
        try:
            dst = WT / "frontend" / "public" / "data"
            dst.mkdir(parents=True, exist_ok=True)
            staged = []
            for name in FILES:
                src = DATA / name
                if not src.exists():
                    continue
                shutil.copy2(src, dst / name)
                staged.append(f"{REL}/{name}")
            if staged:
                _git("add", *staged, cwd=WT)
            if subprocess.run(["git", "diff", "--cached", "--quiet"],
                              cwd=WT).returncode == 0:
                print("intraday push: no changes to deploy")
                return 0
            _git("commit", "-m",
                 f"Update intraday data ({date.today().isoformat()})", cwd=WT)
            if subprocess.run(["git", "push", "origin", f"HEAD:{TARGET}"],
                              cwd=WT).returncode == 0:
                print("intraday push: pushed - Vercel will auto-deploy")
                return 0
            print(f"intraday push: rejected (attempt {attempt + 1}); "
                  f"refetching and retrying ...")
        finally:
            subprocess.run(["git", "worktree", "remove", "--force", str(WT)],
                           cwd=REPO, check=False)
    print("intraday push: FAILED after retries")
    return 2


if __name__ == "__main__":
    sys.exit(push())
