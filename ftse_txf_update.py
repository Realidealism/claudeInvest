"""
夜盤參考 standalone updater — 富台指數 (SGX) + 台指期夜盤 (TXF).

Pulls the continuous SGX FTSE Taiwan future and TAIFEX TXF night-session
quotes from cnyes into tw.ftse_taiwan, regenerates ftse_taiwan.json, and
pushes to GitHub for Vercel auto-deploy.

Designed for Task Scheduler at 08:00 TPE Tue-Sat: a TW trading day's night
session (15:00-05:00 TPE) finishes at ~05:00 the next morning, so an 08:00
run captures the just-completed overnight action before the cash market
opens at 09:00 — i.e. a pre-open confirmation. Tue-Sat covers Mon-Fri
night sessions (Sat 08:00 catches the Friday-night close); no Monday run is
needed because the weekend has no fresh session.

Usage:
  python ftse_txf_update.py            # fetch + export + git push
  python ftse_txf_update.py --no-push  # local only, skip git
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import time
from datetime import date
from pathlib import Path

from db.connection import init_db, get_cursor
from publish_lock import publish_lock
from scrapers.ftse_taiwan import scrape_date
from export.generate import export_ftse_taiwan


def _git_push(repo: Path, data_file: Path, target_branch: str = "main") -> None:
    """Publish ftse_taiwan.json to origin/<target> for Vercel.

    Pushes through a throwaway worktree pinned to origin/<target> (mirrors
    daily_update._git_push_frontend). A plain `git push` is unreliable here:
    this repo's local main routinely trails origin/main because intraday and
    daily pushes also go through worktrees and never advance local main, so a
    fast-forward push would be rejected. The retry re-fetches origin and
    re-applies, covering the race with those other pushers."""
    rel = f"frontend/public/data/{data_file.name}"
    wt = repo.parent / "_invest_publish_ftse"

    def git(*args, cwd, check=True):
        return subprocess.run(["git", *args], cwd=cwd, check=check)

    with publish_lock():
        for attempt in range(2):
            if wt.exists():  # clean a leftover from a previously crashed run
                subprocess.run(["git", "worktree", "remove", "--force", str(wt)],
                               cwd=repo, check=False)
                subprocess.run(["git", "worktree", "prune"], cwd=repo, check=False)
            git("fetch", "origin", target_branch, cwd=repo)
            git("worktree", "add", "--force", "--detach", str(wt),
                f"origin/{target_branch}", cwd=repo)
            try:
                dest = wt / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(data_file, dest)
                git("add", rel, cwd=wt)
                if subprocess.run(["git", "diff", "--cached", "--quiet", "--", rel],
                                  cwd=wt).returncode == 0:
                    print(f"  No {data_file.name} change to deploy.")
                    return
                git("commit", "-m",
                    f"Update FTSE Taiwan & TXF night reference ({date.today().isoformat()})",
                    cwd=wt)
                if subprocess.run(["git", "push", "origin", f"HEAD:{target_branch}"],
                                  cwd=wt).returncode == 0:
                    print(f"  Pushed to GitHub {target_branch} — Vercel will auto-deploy.")
                    return
                print(f"  push to {target_branch} rejected (attempt {attempt + 1}); "
                      f"refetching and retrying ...")
            finally:
                subprocess.run(["git", "worktree", "remove", "--force", str(wt)],
                               cwd=repo, check=False)
    print(f"  [WARN] {data_file.name} push to {target_branch} failed after retries.")


# When cnyes hasn't posted the latest 富台 settlement yet, scrape_date skips
# (records=0, no write — see scrapers.ftse_taiwan's missing-timestamp guard).
# The scheduler fires at 08:40 TPE and the weekday brief reads the row at
# 08:50, so poll for a bounded window: a run that starts a few minutes before
# the settlement lands still captures it once it does, instead of leaving the
# page on the previous row until the next scheduled run. Only a valid quote is
# ever written, so the page is never updated with a misaligned base.
_RETRY_WINDOW_S = 8 * 60
_RETRY_POLL_S = 45


def main() -> int:
    init_db()

    deadline = time.monotonic() + _RETRY_WINDOW_S
    while True:
        result = scrape_date(date.today())
        print(f"  Scraper records={result.records} api_rows={result.api_rows}")
        if result.records >= 1 or time.monotonic() >= deadline:
            break
        print(f"  FTSE-TW: data not in place yet; retrying in {_RETRY_POLL_S}s ...")
        time.sleep(_RETRY_POLL_S)

    repo = Path(__file__).parent
    data_dir = repo / "frontend" / "public" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    with get_cursor(commit=False) as cur:
        export_ftse_taiwan(cur, data_dir)

    if "--no-push" not in sys.argv:
        try:
            _git_push(repo, data_dir / "ftse_taiwan.json")
        except subprocess.CalledProcessError as e:
            print(f"  [ERROR] git step failed (exit {e.returncode}); JSON is updated locally.")
            return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
