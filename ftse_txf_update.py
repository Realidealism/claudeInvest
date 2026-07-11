"""
夜盤參考 standalone updater — 富台指數 (SGX) + 台指期夜盤 (TXF).

Pulls the SGX FTSE Taiwan future (富台, via Capital 海期 — see
scrapers.ftse_capital) and the TAIFEX TXF night-session quote (via cnyes) into
tw.ftse_taiwan, regenerates ftse_taiwan.json, and pushes to GitHub for Vercel
auto-deploy.

Both legs are live quotes carrying the 夜盤 settle before the 09:00 cash open,
so a single pass suffices — no polling. Run via Task Scheduler Tue-Sat at 08:00
(covers Mon-Fri night sessions; Sat catches Friday's, which is also what Monday
morning shows since the weekend has no fresh session). 08:00 sits inside the SGX
quiet gap (T+1 closed 05:15, T opens 08:45), where the 富台 last trade IS the
night settle — outside that gap the 富台 leg refuses to write (see
scrapers.ftse_taiwan.scrape_date).

Usage:
  python ftse_txf_update.py            # fetch + export + git push
  python ftse_txf_update.py --no-push  # local only, skip git
  python ftse_txf_update.py --force    # write 富台 outside the quiet gap
"""

from __future__ import annotations

import shutil
import subprocess
import sys
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


def main() -> int:
    init_db()

    # 富台 (Capital 海期) and TXF (cnyes) are both live quotes — one pass per
    # run, no polling. 富台 carries the 夜盤 settle pre-open (unlike the old
    # cnyes 富台 that froze until the 08:45 day open), so there is nothing to
    # wait for; each leg is written best-effort and decoupled in scrape_date.
    result, ftse_ok = scrape_date(date.today(), force="--force" in sys.argv)
    print(f"  Scraper records={result.records} api_rows={result.api_rows} "
          f"ftse_ok={ftse_ok}")

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
