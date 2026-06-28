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

import subprocess
import sys
from datetime import date
from pathlib import Path

from db.connection import init_db, get_cursor
from scrapers.ftse_taiwan import scrape_date
from export.generate import export_ftse_taiwan


def _git_push(repo: Path, data_file: Path) -> None:
    status = subprocess.run(
        ["git", "status", "--porcelain", str(data_file)],
        capture_output=True, text=True, cwd=repo,
    )
    if not status.stdout.strip():
        print("  No ftse_taiwan.json change to deploy.")
        return

    subprocess.run(["git", "add", str(data_file)], cwd=repo, check=True)
    subprocess.run(
        ["git", "commit", "-m", f"Update FTSE Taiwan & TXF night reference ({date.today().isoformat()})"],
        cwd=repo, check=True,
    )
    subprocess.run(["git", "push"], cwd=repo, check=True)
    print("  Pushed to GitHub — Vercel will auto-deploy.")


def main() -> int:
    init_db()

    result = scrape_date(date.today())
    print(f"  Scraper records={result.records} api_rows={result.api_rows}")

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
