"""
US Treasury yield curve standalone updater.

Pulls FRED DGS10 / DGS2 / DGS3MO into tw.yield_curve, regenerates
yield_curve.json, pushes to GitHub for Vercel auto-deploy.

Designed for Task Scheduler at 09:35 TPE on weekdays — FRED updates
~17:00 US/Eastern (~06:00 TPE next day), so 09:35 reliably catches the
previous US trading day's value.

Usage:
  python yield_update.py            # fetch + export + git push
  python yield_update.py --no-push  # local only, skip git
"""

from __future__ import annotations

import subprocess
import sys
from datetime import date
from pathlib import Path

from db.connection import init_db, get_cursor
from scrapers.yield_curve import scrape_date
from export.generate import export_yield_curve


def _git_push(repo: Path, data_file: Path) -> None:
    status = subprocess.run(
        ["git", "status", "--porcelain", str(data_file)],
        capture_output=True, text=True, cwd=repo,
    )
    if not status.stdout.strip():
        print("  No yield_curve.json change to deploy.")
        return

    subprocess.run(["git", "add", str(data_file)], cwd=repo, check=True)
    subprocess.run(
        ["git", "commit", "-m", f"Update yield curve ({date.today().isoformat()})"],
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
        export_yield_curve(cur, data_dir)

    if "--no-push" not in sys.argv:
        try:
            _git_push(repo, data_dir / "yield_curve.json")
        except subprocess.CalledProcessError as e:
            print(f"  [ERROR] git step failed (exit {e.returncode}); JSON is updated locally.")
            return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
