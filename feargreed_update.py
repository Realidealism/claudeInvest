"""
CNN Fear & Greed standalone updater.

Designed for Task Scheduler at 09:35 TPE — by 09:35 CNN has closed its
US trading-day historical point (~04:00 TPE) and published the latest
date, so this run captures the most recent overnight US session before
daily_update.py runs in the afternoon.

Fetches CNN payload, upserts into tw.cnn_fear_greed, regenerates
fear_greed.json, and pushes to GitHub for Vercel auto-deploy. Wholly
independent of daily_update.py / TWSE pipeline.

Usage:
  python feargreed_update.py            # fetch + export + git push
  python feargreed_update.py --no-push  # local only, skip git
"""

from __future__ import annotations

import subprocess
import sys
from datetime import date
from pathlib import Path

from db.connection import init_db, get_cursor
from scrapers.cnn_feargreed import scrape_date
from export.generate import export_fear_greed


def _git_push(repo: Path, data_file: Path) -> None:
    """Commit + push the regenerated fear_greed.json only. Returns silently
    if there's nothing to commit (CNN payload identical to last run)."""
    status = subprocess.run(
        ["git", "status", "--porcelain", str(data_file)],
        capture_output=True, text=True, cwd=repo,
    )
    if not status.stdout.strip():
        print("  No fear_greed.json change to deploy.")
        return

    subprocess.run(["git", "add", str(data_file)], cwd=repo, check=True)
    subprocess.run(
        ["git", "commit", "-m", f"Update CNN Fear & Greed ({date.today().isoformat()})"],
        cwd=repo, check=True,
    )
    subprocess.run(["git", "push"], cwd=repo, check=True)
    print("  Pushed to GitHub — Vercel will auto-deploy.")


def main() -> int:
    init_db()  # idempotent — fast no-op if DB is current

    result = scrape_date(date.today())
    print(f"  Scraper records={result.records} api_rows={result.api_rows}")

    repo = Path(__file__).parent
    data_dir = repo / "frontend" / "public" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    with get_cursor(commit=False) as cur:
        export_fear_greed(cur, data_dir)

    if "--no-push" not in sys.argv:
        try:
            _git_push(repo, data_dir / "fear_greed.json")
        except subprocess.CalledProcessError as e:
            print(f"  [ERROR] git step failed (exit {e.returncode}); JSON is updated locally.")
            return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
