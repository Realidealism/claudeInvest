"""
VIX standalone updater — two modes:

  Daily (default, run after TWSE close):
    - Pulls the official TAIFEX minute-level file via scrapers.vix_tw,
      which writes the settled close and clears intraday_time = NULL.
    - Pulls Cboe VIX + VXSMH daily CSVs via scrapers.vix_cboe (that
      scraper self-throttles, so the every-10-min cadence of
      intraday_publish.bat only hits Cboe once a day).
    - Regenerates vix.json, pushes to GitHub.
    - Schedule: 14:30 TPE on weekdays.

  Intraday (--intraday flag):
    - Pulls a single snapshot from MIS getQuoteListVIX via
      scrapers.vix_tw_intraday, which writes intraday_time = HHMMSS.
    - Regenerates vix.json, pushes to GitHub (so Vercel reflects the
      live value).
    - Schedule: every 15 min, 09:00–13:45 TPE on weekdays.

Both modes share export_vix output and git push. Pass --no-push to
skip the git step (e.g. for ad-hoc local refresh).
"""

from __future__ import annotations

import subprocess
import sys
from datetime import date
from pathlib import Path

from db.connection import init_db, get_cursor
from scrapers.vix_tw import scrape_date as scrape_daily
from scrapers.vix_tw_intraday import scrape_date as scrape_intraday
from scrapers.vix_cboe import scrape_date as scrape_cboe
from export.generate import export_vix


def _git_push(repo: Path, data_file: Path, mode: str = "daily") -> None:
    """Commit + push the regenerated vix.json only. Returns silently if
    there's nothing to commit (payload identical to last run)."""
    status = subprocess.run(
        ["git", "status", "--porcelain", str(data_file)],
        capture_output=True, text=True, cwd=repo,
    )
    if not status.stdout.strip():
        print("  No vix.json change to deploy.")
        return

    msg = (
        f"Update VIX intraday ({date.today().isoformat()})"
        if mode == "intraday"
        else f"Update VIX ({date.today().isoformat()})"
    )
    subprocess.run(["git", "add", str(data_file)], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-m", msg], cwd=repo, check=True)
    subprocess.run(["git", "push"], cwd=repo, check=True)
    print("  Pushed to GitHub — Vercel will auto-deploy.")


def main() -> int:
    init_db()

    intraday = "--intraday" in sys.argv
    scrape_fn = scrape_intraday if intraday else scrape_daily
    mode = "intraday" if intraday else "daily"

    result = scrape_fn(date.today())
    print(f"  [{mode}] records={result.records} api_rows={result.api_rows}")

    # Cboe series (VIX, VXSMH) only publish a daily file — no intraday
    # counterpart. Non-fatal: a Cboe outage leaves yesterday's rows in place.
    if not intraday:
        try:
            scrape_cboe(date.today())
        except Exception as e:
            print(f"  [WARN] Cboe scrape failed: {e}")

    repo = Path(__file__).parent
    data_dir = repo / "frontend" / "public" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    with get_cursor(commit=False) as cur:
        export_vix(cur, data_dir)

    if "--no-push" not in sys.argv:
        try:
            _git_push(repo, data_dir / "vix.json", mode=mode)
        except subprocess.CalledProcessError as e:
            print(f"  [ERROR] git step failed (exit {e.returncode}); JSON is updated locally.")
            return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
