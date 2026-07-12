"""
Market quotes manual updater — commodities, FX, foreign indices, shipping
rates and memory spot prices.

Runs the two market scrapers and rewrites commodities.json locally. This is
the hand-run tool for re-fetching just this feature after a source hiccup,
without sitting through a full daily_update.

Scheduling is NOT this script's job: daily_update.py already registers both
scrapers and its export_all() rewrites commodities.json, then pushes the
whole frontend data dir with the branch/lock handling this script would only
get wrong. So there is deliberately no git push here.

Usage:
  python commodity_update.py            # fetch + rewrite commodities.json
  python commodity_update.py backfill   # same, but pull 10y of Yahoo history
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

from db.connection import init_db, get_cursor
from scrapers import market_html, market_quote
from export.generate import export_market_quote


def main() -> int:
    init_db()

    today = date.today()
    market_quote.scrape("10y" if "backfill" in sys.argv else "1mo")
    market_html.scrape_date(today)

    data_dir = Path(__file__).parent / "frontend" / "public" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    with get_cursor(commit=False) as cur:
        export_market_quote(cur, data_dir)

    return 0


if __name__ == "__main__":
    sys.exit(main())
