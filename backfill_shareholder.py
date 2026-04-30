"""
Backfill TDCC shareholder distribution for the ~50 historical weeks the
portal retains. Single session with rate limiting to avoid 403 blocks.
Interrupted runs can be safely re-started (skips existing rows).
"""

import re
import sys
import time
from datetime import date

import requests

from db.connection import get_cursor
from scrapers.shareholder_distribution import (
    get_available_dates, save_records, PORTAL_URL, TIER_COUNT,
    _parse_int, _parse_float,
)
from utils.http_client import DEFAULT_HEADERS

DELAY = 1.0  # seconds between requests


def _load_stock_universe() -> list[str]:
    """All active TWSE / TPEx stock IDs (excludes ESB — no TDCC data)."""
    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT stock_id FROM tw.stocks
            WHERE market IN ('TWSE', 'TPEx') AND is_active = true
            ORDER BY stock_id
        """)
        return [r["stock_id"] for r in cur.fetchall()]


def _existing_pairs(data_date: date) -> set[str]:
    """Return set of stock_ids already saved for this date."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT stock_id FROM tw.shareholder_distribution WHERE data_date = %s",
            (data_date,),
        )
        return {r["stock_id"] for r in cur.fetchall()}


class PortalSession:
    """Single reusable session with auto-retry on 403."""

    def __init__(self):
        self.session = None
        self.tok = None
        self.uri = None
        self._init()

    def _init(self):
        self._debug_count = 0
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.session.headers["Referer"] = PORTAL_URL
        r = self.session.get(PORTAL_URL, timeout=30)
        r.raise_for_status()
        m = re.search(r'SYNCHRONIZER_TOKEN[^>]*value="([^"]+)"', r.text)
        if not m:
            raise RuntimeError(f"Cannot get CSRF token (HTTP {r.status_code}, len={len(r.text)})")
        self.tok = m.group(1)
        self.uri = re.search(r'SYNCHRONIZER_URI[^>]*value="([^"]+)"', r.text).group(1)

    def fetch(self, stock_id: str, sca_date: str, _retry: int = 0):
        """Fetch one stock. Returns list of tier tuples or None."""
        payload = {
            "SYNCHRONIZER_TOKEN": self.tok,
            "SYNCHRONIZER_URI":   self.uri,
            "method":     "submit",
            "firDate":    sca_date,
            "scaDate":    sca_date,
            "sqlMethod":  "StockNo",
            "stockNo":    stock_id,
            "stockName":  "",
        }
        try:
            r = self.session.post(PORTAL_URL, data=payload, timeout=60)
        except requests.RequestException as e:
            if _retry >= 2:
                print(f"  [{stock_id}] request error after retries: {e}")
                return None
            wait = 60 * (_retry + 1)
            print(f"  [{stock_id}] connection error — waiting {wait}s then retrying ...", flush=True)
            time.sleep(wait)
            self._init()
            return self.fetch(stock_id, sca_date, _retry + 1)

        if r.status_code == 403 or len(r.text) < 1000:
            if _retry >= 2:
                print(f"  [{stock_id}] blocked after retries (HTTP {r.status_code})")
                return None
            wait = 60 * (_retry + 1)
            print(f"  [{stock_id}] HTTP {r.status_code} — waiting {wait}s then retrying ...", flush=True)
            time.sleep(wait)
            self._init()
            return self.fetch(stock_id, sca_date, _retry + 1)

        # Refresh token from response
        new_tok = re.search(r'SYNCHRONIZER_TOKEN[^>]*value="([^"]+)"', r.text)
        if new_tok:
            self.tok = new_tok.group(1)
            new_uri = re.search(r'SYNCHRONIZER_URI[^>]*value="([^"]+)"', r.text)
            if new_uri:
                self.uri = new_uri.group(1)

        tables = re.findall(r'<table[^>]*class="table"[^>]*>(.*?)</table>', r.text, re.S)
        if not tables:
            if self._debug_count < 3:
                print(f"  [{stock_id}] no table found (HTTP {r.status_code}, len={len(r.text)})", flush=True)
                self._debug_count += 1
            return None

        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tables[0], re.S)
        results = []
        for row in rows[1:]:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.S)
            if len(cells) < 5:
                continue
            clean = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
            tier = _parse_int(clean[0])
            holders = _parse_int(clean[2])
            shares = _parse_int(clean[3])
            pct = _parse_float(clean[4])
            if tier is None:
                continue
            results.append((tier, holders, shares, pct))

        if len(results) < 15:
            if self._debug_count < 3:
                print(f"  [{stock_id}] too few tiers: got {len(results)}", flush=True)
                self._debug_count += 1
            return None
        # Pad to TIER_COUNT if portal returns fewer tiers (older format has 16)
        while len(results) < TIER_COUNT:
            results.append((len(results) + 1, 0, 0, 0.0))
        return results[:TIER_COUNT]


def backfill(dates: list[date], stocks: list[str]):
    total_weeks = len(dates)
    portal = PortalSession()

    for wi, d in enumerate(dates):
        existing = _existing_pairs(d)
        todo = [s for s in stocks if s not in existing]
        print(f"\n[{wi+1}/{total_weeks}] {d}: {len(todo)} to fetch, "
              f"{len(existing)} already done", flush=True)
        if not todo:
            continue

        sca = d.strftime("%Y%m%d")
        saved = 0
        missing = 0
        batch_start = time.time()

        for i, sid in enumerate(todo):
            tiers = portal.fetch(sid, sca)
            if tiers:
                save_records({sid: tiers}, d)
                saved += 1
            else:
                missing += 1

            done = i + 1
            if done % 100 == 0:
                elapsed = time.time() - batch_start
                rate = done / elapsed if elapsed > 0 else 0
                eta = (len(todo) - done) / rate if rate > 0 else 0
                print(f"  {done}/{len(todo)} "
                      f"saved={saved} missing={missing} "
                      f"({rate:.1f}/s, ETA {eta:.0f}s)", flush=True)

            time.sleep(DELAY)

        elapsed = time.time() - batch_start
        print(f"  Done: saved={saved}, missing={missing}, {elapsed:.0f}s", flush=True)


if __name__ == "__main__":
    available = get_available_dates()
    print(f"Portal offers {len(available)} weeks "
          f"({available[-1]} to {available[0]})")

    if len(sys.argv) >= 3:
        start = date.fromisoformat(sys.argv[1])
        end   = date.fromisoformat(sys.argv[2])
        target = [d for d in available if start <= d <= end]
    else:
        target = available

    # Oldest first
    target.sort()

    print(f"Target: {len(target)} weeks")
    stocks = _load_stock_universe()
    print(f"Universe: {len(stocks)} stocks")
    est = len(target) * len(stocks) * (DELAY + 0.1) / 3600
    print(f"Estimated time: ~{est:.1f} hours\n")

    try:
        backfill(target, stocks)
    except KeyboardInterrupt:
        print("\nInterrupted. Re-run to resume.")
