"""
StatementDog tag scraper — 財報狗概念股標籤 (每日增量).

Tag pages are paywalled with a daily free-view quota. This scraper runs
incrementally — each day it picks up IDs not yet successfully scraped, and
stops once the daily quota is exhausted (detected by a run of paywalled
responses in a row).

State is tracked in tw.sd_tag_scan:
  status='ok'       → already parsed, skip
  status='404'      → no such tag id, skip
  status='paywall'  → retry on future runs

Usage:
  python -m scrapers.statementdog_tags              # default range 1..50000
  python -m scrapers.statementdog_tags 1 10000      # narrower range
  python -m scrapers.statementdog_tags 1 50000 20   # stop after 20 consecutive paywalls
"""

import re
import sys
import time

import requests
from bs4 import BeautifulSoup

from db.connection import get_cursor


BASE_URL = "https://statementdog.com/tags/{id}"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
HEADERS = {"User-Agent": UA}
REQUEST_INTERVAL = 3.0
RATE_LIMIT_BACKOFF = 60.0           # seconds to wait on 429
DEFAULT_PAYWALL_STREAK_STOP = 15   # stop run after this many paywalls in a row


# ---------- Parsing ----------

# Paywall placeholder ticker names that appear in all 5 blur-paywall rows.
PAYWALL_MARKERS = ("blur-paywall", "財報狗說明")


def _is_paywalled(html: str) -> bool:
    return "blur-paywall" in html


def _parse(html: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    title_el = soup.select_one("h1.tags-heading-title")
    if not title_el:
        return None
    raw = title_el.get_text(strip=True)
    name = re.sub(r"概念股股票行情$|概念股$", "", raw).strip()
    if not name:
        return None

    members: list[str] = []
    for a in soup.select("a.stock-list-item-link"):
        href = a.get("href", "")
        m = re.search(r"/analysis/(\w+)", href)
        if not m:
            continue
        code = m.group(1)
        if re.match(r"^\d{4}$", code):
            members.append(code)
    seen = set()
    uniq = [c for c in members if not (c in seen or seen.add(c))]
    return {"name": name, "members": uniq}


# ---------- State ----------

def _load_state() -> dict[int, str]:
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT tag_id, status FROM tw.sd_tag_scan")
        return {r["tag_id"]: r["status"] for r in cur.fetchall()}


def _record_state(tag_id: int, status: str, name: str | None = None,
                  member_count: int | None = None):
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO tw.sd_tag_scan (tag_id, status, tag_name, member_count)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (tag_id) DO UPDATE SET
                status = EXCLUDED.status,
                tag_name = COALESCE(EXCLUDED.tag_name, tw.sd_tag_scan.tag_name),
                member_count = COALESCE(EXCLUDED.member_count, tw.sd_tag_scan.member_count),
                last_attempt = NOW(),
                attempts = tw.sd_tag_scan.attempts + 1
            """,
            (tag_id, status, name, member_count),
        )


# ---------- Save parsed tag ----------

def _save_theme(tag_id: int, name: str, members: list[str]) -> int:
    theme_name = f"{name} [tag{tag_id}]"
    with get_cursor() as cur:
        cur.execute(
            "INSERT INTO tw.themes (name, category, description) VALUES (%s, %s, %s) "
            "ON CONFLICT (name) DO UPDATE SET updated_at = NOW() "
            "RETURNING theme_id",
            (theme_name, "財報狗標籤", f"https://statementdog.com/tags/{tag_id}"),
        )
        theme_id = cur.fetchone()["theme_id"]

        linked = 0
        for sid in members:
            cur.execute("SELECT 1 FROM tw.stocks WHERE stock_id = %s", (sid,))
            if not cur.fetchone():
                continue
            cur.execute(
                "INSERT INTO tw.stock_themes (stock_id, theme_id, note) "
                "VALUES (%s, %s, %s) "
                "ON CONFLICT (stock_id, theme_id) DO UPDATE SET note = EXCLUDED.note",
                (sid, theme_id, name),
            )
            linked += 1
    return linked


# ---------- Main loop ----------

def scrape_range(start_id: int = 1, end_id: int = 50000,
                 paywall_stop: int = DEFAULT_PAYWALL_STREAK_STOP):
    state = _load_state()
    sess = requests.Session()
    sess.headers.update(HEADERS)

    stats = {"ok": 0, "empty": 0, "404": 0, "paywall": 0, "skip": 0, "err": 0}
    paywall_streak = 0
    t0 = time.time()

    for tid in range(start_id, end_id + 1):
        prior = state.get(tid)
        # Skip already-resolved ids; retry only paywalled ones
        if prior in ("ok", "404"):
            stats["skip"] += 1
            continue

        try:
            r = sess.get(BASE_URL.format(id=tid), timeout=15, allow_redirects=False)
        except requests.RequestException as e:
            stats["err"] += 1
            print(f"  [{tid}] request error: {e}")
            time.sleep(REQUEST_INTERVAL)
            continue

        if r.status_code == 429:
            stats["err"] += 1
            print(f"  [{tid}] 429 rate limited, backing off {RATE_LIMIT_BACKOFF}s")
            time.sleep(RATE_LIMIT_BACKOFF)
            continue   # don't record state, retry later
        if r.status_code == 404:
            _record_state(tid, "404")
            stats["404"] += 1
            paywall_streak = 0
        elif r.status_code == 200:
            if _is_paywalled(r.text):
                _record_state(tid, "paywall")
                stats["paywall"] += 1
                paywall_streak += 1
                print(f"  [{tid}] paywalled (streak={paywall_streak})")
                if paywall_streak >= paywall_stop:
                    print(f"\nStopped: hit {paywall_streak} paywalls in a row "
                          f"(daily quota drained). Re-run tomorrow.")
                    break
            else:
                parsed = _parse(r.text)
                if not parsed or not parsed["members"]:
                    _record_state(tid, "empty", parsed["name"] if parsed else None, 0)
                    stats["empty"] += 1
                else:
                    linked = _save_theme(tid, parsed["name"], parsed["members"])
                    _record_state(tid, "ok", parsed["name"], len(parsed["members"]))
                    stats["ok"] += 1
                    paywall_streak = 0
                    print(f"  [{tid}] {parsed['name']}  →  "
                          f"{linked}/{len(parsed['members'])} linked")
        else:
            stats["err"] += 1
            print(f"  [{tid}] status {r.status_code}")

        if tid % 500 == 0:
            dt = time.time() - t0
            print(f"  -- progress {tid}/{end_id}  {stats}  elapsed={dt:.0f}s")

        time.sleep(REQUEST_INTERVAL)

    print(f"\nRun done. {stats}")


if __name__ == "__main__":
    s = int(sys.argv[1]) if len(sys.argv) >= 2 else 1
    e = int(sys.argv[2]) if len(sys.argv) >= 3 else 50000
    stop = int(sys.argv[3]) if len(sys.argv) >= 4 else DEFAULT_PAYWALL_STREAK_STOP
    scrape_range(s, e, stop)
