"""
StatementDog theme scraper — 財報狗題材分類.

Fetches theme pages at https://statementdog.com/taiex/{id} (id = 1..47).
Each page lists upstream/midstream/downstream sections, with sub-industry groupings
and member stocks. We preserve the sub-industry name in stock_themes.note.

The page is JS-rendered; Playwright is used to obtain the fully-rendered HTML.
Rate limit: 10s between themes.

Usage:
  python -m scrapers.statementdog_themes [start_id] [end_id]
"""

import re
import sys
import time

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from db.connection import get_cursor


BASE_URL = "https://statementdog.com/taiex/{id}"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
INTERVAL = 10.0   # seconds between themes


def _fetch(page, theme_id: int) -> str | None:
    url = BASE_URL.format(id=theme_id)
    try:
        page.goto(url, timeout=30000, wait_until="domcontentloaded")
        page.wait_for_selector("h1.industry-title", timeout=10000)
        return page.content()
    except Exception as e:
        print(f"  [{theme_id}] fetch error: {e}")
        return None


def _parse(html: str) -> dict | None:
    """
    Returns {
      'name': '半導體產業',
      'members': [(stock_id, stream, sub_industry), ...]
    }
    stream ∈ {'上游', '中游', '下游', ''}
    """
    soup = BeautifulSoup(html, "html.parser")
    title_el = soup.select_one("h1.industry-title")
    if not title_el:
        return None
    theme_name = title_el.get_text(strip=True)

    members: list[tuple[str, str, str]] = []
    current_stream = ""
    # Walk all relevant nodes in document order
    for el in soup.select("div.industry-box-subtitle, ul.industry-stream-item"):
        if "industry-box-subtitle" in el.get("class", []):
            txt = el.get_text(strip=True)
            if "上游" in txt:
                current_stream = "上游"
            elif "中游" in txt:
                current_stream = "中游"
            elif "下游" in txt:
                current_stream = "下游"
            else:
                current_stream = ""
            continue
        # ul.industry-stream-item
        sub_el = el.select_one(".industry-stream-sub-industry-name")
        sub_name = sub_el.get_text(strip=True) if sub_el else ""
        for a in el.select("a.industry-stream-company"):
            href = a.get("href", "")
            m = re.search(r"/analysis/(\w+)", href)
            if not m:
                continue
            code = m.group(1)
            if not re.match(r"^\d{4}$", code):
                continue   # skip non-ordinary tickers
            members.append((code, current_stream, sub_name))

    # Fallback: concept-style themes (e.g. AI) use a ranking table instead of
    # stream sections. Extract from industry-ranking-item rows.
    if not members:
        for ul in soup.select("ul.industry-ranking-item"):
            a = ul.select_one(".industry-ranking-ticker-name a")
            if not a:
                continue
            m = re.search(r"/analysis/(\w+)", a.get("href", ""))
            if not m or not re.match(r"^\d{4}$", m.group(1)):
                continue
            pos_el = ul.select_one(".industry-ranking-industry-positon")
            sub_el = ul.select_one(".industry-ranking-sub-industry")
            stream = pos_el.get_text(strip=True) if pos_el else ""
            if stream == "無":
                stream = ""
            sub = sub_el.get_text(strip=True) if sub_el else ""
            members.append((m.group(1), stream, sub))

    return {"name": theme_name, "members": members}


def _save(theme_name: str, members: list[tuple[str, str, str]]) -> tuple[int, int]:
    """Upsert theme + member relationships. Returns (theme_id, inserted_count)."""
    with get_cursor() as cur:
        cur.execute(
            "INSERT INTO tw.themes (name, category) VALUES (%s, %s) "
            "ON CONFLICT (name) DO UPDATE SET updated_at = NOW() "
            "RETURNING theme_id",
            (theme_name, "財報狗"),
        )
        theme_id = cur.fetchone()["theme_id"]

        inserted = 0
        for sid, stream, sub in members:
            # Only link if stock exists in tw.stocks
            cur.execute("SELECT 1 FROM tw.stocks WHERE stock_id = %s", (sid,))
            if not cur.fetchone():
                continue
            note = f"{stream}/{sub}" if stream else sub
            cur.execute(
                "INSERT INTO tw.stock_themes (stock_id, theme_id, note) "
                "VALUES (%s, %s, %s) "
                "ON CONFLICT (stock_id, theme_id) DO UPDATE SET note = EXCLUDED.note",
                (sid, theme_id, note),
            )
            inserted += cur.rowcount
    return theme_id, inserted


def scrape_range(start_id: int = 1, end_id: int = 47):
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=UA)
        page = ctx.new_page()

        for tid in range(start_id, end_id + 1):
            print(f"[{tid}] fetching ...")
            html = _fetch(page, tid)
            if not html:
                time.sleep(INTERVAL)
                continue
            parsed = _parse(html)
            if not parsed or not parsed["members"]:
                print(f"  [{tid}] no members parsed")
                time.sleep(INTERVAL)
                continue
            theme_id, n = _save(parsed["name"], parsed["members"])
            print(f"  [{tid}] {parsed['name']}  →  theme_id={theme_id}, "
                  f"{n}/{len(parsed['members'])} members linked")
            if tid < end_id:
                time.sleep(INTERVAL)

        browser.close()


if __name__ == "__main__":
    s = int(sys.argv[1]) if len(sys.argv) >= 2 else 1
    e = int(sys.argv[2]) if len(sys.argv) >= 3 else 47
    scrape_range(s, e)
