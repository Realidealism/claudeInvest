"""
Investor conference index (法人說明會, MOPS ajax_t100sb02_1).

Indexes who held a conference, when, and the filenames of the Chinese/English
decks. The decks themselves are fetched on demand (--download) rather than in
bulk: a survey of six companies found only 聯發科 publishes a machine-readable
product-revenue table, 台積電 draws it as a bitmap, and 大成/漢唐/神盾 omit it
entirely — so a full 15-30GB mirror would be mostly unusable weight.

The listing is queried per market per month; 24 months x 2 markets is 48 posts.

  python -m scrapers.investor_conferences --years 114,115
  python -m scrapers.investor_conferences --download 2454,2049 --out tmp/conf
"""

import argparse
import os
import random
import re
import sys
import time

import requests

from datetime import date, timedelta

from db.connection import get_cursor
from utils.format_shift import EMPTY, NO_RESPONSE, OK, UNREADABLE, ScrapeResult, failed_legs
from utils.http_client import DEFAULT_HEADERS

LIST_URL = "https://mopsov.twse.com.tw/mops/web/ajax_t100sb02_1"
FILE_URL = "https://mopsov.twse.com.tw/server-java/FileDownLoad"
ARCHIVE_DIR = r"D:\invest_data\conf"
MIN_INTERVAL = 1.0
MAX_JITTER = 0.5
MAX_RETRIES = 3

_session = None
_last_request = 0.0

ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S)
CELL_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S)
TAG_RE = re.compile(r"<[^>]+>")
ROC_DATE_RE = re.compile(r"^(\d{2,3})/(\d{1,2})/(\d{1,2})$")


def _rate_limit():
    global _last_request
    elapsed = time.time() - _last_request
    needed = MIN_INTERVAL + random.uniform(0, MAX_JITTER)
    if elapsed < needed:
        time.sleep(needed - elapsed)
    _last_request = time.time()


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        s = requests.Session()
        s.headers.update(DEFAULT_HEADERS)
        _session = s
    return _session


def _post(url: str, data: dict, binary: bool = False):
    for attempt in range(MAX_RETRIES):
        _rate_limit()
        try:
            # allow_redirects=False: MOPS intermittently 307s to an empty page.
            r = _get_session().post(url, data=data, timeout=120, allow_redirects=False)
            if r.status_code == 200:
                return r.content if binary else r.content.decode("utf-8", errors="replace")
        except requests.RequestException as e:
            print(f"  request error ({attempt + 1}): {e}")
        time.sleep(2 * (attempt + 1))
    return None


def _text(cell: str) -> str:
    return TAG_RE.sub("", cell).replace("&nbsp;", " ").strip()


def fetch_month(market: str, roc_year: int, month: int) -> list[dict]:
    return fetch_month_state(market, roc_year, month)[0]


# What MOPS answers for a month with no conference filed (probed 2026-10-17).
_NO_FILINGS_TEXT = "查無資料"


def fetch_month_state(market: str, roc_year: int, month: int) -> tuple[list[dict], str]:
    """fetch_month() plus how the request went: NO_RESPONSE, UNREADABLE, EMPTY
    (MOPS said nothing was filed) or OK."""
    html = _post(LIST_URL, {"step": "1", "firstin": "ture", "off": "1",
                            "TYPEK": market, "year": str(roc_year),
                            "month": f"{month:02d}", "co_id": ""})
    if not html:
        return [], NO_RESPONSE
    out = []
    for row in ROW_RE.findall(html):
        cells = [_text(c) for c in CELL_RE.findall(row)]
        cells = [c for c in cells if c]
        if len(cells) < 5 or not re.fullmatch(r"\d{4,6}", cells[0]):
            continue
        m = ROC_DATE_RE.match(cells[2])
        if not m:
            continue
        conf_date = f"{int(m.group(1)) + 1911}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        files = [c for c in cells if c.lower().endswith((".pdf", ".pptx", ".ppt"))]
        out.append({
            "stock_id": cells[0], "conf_date": conf_date, "conf_time": cells[3][:10],
            "conf_type": cells[4], "subject": cells[5] if len(cells) > 5 else None,
            "file_zh": next((f for f in files if "M" in f.rsplit(".", 1)[0][-4:]), None),
            "file_en": next((f for f in files if "E" in f.rsplit(".", 1)[0][-4:]), None),
            "market": market,
        })
    if out:
        return out, OK
    if _NO_FILINGS_TEXT in html:
        return out, EMPTY
    print(f"  {market} {roc_year}/{month:02d}: no rows and no '{_NO_FILINGS_TEXT}' "
          f"({len(html)} chars)")
    return out, UNREADABLE


def save(rows: list[dict]) -> int:
    if not rows:
        return 0
    saved = 0
    with get_cursor() as cur:
        cur.execute("SELECT stock_id FROM tw.stocks")
        known = {r["stock_id"] for r in cur.fetchall()}
        for row in rows:
            if row["stock_id"] not in known or not row["file_zh"]:
                continue
            cur.execute(
                """
                INSERT INTO tw.investor_conferences
                    (stock_id, conf_date, conf_time, conf_type, subject,
                     file_zh, file_en, market)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_id, conf_date, file_zh) DO UPDATE SET
                    conf_time = EXCLUDED.conf_time, conf_type = EXCLUDED.conf_type,
                    subject = EXCLUDED.subject, file_en = EXCLUDED.file_en,
                    updated_at = NOW()
                """,
                (row["stock_id"], row["conf_date"], row["conf_time"], row["conf_type"],
                 row["subject"], row["file_zh"], row["file_en"], row["market"]),
            )
            saved += 1
    return saved


# What a deck can start with: PDF, an Office Open XML zip (.pptx), or the
# legacy OLE2 compound file (.ppt), which some companies still upload. Anything
# else (an HTML error page) is not a deck.
_DECK_MAGIC = (b"%PDF", b"PK\x03\x04", b"\xd0\xcf\x11\xe0")


def download(file_name: str, out_dir: str) -> str | None:
    """Fetch one deck; returns the local path or None."""
    content = _post(FILE_URL, {"step": "9", "functionName": "t100sb02_1",
                               "filePath": "/home/html/nas/STR/",
                               "fileName": file_name}, binary=True)
    if not content or content[:4] not in _DECK_MAGIC:
        return None
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, file_name)
    with open(path, "wb") as f:
        f.write(content)
    return path


def _mark_downloaded(stock_id: str, conf_date, file_zh: str, path: str):
    with get_cursor() as cur:
        cur.execute(
            "UPDATE tw.investor_conferences SET local_path = %s, downloaded_at = NOW() "
            "WHERE stock_id = %s AND conf_date = %s AND file_zh = %s",
            (path, stock_id, conf_date, file_zh))


_DECK_WINDOW_DAYS = 30  # a deck is retried nightly for this long after its conference


def _next_trading_day(d: date) -> date:
    """The next weekday tw.market_holidays does not close. Read-only: it never
    refreshes the calendar. For a year the calendar does not cover yet this is
    simply the next weekday, which can only make a deck's last try look one
    night later than it is -- a missed report, never a false one."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT holiday_date FROM tw.market_holidays "
            "WHERE is_closed AND holiday_date BETWEEN %s AND %s",
            (d + timedelta(days=1), d + timedelta(days=31)))
        closed = {r["holiday_date"] for r in cur.fetchall()}
    nxt = d + timedelta(days=1)
    while nxt.weekday() >= 5 or nxt in closed:
        nxt += timedelta(days=1)
    return nxt


def scrape_date(trade_date: date) -> ScrapeResult:
    """Daily hook: refresh the announcement index, then archive decks that are due.

    Conferences are announced ahead of time (the index already holds dates weeks
    out), so the index is refreshed on the current and next month to pick up new
    announcements, and only decks whose conference has happened are downloaded.
    """
    roc_year = trade_date.year - 1911
    indexed = 0
    legs = {}
    for market in ("sii", "otc"):
        for month in (trade_date.month, trade_date.month % 12 + 1):
            rows, state = fetch_month_state(market, roc_year, month)
            legs[f"法說會索引 {'上市' if market == 'sii' else '上櫃'} {roc_year}/{month:02d}"] = state
            indexed += save(rows)
    print(f"  Investor conferences: indexed {indexed} filings; legs {legs}")

    with get_cursor() as cur:
        cur.execute(
            "SELECT stock_id, conf_date, file_zh FROM tw.investor_conferences "
            "WHERE local_path IS NULL AND file_zh IS NOT NULL "
            "  AND conf_date <= %s AND conf_date >= %s - %s "
            "ORDER BY conf_date", (trade_date, trade_date, timedelta(days=_DECK_WINDOW_DAYS)))
        due = [dict(r) for r in cur.fetchall()]
    fetched, missed = 0, []
    for row in due:
        path = download(row["file_zh"], ARCHIVE_DIR)
        if path:
            _mark_downloaded(row["stock_id"], row["conf_date"], row["file_zh"], path)
            fetched += 1
        else:
            missed.append(row)
    print(f"  Investor conferences: archived {fetched}/{len(due)} decks"
          + "".join(f"\n    not archived: {r['stock_id']} {r['conf_date']} {r['file_zh']}"
                    for r in missed))

    # A few decks fail every night and most come through on a later night, so
    # only a deck on its last try is reported: by the next trading night it
    # will have left the 30-day window above.
    last_try_before = _next_trading_day(trade_date) - timedelta(days=_DECK_WINDOW_DAYS)
    lost = [f"法說會簡報 {r['stock_id']} {r['conf_date']}（最後一次下載仍失敗）"
            for r in missed if r["conf_date"] < last_try_before]
    return ScrapeResult(records=indexed + fetched, api_rows=indexed, parse_errors=0,
                        failed_sources=failed_legs(legs) + tuple(lost))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", help="ROC years to index, e.g. 114,115")
    ap.add_argument("--download", help="comma-separated stock ids: fetch their latest deck")
    ap.add_argument("--out", default="tmp/conf", help="download directory")
    args = ap.parse_args()
    if not (args.years or args.download):
        sys.exit("Pass --years 114,115 or --download 2454,2049")

    if args.years:
        total = 0
        for roc_year in [int(y) for y in args.years.split(",")]:
            for market in ("sii", "otc"):
                for month in range(1, 13):
                    rows = fetch_month(market, roc_year, month)
                    total += save(rows)
                    print(f"  {roc_year}/{month:02d} {market}: {len(rows)} rows")
        print(f"Indexed {total} filings.")

    if args.download:
        for stock_id in [s.strip() for s in args.download.split(",")]:
            with get_cursor() as cur:
                cur.execute(
                    "SELECT conf_date, file_zh FROM tw.investor_conferences "
                    "WHERE stock_id = %s ORDER BY conf_date DESC LIMIT 1", (stock_id,))
                row = cur.fetchone()
            if not row:
                print(f"{stock_id}: no filing indexed")
                continue
            path = download(row["file_zh"], args.out)
            print(f"{stock_id} {row['conf_date']} -> {path or 'download failed'}")


if __name__ == "__main__":
    main()
