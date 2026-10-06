"""Push the two-week investor conference calendar after the post-close pipeline.

tw.investor_conferences is refreshed daily by scrapers.investor_conferences and
already holds future dates, because MOPS announces conferences weeks ahead. This
module is the presentation side: the full market (no watchlist filter, since the
point of the calendar is companies not yet on the reader's radar) for the next
_HORIZON_DAYS calendar days, grouped by date.

The whole window is re-sent every day rather than only the deltas, so a missed
run self-heals and the reader can scroll back to one message for the fortnight.
Newly announced sessions carry a 🆕 marker to keep the repetition scannable.

Guards:
  * The window is always anchored on the wall clock, never on the caller's
    trade_date: a calendar of upcoming events has nothing to say about a past
    date. Together with the sentinel below that also caps a multi-day backfill
    (daily_update.update_range loops update_date once per day) at one push —
    every iteration of the loop asks for the same day, and only the first is
    let through.
  * A logs/ sentinel remembers the last pushed date; a second run on that day
    sends nothing.
  * The sentinel also stores the keys of ALL future sessions, not just the ones
    displayed: the window slides forward one day per run, so diffing against the
    displayed set alone would flag every session entering the horizon as new.
    Sessions announced beyond the horizon therefore never get a 🆕 — accepted, as
    the alternative is a fistful of false 🆕 every single day.
  * updated_at is unusable as a "new announcement" signal: the scraper's upsert
    sets it to NOW() on every run, so the whole table looks like it changed today.

CLI:
    python -m telegram_bot.push_conference_calendar
    python -m telegram_bot.push_conference_calendar --dry-run  # print, no send/stamp
    python -m telegram_bot.push_conference_calendar --force    # ignore sentinel
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
from datetime import date, timedelta

from db.connection import get_cursor
from telegram_bot.notify import send_sync
from telegram_bot.watchers.morning_brief import _trading_days_until, _weekday_zh

_HORIZON_DAYS = 14

# Telegram hard limit is 4096; leave headroom for the header.
_MAX_CHARS = 3800

_HEADER = "📣 法說會預告（未來兩週）"

_SENTINEL = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "logs",
    ".conf_calendar_pushed",
)

# ── Host broker ───────────────────────────────────────────────────────────
# A company often appears twice in one window because two brokers invited it
# to their own forum (same deck, different audience), so the host is what
# tells the two rows apart. It only exists inside the filing's free-text
# subject, in one of a handful of phrasings.

_WS = re.compile(r"\s+")
_PARENS = re.compile(r"[（(][^）)]{0,40}[）)]")
_LEAD = re.compile(r"^(?:由|向|與|及|其|將|本公司|公告|受邀參加|受邀|參加"
                   r"|第[一二三四]季|Q[1-4])+")
_TRIM_EDGE = re.compile(r"^[\s「」『』\"'（）():：,、．.]+|[\s「」『』\"'（）():：,、．.]+$")
# A house name carries no digits, so the forum's year/quarter marks where the
# name ends ("凱基證券2026年第三季投資論壇" -> 凱基證券).
_NUM_TAIL = re.compile(r"\s*\d.*$")
_SUFFIX = re.compile(
    r"(?:股份有限公司|有限公司|\(股\)公司|股份公司|公司|綜合證券|證券投資顧問"
    r"|證券|投顧|國際法人部|法人部|召開|邀請|舉辦|辦理|線上)+$")
_SUFFIX_EN = re.compile(r"\s+(?:securities|capital|group|inc\.?|ltd\.?|llc)$", re.I)
# Co-hosted sessions ("元富證券與IR Trust共同", "富邦證券/Jefferies") — the first
# house named is enough to tell two rows of the same company apart.
_COHOST = re.compile(r"\s*(?:與|及|暨|/|、).*$")
_ASCII = re.compile(r"[A-Za-z]")
_CJK = re.compile(r"[一-鿿]")
# A date or a venue that slipped into the capture instead of a house name.
_NOT_A_HOST = re.compile(r"^於|\d\s*[年月日]|^自行")
_ASCII_HEAD = re.compile(r"^[A-Za-z0-9.&' -]+")

_HOST_PATTERNS = [
    re.compile(r"參加\s*([^，。、]{2,24}?)\s*(?:所|於[^，。]{0,24}?)?"
               r"(?:舉辦|舉行|辦理|主辦|召開)"),
    re.compile(r"受邀\s*([^，。、]{2,20}?)\s*(?:所)?(?:舉辦|舉行|辦理|主辦)"),
    re.compile(r"應\s*([^，。、]{2,20}?)\s*之邀"),
    re.compile(r"受\s*([^，。、]{2,20}?)\s*(?:之)?邀請"),
    re.compile(r"參加\s*([^，。、]{2,20}?)\s*"
               r"(?:法人說明會|法說會|座談會|投資論壇|產業論壇)"),
    re.compile(r"^\s*([^，。、]{2,20}?)(?:舉辦|辦理)之"),
    # Terse subjects that open with the house itself ("元大2026第三季投資論壇").
    re.compile(r"^\s*([^，。、0-9]{2,10}?)(?:\d{4}|第[一二三四]季)[^，。、]{0,20}"
               r"(?:論壇|說明會|法說會|座談會|Day|Conference)"),
]

# Foreign houses file in English while the Chinese filings name them in
# Chinese; fold both spellings onto one label so the same host always reads
# the same way in the message.
_HOST_ALIASES = {
    "goldman sachs": "高盛", "goldman": "高盛",
    "j.p. morgan": "摩根大通", "j.p.morgan": "摩根大通", "jpmorgan": "摩根大通",
    "jp morgan": "摩根大通",
    "morgan stanley": "摩根士丹利",
    "citi": "花旗", "citi's": "花旗", "citigroup": "花旗",
    "ubs": "瑞銀", "hsbc": "匯豐", "nomura": "野村", "daiwa": "大和",
    "macquarie": "麥格理", "jefferies": "傑富瑞", "clsa": "里昂",
    "bofa": "美銀", "bank of america": "美銀", "merrill": "美銀",
    "fidelity": "富達", "barclays": "巴克萊", "mizuho": "瑞穗",
    "deutsche": "德銀", "bnp": "法巴",
    "中國信託": "中信", "香港上海匯豐": "匯豐", "麥格理資本": "麥格理",
    # The 所 in 交易所/櫃買中心 doubles as the pattern's separator, so these
    # arrive one character short.
    "臺灣證券交易": "證交所", "台灣證券交易": "證交所", "證交": "證交所",
    "證券櫃檯買賣中心": "櫃買中心", "櫃買": "櫃買中心",
}

# Filings that only name an English conference series ("Goldman Sachs Tech
# Tour") carry no 舉辦 verb for the patterns above to anchor on.
_HOST_FALLBACK = re.compile(
    r"(goldman sachs|j\.p\. ?morgan|jpmorgan|jp morgan|morgan stanley|citi'?s?"
    r"|ubs|hsbc|nomura|daiwa|macquarie|jefferies|clsa|bofa|bank of america"
    r"|merrill|fidelity|barclays|mizuho|deutsche|bnp)\b", re.I)


def _host(subject: str | None) -> str | None:
    """The broker hosting the session, or None for a self-hosted 法說會.

    Covers ~65% of the index; the rest are companies holding their own
    conference, where there is no host to name.
    """
    if not subject:
        return None
    text = _WS.sub(" ", html.unescape(subject)).strip()
    for pattern in _HOST_PATTERNS:
        m = pattern.search(text)
        if not m:
            continue
        name = _COHOST.sub("", _PARENS.sub("", m.group(1)).strip())
        if _CJK.search(name):
            name = _NUM_TAIL.sub("", name)
        if _NOT_A_HOST.search(_LEAD.sub("", name).strip()):
            continue
        name = _SUFFIX_EN.sub("", name)
        # Lead and suffix noise nest ("本公司受邀參加富邦證券舉辦"), so peel twice.
        for _ in range(2):
            name = _TRIM_EDGE.sub("", _SUFFIX.sub("", _LEAD.sub("", name)))
        # A mixed house (Horizon宏遠證券) reads better as its Chinese half.
        cjk = _ASCII_HEAD.sub("", name)
        if cjk and _CJK.search(cjk):
            name = cjk
        if not _ASCII.search(name):
            name = name.replace(" ", "")
        name = _HOST_ALIASES.get(name.lower(), name)
        if 1 < len(name) <= (16 if _ASCII.search(name) else 10):
            return name
    m = _HOST_FALLBACK.search(text)
    return _HOST_ALIASES.get(m.group(1).lower(), m.group(1)) if m else None


def _load_sentinel() -> tuple[date | None, set[str]]:
    try:
        with open(_SENTINEL, encoding="utf-8") as f:
            data = json.load(f)
        return date.fromisoformat(data["date"]), set(data.get("keys", []))
    except (OSError, ValueError, KeyError):
        return None, set()


def _save_sentinel(pushed: date, keys: set[str]) -> None:
    try:
        os.makedirs(os.path.dirname(_SENTINEL), exist_ok=True)
        with open(_SENTINEL, "w", encoding="utf-8") as f:
            json.dump({"date": pushed.isoformat(), "keys": sorted(keys)}, f)
    except OSError:
        pass


def _key(row: dict) -> str:
    return f"{row['stock_id']}|{row['conf_date'].isoformat()}|{row['file_zh']}"


def _fetch(cur, today: date) -> tuple[list[dict], set[str]]:
    """Rows inside the horizon, plus the keys of every future session."""
    cur.execute(
        """
        SELECT c.stock_id, s.name, c.conf_date, c.conf_time, c.market,
               c.subject, c.file_zh
        FROM tw.investor_conferences c
        LEFT JOIN tw.stocks s ON s.stock_id = c.stock_id
        WHERE c.conf_date >= %s
        ORDER BY c.conf_date, c.conf_time, c.stock_id
        """,
        (today,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    horizon = today + timedelta(days=_HORIZON_DAYS)
    return [r for r in rows if r["conf_date"] <= horizon], {_key(r) for r in rows}


def _date_blocks(rows: list[dict], today: date, seen: set[str],
                 mark_new: bool) -> list[str]:
    """One text block per conference date, sessions listed underneath."""
    blocks: list[str] = []
    for d in sorted({r["conf_date"] for r in rows}):
        left = _trading_days_until(today, d)
        when = "今日" if left == 0 else f"{left} 個交易日後"
        lines = [f"{d:%m/%d}（{_weekday_zh(d)}，{when}）"]
        for r in [x for x in rows if x["conf_date"] == d]:
            mark = "🆕" if mark_new and _key(r) not in seen else "  "
            venue = " 櫃" if r["market"] == "otc" else ""
            when_time = f"  {r['conf_time']}" if r["conf_time"] else ""
            host = _host(r["subject"])
            by = f"（{host}）" if host else ""
            lines.append(f"{mark} {r['stock_id']} {r.get('name') or ''}"
                         f"{when_time}{venue}{by}")
        blocks.append("\n".join(lines))
    return blocks


def _build_messages(blocks: list[str], count: int) -> list[str]:
    header = f"{_HEADER}　共 {count} 場"
    messages, cur = [], header
    for block in blocks:
        if len(cur) + 2 + len(block) <= _MAX_CHARS:
            cur = f"{cur}\n\n{block}"
        else:
            messages.append(cur)
            cur = f"{_HEADER}（續）\n\n{block}"
    messages.append(cur)
    return messages


def push_conference_calendar(*, dry_run: bool = False, force: bool = False) -> int:
    """Send today's calendar of upcoming conferences.

    Takes no date on purpose — see the module docstring's first guard.
    Returns 0 on success or a legitimate no-op, 2 when a send failed.
    """
    today = date.today()

    last_pushed, seen = _load_sentinel()
    if not force and last_pushed is not None and last_pushed >= today:
        print(f"push_conference_calendar: already pushed through {last_pushed}, skipping")
        return 0

    with get_cursor(commit=False) as cur:
        rows, future_keys = _fetch(cur, today)

    if not rows:
        print(f"push_conference_calendar: no conferences within "
              f"{_HORIZON_DAYS} days of {today}, nothing to send")
        return 0

    blocks = _date_blocks(rows, today, seen, mark_new=last_pushed is not None)
    messages = _build_messages(blocks, len(rows))

    if dry_run:
        print("push_conference_calendar: dry-run, would have sent:")
        print("\n\n----- 訊息分隔 -----\n\n".join(messages))
        return 0

    for msg in messages:
        if not send_sync(msg):
            print("push_conference_calendar: send_sync failed", file=sys.stderr)
            return 2

    _save_sentinel(today, future_keys)
    print(f"push_conference_calendar: sent {len(rows)} session(s) "
          f"in {len(messages)} message(s)")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m telegram_bot.push_conference_calendar",
        description="Push the two-week investor conference calendar to Telegram.",
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Build and print the message without sending.")
    p.add_argument("--force", action="store_true",
                   help="Send even if today was already pushed.")
    args = p.parse_args(argv)
    return push_conference_calendar(dry_run=args.dry_run, force=args.force)


if __name__ == "__main__":
    sys.exit(main())
