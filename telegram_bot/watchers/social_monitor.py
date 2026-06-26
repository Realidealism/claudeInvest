"""Phase-1 social-monitor watcher.

Polls every 30 minutes. For each row in ``tw.social_subscriptions`` it
fetches the source feed:

  • Threads  → ``https://rsshub.app/threads/user/<user>`` (RSS)
  • YouTube  → ``https://www.youtube.com/feeds/videos.xml?channel_id=<id>``

Items newer than ``last_seen_id`` are pushed to Telegram as raw notifications.
Phase 2 will swap the raw push for Claude-based extraction + sentiment;
Phase 3 will inline /score output for any tickers found.

CLI (force-poll, bypasses the trading-hours gate):
    python -m telegram_bot.watchers.social_monitor
"""

from __future__ import annotations

import asyncio
import logging
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import requests

from db.connection import get_cursor
from telegram_bot.notify import send_sync

logger = logging.getLogger(__name__)

# ── Tunables ──────────────────────────────────────────────────────────────

PERIOD_S = 1800             # 30 minutes
HTTP_TIMEOUT = 12
MAX_NEW_PER_RUN = 5         # truncate to avoid flooding when a feed is new

JOB_NAME = "social_monitor"

_TPE_TZ = timezone(timedelta(hours=8))
_RSSHUB_THREADS = "https://rsshub.app/threads/user/{user}"
_YOUTUBE_FEED = "https://www.youtube.com/feeds/videos.xml?channel_id={cid}"
_USER_AGENT = (
    "Mozilla/5.0 (compatible; InvestBot/1.0; +https://github.com/Realidealism)"
)

_ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}


# ── Helpers ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class FeedItem:
    item_id: str       # platform-specific unique id (video_id / post URL)
    title: str
    url: str
    published: str | None      # ISO string when available
    body: str          # plain text body (best-effort)


def _now_tpe() -> datetime:
    return datetime.now(_TPE_TZ)


def _http_get(url: str) -> str | None:
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": _USER_AGENT, "Accept-Language": "zh-TW,en;q=0.8"},
            timeout=HTTP_TIMEOUT,
        )
    except requests.RequestException as exc:
        logger.warning("social_monitor: GET %s failed: %s", url, exc)
        return None
    if resp.status_code != 200:
        logger.warning(
            "social_monitor: GET %s → HTTP %s", url, resp.status_code
        )
        return None
    return resp.text


# ── Feed parsers ──────────────────────────────────────────────────────────


def _parse_youtube_feed(xml_text: str) -> list[FeedItem]:
    """YouTube /feeds/videos.xml is Atom. Each <entry> has <yt:videoId>,
    <title>, <link>, <published>, <media:description>."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        logger.warning("social_monitor: youtube xml parse failed: %s", exc)
        return []
    items: list[FeedItem] = []
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "yt":   "http://www.youtube.com/xml/schemas/2015",
        "media": "http://search.yahoo.com/mrss/",
    }
    for entry in root.findall("atom:entry", ns):
        vid_el = entry.find("yt:videoId", ns)
        if vid_el is None or not vid_el.text:
            continue
        video_id = vid_el.text.strip()
        title_el = entry.find("atom:title", ns)
        link_el = entry.find("atom:link", ns)
        pub_el = entry.find("atom:published", ns)
        desc_el = entry.find(".//media:description", ns)
        items.append(
            FeedItem(
                item_id=video_id,
                title=(title_el.text or "").strip() if title_el is not None else "",
                url=(link_el.get("href") if link_el is not None else "") or "",
                published=(pub_el.text or "").strip() if pub_el is not None else None,
                body=(desc_el.text or "").strip() if desc_el is not None else "",
            )
        )
    return items


def _parse_rss_threads(xml_text: str) -> list[FeedItem]:
    """RSSHub serves an RSS-2.0 channel: <item><guid>, <title>, <link>,
    <pubDate>, <description>."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        logger.warning("social_monitor: threads rss parse failed: %s", exc)
        return []
    items: list[FeedItem] = []
    channel = root.find("channel")
    if channel is None:
        return items
    for it in channel.findall("item"):
        guid_el = it.find("guid")
        link_el = it.find("link")
        item_id = (
            (guid_el.text or "").strip() if guid_el is not None and guid_el.text else ""
        )
        if not item_id and link_el is not None and link_el.text:
            item_id = link_el.text.strip()
        if not item_id:
            continue
        title_el = it.find("title")
        pub_el = it.find("pubDate")
        desc_el = it.find("description")
        items.append(
            FeedItem(
                item_id=item_id,
                title=(title_el.text or "").strip() if title_el is not None else "",
                url=(link_el.text or "").strip() if link_el is not None else "",
                published=(pub_el.text or "").strip() if pub_el is not None else None,
                body=(desc_el.text or "").strip() if desc_el is not None else "",
            )
        )
    return items


# ── DB ops ────────────────────────────────────────────────────────────────


def _load_subscriptions() -> list[dict]:
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT id, platform, identifier, display_name, last_seen_id
            FROM tw.social_subscriptions
            ORDER BY id
            """
        )
        return [dict(r) for r in cur.fetchall()]


def _update_seen(sub_id: int, last_seen_id: str, polled_at: datetime) -> None:
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE tw.social_subscriptions
            SET last_seen_id = %s, last_polled_at = %s
            WHERE id = %s
            """,
            (last_seen_id, polled_at, sub_id),
        )


def _claim_item(sub_id: int, item_id: str) -> bool:
    """Atomically reserve (sub_id, item_id) for pushing. Returns True only for
    the caller that wins the insert; a concurrent or prior run that already
    claimed it gets False. This is the real dedup gate — last_seen_id alone
    races because it's only advanced at the end of a poll cycle."""
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO tw.social_pushed (sub_id, item_id)
            VALUES (%s, %s)
            ON CONFLICT (sub_id, item_id) DO NOTHING
            """,
            (sub_id, item_id),
        )
        return cur.rowcount > 0


def _unclaim_item(sub_id: int, item_id: str) -> None:
    """Release a claim so the item is retried next cycle (used when the send
    failed after claiming)."""
    with get_cursor() as cur:
        cur.execute(
            "DELETE FROM tw.social_pushed WHERE sub_id = %s AND item_id = %s",
            (sub_id, item_id),
        )


# ── Per-source poll ───────────────────────────────────────────────────────


def _poll_source(sub: dict) -> tuple[list[FeedItem], str | None]:
    """Returns (new_items_oldest_to_newest, latest_item_id_for_bookmark)."""
    platform = sub["platform"]
    identifier = sub["identifier"]
    if platform == "youtube":
        xml = _http_get(_YOUTUBE_FEED.format(cid=identifier))
        items = _parse_youtube_feed(xml) if xml else []
    elif platform == "threads":
        xml = _http_get(_RSSHUB_THREADS.format(user=identifier))
        items = _parse_rss_threads(xml) if xml else []
    else:
        logger.warning("social_monitor: unsupported platform %s", platform)
        return [], None

    if not items:
        return [], None

    # Feeds arrive newest-first. We want to push oldest-first so notification
    # order matches chronological order.
    last_seen = sub.get("last_seen_id")
    new_items: list[FeedItem] = []
    for it in items:
        if last_seen and it.item_id == last_seen:
            break
        new_items.append(it)
    # If this is the first poll (no last_seen), don't replay history —
    # bookmark to the latest entry and emit nothing.
    if not last_seen:
        return [], items[0].item_id

    new_items.reverse()  # oldest first
    if len(new_items) > MAX_NEW_PER_RUN:
        new_items = new_items[-MAX_NEW_PER_RUN:]
    return new_items, items[0].item_id


# ── Analysis + /score inlining (Phase 2/3) ────────────────────────────────


_SENTIMENT_EMOJI = {
    "看多": "🔴",
    "看空": "🟢",
    "中立": "⚪",
    "不明": "⚪",
}

# Cap inlined /score replies per push so a video referencing 20 stocks
# doesn't blow Telegram's 4096-char message limit.
_MAX_SCORE_INLINE = 3


def _assemble_text_for_analysis(item: FeedItem) -> str:
    """Concatenate title + body for text-only analysis (Threads, or YouTube
    fallback when the video Part path fails)."""
    pieces: list[str] = []
    if item.title:
        pieces.append(f"標題：{item.title}")
    if item.body:
        pieces.append(f"內容：{item.body}")
    return "\n\n".join(pieces)


def _format_analysis(analysis) -> list[str]:
    """Render the Gemini Analysis into message lines (without header/URL)."""
    lines: list[str] = []
    if analysis.summary:
        lines.append(f"📝 {analysis.summary}")
    key_points = getattr(analysis, "key_points", None) or []
    if key_points:
        lines.append("")
        lines.append("🎯 重點：")
        for i, p in enumerate(key_points, start=1):
            lines.append(f"  {i}. {p}")
    concepts = getattr(analysis, "investment_concepts", "") or ""
    if concepts:
        lines.append("")
        lines.append("💡 投資概念：")
        lines.append(f"  {concepts}")
    quotes = getattr(analysis, "quotes", None) or []
    if quotes:
        lines.append("")
        lines.append("💬 金句：")
        for q in quotes:
            lines.append(f"  ・{q}")
    lines.append("")
    gen_emoji = _SENTIMENT_EMOJI.get(analysis.general_sentiment, "")
    lines.append(f"整體傾向：{gen_emoji} {analysis.general_sentiment}")
    if analysis.mentions:
        lines.append("提到個股：")
        for m in analysis.mentions:
            tk_part = m.ticker or "—"
            name_part = f" {m.name}" if m.name else ""
            emoji = _SENTIMENT_EMOJI.get(m.sentiment, "")
            line = f"  {emoji} {tk_part}{name_part}  {m.sentiment}"
            if m.reasoning:
                line += f"（{m.reasoning}）"
            lines.append(line)
    else:
        lines.append("提到個股：無")
    return lines


def _inline_scores(mentions) -> list[str]:
    """For each ticker that Gemini extracted, run /score's reply builder and
    append the result inline. Capped at _MAX_SCORE_INLINE entries."""
    tickers: list[str] = []
    seen: set[str] = set()
    for m in mentions:
        if m.ticker and m.ticker not in seen:
            seen.add(m.ticker)
            tickers.append(m.ticker)
        if len(tickers) >= _MAX_SCORE_INLINE:
            break
    if not tickers:
        return []
    try:
        from telegram_bot.handlers.score import _build_reply
    except Exception as exc:
        logger.warning("social_monitor: cannot import /score builder: %s", exc)
        return []
    blocks: list[str] = []
    for t in tickers:
        try:
            blocks.append(_build_reply(t))
        except Exception as exc:
            logger.warning("/score(%s) inline build failed: %s", t, exc)
    return blocks


# ── Notification ──────────────────────────────────────────────────────────


def _build_message(sub: dict, item: FeedItem) -> tuple[str, list[str]]:
    """Returns (primary_message, follow_up_score_blocks).

    Each /score block is sent as a separate Telegram message — keeps the
    primary digest compact and avoids hitting the 4096-char limit when
    multiple stocks are referenced."""
    label = "Threads" if sub["platform"] == "threads" else "YouTube"
    author = sub.get("display_name") or sub["identifier"]
    head = f"[社群動態] {label} · {author}"
    if item.published:
        head += f"  ({item.published})"
    lines: list[str] = [head]
    if item.title:
        lines.append(f"📰 {item.title}")

    score_blocks: list[str] = []

    # Run Gemini analysis. YouTube goes through the native video-Part path
    # (model watches the video directly, no transcript needed). Threads and
    # any future text-only sources go through the text path. If the YouTube
    # path fails (quota, transient API error), fall back to text-only on
    # title+description; if that also fails, fall back to raw snippet push.
    analysis = None
    try:
        if sub["platform"] == "youtube":
            from analysis.social_llm import analyze_youtube_video, analyze_post

            analysis = analyze_youtube_video(item.item_id, item.title, item.body)
            if analysis is None:
                raw = _assemble_text_for_analysis(item)
                analysis = analyze_post(raw) if raw else None
        else:
            from analysis.social_llm import analyze_post

            raw = _assemble_text_for_analysis(item)
            analysis = analyze_post(raw) if raw else None
    except Exception as exc:
        logger.warning("social_monitor: analyze crashed: %s", exc)
        analysis = None

    if analysis is not None:
        lines.extend(_format_analysis(analysis))
        score_blocks = _inline_scores(analysis.mentions)
    elif item.body:
        snippet = item.body.strip().replace("\r", "")
        if len(snippet) > 400:
            snippet = snippet[:400] + "…"
        lines.append(snippet)

    if item.url:
        lines.append(item.url)

    return "\n".join(lines), score_blocks


# ── Core check ────────────────────────────────────────────────────────────


def _run_check(*, respect_gates: bool) -> str:
    """Synchronous poll cycle — used by both JobQueue and CLI/manual trigger."""
    now = _now_tpe()
    subs = _load_subscriptions()
    if not subs:
        return "no subscriptions"

    total_new = 0
    sent = 0
    for sub in subs:
        new_items, latest_id = _poll_source(sub)
        if latest_id is None:
            continue
        total_new += len(new_items)
        for item in new_items:
            # Claim before the (slow) Gemini build so a concurrent run neither
            # double-pushes nor double-analyzes the same item.
            if respect_gates and not _claim_item(sub["id"], item.item_id):
                continue
            primary, score_blocks = _build_message(sub, item)
            if respect_gates:
                ok = send_sync(primary)
                if ok:
                    sent += 1
                    for block in score_blocks:
                        send_sync(block)
                else:
                    _unclaim_item(sub["id"], item.item_id)
            else:
                sent += 1
        _update_seen(sub["id"], latest_id, now)

    return (
        f"polled {len(subs)} sub(s); new={total_new}, sent={sent}"
        if respect_gates
        else f"polled {len(subs)} sub(s); new={total_new} (dry-run)"
    )


async def check_once(context) -> None:  # noqa: ANN001 — PTB context
    try:
        status = await asyncio.to_thread(_run_check, respect_gates=True)
        logger.info("social_monitor: %s", status)
    except Exception as exc:
        logger.exception("social_monitor crashed: %s", exc)


# ── Wiring ────────────────────────────────────────────────────────────────


def register(application) -> None:  # noqa: ANN001 — PTB Application
    jq = application.job_queue
    if jq is None:
        logger.warning(
            "JobQueue unavailable; social_monitor not scheduled. "
            "Install python-telegram-bot[job-queue]."
        )
        return
    jq.run_repeating(check_once, interval=PERIOD_S, first=60, name=JOB_NAME)
    logger.info("Scheduled %s every %ds", JOB_NAME, PERIOD_S)


# ── CLI ───────────────────────────────────────────────────────────────────


def _cli() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    print(_run_check(respect_gates=False))
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
