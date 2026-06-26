"""`/social` — manage Threads / YouTube subscriptions monitored by the
``social_monitor`` watcher.

Subcommands:
    /social                          → list all subscriptions
    /social add threads <user>       → add a Threads username (sans @)
    /social add youtube <id_or_url>  → add a YouTube channel (UC… id, @handle,
                                       or full channel URL — handle is resolved
                                       by scraping the canonical link)
    /social remove <id>              → delete by primary key (see list)
    /social poll                     → trigger an immediate poll cycle
    /social help                     → usage hint

Phase 1 — subscription CRUD + a manual poll button. The actual poll loop
lives in ``telegram_bot/watchers/social_monitor.py`` and is wired into PTB
JobQueue from ``app.py``.
"""

from __future__ import annotations

import re

import requests
from telegram import Update
from telegram.ext import CommandHandler, ContextTypes

from db.connection import get_cursor
from telegram_bot.auth import restricted

# ── YouTube channel-id resolution ─────────────────────────────────────────

_YT_CHANNEL_ID_RE = re.compile(r"^UC[0-9A-Za-z_-]{20,30}$")
_YT_HANDLE_RE = re.compile(r"^@?[\w.-]{3,40}$")
_YT_CANONICAL_CHANNEL_RE = re.compile(
    r'<link\s+rel="canonical"\s+href="https://www\.youtube\.com/channel/(UC[0-9A-Za-z_-]+)"'
)
# YouTube no longer emits <meta name="title">; primary pattern is now the
# ytInitialData blob, with the <title> tag as a last-resort fallback.
_YT_CHANNEL_TITLE_RES = (
    re.compile(r'"channelMetadataRenderer":\{"title":"([^"]+)"'),
    re.compile(r"<title>([^<]+?)\s*-\s*YouTube</title>"),
)
_YT_USER_AGENT = (
    "Mozilla/5.0 (compatible; InvestBot/1.0; +https://github.com/Realidealism)"
)


def _yt_resolve(channel_input: str) -> tuple[str | None, str | None]:
    """Return (channel_id, display_name). Accepts raw UC… id, @handle, or
    a YouTube channel URL. Scrapes the channel landing page for the canonical
    link when handed a handle/URL."""
    s = channel_input.strip()
    # Already a channel ID?
    if _YT_CHANNEL_ID_RE.match(s):
        return s, _yt_fetch_title(f"https://www.youtube.com/channel/{s}")
    # URL: extract the path
    if s.startswith("http"):
        url = s
    elif _YT_HANDLE_RE.match(s):
        handle = s.lstrip("@")
        url = f"https://www.youtube.com/@{handle}"
    else:
        return None, None
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": _YT_USER_AGENT, "Accept-Language": "zh-TW,en;q=0.8"},
            timeout=10,
        )
    except requests.RequestException:
        return None, None
    if resp.status_code != 200:
        return None, None
    m = _YT_CANONICAL_CHANNEL_RE.search(resp.text)
    if not m:
        return None, None
    channel_id = m.group(1)
    display = _extract_channel_title(resp.text)
    return channel_id, display


def _extract_channel_title(html: str) -> str | None:
    for rx in _YT_CHANNEL_TITLE_RES:
        m = rx.search(html)
        if m:
            return m.group(1).strip() or None
    return None


def _yt_fetch_title(url: str) -> str | None:
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": _YT_USER_AGENT, "Accept-Language": "zh-TW,en;q=0.8"},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        return _extract_channel_title(resp.text)
    except requests.RequestException:
        return None


# ── DB ops ────────────────────────────────────────────────────────────────


def _list_subs() -> list[dict]:
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT id, platform, identifier, display_name, last_polled_at
            FROM tw.social_subscriptions
            ORDER BY platform, identifier
            """
        )
        return [dict(r) for r in cur.fetchall()]


def _insert_sub(platform: str, identifier: str, display_name: str | None) -> tuple[bool, int | None]:
    """Returns (created, id). created=False when row already existed."""
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO tw.social_subscriptions (platform, identifier, display_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (platform, identifier) DO NOTHING
            RETURNING id
            """,
            (platform, identifier, display_name),
        )
        row = cur.fetchone()
        if row:
            return True, row["id"]
        cur.execute(
            "SELECT id FROM tw.social_subscriptions "
            "WHERE platform = %s AND identifier = %s",
            (platform, identifier),
        )
        existing = cur.fetchone()
        return False, existing["id"] if existing else None


def _remove_sub(sub_id: int) -> bool:
    with get_cursor() as cur:
        cur.execute(
            "DELETE FROM tw.social_subscriptions WHERE id = %s RETURNING id",
            (sub_id,),
        )
        return cur.fetchone() is not None


# ── Formatting ────────────────────────────────────────────────────────────


_PLATFORM_LABEL = {"threads": "Threads", "youtube": "YouTube"}


def _format_subs(subs: list[dict]) -> str:
    if not subs:
        return "[社群訂閱] 目前為空\n加入方式：/social add threads <user>"
    lines = [f"[社群訂閱] 共 {len(subs)} 個來源"]
    for s in subs:
        label = _PLATFORM_LABEL.get(s["platform"], s["platform"])
        display = s.get("display_name") or s["identifier"]
        polled = (
            s["last_polled_at"].strftime("%m-%d %H:%M")
            if s.get("last_polled_at")
            else "從未"
        )
        lines.append(f"  #{s['id']}  {label}  {display}  ({s['identifier']})  上次抓取：{polled}")
    return "\n".join(lines)


_HELP = (
    "用法：\n"
    "  /social                          — 列出全部\n"
    "  /social add threads <user>       — 加 Threads 帳號（不含 @）\n"
    "  /social add youtube <id|@handle|URL> — 加 YouTube 頻道\n"
    "  /social remove <id>              — 移除（id 由 /social 取得）\n"
    "  /social poll                     — 立即跑一次抓取\n"
    "  /social help                     — 顯示本說明"
)


# ── Handlers ──────────────────────────────────────────────────────────────


@restricted
async def cmd_social(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = [a.strip() for a in (context.args or []) if a.strip()]

    if not args:
        await update.message.reply_text(_format_subs(_list_subs()))
        return

    sub = args[0].lower()

    if sub == "help":
        await update.message.reply_text(_HELP)
        return

    if sub == "add":
        if len(args) < 3:
            await update.message.reply_text(
                "用法：/social add threads <user> 或 /social add youtube <id|@handle|URL>"
            )
            return
        platform = args[1].lower()
        target = args[2]
        if platform == "threads":
            user = target.lstrip("@")
            if not _YT_HANDLE_RE.match(user):
                await update.message.reply_text(
                    f"❌ Threads username 看起來不對：「{user}」"
                )
                return
            created, sub_id = _insert_sub("threads", user, None)
            verb = "已加入" if created else "已在訂閱中"
            await update.message.reply_text(
                f"✅ Threads {user}：{verb}（#{sub_id}）"
            )
        elif platform == "youtube":
            await update.message.reply_text("🔍 解析中…")
            channel_id, display_name = _yt_resolve(target)
            if channel_id is None:
                await update.message.reply_text(
                    f"❌ YouTube 解析失敗：「{target}」。試試直接傳 UC… 開頭的 channel ID。"
                )
                return
            created, sub_id = _insert_sub("youtube", channel_id, display_name)
            verb = "已加入" if created else "已在訂閱中"
            name_part = f"「{display_name}」" if display_name else ""
            await update.message.reply_text(
                f"✅ YouTube {name_part} ({channel_id})：{verb}（#{sub_id}）"
            )
        else:
            await update.message.reply_text(
                f"❌ 未知平台「{platform}」。支援：threads / youtube"
            )
        return

    if sub in ("remove", "rm", "del"):
        if len(args) < 2:
            await update.message.reply_text("用法：/social remove <id>")
            return
        try:
            sub_id = int(args[1])
        except ValueError:
            await update.message.reply_text(f"❌ id 必須是數字：「{args[1]}」")
            return
        ok = _remove_sub(sub_id)
        await update.message.reply_text(
            f"✅ #{sub_id} 已移除" if ok else f"ℹ️ #{sub_id} 不存在"
        )
        return

    if sub == "poll":
        # Lazy import — the watcher pulls Postgres + http and we don't want
        # to fail handler registration if its deps are missing.
        from telegram_bot.watchers.social_monitor import _run_check

        await update.message.reply_text("⏳ 立即抓取中…")
        try:
            result = _run_check(respect_gates=True)
        except Exception as exc:
            await update.message.reply_text(f"❌ 抓取失敗：{exc}")
            return
        await update.message.reply_text(f"✅ {result}")
        return

    await update.message.reply_text(f"未知子指令「{sub}」。\n\n{_HELP}")


def register(application) -> None:
    application.add_handler(CommandHandler("social", cmd_social))
