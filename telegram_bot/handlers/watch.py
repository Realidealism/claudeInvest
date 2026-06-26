"""`/watch` — manage the TW watchlist stored in portfolio.watchlist.

Subcommands:
    /watch              → list current TW watchlist (sorted by symbol)
    /watch add <tk>     → INSERT (market='TW', symbol=<tk>); idempotent on UNIQUE
    /watch add          → enter batch-add mode; every subsequent plain-text
                          message is parsed as one-or-more tickers (split on
                          whitespace / , / ;) until `/watch done`.
    /watch remove <tk>  → DELETE matching (market='TW', symbol=<tk>)
    /watch remove       → enter batch-remove mode (mirror of batch-add).
    /watch done         → exit batch mode.
    /watch score        → one-line per-stock summary (positions-first)
    /watch help         → usage hint
"""

from __future__ import annotations

import re
import time

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.ext import (
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from db.connection import get_cursor
from telegram_bot.auth import restricted
from telegram_bot.handlers.status import (
    BTN_MARKET,
    BTN_SIGNALS,
    BTN_WATCH,
    BTN_WATCH_ADD,
    BTN_WATCH_REMOVE,
)
from utils.classifier import classify_tw_security

# Batch-mode state lives on context.user_data. Idle timeout prevents stray
# next-day "2330" messages from being silently added.
_MODE_KEY = "_watch_mode"
_MODE_TS_KEY = "_watch_mode_ts"
_MODE_TIMEOUT_S = 600

_TOKEN_SPLIT_RE = re.compile(r"[\s,，;；]+")

# Callback data for the inline "完成" button attached to the batch-mode prompt.
_DONE_CALLBACK = "watch_batch_done"

_BATCH_DONE_MARKUP = InlineKeyboardMarkup(
    [[InlineKeyboardButton("✅ 完成", callback_data=_DONE_CALLBACK)]]
)


def _list_watchlist() -> list[tuple[str, str | None, str | None]]:
    """Return [(symbol, name, notes), ...] ordered by symbol."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT w.symbol, s.name, w.notes
            FROM portfolio.watchlist w
            LEFT JOIN tw.stocks s ON s.stock_id = w.symbol
            WHERE w.market = 'TW'
            ORDER BY w.symbol
            """
        )
        return [(r["symbol"], r.get("name"), r.get("notes")) for r in cur.fetchall()]


def _lookup_stock_in_db(symbol: str) -> dict | None:
    """Return {name, market} for an active stock, or None if not found."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT name, market FROM tw.stocks "
            "WHERE stock_id = %s AND is_active = TRUE LIMIT 1",
            (symbol,),
        )
        r = cur.fetchone()
        return dict(r) if r else None


def _add(symbol: str) -> str:
    """Per-ticker add. Validates format → DB existence → upsert."""
    if classify_tw_security(symbol) is None:
        return f"❌ {symbol} 不是有效的台股代號格式（權證 / TDR / 債券？）"
    stock = _lookup_stock_in_db(symbol)
    if stock is None:
        return f"❌ {symbol} 資料庫找不到（可能已下市或代號錯誤）"
    name = stock.get("name") or ""
    with get_cursor() as cur:
        cur.execute(
            "INSERT INTO portfolio.watchlist (market, symbol) VALUES ('TW', %s) "
            "ON CONFLICT (market, symbol) DO NOTHING RETURNING id",
            (symbol,),
        )
        row = cur.fetchone()
    return f"✅ {symbol} {name} 已加入" if row else f"ℹ️ {symbol} {name} 已在追蹤清單中"


def _remove(symbol: str) -> str:
    """Per-ticker remove. Format-checks skipped — let any typo fail naturally
    with 「不在追蹤清單中」 rather than rejecting up front."""
    stock = _lookup_stock_in_db(symbol)
    name_part = f" {stock['name']}" if stock and stock.get("name") else ""
    with get_cursor() as cur:
        cur.execute(
            "DELETE FROM portfolio.watchlist "
            "WHERE market = 'TW' AND symbol = %s RETURNING id",
            (symbol,),
        )
        row = cur.fetchone()
    return (
        f"✅ {symbol}{name_part} 已移除"
        if row
        else f"ℹ️ {symbol}{name_part} 不在追蹤清單中"
    )


def _format_list(items: list[tuple[str, str | None, str | None]]) -> str:
    if not items:
        return "[追蹤清單] 目前為空\n加入方式：/watch add <股號>"
    lines = [f"[追蹤清單] 共 {len(items)} 檔台股"]
    for sym, name, notes in items:
        head = f"  {sym} {name}" if name else f"  {sym}"
        lines.append(head + (f"  — {notes}" if notes else ""))
    return "\n".join(lines)


_HELP = (
    "用法：\n"
    "  /watch              — 列出全部\n"
    "  /watch add <股號>    — 一次加入單一檔\n"
    "  /watch add          — 進入批次加入模式（後續訊息每段空白/換行為一檔）\n"
    "  /watch remove <股號> — 一次移除單一檔\n"
    "  /watch remove       — 進入批次移除模式\n"
    "  /watch done         — 結束批次模式\n"
    "  /watch score        — 一次評分全部追蹤股\n"
    "  /watch help         — 顯示本說明"
)


def _enter_batch_mode(context: ContextTypes.DEFAULT_TYPE, mode: str) -> None:
    context.user_data[_MODE_KEY] = mode
    context.user_data[_MODE_TS_KEY] = time.time()


def _clear_batch_mode(context: ContextTypes.DEFAULT_TYPE) -> str | None:
    """Returns the previously-set mode (or None) and clears state."""
    prev = context.user_data.pop(_MODE_KEY, None)
    context.user_data.pop(_MODE_TS_KEY, None)
    return prev


def _batch_mode_is_fresh(context: ContextTypes.DEFAULT_TYPE) -> bool:
    ts = context.user_data.get(_MODE_TS_KEY)
    return bool(ts and (time.time() - ts) <= _MODE_TIMEOUT_S)


async def _dispatch_watch(
    update: Update, context: ContextTypes.DEFAULT_TYPE, args: list[str]
) -> None:
    """Core dispatch shared by /watch and the ➕ / ➖ / ✅ keyboard buttons.
    Already-authorized by the caller's @restricted wrapper."""
    if not args:
        items = _list_watchlist()
        await update.message.reply_text(_format_list(items))
        return

    sub = args[0].lower()

    if sub == "help":
        await update.message.reply_text(_HELP)
        return

    if sub == "done":
        prev = _clear_batch_mode(context)
        if prev == "add":
            await update.message.reply_text("已結束批次加入模式。")
        elif prev == "remove":
            await update.message.reply_text("已結束批次移除模式。")
        else:
            await update.message.reply_text("目前沒有進行中的批次模式。")
        return

    if sub == "score":
        # Lazy import to avoid load-time circular dependency between
        # watch.py and score.py (both import from telegram_bot.auth etc.).
        from telegram_bot.handlers.score import build_watchlist_summary
        items = _list_watchlist()
        tickers = [sym for sym, _, _ in items]
        await update.message.reply_text(build_watchlist_summary(tickers))
        return

    if sub in ("add", "remove", "rm", "del"):
        action = "add" if sub == "add" else "remove"
        if len(args) >= 2:
            # One-shot: process all extra args as tickers.
            for ticker in args[1:]:
                result = _add(ticker) if action == "add" else _remove(ticker)
                await update.message.reply_text(result)
            return
        # No ticker → enter batch mode.
        _enter_batch_mode(context, action)
        verb = "加入" if action == "add" else "移除"
        await update.message.reply_text(
            f"進入批次{verb}模式。\n"
            f"請輸入要{verb}的股號（可多檔，空格或換行分隔）。\n"
            f"完成後按下方「✅ 完成」或輸入 /watch done。",
            reply_markup=_BATCH_DONE_MARKUP,
        )
        return

    await update.message.reply_text(f"未知子指令「{sub}」。\n\n{_HELP}")


@restricted
async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = [a.strip() for a in (context.args or []) if a.strip()]
    await _dispatch_watch(update, context, args)


@restricted
async def cmd_watch_add_button(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    await _dispatch_watch(update, context, ["add"])


@restricted
async def cmd_watch_remove_button(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    await _dispatch_watch(update, context, ["remove"])


@restricted
async def cmd_watch_done_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Handles taps on the inline 「✅ 完成」 attached to the batch prompt.
    Inline buttons stay visible inside the chat even when the soft keyboard
    is open, so the user always has a way to exit mid-typing."""
    query = update.callback_query
    await query.answer()
    prev = _clear_batch_mode(context)
    if prev == "add":
        msg = "已結束批次加入模式。"
    elif prev == "remove":
        msg = "已結束批次移除模式。"
    else:
        msg = "目前沒有進行中的批次模式。"
    # Strip the inline button off the original prompt so the user can't tap
    # it again after the batch already ended.
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    # query.message can be None when the original prompt was deleted.
    if query.message is not None:
        await query.message.reply_text(msg)


@restricted
async def cmd_watch_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """MessageHandler callback: route plain-text messages to the active batch
    mode, if any. Silently no-op when no batch mode is active or it has
    timed out, so casual chat doesn't accidentally hit the watchlist."""
    mode = context.user_data.get(_MODE_KEY)
    if not mode:
        return
    if not _batch_mode_is_fresh(context):
        _clear_batch_mode(context)
        return

    text = (update.message.text or "").strip()
    if not text:
        return
    tokens = [t for t in _TOKEN_SPLIT_RE.split(text) if t]
    if not tokens:
        return

    # Refresh the idle clock on activity so a long add session doesn't expire
    # mid-stream.
    context.user_data[_MODE_TS_KEY] = time.time()

    for tok in tokens:
        result = _add(tok) if mode == "add" else _remove(tok)
        await update.message.reply_text(result)


@restricted
async def cmd_watch_score(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Direct entry point for the 📊 追蹤清單 keyboard button — equivalent
    to typing `/watch score` but skips the subcommand dispatch."""
    from telegram_bot.handlers.score import build_watchlist_summary
    items = _list_watchlist()
    tickers = [sym for sym, _, _ in items]
    await update.message.reply_text(build_watchlist_summary(tickers))


def register(application) -> None:
    application.add_handler(CommandHandler("watch", cmd_watch))
    application.add_handler(
        CallbackQueryHandler(cmd_watch_done_callback, pattern=rf"^{_DONE_CALLBACK}$")
    )

    # Plain-text router for batch mode. Excludes every keyboard button so
    # 🌡️ / 📊 / 📈 / ➕ / ➖ presses stay routed to their dedicated handlers
    # in keyboard_router.py instead of being parsed as tickers.
    button_alts = "|".join(
        re.escape(b)
        for b in (
            BTN_MARKET,
            BTN_WATCH,
            BTN_SIGNALS,
            BTN_WATCH_ADD,
            BTN_WATCH_REMOVE,
        )
    )
    ticker_filter = (
        filters.TEXT
        & ~filters.COMMAND
        & ~filters.Regex(rf"^({button_alts})$")
    )
    application.add_handler(MessageHandler(ticker_filter, cmd_watch_input))
