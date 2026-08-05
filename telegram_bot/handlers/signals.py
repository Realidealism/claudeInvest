"""`/signals` — list today's 6-signal-factory hits across the whole market.

Reads frontend/public/data/operations_intraday.json (refreshed by the 12:50
intraday snapshot cron). Unlike push_intraday_signals, this does NOT filter
by watchlist — it shows every hit in every signal kind, capped per kind so
the message fits Telegram's 4096-char limit.
"""

from __future__ import annotations

import json
from pathlib import Path

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes

from datetime import datetime, timedelta, timezone

from telegram_bot.auth import restricted
from telegram_bot.push_intraday_signals import _SIGNAL_LABEL, _SIGNAL_ORDER

_TPE_TZ = timezone(timedelta(hours=8))


def _format_snapshot_time(raw: str | None) -> str:
    """ISO timestamp → 'HH:MM' in Taipei time. Returns raw on parse failure
    (operations_intraday.json stores snapshot_time as an ISO string)."""
    if not raw:
        return ""
    try:
        return datetime.fromisoformat(raw).astimezone(_TPE_TZ).strftime("%H:%M")
    except (TypeError, ValueError):
        return str(raw)


def _format_hit(entry: dict) -> str:
    """Render one operations.json signal hit. Local copy decoupled from
    push_intraday_signals._format_hit, which now reads DB rows (stock_id)
    rather than the JSON entries (ticker)."""
    ticker = entry.get("ticker") or entry.get("stock_id") or "?"
    name = entry.get("name") or ""
    market = entry.get("market") or ""
    streak = entry.get("streak")
    streak_part = f"  連續 {streak} 日" if streak is not None else ""
    return f"  {ticker} {name}（{market}）{streak_part}"

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_OPERATIONS_JSON = _REPO_ROOT / "frontend" / "public" / "data" / "operations_intraday.json"
_POSITIONS_JSON = _REPO_ROOT / "frontend" / "public" / "data" / "positions_intraday.json"
_STANCE_JSON = _REPO_ROOT / "frontend" / "public" / "data" / "thermometer_stance.json"

PER_KIND_LIMIT = 100

_NEW_ENTRY_MARK = "🆕"
_DEFENSIVE_MARK = "⚠️"


def _collect_today_exits(positions_data: dict | None) -> list[dict]:
    """Flatten exited_long + exited_short buckets. Both buckets only contain
    positions whose exit_date == snapshot_date (per intraday_snapshot's
    _extract_position), so no extra date filter is needed here."""
    out: list[dict] = []
    if not positions_data:
        return out
    for bucket, side in (("exited_long", "long"), ("exited_short", "short")):
        for p in positions_data.get(bucket, []):
            out.append({**p, "_side": side})
    return out


def _format_exit_hit(position: dict) -> str:
    ticker = position.get("ticker", "?")
    name = position.get("name", "")
    market = position.get("market", "")
    side_zh = "多單" if position["_side"] == "long" else "空單"
    pnl = float(position.get("pnl_pct") or 0.0)
    bars = int(position.get("bars_held") or 0)
    reason = position.get("exit_reason") or ""
    reason_part = f"（{reason}）" if reason else ""
    return (
        f"  {ticker} {name}（{market}）"
        f"  {side_zh} {pnl:+.2f}% 持 {bars} 根{reason_part}"
    )


def _stance_defensive(snapshot_date: str) -> bool:
    """Is the live 攻防 stance defensive? Same sidecar the /market reply and the
    positions-page banner read. The date must match this batch of signals — a
    sidecar left over from a previous session says nothing about today."""
    try:
        with open(_STANCE_JSON, encoding="utf-8") as f:
            live = json.load(f)
    except (OSError, ValueError):
        return False
    stance = live.get("stance3") or live.get("stance")
    return live.get("as_of") == snapshot_date and stance == "防守"


def _build_position_index(positions_data: dict | None) -> dict[str, list[dict]]:
    """{ticker: [position dicts]} across all 4 buckets, so a single stock with
    e.g. both long-active and short-exited entries is still discoverable from
    either side."""
    index: dict[str, list[dict]] = {}
    if not positions_data:
        return index
    for bucket in ("long", "short", "exited_long", "exited_short"):
        for p in positions_data.get(bucket, []):
            index.setdefault(p["ticker"], []).append(p)
    return index


def _signal_sort_decoration(
    entry: dict,
    kind: str,
    position_index: dict[str, list[dict]],
    today: str,
) -> tuple[tuple, bool]:
    """Return ((group, dist_pct, ticker), is_new_entry).

    Group 0: a position whose entry_tier equals this signal kind landed today
    — that is the strategy actually opening a fresh trade off this hit, so it
    floats to the top with a marker.
    Group 1: everything else, ordered by absolute % distance from defense
    (closest first). Tickers without a position fall back to dist=+inf and
    are tiebroken by symbol."""
    ticker = entry.get("ticker", "")
    positions = position_index.get(ticker, [])

    is_new = any(
        p.get("entry_date") == today and p.get("entry_tier") == kind
        for p in positions
    )

    dist_pct = float("inf")
    for p in positions:
        defense = p.get("defense_price")
        current = p.get("current_close")
        if defense is None or current is None or float(defense) == 0:
            continue
        d = abs(float(current) - float(defense)) / float(defense)
        if d < dist_pct:
            dist_pct = d

    group = 0 if is_new else 1
    return (group, dist_pct, ticker), is_new


def _build_all_signals_message(
    operations: dict,
    positions_data: dict | None = None,
    per_kind_limit: int = PER_KIND_LIMIT,
) -> str:
    """Build the Chinese message listing all hits, capped per kind."""
    snapshot_date = operations.get("snapshot_date", "?")
    snapshot_time = operations.get("snapshot_time", "")
    signals = operations.get("signals", {})

    header = f"[今日訊號] {snapshot_date}"
    pretty_time = _format_snapshot_time(snapshot_time)
    if pretty_time:
        header += f"  快照於 {pretty_time}"

    position_index = _build_position_index(positions_data)
    stance_defensive = _stance_defensive(snapshot_date)

    sections: list[str] = []
    total_hits = 0
    for kind in _SIGNAL_ORDER:
        entries = signals.get(kind, [])
        if not entries:
            continue
        total_hits += len(entries)
        label = _SIGNAL_LABEL.get(kind, kind)

        decorated = [
            (_signal_sort_decoration(e, kind, position_index, snapshot_date), e)
            for e in entries
        ]
        decorated.sort(key=lambda x: x[0][0])

        shown = decorated[:per_kind_limit]
        body_lines: list[str] = []
        for (_, is_new), e in shown:
            line = _format_hit(e)
            if is_new:
                # _format_hit pads with 2 leading spaces; swap them for the marker.
                line = f"  {_NEW_ENTRY_MARK}" + line[1:]
                if kind == "buy" and stance_defensive:
                    line += f"  {_DEFENSIVE_MARK} 盤中防守"
            body_lines.append(line)
        if len(entries) > per_kind_limit:
            body_lines.append(f"  …其餘 {len(entries) - per_kind_limit} 檔")
        sections.append(f"{label}（{len(entries)} 檔）\n" + "\n".join(body_lines))

    # Today's actual position exits — sourced from positions JSON, not the
    # 6-signal buckets. Distinct concept: signals are "the condition fired",
    # exits are "the strategy actually closed a trade today" (often via the
    # locked defense price, which is not the same as a buy_flee/sell_flee
    # signal firing on this bar).
    exits = _collect_today_exits(positions_data)
    exits.sort(key=lambda p: float(p.get("pnl_pct") or 0.0))
    if exits:
        exit_lines = [_format_exit_hit(p) for p in exits]
        sections.append(f"今日實際出場（{len(exits)} 檔）\n" + "\n".join(exit_lines))

    if total_hits == 0 and not exits:
        return f"{header}\n\n今日無 6 訊號觸發。"
    return header + "\n\n" + "\n\n".join(sections)


def _load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def _load_operations() -> dict | None:
    return _load_json(_OPERATIONS_JSON)


def _load_positions() -> dict | None:
    return _load_json(_POSITIONS_JSON)


@restricted
async def cmd_signals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    operations = _load_operations()
    if operations is None:
        await update.message.reply_text(
            "找不到 operations_intraday.json — 請執行 /snapshot 或等 12:50 排程。"
        )
        return
    positions = _load_positions()
    await update.message.reply_text(_build_all_signals_message(operations, positions))


def register(application) -> None:
    application.add_handler(CommandHandler("signals", cmd_signals))
