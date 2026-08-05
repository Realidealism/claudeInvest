"""Intraday thermometer stance watcher — pushes when the market flips defensive.

Polls every 5 minutes during trading hours and recomputes the live 攻防 stance
itself (1.6s: _compute_volume_scale + build_intraday_stance) rather than reading
thermometer_stance.json, whose writer (the snapshot daemon) only lands a new pass
every 40-80 minutes.

Three transitions push, everything else stays silent:
  • not-defensive → 防守   : 攻防轉防守, listing today's fresh 波段多 entries
  • 防守 (still) + new 波段多 : 防守中新進波段多, one push per ticker
  • 防守 → not-defensive   : 攻防解除

觀望 is not a warning state (攻擊 ↔ 觀望 flapping would be noisy). Today's long
entries come from positions_intraday.json; only entry_tier=buy is listed in full
because buy is the signal the thermometer gate actually governs (v359) — pick /
sell_flee entering a weak market is their edge, so they are only counted.

CLI:
    python -m telegram_bot.watchers.stance_alert --dry-run   # print only
    python -m telegram_bot.watchers.stance_alert             # really pushes
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path

from db.connection import get_cursor
from telegram_bot.notify import send_sync

logger = logging.getLogger(__name__)

# ── Tunables ──────────────────────────────────────────────────────────────

PERIOD_S = 300              # job cadence

DEFENSIVE = "防守"

_TPE_TZ = timezone(timedelta(hours=8))
_SESSION_OPEN = dtime(hour=9, minute=5)
_SESSION_CLOSE = dtime(hour=13, minute=30)

JOB_NAME = "stance_alert"

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_POSITIONS_JSON = _REPO_ROOT / "frontend" / "public" / "data" / "positions_intraday.json"
_STATE_FILE = _REPO_ROOT / "logs" / ".stance_alert_state"

# Long-side tier labels, same wording as the frontend's TIER_LABEL.
_LONG_TIERS = {"buy": "波段多", "pick": "抄底", "sell_flee": "空轉多"}

_STANCE_EMOJI = {"防守": "🛡️", "觀望": "👀"}


# ── State ─────────────────────────────────────────────────────────────────

# {"date": ISO, "stance": str|None, "pushed": [ticker]}. Mirrored in-process so
# a failed file write can't turn into a re-push every 5 minutes.
_state_in_process: dict | None = None


def _now_tpe() -> datetime:
    return datetime.now(_TPE_TZ)


def _in_trading_hours(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    t = now.astimezone(_TPE_TZ).time() if now.tzinfo else now.time()
    return _SESSION_OPEN <= t <= _SESSION_CLOSE


def _blank_state(today: date) -> dict:
    return {"date": today.isoformat(), "stance": None, "pushed": []}


def _load_state(today: date) -> dict:
    state = _state_in_process
    if state is None:
        try:
            with open(_STATE_FILE, encoding="utf-8") as f:
                state = json.load(f)
        except (OSError, ValueError):
            state = None
    if not isinstance(state, dict) or state.get("date") != today.isoformat():
        return _blank_state(today)
    return state


def _save_state(state: dict) -> None:
    global _state_in_process
    _state_in_process = state
    try:
        os.makedirs(_STATE_FILE.parent, exist_ok=True)
        with open(_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
    except OSError as exc:
        logger.error("stance_alert: state write failed (%s): %s", _STATE_FILE, exc)


# ── Data ──────────────────────────────────────────────────────────────────


def _live_stance(now: datetime) -> dict | None:
    """Recompute the intraday stance the same way the snapshot daemon does.
    Imported lazily so the bot doesn't pull the analysis stack at startup."""
    from analysis.intraday_snapshot import _compute_volume_scale
    from analysis.market_thermometer import build_intraday_stance

    scale = _compute_volume_scale(now)
    with get_cursor(commit=False) as cur:
        return build_intraday_stance(cur, scale, now)


def _todays_longs() -> tuple[list[dict], dict[str, int]]:
    """(today's buy entries, {other long tier: count}) from the intraday
    positions snapshot. Empty on any read problem — a missing file must not
    block the stance push itself."""
    try:
        with open(_POSITIONS_JSON, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return [], {}
    snap = data.get("snapshot_date")
    buys: list[dict] = []
    others: dict[str, int] = {}
    for p in data.get("long", []):
        if not snap or p.get("entry_date") != snap:
            continue
        tier = p.get("entry_tier")
        if tier == "buy":
            buys.append(p)
        else:
            others[tier] = others.get(tier, 0) + 1
    return buys, others


# ── Message building ──────────────────────────────────────────────────────


def _long_lines(buys: list[dict], others: dict[str, int]) -> list[str]:
    lines: list[str] = []
    if buys:
        lines.append(f"今日新進波段多 {len(buys)} 檔：")
        for p in buys:
            price = p.get("entry_price")
            price_s = f"{float(price):g}" if price is not None else "?"
            lines.append(f"  {p.get('ticker', '?')} {p.get('name', '')}  {price_s}")
    if others:
        parts = [f"{_LONG_TIERS.get(t, t)} {n} 檔" for t, n in sorted(others.items())]
        lines.append(f"（另有{'、'.join(parts)}）")
    return lines


def _stance_line(kind: str, stance: str, prev: str | None, exposure: str | None) -> str:
    emoji = _STANCE_EMOJI.get(stance, "⚔️")
    if kind == "fresh":
        state = f"{stance}（持續中）"          # no arrow: 防守 → 防守 reads like a bug
    elif prev:
        state = f"{prev} → {stance}"
    else:
        state = f"{stance}（今日首次判定）"
    pos = f"（部位 {exposure}）" if exposure else ""
    return f"{emoji} 盤中攻防：{state}{pos}"


def _reason_line(stance_data: dict) -> list[str]:
    """Keep only the 'why it is defensive' half; the text after 「；」 lists the
    conditions that would flip it back and doubles the message length."""
    reason = (stance_data.get("stance3_reason") or stance_data.get("stance_reason") or "")
    head = reason.split("；")[0].strip()
    return [head] if head else []


def _build_message(kind: str, now: datetime, stance: str, prev: str | None,
                   stance_data: dict, buys: list[dict],
                   others: dict[str, int]) -> str:
    title = {"enter": "攻防轉防守",
             "fresh": "防守中新進波段多",
             "exit": "攻防解除"}[kind]
    lines = [f"[{title}] {now:%H:%M}",
             _stance_line(kind, stance, prev, stance_data.get("exposure"))]
    if kind == "enter":
        # Only on the flip: the reason text is identical on every later push.
        lines += _reason_line(stance_data)
    if kind != "exit":
        lines += _long_lines(buys, others)
    return "\n".join(lines)


# ── Check ─────────────────────────────────────────────────────────────────


def _run_check(*, respect_gates: bool, dry_run: bool = False) -> tuple[str | None, str]:
    """Synchronous worker: returns (message_or_None, status_string)."""
    now = _now_tpe()
    today = now.date()

    if respect_gates and not _in_trading_hours(now):
        return None, "skipped: outside trading hours"

    try:
        stance_data = _live_stance(now)
    except Exception as exc:
        # h(t) not ready in the first minutes, DB hiccup, … — never record a
        # stance we failed to compute, or the next tick reads it as a flip.
        return None, f"skipped: live stance unavailable ({exc})"
    if not stance_data:
        return None, "skipped: live stance unavailable (None)"
    stance = stance_data.get("stance3") or stance_data.get("stance")
    if not stance:
        return None, "skipped: stance missing from live payload"

    state = _load_state(today)
    prev = state.get("stance")
    pushed: list[str] = list(state.get("pushed") or [])
    buys, others = _todays_longs()

    if stance == DEFENSIVE and prev != DEFENSIVE:
        kind, shown = "enter", buys
        new_pushed = [p.get("ticker") for p in buys]
    elif stance != DEFENSIVE and prev == DEFENSIVE:
        kind, shown = "exit", []
        new_pushed = []
    elif stance == DEFENSIVE:
        shown = [p for p in buys if p.get("ticker") not in set(pushed)]
        if not shown:
            return None, "no change (defensive, no new 波段多)"
        kind = "fresh"
        new_pushed = pushed + [p.get("ticker") for p in shown]
    else:
        if stance != prev and not dry_run:
            # 攻擊 ↔ 觀望: nothing to push, but keep prev honest for the arrow.
            _save_state({**state, "stance": stance})
        return None, f"no change ({stance})"

    msg = _build_message(kind, now, stance, prev, stance_data, shown, others)
    if dry_run:
        return msg, f"dry-run: would push [{kind}]"
    ok = send_sync(msg)
    if ok:
        _save_state({"date": today.isoformat(), "stance": stance, "pushed": new_pushed})
    return msg, f"pushed [{kind}], send_ok={ok}"


async def check_once(context) -> None:  # noqa: ANN001 — PTB context type
    """JobQueue callback. Offloads the DB + stance computation to a worker
    thread so the bot event loop stays responsive."""
    try:
        _, status = await asyncio.to_thread(_run_check, respect_gates=True)
        logger.info("stance_alert: %s", status)
    except Exception as exc:  # never let JobQueue swallow silently
        logger.exception("stance_alert crashed: %s", exc)


# ── Wiring ────────────────────────────────────────────────────────────────


def register(application) -> None:  # noqa: ANN001 — PTB Application
    jq = application.job_queue
    if jq is None:
        logger.warning(
            "JobQueue unavailable; stance_alert not scheduled. "
            "Install python-telegram-bot[job-queue]."
        )
        return
    jq.run_repeating(check_once, interval=PERIOD_S, first=20, name=JOB_NAME)
    logger.info("Scheduled %s every %ds", JOB_NAME, PERIOD_S)


# ── CLI ───────────────────────────────────────────────────────────────────


def _cli() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    msg, status = _run_check(respect_gates=False, dry_run="--dry-run" in sys.argv[1:])
    print(f"[stance_alert] {status}")
    if msg:
        print(msg)
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
