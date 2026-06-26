"""Watchlist-position defense-proximity watcher.

Polls every 5 minutes. For each (watchlist ∩ active-position) ticker:
  • Reads defense_price from positions_intraday.json (locked at 12:50 snapshot).
  • Reads live last_price from tw.intraday_quotes.
  • Computes signed distance — long: (current − defense) / defense;
    short: (defense − current) / defense.
  • If distance falls below the tightest tier in TIERS (5% / 3% / 1%)
    and that (ticker, tier) hasn't pushed in COOLDOWN_S, fires an alert.

Skipped entries (already-exited positions, distance ≥ 5%, distance < 0
i.e. already past defense) generate no notification.

CLI (force-check, skips trading-hours + cooldown gates):
    python -m telegram_bot.watchers.defense_proximity_alert
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path

from db.connection import get_cursor
from intraday.watchlist import load_tw_watchlist
from telegram_bot.notify import send_sync

logger = logging.getLogger(__name__)

# ── Tunables ──────────────────────────────────────────────────────────────

PERIOD_S = 300              # job cadence
COOLDOWN_S = 1800           # per (ticker, tier) — 30 min before that tier may re-fire

# (threshold_pct, emoji). Ordered loosest → tightest; loop selects the
# tightest tier whose threshold is breached.
TIERS: tuple[tuple[float, str], ...] = (
    (5.0, "🟡"),
    (3.0, "🟠"),
    (1.0, "🔴"),
)

_TPE_TZ = timezone(timedelta(hours=8))
_SESSION_OPEN = dtime(hour=9, minute=5)
_SESSION_CLOSE = dtime(hour=13, minute=25)

JOB_NAME = "defense_proximity_alert"

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_POSITIONS_JSON = _REPO_ROOT / "frontend" / "public" / "data" / "positions_intraday.json"


# ── State ─────────────────────────────────────────────────────────────────

# Cooldown table keyed by (ticker, tier_pct, trade_date_TPE) → last pushed.
# trade_date kept in the key so old entries can be purged at day rollover.
_last_pushed: dict[tuple[str, float, date], datetime] = {}


def _purge_old_cooldown(today: date) -> None:
    stale = [k for k in _last_pushed if k[2] != today]
    for k in stale:
        del _last_pushed[k]


# ── Pure helpers ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Hit:
    ticker: str
    name: str
    side: str                # "long" | "short"
    current_price: float
    defense_price: float
    distance_pct: float      # signed; positive = still safe
    tier_pct: float
    tier_emoji: str


def _now_tpe() -> datetime:
    return datetime.now(_TPE_TZ)


def _in_trading_hours(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    t = now.astimezone(_TPE_TZ).time() if now.tzinfo else now.time()
    return _SESSION_OPEN <= t <= _SESSION_CLOSE


def _signed_distance_pct(side: str, current: float, defense: float) -> float:
    """Positive = safe margin; negative = already past defense."""
    if defense == 0:
        return 0.0
    raw = (current - defense) / defense * 100.0
    return raw if side == "long" else -raw


def _select_tier(distance_pct: float) -> tuple[float, str] | None:
    """Return tightest (pct, emoji) whose threshold the distance breaches,
    or None if still beyond the loosest tier (or already past defense)."""
    if distance_pct <= 0 or distance_pct >= TIERS[0][0]:
        return None
    tightest: tuple[float, str] | None = None
    for pct, emoji in TIERS:
        if distance_pct < pct:
            tightest = (pct, emoji)
    return tightest


def _build_message(now_tpe: datetime, hits: list[Hit]) -> str:
    header = f"[防守接近] {now_tpe.strftime('%H:%M')}"
    lines = [header]
    for h in hits:
        side_zh = "多單" if h.side == "long" else "空單"
        lines.append(
            f"{h.tier_emoji} {h.ticker} {h.name}  {side_zh}  "
            f"距防 {h.distance_pct:+.2f}%  "
            f"{h.current_price:g} / 防 {h.defense_price:g}"
        )
    return "\n".join(lines)


# ── DB / file access ──────────────────────────────────────────────────────


def _load_active_positions() -> list[dict]:
    """Flatten long + short active buckets from positions_intraday.json.
    Each row keeps the JSON shape plus an explicit ``_side`` field."""
    if not _POSITIONS_JSON.exists():
        return []
    with _POSITIONS_JSON.open(encoding="utf-8") as f:
        data = json.load(f)
    out: list[dict] = []
    for bucket, side in (("long", "long"), ("short", "short")):
        for p in data.get(bucket, []):
            out.append({**p, "_side": side})
    return out


def _load_last_prices(tickers: list[str]) -> dict[str, float]:
    """Latest intraday_quotes.last_price keyed by stock_id."""
    if not tickers:
        return {}
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (stock_id) stock_id, last_price
            FROM tw.intraday_quotes
            WHERE stock_id = ANY(%s)
            ORDER BY stock_id, updated_at DESC
            """,
            (tickers,),
        )
        return {
            r["stock_id"]: float(r["last_price"])
            for r in cur.fetchall()
            if r["last_price"] is not None
        }


# ── Core check ────────────────────────────────────────────────────────────


def _compute_hits(
    positions: list[dict],
    watchlist: set[str],
    prices: dict[str, float],
    today: date,
    now_tpe: datetime,
    cooldown: dict[tuple[str, float, date], datetime] | None,
) -> list[Hit]:
    """Pure compute path — used by both JobQueue callback and CLI test."""
    hits: list[Hit] = []
    for pos in positions:
        ticker = pos.get("ticker")
        if not ticker or ticker not in watchlist:
            continue
        defense = pos.get("defense_price")
        if defense is None or float(defense) == 0:
            continue
        current = prices.get(ticker)
        if current is None:
            continue
        side = pos.get("_side", "long")
        dist = _signed_distance_pct(side, current, float(defense))
        tier = _select_tier(dist)
        if tier is None:
            continue
        tier_pct, tier_emoji = tier
        if cooldown is not None:
            last = cooldown.get((ticker, tier_pct, today))
            if last and (now_tpe - last).total_seconds() < COOLDOWN_S:
                continue
        hits.append(
            Hit(
                ticker=ticker,
                name=pos.get("name") or "",
                side=side,
                current_price=float(current),
                defense_price=float(defense),
                distance_pct=dist,
                tier_pct=tier_pct,
                tier_emoji=tier_emoji,
            )
        )
    # Sort by tier (tightest first), then ascending distance so most-urgent
    # appears at the top of the consolidated message.
    hits.sort(key=lambda h: (h.tier_pct, h.distance_pct))
    return hits


def _run_check(*, respect_gates: bool) -> tuple[list[Hit], str]:
    """Synchronous worker — returns (hits, status_string)."""
    now_tpe = _now_tpe()
    today = now_tpe.date()

    if respect_gates and not _in_trading_hours(now_tpe):
        return [], "skipped: outside trading hours"

    _purge_old_cooldown(today)

    watchlist = set(load_tw_watchlist(include_etf=True))
    if not watchlist:
        return [], "skipped: watchlist empty"

    positions = _load_active_positions()
    if not positions:
        return [], "skipped: positions_intraday.json empty / missing"

    # Narrow to watchlist ∩ active positions before doing any further work.
    wl_positions = [p for p in positions if p.get("ticker") in watchlist]
    if not wl_positions:
        return [], "skipped: no watchlist position overlap"

    prices = _load_last_prices([p["ticker"] for p in wl_positions])
    if not prices:
        return [], "skipped: no intraday_quotes for watchlist positions"

    cooldown = _last_pushed if respect_gates else None
    hits = _compute_hits(wl_positions, watchlist, prices, today, now_tpe, cooldown)

    if not hits:
        return [], f"no hits (checked {len(wl_positions)} positions)"

    msg = _build_message(now_tpe, hits)
    ok = send_sync(msg)
    if ok and respect_gates:
        for h in hits:
            _last_pushed[(h.ticker, h.tier_pct, today)] = now_tpe
    return hits, f"sent {len(hits)} hit(s), send_ok={ok}"


async def check_once(context) -> None:  # noqa: ANN001 — PTB context
    try:
        hits, status = await asyncio.to_thread(_run_check, respect_gates=True)
        logger.info("defense_proximity_alert: %s", status)
    except Exception as exc:
        logger.exception("defense_proximity_alert crashed: %s", exc)


# ── Wiring ────────────────────────────────────────────────────────────────


def register(application) -> None:  # noqa: ANN001 — PTB Application
    jq = application.job_queue
    if jq is None:
        logger.warning(
            "JobQueue unavailable; defense_proximity_alert not scheduled. "
            "Install python-telegram-bot[job-queue]."
        )
        return
    jq.run_repeating(check_once, interval=PERIOD_S, first=15, name=JOB_NAME)
    logger.info("Scheduled %s every %ds", JOB_NAME, PERIOD_S)


# ── CLI ───────────────────────────────────────────────────────────────────


def _cli() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    hits, status = _run_check(respect_gates=False)
    print(f"[defense_proximity_alert] {status}")
    for h in hits:
        print(
            f"  {h.tier_emoji} {h.ticker} {h.name}  {h.side}  "
            f"dist={h.distance_pct:+.2f}%  current={h.current_price}  "
            f"defense={h.defense_price}  tier={h.tier_pct}%"
        )
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
