"""Daemon-side periodic watcher that flags watchlist symbols whose projected
full-day turnover (current cumulative / market h(t)) exceeds N× their 20-day
average. Runs as a PTB JobQueue task every 5 minutes during trading hours.

CLI (force-check, skips trading-hours + cooldown gates):
    python -m telegram_bot.watchers.intraday_volume_alert
"""

from __future__ import annotations

import asyncio
import logging
import sys
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import Iterable

from db.connection import get_cursor
from intraday.estimate import get_h_curve, _get_h
from intraday.watchlist import load_tw_watchlist
from telegram_bot.notify import send_sync

logger = logging.getLogger(__name__)

# ── Tunables ──────────────────────────────────────────────────────────────

PERIOD_S = 300              # job cadence
THRESHOLD = 2.5             # projected / 20d_avg ratio that triggers
PROJECTED_FLOOR = 100_000_000   # NTD; suppress dead-fish noise
COOLDOWN_S = 1800           # per-symbol resume after this many seconds
H_MIN = 0.10                # don't project when h(t) is too small (early morning)

_TPE_TZ = timezone(timedelta(hours=8))
_SESSION_OPEN = dtime(hour=9, minute=5)
_SESSION_CLOSE = dtime(hour=13, minute=25)

JOB_NAME = "intraday_volume_alert"


# ── State ─────────────────────────────────────────────────────────────────

# Module-level cooldown table: (symbol, trade_date) -> last_pushed_at (TPE).
# Keys older than today are purged at the start of each cycle to keep it small.
_last_pushed: dict[tuple[str, date], datetime] = {}


def _purge_old_cooldown(today: date) -> None:
    stale = [k for k in _last_pushed if k[1] != today]
    for k in stale:
        del _last_pushed[k]


# ── Pure helpers (testable) ───────────────────────────────────────────────


@dataclass(frozen=True)
class Hit:
    ticker: str
    name: str
    ratio: float
    projected_value: float
    avg_value: float
    last_price: float | None
    change_pct: float | None


def _now_tpe() -> datetime:
    return datetime.now(_TPE_TZ)


def _in_trading_hours(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    t = now.astimezone(_TPE_TZ).time() if now.tzinfo else now.time()
    return _SESSION_OPEN <= t <= _SESSION_CLOSE


def _build_message(now_tpe: datetime, h: float, hits: list[Hit]) -> str:
    header = f"[爆量警報] {now_tpe.strftime('%H:%M')}  h={h * 100:.0f}%"

    def fmt_change(pct: float | None) -> str:
        if pct is None:
            return "—"
        # Taiwan convention: 紅漲 🔴 / 綠跌 🟢
        if pct > 0:
            return f"🔴 +{pct:.2f}%"
        if pct < 0:
            return f"🟢 {pct:.2f}%"
        return f"⚪ 0.00%"

    lines = [header]
    for hit in hits:
        lines.append(
            f"{hit.ticker} {hit.name}  "
            f"{hit.ratio:.1f}×  "
            f"{fmt_change(hit.change_pct)}"
        )
    return "\n".join(lines)


# ── DB access ─────────────────────────────────────────────────────────────


def _load_watchlist_quotes(symbols: list[str]) -> dict[str, dict]:
    """Return {stock_id: {total_value, total_volume, last_price, name}} for
    symbols that currently have an intraday_quotes row."""
    if not symbols:
        return {}
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT iq.stock_id, iq.total_value, iq.total_volume, iq.last_price,
                   iq.change_pct,
                   s.name AS stock_name
            FROM tw.intraday_quotes iq
            LEFT JOIN tw.stocks s ON s.stock_id = iq.stock_id
            WHERE iq.stock_id = ANY(%s)
              AND iq.total_value IS NOT NULL
            """,
            (symbols,),
        )
        return {r["stock_id"]: dict(r) for r in cur.fetchall()}


def _load_20d_avg_value(symbols: list[str], today: date) -> dict[str, float]:
    """Average daily turnover (NTD) over the last ~20 trading days, excluding today."""
    if not symbols:
        return {}
    start = today - timedelta(days=35)  # ~25 trading days; we cap inside SQL
    with get_cursor(commit=False) as cur:
        # Per-symbol average of the latest 20 rows before today.
        cur.execute(
            """
            SELECT stock_id, AVG(turnover)::float8 AS avg_value
            FROM (
                SELECT stock_id, trade_date, turnover,
                       ROW_NUMBER() OVER (PARTITION BY stock_id ORDER BY trade_date DESC) AS rn
                FROM tw.daily_prices
                WHERE stock_id = ANY(%s)
                  AND trade_date < %s
                  AND trade_date >= %s
                  AND turnover IS NOT NULL
            ) t
            WHERE rn <= 20
            GROUP BY stock_id
            """,
            (symbols, today, start),
        )
        return {r["stock_id"]: r["avg_value"] for r in cur.fetchall() if r["avg_value"]}


# ── Core check ────────────────────────────────────────────────────────────


def _compute_hits(
    quotes: dict[str, dict],
    avg_values: dict[str, float],
    h: float,
    cooldown: dict[tuple[str, date], datetime] | None,
    today: date,
    now_tpe: datetime,
) -> list[Hit]:
    """Pure compute path — used by both JobQueue callback and CLI test."""
    hits: list[Hit] = []
    for ticker, quote in quotes.items():
        total_value = quote.get("total_value")
        if not total_value or total_value <= 0:
            continue
        projected = float(total_value) / h
        if projected < PROJECTED_FLOOR:
            continue
        avg = avg_values.get(ticker)
        if not avg or avg <= 0:
            continue
        ratio = projected / avg
        if ratio < THRESHOLD:
            continue
        if cooldown is not None:
            last = cooldown.get((ticker, today))
            if last and (now_tpe - last).total_seconds() < COOLDOWN_S:
                continue
        hits.append(
            Hit(
                ticker=ticker,
                name=quote.get("stock_name") or "",
                ratio=ratio,
                projected_value=projected,
                avg_value=float(avg),
                last_price=float(quote["last_price"]) if quote.get("last_price") is not None else None,
                change_pct=float(quote["change_pct"]) if quote.get("change_pct") is not None else None,
            )
        )
    # Sort by ratio descending so the loudest signal is at the top.
    hits.sort(key=lambda h: h.ratio, reverse=True)
    return hits


def _run_check(*, respect_gates: bool) -> tuple[list[Hit], str]:
    """Synchronous worker: returns (hits, status_string). Called by both
    the async JobQueue callback (via to_thread) and the CLI entry."""
    now_tpe = _now_tpe()
    today = now_tpe.date()

    if respect_gates and not _in_trading_hours(now_tpe):
        return [], "skipped: outside trading hours"

    _purge_old_cooldown(today)

    h_curve = get_h_curve()
    if not h_curve:
        return [], "skipped: h(t) curve unavailable"
    h = _get_h(now_tpe, h_curve)
    if h is None or h < H_MIN:
        return [], f"skipped: h={h} too small"

    symbols = load_tw_watchlist(include_etf=True)
    if not symbols:
        return [], "skipped: watchlist empty"

    quotes = _load_watchlist_quotes(symbols)
    if not quotes:
        return [], "skipped: no intraday quotes for watchlist"

    avg_values = _load_20d_avg_value(list(quotes.keys()), today)

    cooldown = _last_pushed if respect_gates else None
    hits = _compute_hits(quotes, avg_values, h, cooldown, today, now_tpe)

    if not hits:
        return [], f"no hits (h={h:.2%}, checked {len(quotes)} symbols)"

    msg = _build_message(now_tpe, h, hits)
    ok = send_sync(msg)
    if ok and respect_gates:
        for hit in hits:
            _last_pushed[(hit.ticker, today)] = now_tpe
    status = f"sent {len(hits)} hit(s), send_ok={ok}"
    return hits, status


async def check_once(context) -> None:  # noqa: ANN001 — PTB context type
    """JobQueue callback. Offloads DB + HTTP to a worker thread so the bot
    event loop stays responsive."""
    try:
        hits, status = await asyncio.to_thread(_run_check, respect_gates=True)
        logger.info("intraday_volume_alert: %s", status)
    except Exception as exc:  # never let JobQueue swallow silently
        logger.exception("intraday_volume_alert crashed: %s", exc)


# ── Wiring ────────────────────────────────────────────────────────────────


def register(application) -> None:  # noqa: ANN001 — PTB Application
    jq = application.job_queue
    if jq is None:
        logger.warning(
            "JobQueue unavailable; intraday_volume_alert not scheduled. "
            "Install python-telegram-bot[job-queue]."
        )
        return
    jq.run_repeating(check_once, interval=PERIOD_S, first=10, name=JOB_NAME)
    logger.info("Scheduled %s every %ds", JOB_NAME, PERIOD_S)


# ── CLI ───────────────────────────────────────────────────────────────────


def _cli() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    hits, status = _run_check(respect_gates=False)
    print(f"[intraday_volume_alert] {status}")
    for hit in hits:
        print(
            f"  {hit.ticker} {hit.name}  {hit.ratio:.2f}×  "
            f"projected={hit.projected_value:,.0f}  avg={hit.avg_value:,.0f}  "
            f"last={hit.last_price}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
