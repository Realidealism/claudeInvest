"""Detect which lifecycle stage today's market data is in.

5 states are tracked so a /score reply can label its data freshness:
  LIVE              — session in progress (09:00–13:30), intraday_quotes ticking
  CLOSED_PENDING    — session ended (13:30+) but daily_prices today's row not yet written
  CLOSED_FINAL      — daily_prices today exists, end-of-day data is authoritative
  PRE_MARKET        — same calendar day but before 09:00; no live trades yet
  STALE_OVERNIGHT   — weekend, holiday, or pre-09:00 of a day whose intraday_quotes
                      hasn't refreshed yet ⇒ latest data is from a prior session

State affects:
  • Header tag shown atop the /score reply
  • Wording of attention prediction ("預期" vs "已" vs "最近交易日")
  • Whether `(粗估)` suffix is appended
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta, timezone
from enum import Enum

from db.connection import get_cursor
from intraday.estimate import _get_h, get_h_curve
from intraday.store import has_close_bucket

_TPE = timezone(timedelta(hours=8))
_OPEN = dtime(hour=9, minute=0)
_CLOSE = dtime(hour=13, minute=30)


class DataState(Enum):
    LIVE = "LIVE"
    CLOSED_PENDING = "CLOSED_PENDING"
    CLOSED_FINAL = "CLOSED_FINAL"
    PRE_MARKET = "PRE_MARKET"
    STALE_OVERNIGHT = "STALE_OVERNIGHT"


@dataclass(frozen=True)
class Freshness:
    state: DataState
    tag: str           # short label shown in /score header
    as_of_date: date   # date the data represents
    is_today: bool     # whether as_of_date == today (TPE)


def _query_dates() -> tuple[date | None, bool]:
    """Returns (latest intraday_quotes.trade_date, daily_prices today exists)."""
    today = datetime.now(_TPE).date()
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT MAX(trade_date) AS m FROM tw.intraday_quotes")
        iq_row = cur.fetchone()
        iq_date = iq_row["m"] if iq_row else None
        cur.execute(
            "SELECT 1 FROM tw.daily_prices WHERE trade_date = %s LIMIT 1",
            (today,),
        )
        dp_today = cur.fetchone() is not None
    return iq_date, dp_today


def _h_percent(now: datetime) -> int | None:
    try:
        h = _get_h(now, get_h_curve())
    except Exception:
        return None
    if h is None:
        return None
    return int(h * 100)


def detect_state() -> Freshness:
    now = datetime.now(_TPE)
    today = now.date()
    now_t = now.time()
    iq_date, dp_today = _query_dates()

    # Weekend or intraday rolled-over to today not yet (e.g. pre-09:00 of a
    # weekday whose pre_market_update hasn't run) → STALE
    weekend = now.weekday() >= 5
    if weekend or (iq_date is not None and iq_date < today):
        as_of = iq_date or today
        return Freshness(
            DataState.STALE_OVERNIGHT,
            f"前日收盤 {as_of.strftime('%m/%d')}",
            as_of,
            False,
        )

    # Daily_prices today exists → CLOSED_FINAL (most authoritative)
    if dp_today:
        return Freshness(
            DataState.CLOSED_FINAL,
            "今日已結算",
            today,
            True,
        )

    # Close bucket present but daily_prices not yet written
    if has_close_bucket(today):
        return Freshness(
            DataState.CLOSED_PENDING,
            "今日剛收盤",
            today,
            True,
        )

    # Otherwise classify by clock
    if now_t < _OPEN:
        return Freshness(
            DataState.PRE_MARKET,
            f"盤前 {now_t.strftime('%H:%M')}",
            today,
            True,
        )
    if now_t < _CLOSE:
        h_pct = _h_percent(now)
        h_part = f" h={h_pct}%" if h_pct is not None else ""
        return Freshness(
            DataState.LIVE,
            f"盤中 {now_t.strftime('%H:%M')}{h_part}",
            today,
            True,
        )
    # Between 13:30 and close bucket arrival
    return Freshness(
        DataState.CLOSED_PENDING,
        "今日剛收盤",
        today,
        True,
    )
