"""
Daily stock liquidity classification — ran at end-of-day by daily_update.py
and (one-off) by backfill_liquidity.py.

For each (trade_date, stock_id) pair we store four flags in
tw.stock_liquidity_daily:

  money_level   0-11, from analysis.money.calculate_money (8-day turnover SMA)
  is_dead_fish  Composite persistent-low-turnover flag (same module)
  is_halted     today_volume == 0 AND >= 3 days of activity in prior 10 days
                — detects short suspensions that would otherwise be flagged
                as "dead fish" by the turnover SMA.
  is_on_alert   Matched by tw.stock_alerts for the same trade_date

The intraday ORB pipeline uses `is_dead_fish = false AND is_halted = false`
to narrow its watch universe (see intraday/orb.py).
"""

from __future__ import annotations

from datetime import date
from typing import Iterable

import numpy as np

from db.connection import get_cursor
from analysis.money import calculate_money

F32 = np.float32

# Window for the is_halted rule: today volume=0, but >= _HALT_MIN_ACTIVE
# days had volume > 0 in the preceding _HALT_LOOKBACK trading days.
_HALT_LOOKBACK = 10
_HALT_MIN_ACTIVE = 3

# calculate_money's widest rolling window is 55 days; give EMA + count
# enough warm-up so the dead_fish flag is stable at trade_date.
_HISTORY_BUFFER_DAYS = 80


def _get_active_tickers() -> list[str]:
    """Active STOCK + EQUITY_ETF + BOND_ETF. ESB (興櫃) excluded."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT stock_id
            FROM tw.stocks
            WHERE is_active = TRUE
              AND market IN ('TWSE', 'TPEx')
              AND security_type IN ('STOCK', 'EQUITY_ETF', 'BOND_ETF')
            ORDER BY stock_id
            """
        )
        return [r["stock_id"] for r in cur.fetchall()]


def _fetch_history(stock_id: str, end_date: date, lookback_days: int) -> list[dict]:
    """Pull oldest-first daily rows up through end_date."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT trade_date, turnover, volume
            FROM tw.daily_prices
            WHERE stock_id = %s
              AND trade_date <= %s
              AND trade_date > %s - (%s * INTERVAL '1 day')
              AND turnover IS NOT NULL
              AND volume IS NOT NULL
            ORDER BY trade_date
            """,
            (stock_id, end_date, end_date, lookback_days),
        )
        return cur.fetchall()


def _fetch_alerts_in_range(start: date, end: date) -> set[tuple[date, str]]:
    """Set of (alert_date, stock_id) for all attention/disposal alerts in range."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT DISTINCT alert_date, stock_id
            FROM tw.stock_alerts
            WHERE alert_date BETWEEN %s AND %s
            """,
            (start, end),
        )
        return {(r["alert_date"], r["stock_id"]) for r in cur.fetchall()}


def _upsert(rows: list[tuple]) -> int:
    """Batch upsert into tw.stock_liquidity_daily."""
    if not rows:
        return 0
    from psycopg2.extras import execute_values
    with get_cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO tw.stock_liquidity_daily
                (trade_date, stock_id, money_level, is_dead_fish,
                 is_halted, is_on_alert, updated_at)
            VALUES %s
            ON CONFLICT (trade_date, stock_id) DO UPDATE SET
                money_level  = EXCLUDED.money_level,
                is_dead_fish = EXCLUDED.is_dead_fish,
                is_halted    = EXCLUDED.is_halted,
                is_on_alert  = EXCLUDED.is_on_alert,
                updated_at   = NOW()
            """,
            rows,
            template="(%s, %s, %s, %s, %s, %s, NOW())",
        )
    return len(rows)


def compute_liquidity(trade_dates: Iterable[date]) -> int:
    """Classify every active ticker on every date in `trade_dates`.

    Fetches each ticker's history once (regardless of how many target dates)
    so a full 80-day backfill isn't 80× the cost of a single-date run.
    Returns the number of rows upserted.
    """
    target_dates = sorted(set(trade_dates))
    if not target_dates:
        return 0

    start_date = target_dates[0]
    end_date = target_dates[-1]

    # History needs enough warm-up that calculate_money's 55-day window is
    # stable at start_date, plus 10 days for the halted lookback.
    history_start_buffer = _HISTORY_BUFFER_DAYS + _HALT_LOOKBACK
    tickers = _get_active_tickers()
    alerts = _fetch_alerts_in_range(start_date, end_date)

    print(f"  Classifying liquidity for {len(tickers)} tickers × {len(target_dates)} date(s) ...")

    target_set = set(target_dates)
    batch: list[tuple] = []
    total = 0

    for i, stock_id in enumerate(tickers, 1):
        rows = _fetch_history(stock_id, end_date, history_start_buffer + len(target_dates))
        if not rows:
            continue

        dates = [r["trade_date"] for r in rows]
        turnover = np.array([float(r["turnover"] or 0) for r in rows], dtype=F32)
        volume = np.array([int(r["volume"] or 0) for r in rows], dtype=np.int64)

        if len(turnover) < 1:
            continue

        # One calculate_money call covers every index we need below.
        money_result = calculate_money(turnover)

        date_to_idx = {d: idx for idx, d in enumerate(dates)}

        for td in target_dates:
            if td not in date_to_idx:
                # Stock had no trading row for this date (may be listed later,
                # or day already filtered out by turnover/volume NULL guard).
                continue
            j = date_to_idx[td]

            # is_halted: today volume=0 AND >= _HALT_MIN_ACTIVE days with
            # volume>0 in the preceding _HALT_LOOKBACK days.
            is_halted = False
            if volume[j] == 0:
                lo = max(0, j - _HALT_LOOKBACK)
                active_recent = int(np.count_nonzero(volume[lo:j]))
                if active_recent >= _HALT_MIN_ACTIVE:
                    is_halted = True

            batch.append((
                td,
                stock_id,
                int(money_result.money_level[j]),
                bool(money_result.dead_fish[j]),
                is_halted,
                (td, stock_id) in alerts,
            ))

        if len(batch) >= 5000:
            total += _upsert(batch)
            batch.clear()

        if i % 200 == 0:
            print(f"    {i}/{len(tickers)} tickers processed")

    if batch:
        total += _upsert(batch)

    print(f"  Upserted {total} liquidity rows.")
    return total


def compute_daily_liquidity(trade_date: date) -> int:
    """Single-date entry used by daily_update.py's post-market step."""
    return compute_liquidity([trade_date])
