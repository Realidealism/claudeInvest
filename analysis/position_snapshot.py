"""Daily snapshot of currently-open positions under the unified strategy.

For each active stock, runs unified_long_factory + unified_short_factory
through the engine. The engine force-closes any still-open position at
the end of history with exit_reason = REASON_EXIT_END, so we use that
last trade as the source of truth for "still open as of today":

    last_trade.exit_reason == REASON_EXIT_END
    last_trade.exit_date   == data.dates[-1]

From that trade we extract entry_date / entry_price / entry_tier and the
latest defense event. Mirrors analysis/daily_snapshot.py for parallelism.

The snapshot computation overlaps with daily_snapshot (load_stock_data
done twice). Acceptable for now; can be merged later for efficiency.
"""

from __future__ import annotations

import os
import re
import sys
import time
from datetime import date
from multiprocessing import Pool, cpu_count

from psycopg2.extras import execute_batch

from db.connection import get_cursor, get_connection
from backtest.data import load_stock_data
# Import via signal.SIGNAL_FACTORIES rather than directly from
# signal_backtest.factories.unified — the latter has a circular import
# with signal_backtest.signal (signal._register_factories pulls it back).
from signal_backtest.signal import SIGNAL_FACTORIES
from signal_backtest.engine import (
    run_side_backtest_tiered, DEFAULT_START_INDEX,
)
from signal_backtest.trade import (
    REASON_EXIT_END, REASON_ENTRY_INIT,
)

unified_long_factory = SIGNAL_FACTORIES["unified_long"]
unified_short_factory = SIGNAL_FACTORIES["unified_short"]

DEFAULT_WORKERS = max(1, min(cpu_count() or 1, 8))
N_WORKERS = int(os.environ.get("POSITION_SNAPSHOT_WORKERS", str(DEFAULT_WORKERS)))

_TIER_RE = re.compile(rf"^{re.escape(REASON_ENTRY_INIT)}\[(\w+)\]$")


def _check_market_breadth_fresh(snapshot_date: date) -> None:
    """Same precondition as daily_snapshot — load_stock_data → market_state
    pulls from tw.market_breadth and would otherwise silently use stale rows."""
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT MAX(trade_date) AS d FROM tw.market_breadth")
        latest = cur.fetchone()["d"]
    if latest is None or latest < snapshot_date:
        raise RuntimeError(
            f"tw.market_breadth latest = {latest}, snapshot_date = {snapshot_date}. "
            f"Run market_breadth update first."
        )


def _parse_entry_tier(reason: str) -> str | None:
    """Defense event 0 has reason like '進場初始[pick]'; pull out 'pick'."""
    m = _TIER_RE.match(reason)
    return m.group(1) if m else None


def _safe_num(x: float) -> float | None:
    """NaN -> None for DB; psycopg2 NUMERIC won't accept NaN."""
    return None if x != x else x


def _extract_open_position(result, side: str, last_trade_date: date) -> dict | None:
    """Inspect a SideResult; return open-position summary or None."""
    if not result.trades:
        return None
    last = result.trades[-1]
    if last.exit_reason != REASON_EXIT_END or last.exit_date != last_trade_date:
        return None
    if not last.defense_events:
        return None

    entry_tier = _parse_entry_tier(last.defense_events[0].reason)
    if entry_tier is None:
        return None

    cur_def = last.defense_events[-1]
    return {
        "side": side,
        "entry_date": last.entry_date,
        "entry_price": float(last.entry_price),
        "entry_tier": entry_tier,
        "current_close": float(last.exit_price),
        # Trade.pnl_pct is a fraction (0.107 = 10.7%); scale to percentage.
        "pnl_pct": float(last.pnl_pct) * 100.0,
        "bars_held": int(last.holding_days),
        "defense_price": _safe_num(float(cur_def.price)),
        "defense_reason": cur_def.reason,
        "defense_date": cur_def.date,
    }


def _eval_stock(sid: str) -> dict | None:
    """Worker entry. Load stock, run unified long + short backtests, return
    any open positions plus today's turnover."""
    try:
        data = load_stock_data(sid)
    except Exception:
        return None
    if data.n < DEFAULT_START_INDEX + 1:
        return None

    last_date = data.dates[-1]
    open_positions: list[dict] = []

    for side, factory in (
        ("long",  unified_long_factory),
        ("short", unified_short_factory),
    ):
        try:
            spec = factory(data)
        except Exception:
            continue
        tiers = spec.long_tiers if side == "long" else spec.short_tiers
        exit_arr = spec.signals.long_exit if side == "long" else spec.signals.short_exit
        if not tiers:
            continue
        try:
            result = run_side_backtest_tiered(
                data, side, tiers,
                exit_=exit_arr,
                start_index=DEFAULT_START_INDEX,
                floor_period=spec.long_floor_period if side == "long" else spec.short_floor_period,
            )
        except Exception:
            continue
        pos = _extract_open_position(result, side, last_date)
        if pos is not None:
            open_positions.append(pos)

    if not open_positions:
        return None

    tv_raw = float(data.turnover[-1])
    tv = tv_raw if tv_raw == tv_raw else 0.0
    return {"sid": sid, "turnover": tv, "open_positions": open_positions}


def _load_active_stock_ids() -> list[str]:
    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT stock_id FROM tw.stocks
            WHERE is_active = TRUE
              AND security_type = 'STOCK'
              AND market IN ('TWSE', 'TPEx')
            ORDER BY stock_id
        """)
        return [r["stock_id"] for r in cur.fetchall()]


def _save(snapshot_date: date, results: list[dict]) -> dict[str, int]:
    rows = []
    counts = {"long": 0, "short": 0}
    for r in results:
        for p in r["open_positions"]:
            counts[p["side"]] += 1
            rows.append((
                snapshot_date,
                r["sid"],
                p["side"],
                p["entry_date"],
                round(p["entry_price"], 2),
                p["entry_tier"],
                round(p["current_close"], 2),
                round(p["pnl_pct"], 3),
                p["bars_held"],
                round(r["turnover"], 2),
                round(p["defense_price"], 2) if p["defense_price"] is not None else None,
                p["defense_reason"],
                p["defense_date"],
            ))

    sql = """
        INSERT INTO tw.open_positions
        (snapshot_date, stock_id, side, entry_date, entry_price, entry_tier,
         current_close, pnl_pct, bars_held, turnover,
         defense_price, defense_reason, defense_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM tw.open_positions WHERE snapshot_date = %s",
            (snapshot_date,),
        )
        execute_batch(cur, sql, rows)
        conn.commit()
    finally:
        conn.close()
    return counts


def run(snapshot_date: date) -> dict:
    _check_market_breadth_fresh(snapshot_date)

    t0 = time.time()
    print(f"  Position snapshot @ {snapshot_date} (workers={N_WORKERS}) ...")
    stock_ids = _load_active_stock_ids()
    print(f"  Loaded {len(stock_ids)} active stocks", file=sys.stderr)

    results: list[dict] = []
    with Pool(N_WORKERS) as pool:
        for r in pool.imap_unordered(_eval_stock, stock_ids, chunksize=20):
            if r is not None:
                results.append(r)
                if len(results) % 200 == 0:
                    print(f"  with-position {len(results)} so far ...", file=sys.stderr)

    counts = _save(snapshot_date, results)
    print(f"  Open positions — long: {counts['long']} / short: {counts['short']}")
    print(f"  Total wall time: {time.time() - t0:.1f}s")
    return counts


if __name__ == "__main__":
    if len(sys.argv) > 1:
        d = date.fromisoformat(sys.argv[1])
    else:
        d = date.today()
    run(d)
