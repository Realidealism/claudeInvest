"""Intraday (12:50) snapshot — preview of ScoreBoard top-300 + 6 signal fires
+ open positions, using a market-wide h(t)-projected full-day bar.

Mirrors analysis.daily_snapshot.py but:
  * Uses analysis.realtime_data.load_stock_data_intraday so the forming
    bar's volume + turnover are scaled to projected full-day values.
  * Writes to tw.score_snapshot_intraday / tw.signal_snapshot_intraday /
    tw.open_positions_intraday (separate tables, daily versions untouched).
  * prev_rank is taken from yesterday's tw.score_snapshot daily row so
    the 變動 column is anchored to the most recent close, matching the
    daily view's convention.

Env vars (same defaults as daily_snapshot):
  INTRADAY_SNAPSHOT_WORKERS    — default 8
  INTRADAY_SNAPSHOT_MIN_LEVEL  — default 4
"""

from __future__ import annotations

import os
import re
import sys
import time
from datetime import date, datetime, timezone, timedelta

from psycopg2.extras import execute_batch

from db.connection import get_cursor, get_connection
from analysis.realtime_data import load_stock_data_intraday
from analysis.score import build_scoreboard
from intraday.estimate import get_h_curve, _get_h
from signal_backtest.factories._conditions import (
    pick_condition, touch_condition,
    buy_condition, sell_condition,
    buy_flee_signal, sell_flee_signal,
)
from signal_backtest.signal import SIGNAL_FACTORIES
from signal_backtest.engine import run_side_backtest_tiered
from signal_backtest.trade import REASON_EXIT_END, REASON_ENTRY_INIT


unified_long_factory = SIGNAL_FACTORIES["unified_long"]
unified_short_factory = SIGNAL_FACTORIES["unified_short"]

# Defense event 0 has reason like '進場初始[pick]'; pull out the tier name.
_TIER_RE = re.compile(rf"^{re.escape(REASON_ENTRY_INIT)}\[(\w+)\]$")


_TPE_TZ = timezone(timedelta(hours=8))

TOP_N = 300
DEFAULT_WORKERS = max(1, min(os.cpu_count() or 1, 8))
N_WORKERS = int(os.environ.get("INTRADAY_SNAPSHOT_WORKERS", str(DEFAULT_WORKERS)))
MIN_MONEY_LEVEL = int(os.environ.get("INTRADAY_SNAPSHOT_MIN_LEVEL", "4"))

SIGNALS = [
    ("pick",      pick_condition),
    ("touch",     touch_condition),
    ("buy",       buy_condition),
    ("sell",      sell_condition),
    ("buy_flee",  buy_flee_signal),
    ("sell_flee", sell_flee_signal),
]


# ── Worker side ────────────────────────────────────────────────────────────

# Both the ScoreBoard (heavy lambda registry) and volume_scale (computed
# from a market-wide h-curve query) are set once per worker via the Pool
# initializer to avoid per-stock recompute.
_BOARD = None
_VOLUME_SCALE: float = 1.0


def _init_worker(volume_scale: float) -> None:
    global _BOARD, _VOLUME_SCALE
    _BOARD = build_scoreboard()
    _VOLUME_SCALE = volume_scale


def _eval_pct(data, idx: int) -> tuple[float | None, float | None]:
    if idx < 0:
        return (None, None)
    try:
        r = _BOARD.evaluate(data, idx)
    except Exception:
        return (None, None)
    return (r.total.long.pct, r.total.short.pct)


def _safe_num(x: float) -> float | None:
    """NaN -> None for DB; psycopg2 NUMERIC won't accept NaN."""
    return None if x != x else x


def _extract_position(result, side: str, last_trade_date) -> dict | None:
    """Inspect the engine's last trade to derive a position summary.

    Three cases by ``last.exit_reason`` and ``last.exit_date``:
      1. exit_reason == REASON_EXIT_END AND exit_date == today
         → still open (engine force-closed at end-of-history)
      2. exit_reason != REASON_EXIT_END AND exit_date == today
         → exited intraday at today's projected bar (the new case)
      3. else → no recent activity, ignore

    Returns dict with ``is_exited`` flag distinguishing 1 vs 2."""
    if not result.trades:
        return None
    last = result.trades[-1]
    if last.exit_date != last_trade_date:
        return None
    if not last.defense_events:
        return None

    m = _TIER_RE.match(last.defense_events[0].reason)
    if m is None:
        return None
    entry_tier = m.group(1)

    is_exited = last.exit_reason != REASON_EXIT_END
    cur_def = last.defense_events[-1]
    return {
        "side": side,
        "entry_date": last.entry_date,
        "entry_price": float(last.entry_price),
        "entry_tier": entry_tier,
        "current_close": float(last.exit_price),
        "pnl_pct": float(last.pnl_pct) * 100.0,
        "bars_held": int(last.holding_days),
        # No active defense after exit — leave the fields populated with
        # the most recent pre-exit value so the daily-snapshot consumer
        # path stays uniform; intraday consumers use is_exited to skip.
        "defense_price": _safe_num(float(cur_def.price)),
        "defense_reason": cur_def.reason,
        "defense_date": cur_def.date,
        "is_exited": is_exited,
        "exit_reason": last.exit_reason if is_exited else None,
    }


def _eval_stock(stock_id: str) -> dict | None:
    """Per-stock worker. Returns flat dict (cheap pickle) or None."""
    try:
        data = load_stock_data_intraday(stock_id, _VOLUME_SCALE)
    except Exception:
        return None
    if data.n < 60:
        return None

    idx = data.n - 1
    last_date = data.dates[-1]

    lp, sp = _eval_pct(data, idx)
    if lp is None:
        return None
    lp_d1, sp_d1 = _eval_pct(data, idx - 1)
    lp_d2, sp_d2 = _eval_pct(data, idx - 2)
    lp_d3, sp_d3 = _eval_pct(data, idx - 3)

    fires: list[str] = []
    for name, fn in SIGNALS:
        try:
            arr = fn(data)
        except Exception:
            continue
        if bool(arr[idx]):
            fires.append(name)

    # Unified-strategy open positions (full backtest each side).
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
                floor_period=spec.long_floor_period if side == "long"
                              else spec.short_floor_period,
            )
        except Exception:
            continue
        pos = _extract_position(result, side, last_date)
        if pos is not None:
            open_positions.append(pos)

    tv_raw = float(data.turnover[idx])
    tv = tv_raw if tv_raw == tv_raw else 0.0

    return {
        "sid": stock_id,
        "lp": lp, "sp": sp, "tv": tv,
        "lp_d1": lp_d1, "sp_d1": sp_d1,
        "lp_d2": lp_d2, "sp_d2": sp_d2,
        "lp_d3": lp_d3, "sp_d3": sp_d3,
        "fires": fires,
        "open_positions": open_positions,
    }


# ── Main side ──────────────────────────────────────────────────────────────

def _load_active_stock_ids(snapshot_date: date) -> list[str]:
    """Same dead-fish filter as daily_snapshot. Falls back to the most
    recent stock_liquidity_daily row when today's hasn't been computed
    yet (intraday runs at 12:50, before daily_liquidity)."""
    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT MAX(trade_date) AS d
            FROM tw.stock_liquidity_daily
            WHERE trade_date <= %s
        """, (snapshot_date,))
        liq_date = cur.fetchone()["d"]
        if liq_date is None:
            raise RuntimeError(
                "tw.stock_liquidity_daily empty — daily_update has never run."
            )
        cur.execute("""
            SELECT s.stock_id
            FROM tw.stocks s
            JOIN tw.stock_liquidity_daily l
              ON l.stock_id = s.stock_id AND l.trade_date = %s
            WHERE s.is_active = TRUE
              AND s.security_type = 'STOCK'
              AND s.market IN ('TWSE', 'TPEx')
              AND l.money_level >= %s
            ORDER BY s.stock_id
        """, (liq_date, MIN_MONEY_LEVEL))
        return [r["stock_id"] for r in cur.fetchall()]


def _load_prev_score_ranks(snapshot_date: date, side: str) -> dict[str, int]:
    """Yesterday's daily snapshot ranks — basis for the 變動 column."""
    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT MAX(snapshot_date) AS d
            FROM tw.score_snapshot
            WHERE snapshot_date < %s AND side = %s
        """, (snapshot_date, side))
        prev_date = cur.fetchone()["d"]
        if prev_date is None:
            return {}
        cur.execute("""
            SELECT stock_id, rank FROM tw.score_snapshot
            WHERE snapshot_date = %s AND side = %s
        """, (prev_date, side))
        return {r["stock_id"]: r["rank"] for r in cur.fetchall()}


def _save_score_side(snapshot_date: date, snapshot_time: datetime, side: str,
                     results: list[dict], prev: dict[str, int]) -> int:
    if side == "long":
        ranked = sorted(results, key=lambda r: (-r["lp"], -r["tv"]))[:TOP_N]
        pct_key, d1_key, d2_key, d3_key = "lp", "lp_d1", "lp_d2", "lp_d3"
    else:
        ranked = sorted(results, key=lambda r: (-r["sp"], -r["tv"]))[:TOP_N]
        pct_key, d1_key, d2_key, d3_key = "sp", "sp_d1", "sp_d2", "sp_d3"

    rows = []
    for i, r in enumerate(ranked, start=1):
        sid = r["sid"]
        prev_rank = prev.get(sid)
        is_new = prev_rank is None
        rank_delta = (prev_rank - i) if prev_rank is not None else None
        rows.append((
            snapshot_date, snapshot_time, side, i, sid,
            round(r[pct_key], 3), round(r["tv"], 2),
            is_new, prev_rank, rank_delta,
            round(r[d1_key], 3) if r[d1_key] is not None else None,
            round(r[d2_key], 3) if r[d2_key] is not None else None,
            round(r[d3_key], 3) if r[d3_key] is not None else None,
        ))

    sql = """
        INSERT INTO tw.score_snapshot_intraday
        (snapshot_date, snapshot_time, side, rank, stock_id, total_pct, turnover,
         is_new, prev_rank, rank_delta, pct_d1, pct_d2, pct_d3)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM tw.score_snapshot_intraday "
            "WHERE snapshot_date = %s AND snapshot_time = %s AND side = %s",
            (snapshot_date, snapshot_time, side),
        )
        execute_batch(cur, sql, rows)
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def _save_open_positions(snapshot_date: date, snapshot_time: datetime,
                         results: list[dict]) -> dict[str, int]:
    counts = {"long": 0, "short": 0, "exited_long": 0, "exited_short": 0}
    rows = []
    for r in results:
        for p in r["open_positions"]:
            bucket = ("exited_" if p["is_exited"] else "") + p["side"]
            counts[bucket] += 1
            rows.append((
                snapshot_date,
                snapshot_time,
                r["sid"],
                p["side"],
                p["entry_date"],
                round(p["entry_price"], 2),
                p["entry_tier"],
                round(p["current_close"], 2),
                round(p["pnl_pct"], 3),
                p["bars_held"],
                round(r["tv"], 2),
                round(p["defense_price"], 2) if p["defense_price"] is not None else None,
                p["defense_reason"],
                p["defense_date"],
                p["is_exited"],
                p["exit_reason"],
            ))

    sql = """
        INSERT INTO tw.open_positions_intraday
        (snapshot_date, snapshot_time, stock_id, side, entry_date, entry_price,
         entry_tier, current_close, pnl_pct, bars_held, turnover,
         defense_price, defense_reason, defense_date, is_exited, exit_reason)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM tw.open_positions_intraday "
            "WHERE snapshot_date = %s AND snapshot_time = %s",
            (snapshot_date, snapshot_time),
        )
        execute_batch(cur, sql, rows)
        conn.commit()
    finally:
        conn.close()
    return counts


def _save_signal_fires(snapshot_date: date, snapshot_time: datetime,
                       results: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {name: 0 for name, _ in SIGNALS}
    rows = []
    for r in results:
        for name in r["fires"]:
            counts[name] += 1
            rows.append((snapshot_date, snapshot_time, name, r["sid"], round(r["tv"], 2)))

    sql = """
        INSERT INTO tw.signal_snapshot_intraday
        (snapshot_date, snapshot_time, signal, stock_id, turnover)
        VALUES (%s, %s, %s, %s, %s)
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM tw.signal_snapshot_intraday "
            "WHERE snapshot_date = %s AND snapshot_time = %s",
            (snapshot_date, snapshot_time),
        )
        execute_batch(cur, sql, rows)
        conn.commit()
    finally:
        conn.close()
    return counts


def _compute_volume_scale(now: datetime) -> float:
    """Build the market-wide h(t) curve once and return scale = 1/h.

    Raises if h is unavailable for the current bucket (too early in
    session, no historical curve data, etc.)."""
    h_curve = get_h_curve()
    if not h_curve:
        raise RuntimeError(
            "h(t) curve has no data — tw.intraday_value_profile is empty. "
            "Run intraday_sweep_update.exe through at least one full session first."
        )
    h = _get_h(now, h_curve)
    if h is None or h <= 0.05:
        raise RuntimeError(
            f"h(t)={h} for {now:%H:%M} TPE — too early or session not active."
        )
    return 1.0 / h


def run() -> dict:
    """Main entry. Builds h(t) scale, parallelizes per-stock evaluation,
    writes intraday score + signal snapshots."""
    # Multiprocessing on Windows requires this top-level guard at the
    # script entry point (root intraday_snapshot.py handles it).
    from multiprocessing import Pool

    now = datetime.now(_TPE_TZ)
    snapshot_date = now.date()
    print(f"  Intraday snapshot @ {now:%Y-%m-%d %H:%M} TPE "
          f"(workers={N_WORKERS}, min_level={MIN_MONEY_LEVEL}) ...")

    scale = _compute_volume_scale(now)
    print(f"  volume scale = {scale:.4f} (h(t) = {1.0 / scale:.4f})")

    stock_ids = _load_active_stock_ids(snapshot_date)
    print(f"  Loaded {len(stock_ids)} active stocks (after dead-fish filter)",
          file=sys.stderr)

    t0 = time.time()
    results: list[dict] = []
    with Pool(N_WORKERS, initializer=_init_worker, initargs=(scale,)) as pool:
        for r in pool.imap_unordered(_eval_stock, stock_ids, chunksize=20):
            if r is not None:
                results.append(r)
                if len(results) % 200 == 0:
                    print(f"  evaluated {len(results)} ...", file=sys.stderr)
    print(f"  scored {len(results)} stocks in {time.time() - t0:.1f}s")

    prev_long  = _load_prev_score_ranks(snapshot_date, "long")
    prev_short = _load_prev_score_ranks(snapshot_date, "short")
    long_n  = _save_score_side(snapshot_date, now, "long",  results, prev_long)
    short_n = _save_score_side(snapshot_date, now, "short", results, prev_short)
    print(f"  Score top-{TOP_N} long: {long_n} / short: {short_n}")

    counts = _save_signal_fires(snapshot_date, now, results)
    for name, _ in SIGNALS:
        print(f"  signal {name}: {counts[name]}")

    pos_counts = _save_open_positions(snapshot_date, now, results)
    print(f"  Open positions long: {pos_counts['long']} / short: {pos_counts['short']}")
    print(f"  Exited intraday  long: {pos_counts['exited_long']} / short: {pos_counts['exited_short']}")

    print(f"  Total wall time: {time.time() - t0:.1f}s")
    return {
        "snapshot_date": snapshot_date,
        "snapshot_time": now,
        "stocks_evaluated": len(results),
        "score_long": long_n,
        "score_short": short_n,
        "signal_counts": counts,
        "position_counts": pos_counts,
    }
