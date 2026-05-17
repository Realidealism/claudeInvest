"""Combined daily snapshot — three outputs from a single per-stock pass:

  1. Top-300 long/short ScoreBoard (tw.score_snapshot)
  2. Signal-factory fires for the 6 conditions (tw.signal_snapshot)
  3. Currently-open unified-strategy positions (tw.open_positions)

Each worker loads a stock once, runs the 4-bar scoreboard evaluations,
the 6 condition functions on the latest bar, and the unified long/short
backtests, then returns a flat dict to the main process. Workers run in
parallel via multiprocessing.Pool.

Replaces the earlier separate score_snapshot / signal_snapshot /
position_snapshot modules; merging eliminates ~50% redundant
load_stock_data calls and a redundant active-stock query per snapshot.

Env vars:
  DAILY_SNAPSHOT_WORKERS      — worker count (default 8, tuned for 8 P-cores)
  DAILY_SNAPSHOT_MIN_LEVEL    — money_level cutoff (default 4)
  DAILY_SNAPSHOT_HISTORY_DAYS — calendar days of history per stock
                                (default 900 ≈ 2.5y; covers all but the
                                very-long-held unified positions while
                                cutting load_stock_data + indicator cost)
"""

from __future__ import annotations

import os
import re
import sys
import time
from datetime import date, timedelta
from multiprocessing import Pool, cpu_count

from psycopg2.extras import execute_batch

from db.connection import get_cursor, get_connection
from backtest.data import load_stock_data
from analysis.score import build_scoreboard
from signal_backtest.factories._conditions import (
    pick_condition, touch_condition,
    buy_condition, sell_condition,
    buy_flee_signal, sell_flee_signal,
)
# Pull unified factories via SIGNAL_FACTORIES rather than direct import
# from signal_backtest.factories.unified — the latter has a circular
# import with signal_backtest.signal._register_factories.
from signal_backtest.signal import SIGNAL_FACTORIES
from signal_backtest.engine import run_side_backtest_tiered
from signal_backtest.trade import REASON_EXIT_END, REASON_ENTRY_INIT

unified_long_factory = SIGNAL_FACTORIES["unified_long"]
unified_short_factory = SIGNAL_FACTORIES["unified_short"]

TOP_N = 300
DEFAULT_WORKERS = max(1, min(cpu_count() or 1, 8))
N_WORKERS = int(os.environ.get("DAILY_SNAPSHOT_WORKERS", str(DEFAULT_WORKERS)))

# Skip stocks with money_level < this on the snapshot date. Default 4
# (8-day SMA turnover < 27M TWD; one tier above the 'dead fish' cutoff).
MIN_MONEY_LEVEL = int(os.environ.get("DAILY_SNAPSHOT_MIN_LEVEL", "4"))

# Calendar-day window of history loaded per stock. ~2.5 years — long enough
# to cover SMA-377 warmup AND past the unified-backtest equilibrium point
# (open-position count converges by ~700 calendar days; longer windows
# don't change the result). Tunable via env var if data growth makes
# this too tight in the future.
HISTORY_DAYS = int(os.environ.get("DAILY_SNAPSHOT_HISTORY_DAYS", "900"))

# (signal_name, condition_fn) — kept in display order; CHECK constraint in
# tw.signal_snapshot enforces the same set of names.
SIGNALS = [
    ("pick",      pick_condition),
    ("touch",     touch_condition),
    ("buy",       buy_condition),
    ("sell",      sell_condition),
    ("buy_flee",  buy_flee_signal),
    ("sell_flee", sell_flee_signal),
]

# Defense event 0 has reason like '進場初始[pick]'; pull out 'pick'.
_TIER_RE = re.compile(rf"^{re.escape(REASON_ENTRY_INIT)}\[(\w+)\]$")


# ── Worker side ────────────────────────────────────────────────────────────

# ScoreBoard is heavy to construct (registers 30+ ScoreItem lambdas); each
# worker builds it once via Pool initializer and reuses across stocks.
_BOARD = None


def _init_worker() -> None:
    global _BOARD
    _BOARD = build_scoreboard()


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


def _extract_open_position(result, side: str, last_trade_date) -> dict | None:
    """Inspect a SideResult; return position summary if the engine's last
    trade closed on the snapshot bar.

    Two cases by ``last.exit_reason``:
      1. exit_reason == REASON_EXIT_END → still open (force-closed at
         end-of-history); is_exited = False.
      2. exit_reason != REASON_EXIT_END → exited on the snapshot bar
         (real exit signal); is_exited = True, exit_reason carried over.

    Earlier bars are ignored — daily snapshot only surfaces today's state."""
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
        # Trade.pnl_pct is a fraction (0.107 = 10.7%); scale to percentage.
        "pnl_pct": float(last.pnl_pct) * 100.0,
        "bars_held": int(last.holding_days),
        "defense_price": _safe_num(float(cur_def.price)),
        "defense_reason": cur_def.reason,
        "defense_date": cur_def.date,
        "is_exited": is_exited,
        "exit_reason": last.exit_reason if is_exited else None,
    }


def _eval_stock(args: tuple[str, date]) -> dict | None:
    """Worker-process entry. One load_stock_data per stock (windowed by
    start_date); produces score pcts at 4 bars, signal-factory fires on
    the latest bar, and unified-strategy open-position info. Returns flat
    dict so result pickles cheaply, or None when the stock is unusable."""
    sid, start_date = args
    try:
        data = load_stock_data(sid, start_date=start_date)
    except Exception:
        return None
    if data.n < 60:
        return None

    idx = data.n - 1
    last_date = data.dates[-1]

    # 1. ScoreBoard at 4 bars.
    lp, sp = _eval_pct(data, idx)
    if lp is None:
        return None
    lp_d1, sp_d1 = _eval_pct(data, idx - 1)
    lp_d2, sp_d2 = _eval_pct(data, idx - 2)
    lp_d3, sp_d3 = _eval_pct(data, idx - 3)

    # 2. Signal conditions on the latest bar.
    fires: list[str] = []
    for name, fn in SIGNALS:
        try:
            arr = fn(data)
        except Exception:
            continue
        if bool(arr[idx]):
            fires.append(name)

    # 3. Unified-strategy open positions (full backtest each side).
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
        pos = _extract_open_position(result, side, last_date)
        if pos is not None:
            open_positions.append(pos)

    tv_raw = float(data.turnover[idx])
    tv = tv_raw if tv_raw == tv_raw else 0.0

    return {
        "sid": sid,
        "lp": lp, "sp": sp, "tv": tv,
        "lp_d1": lp_d1, "sp_d1": sp_d1,
        "lp_d2": lp_d2, "sp_d2": sp_d2,
        "lp_d3": lp_d3, "sp_d3": sp_d3,
        "fires": fires,
        "open_positions": open_positions,
    }


# ── Main side ──────────────────────────────────────────────────────────────

def _check_liquidity_fresh(snapshot_date: date) -> None:
    """Precondition: stock_liquidity_daily must have rows for snapshot_date.
    Without it the JOIN below silently returns 0 rows and the snapshot
    writes nothing."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM tw.stock_liquidity_daily WHERE trade_date = %s",
            (snapshot_date,),
        )
        n = cur.fetchone()["n"]
    if n == 0:
        raise RuntimeError(
            f"tw.stock_liquidity_daily empty for {snapshot_date}. "
            f"Run analysis.daily_liquidity.compute_daily_liquidity({snapshot_date}) first."
        )


def _load_active_stock_ids(snapshot_date: date) -> list[str]:
    """Active TWSE/TPEx stocks with money_level >= MIN_MONEY_LEVEL on the
    snapshot date. Dead-fish (level < 3) are excluded — their signals are
    too noisy from anaemic turnover."""
    with get_cursor(commit=False) as cur:
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
        """, (snapshot_date, MIN_MONEY_LEVEL))
        return [r["stock_id"] for r in cur.fetchall()]


def _load_prev_score_ranks(snapshot_date: date, side: str) -> dict[str, int]:
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


def _save_score_side(snapshot_date: date, side: str, results: list[dict],
                     prev: dict[str, int]) -> int:
    """Sort by side's pct desc (turnover tie-breaker), keep top N, write."""
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
            snapshot_date, side, i, sid,
            round(r[pct_key], 3), round(r["tv"], 2),
            is_new, prev_rank, rank_delta,
            round(r[d1_key], 3) if r[d1_key] is not None else None,
            round(r[d2_key], 3) if r[d2_key] is not None else None,
            round(r[d3_key], 3) if r[d3_key] is not None else None,
        ))

    sql = """
        INSERT INTO tw.score_snapshot
        (snapshot_date, side, rank, stock_id, total_pct, turnover,
         is_new, prev_rank, rank_delta, pct_d1, pct_d2, pct_d3)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM tw.score_snapshot WHERE snapshot_date = %s AND side = %s",
            (snapshot_date, side),
        )
        execute_batch(cur, sql, rows)
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def _save_signal_fires(snapshot_date: date, results: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {name: 0 for name, _ in SIGNALS}
    rows = []
    for r in results:
        for name in r["fires"]:
            counts[name] += 1
            rows.append((snapshot_date, name, r["sid"], round(r["tv"], 2)))

    sql = """
        INSERT INTO tw.signal_snapshot (snapshot_date, signal, stock_id, turnover)
        VALUES (%s, %s, %s, %s)
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM tw.signal_snapshot WHERE snapshot_date = %s",
            (snapshot_date,),
        )
        execute_batch(cur, sql, rows)
        conn.commit()
    finally:
        conn.close()
    return counts


def _save_open_positions(snapshot_date: date, results: list[dict]) -> dict[str, int]:
    counts = {"long": 0, "short": 0, "exited_long": 0, "exited_short": 0}
    rows = []
    for r in results:
        for p in r["open_positions"]:
            bucket = ("exited_" if p["is_exited"] else "") + p["side"]
            counts[bucket] += 1
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
                round(r["tv"], 2),
                round(p["defense_price"], 2) if p["defense_price"] is not None else None,
                p["defense_reason"],
                p["defense_date"],
                p["is_exited"],
                p["exit_reason"],
            ))

    sql = """
        INSERT INTO tw.open_positions
        (snapshot_date, stock_id, side, entry_date, entry_price, entry_tier,
         current_close, pnl_pct, bars_held, turnover,
         defense_price, defense_reason, defense_date, is_exited, exit_reason)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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


def _check_market_breadth_fresh(snapshot_date: date) -> None:
    """Precondition: tw.market_breadth must reach snapshot_date.

    load_stock_data() pulls market_state from this table; stale rows
    silently degrade signal evaluation rather than failing loudly. We
    enforce the freshness contract here so manual / cron callers fail
    fast instead of producing snapshots with mixed-vintage market state.
    """
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT MAX(trade_date) AS d FROM tw.market_breadth")
        latest = cur.fetchone()["d"]
    if latest is None or latest < snapshot_date:
        raise RuntimeError(
            f"tw.market_breadth latest = {latest}, snapshot_date = {snapshot_date}. "
            f"Run analysis.market_breadth.calculate_market_breadth + save first."
        )


def run(snapshot_date: date) -> dict:
    """Main entry. Parallelizes per-stock evaluation across N_WORKERS,
    then writes both score and signal snapshots."""
    _check_market_breadth_fresh(snapshot_date)
    _check_liquidity_fresh(snapshot_date)

    t0 = time.time()
    print(f"  Daily snapshot @ {snapshot_date} (workers={N_WORKERS}, min_level={MIN_MONEY_LEVEL}, history={HISTORY_DAYS}d) ...")
    stock_ids = _load_active_stock_ids(snapshot_date)
    print(f"  Loaded {len(stock_ids)} active stocks (after dead-fish filter)", file=sys.stderr)

    start_date = snapshot_date - timedelta(days=HISTORY_DAYS)
    work_items = [(sid, start_date) for sid in stock_ids]

    results: list[dict] = []
    with Pool(N_WORKERS, initializer=_init_worker) as pool:
        for r in pool.imap_unordered(_eval_stock, work_items, chunksize=20):
            if r is not None:
                results.append(r)
                if len(results) % 200 == 0:
                    print(f"  evaluated {len(results)} ...", file=sys.stderr)

    print(f"  scored {len(results)} stocks in {time.time() - t0:.1f}s")

    prev_long  = _load_prev_score_ranks(snapshot_date, "long")
    prev_short = _load_prev_score_ranks(snapshot_date, "short")
    long_n  = _save_score_side(snapshot_date, "long",  results, prev_long)
    short_n = _save_score_side(snapshot_date, "short", results, prev_short)
    print(f"  Score top-{TOP_N} long: {long_n} / short: {short_n}")

    counts = _save_signal_fires(snapshot_date, results)
    for name, _ in SIGNALS:
        print(f"  signal {name}: {counts[name]}")

    pos_counts = _save_open_positions(snapshot_date, results)
    print(f"  Open positions long: {pos_counts['long']} / short: {pos_counts['short']}")
    print(f"  Exited today    long: {pos_counts['exited_long']} / short: {pos_counts['exited_short']}")

    print(f"  Total wall time: {time.time() - t0:.1f}s")
    return {
        "stocks_evaluated": len(results),
        "score_long": long_n,
        "score_short": short_n,
        "signal_counts": counts,
        "position_counts": pos_counts,
    }


if __name__ == "__main__":
    if len(sys.argv) > 1:
        d = date.fromisoformat(sys.argv[1])
    else:
        d = date.today()
    run(d)
