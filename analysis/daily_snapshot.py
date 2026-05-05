"""Combined daily snapshot — score top-100 long/short + signal-factory fires
for the 6 conditions, in a single multiprocessing pass.

Replaces the previous separate analysis/score_snapshot.py and
analysis/signal_snapshot.py. Each worker process loads a stock once, then
runs both the 4-bar scoreboard evaluations and the 6 condition functions
before moving on. Workers run in parallel via multiprocessing.Pool.

Set DAILY_SNAPSHOT_WORKERS env var to override the worker count (default 8,
tuned for an 8 P-core CPU; raise on bigger machines, lower if memory is
tight).
"""

from __future__ import annotations

import os
import sys
import time
from datetime import date
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

TOP_N = 100
DEFAULT_WORKERS = max(1, min(cpu_count() or 1, 8))
N_WORKERS = int(os.environ.get("DAILY_SNAPSHOT_WORKERS", str(DEFAULT_WORKERS)))

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


def _eval_stock(sid: str) -> dict | None:
    """Worker-process entry. Load one stock, evaluate scoreboard at 4 bars
    and 6 signal conditions on the latest bar. Returns flat dict of
    primitives so the result pickles cheaply, or None when unusable."""
    try:
        data = load_stock_data(sid)
    except Exception:
        return None
    if data.n < 60:
        return None

    idx = data.n - 1
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

    tv_raw = float(data.turnover[idx])
    tv = tv_raw if tv_raw == tv_raw else 0.0

    return {
        "sid": sid,
        "lp": lp, "sp": sp, "tv": tv,
        "lp_d1": lp_d1, "sp_d1": sp_d1,
        "lp_d2": lp_d2, "sp_d2": sp_d2,
        "lp_d3": lp_d3, "sp_d3": sp_d3,
        "fires": fires,
    }


# ── Main side ──────────────────────────────────────────────────────────────

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

    t0 = time.time()
    print(f"  Daily snapshot @ {snapshot_date} (workers={N_WORKERS}) ...")
    stock_ids = _load_active_stock_ids()
    print(f"  Loaded {len(stock_ids)} active stocks", file=sys.stderr)

    results: list[dict] = []
    with Pool(N_WORKERS, initializer=_init_worker) as pool:
        for r in pool.imap_unordered(_eval_stock, stock_ids, chunksize=20):
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

    print(f"  Total wall time: {time.time() - t0:.1f}s")
    return {
        "stocks_evaluated": len(results),
        "score_long": long_n,
        "score_short": short_n,
        "signal_counts": counts,
    }


if __name__ == "__main__":
    if len(sys.argv) > 1:
        d = date.fromisoformat(sys.argv[1])
    else:
        d = date.today()
    run(d)
