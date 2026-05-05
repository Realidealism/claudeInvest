"""Daily ScoreBoard snapshot — top-100 long + top-100 short.

Runs build_scoreboard() on every active stock (TWSE/TPEx, security_type=STOCK)
at its latest bar, ranks by total.long.pct and total.short.pct (turnover as
tie-breaker), and writes the top 100 of each side to tw.score_snapshot. Diff
vs previous snapshot of the same side is computed (is_new / prev_rank /
rank_delta), mirroring the hermit_screen_snapshot pattern.

Cost is ~minutes (1700 stocks × load_stock_data + evaluate). Storage is
~30 KB per day.
"""

from __future__ import annotations

import sys
from datetime import date

from psycopg2.extras import execute_batch

from db.connection import get_cursor, get_connection
from backtest.data import load_stock_data
from analysis.score import build_scoreboard

TOP_N = 100


def _load_active_stocks() -> list[tuple[str, str]]:
    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT stock_id, name FROM tw.stocks
            WHERE is_active = TRUE
              AND security_type = 'STOCK'
              AND market IN ('TWSE', 'TPEx')
            ORDER BY stock_id
        """)
        return [(r["stock_id"], r["name"]) for r in cur.fetchall()]


def _load_prev_snapshot(snapshot_date: date, side: str) -> dict[str, int]:
    """Return {stock_id: rank} from the most recent snapshot before given date."""
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


def _eval_pct(board, data, idx: int) -> tuple[float | None, float | None]:
    """Evaluate scoreboard at bar idx; return (long_pct, short_pct) or (None, None)."""
    if idx < 0:
        return (None, None)
    try:
        r = board.evaluate(data, idx)
    except Exception:
        return (None, None)
    return (r.total.long.pct, r.total.short.pct)


def _evaluate_all() -> list[tuple]:
    """Return per-stock tuple
        (stock_id, long_pct, short_pct, turnover,
         long_pct_d1, short_pct_d1, long_pct_d2, short_pct_d2,
         long_pct_d3, short_pct_d3)
    Today's bar is data.n-1; d1 = data.n-2; d2 = data.n-3; d3 = data.n-4."""
    stocks = _load_active_stocks()
    print(f"  Loaded {len(stocks)} active stocks", file=sys.stderr)

    board = build_scoreboard()
    out: list[tuple] = []

    for k, (sid, _name) in enumerate(stocks):
        if k % 200 == 0 and k > 0:
            print(f"  evaluated {k}/{len(stocks)} ...", file=sys.stderr)
        try:
            data = load_stock_data(sid)
        except Exception:
            continue
        if data.n < 60:
            continue
        lp, sp = _eval_pct(board, data, data.n - 1)
        if lp is None:
            continue
        lp_d1, sp_d1 = _eval_pct(board, data, data.n - 2)
        lp_d2, sp_d2 = _eval_pct(board, data, data.n - 3)
        lp_d3, sp_d3 = _eval_pct(board, data, data.n - 4)
        # turnover may be NaN for halted bars
        tv_raw = float(data.turnover[-1])
        tv = tv_raw if tv_raw == tv_raw else 0.0
        out.append((sid, lp, sp, tv,
                    lp_d1, sp_d1, lp_d2, sp_d2, lp_d3, sp_d3))

    return out


def _save_side(snapshot_date: date, side: str,
               results: list[tuple],
               prev: dict[str, int]) -> int:
    """Sort by side's pct desc (turnover tie-breaker), keep top N, write rows.

    For each row, pct_d1/pct_d2 store the SAME side's pct evaluated at the
    previous 2 trading bars."""
    # tuple layout:
    #   (sid, long, short, tv,
    #    long_d1, short_d1, long_d2, short_d2, long_d3, short_d3)
    if side == "long":
        ranked = sorted(results, key=lambda r: (-r[1], -r[3]))[:TOP_N]
        pct_idx, d1_idx, d2_idx, d3_idx = 1, 4, 6, 8
    else:
        ranked = sorted(results, key=lambda r: (-r[2], -r[3]))[:TOP_N]
        pct_idx, d1_idx, d2_idx, d3_idx = 2, 5, 7, 9

    rows = []
    for i, r in enumerate(ranked, start=1):
        sid = r[0]
        pct = r[pct_idx]
        turnover = r[3]
        pct_d1 = r[d1_idx]
        pct_d2 = r[d2_idx]
        pct_d3 = r[d3_idx]
        prev_rank = prev.get(sid)
        is_new = prev_rank is None
        rank_delta = (prev_rank - i) if prev_rank is not None else None
        rows.append((
            snapshot_date, side, i, sid,
            round(pct, 3), round(turnover, 2),
            is_new, prev_rank, rank_delta,
            round(pct_d1, 3) if pct_d1 is not None else None,
            round(pct_d2, 3) if pct_d2 is not None else None,
            round(pct_d3, 3) if pct_d3 is not None else None,
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


def run(snapshot_date: date) -> dict[str, int]:
    """Main entry. Returns {long_n, short_n, new_long, new_short}."""
    print(f"  ScoreBoard snapshot @ {snapshot_date} ...")

    results = _evaluate_all()
    print(f"  scored {len(results)} stocks")

    prev_long = _load_prev_snapshot(snapshot_date, "long")
    prev_short = _load_prev_snapshot(snapshot_date, "short")

    long_n = _save_side(snapshot_date, "long", results, prev_long)
    short_n = _save_side(snapshot_date, "short", results, prev_short)

    new_long = sum(1 for r in
                   sorted(results, key=lambda r: (-r[1], -r[3]))[:TOP_N]
                   if r[0] not in prev_long)
    new_short = sum(1 for r in
                    sorted(results, key=lambda r: (-r[2], -r[3]))[:TOP_N]
                    if r[0] not in prev_short)

    print(f"  Top-{TOP_N} long: {long_n} rows ({new_long} NEW)")
    print(f"  Top-{TOP_N} short: {short_n} rows ({new_short} NEW)")

    return {
        "long_n": long_n,
        "short_n": short_n,
        "new_long": new_long,
        "new_short": new_short,
    }


if __name__ == "__main__":
    if len(sys.argv) > 1:
        d = date.fromisoformat(sys.argv[1])
    else:
        d = date.today()
    run(d)
