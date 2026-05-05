"""Daily signal-factory snapshot — for each of the 6 conditions, list every
active stock whose latest bar triggered the condition.

Conditions are O(1) lookups once the BoolArray is computed for the stock,
so cost is dominated by load_stock_data + condition computation. Similar to
score_snapshot.py in shape; data goes to tw.signal_snapshot, one row per
(snapshot_date, signal, stock_id)."""

from __future__ import annotations

import sys
from datetime import date

from psycopg2.extras import execute_batch

from db.connection import get_cursor, get_connection
from backtest.data import load_stock_data
from signal_backtest.factories._conditions import (
    pick_condition, touch_condition,
    buy_condition, sell_condition,
    buy_flee_signal, sell_flee_signal,
)

# Insertion order = display order on the frontend (kept stable in JSON dict).
SIGNALS = [
    ("pick",      pick_condition),
    ("touch",     touch_condition),
    ("buy",       buy_condition),
    ("sell",      sell_condition),
    ("buy_flee",  buy_flee_signal),
    ("sell_flee", sell_flee_signal),
]


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


def _scan() -> list[tuple[str, str, float]]:
    """Return [(signal_name, stock_id, turnover), ...] for stocks where any
    of the 6 signal conditions fired on data.n - 1."""
    stocks = _load_active_stocks()
    print(f"  Loaded {len(stocks)} active stocks", file=sys.stderr)

    out: list[tuple[str, str, float]] = []
    for k, (sid, _name) in enumerate(stocks):
        if k % 200 == 0 and k > 0:
            print(f"  scanned {k}/{len(stocks)} ...", file=sys.stderr)
        try:
            data = load_stock_data(sid)
        except Exception:
            continue
        if data.n < 60:
            continue
        idx = data.n - 1
        tv_raw = float(data.turnover[idx])
        tv = tv_raw if tv_raw == tv_raw else 0.0
        for sig_name, fn in SIGNALS:
            try:
                arr = fn(data)
            except Exception:
                continue
            if bool(arr[idx]):
                out.append((sig_name, sid, tv))
    return out


def run(snapshot_date: date) -> dict[str, int]:
    """Main entry. Returns {signal_name: count} for the 6 signals."""
    print(f"  Signal snapshot @ {snapshot_date} ...")

    fires = _scan()
    counts: dict[str, int] = {name: 0 for name, _ in SIGNALS}
    for sig_name, _sid, _tv in fires:
        counts[sig_name] += 1

    rows = [
        (snapshot_date, sig, sid, round(tv, 2))
        for (sig, sid, tv) in fires
    ]
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

    for name, _ in SIGNALS:
        print(f"  {name}: {counts[name]} stocks")
    print(f"  Total fires: {len(fires)}")
    return counts


if __name__ == "__main__":
    if len(sys.argv) > 1:
        d = date.fromisoformat(sys.argv[1])
    else:
        d = date.today()
    run(d)
