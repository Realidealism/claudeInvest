"""Ad-hoc Stage A: build (stock, day, score, forward return) panel.

For every active TWSE/TPEx stock and every bar with i >= 400 (long-MA warmup),
record total_long, the 18 category × timeframe contributions on the long side,
close, and forward returns at H=5/20/60.

Output: tmp/score_panel.parquet

Usage:
    python -m analysis._score_panel              # full universe
    python -m analysis._score_panel 20           # first 20 stocks (smoke test)
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

from db.connection import get_cursor
from backtest.data import load_stock_data
from analysis.score import build_scoreboard

START_INDEX = 400
HORIZONS = (5, 20, 60)
CATEGORIES = [
    "扣抵", "排列", "大盤", "波浪", "洪量", "OBV", "MACD", "Donchian",
    "距離_p55", "距離_p89", "距離_p144", "距離_p233", "距離_p377",
    "波動",
]
TIMEFRAMES = ["short", "medium", "long"]
# Cells dropped from score.py 2026-07-07 (breadth-regime re-review):
# MACD_medium / 大盤_long — exclude their columns from the panel schema.
DROPPED_CELLS = {"MACD_medium", "大盤_long"}
OUTPUT = Path("tmp/score_panel.parquet")


def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT stock_id, name FROM tw.stocks
            WHERE is_active = TRUE
              AND security_type = 'STOCK'
              AND market IN ('TWSE', 'TPEx')
            ORDER BY stock_id
        """)
        stocks = [(r["stock_id"], r["name"]) for r in cur.fetchall()]

    print(f"Loaded {len(stocks)} active stocks", file=sys.stderr)

    if len(sys.argv) > 1:
        limit = int(sys.argv[1])
        stocks = stocks[:limit]
        print(f"  (limited to first {limit} for smoke test)", file=sys.stderr)

    board = build_scoreboard()
    print("Built scoreboard", file=sys.stderr)

    cat_tf_cols = [
        f"{c}_{tf}" for c in CATEGORIES for tf in TIMEFRAMES
        if f"{c}_{tf}" not in DROPPED_CELLS
    ]

    rows: list[dict] = []
    t0 = time.time()
    skipped = 0

    for k, (sid, _name) in enumerate(stocks):
        if k % 50 == 0:
            elapsed = time.time() - t0
            print(
                f"  {k}/{len(stocks)} stocks  ({elapsed:.0f}s, "
                f"{len(rows):,} rows, skipped {skipped})",
                file=sys.stderr,
            )
        try:
            data = load_stock_data(sid)
        except Exception as e:
            skipped += 1
            print(f"  skip {sid}: {e}", file=sys.stderr)
            continue
        if data.n < START_INDEX + 1:
            skipped += 1
            continue

        # Vectorized forward returns
        close = data.close.astype(np.float64)
        fwd = {}
        for h in HORIZONS:
            r = np.full(data.n, np.nan, dtype=np.float64)
            if data.n > h:
                r[: data.n - h] = close[h:] / close[: data.n - h] - 1.0
            fwd[h] = r

        for i in range(START_INDEX, data.n):
            try:
                result = board.evaluate(data, i)
            except Exception:
                continue

            # Aggregate category × timeframe (long side only)
            agg = {col: 0.0 for col in cat_tf_cols}
            for tf_name in TIMEFRAMES:
                tf_score = getattr(result, tf_name)
                for d in tf_score.long.details:
                    if d.category in CATEGORIES:
                        agg[f"{d.category}_{tf_name}"] += d.points

            row = {
                "date": data.dates[i],
                "stock_id": sid,
                "close": float(close[i]),
                "total_long": float(result.total.long_score),
                **agg,
            }
            for h in HORIZONS:
                v = fwd[h][i]
                row[f"fwd_{h}"] = float(v) if not np.isnan(v) else np.nan
            rows.append(row)

    elapsed = time.time() - t0
    print(
        f"Done: {len(rows):,} rows from {len(stocks) - skipped} stocks "
        f"in {elapsed:.0f}s",
        file=sys.stderr,
    )

    df = pd.DataFrame(rows)
    df.to_parquet(OUTPUT, index=False)
    mb = df.memory_usage(deep=True).sum() / 1e6
    print(f"Wrote {OUTPUT}  ({mb:.1f} MB in memory)", file=sys.stderr)


if __name__ == "__main__":
    main()
