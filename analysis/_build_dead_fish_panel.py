"""Ad-hoc: build (date, stock_id, is_dead_fish) panel for ablation filtering.

Pulls turnover-only from tw.daily_prices for each stock present in the
score panel, runs analysis.money.calculate_money() to derive dead_fish
flag per bar, writes tmp/dead_fish_panel.parquet.

Usage:
    python -m analysis._build_dead_fish_panel
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

from db.connection import get_cursor
from analysis.money import calculate_money

PANEL_IN = Path("tmp/score_panel.parquet")
OUTPUT = Path("tmp/dead_fish_panel.parquet")


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    stock_ids = sorted(
        pd.read_parquet(PANEL_IN, columns=["stock_id"])["stock_id"].unique()
    )
    print(f"Stocks to process: {len(stock_ids)}", file=sys.stderr)

    rows: list[dict] = []
    t0 = time.time()

    for k, sid in enumerate(stock_ids):
        if k % 100 == 0:
            elapsed = time.time() - t0
            print(
                f"  {k}/{len(stock_ids)} ({elapsed:.0f}s, {len(rows):,} rows)",
                file=sys.stderr,
            )

        with get_cursor(commit=False) as cur:
            cur.execute(
                """
                SELECT trade_date, turnover
                FROM tw.daily_prices
                WHERE stock_id = %s AND turnover IS NOT NULL
                ORDER BY trade_date
                """,
                (sid,),
            )
            data = cur.fetchall()

        if not data:
            continue

        turnover = np.array([float(r["turnover"]) for r in data], dtype=np.float32)
        result = calculate_money(turnover)

        for i, r in enumerate(data):
            rows.append({
                "date": r["trade_date"],
                "stock_id": sid,
                "is_dead_fish": bool(result.dead_fish[i]),
            })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df.to_parquet(OUTPUT, index=False)

    dead_pct = df["is_dead_fish"].mean() * 100
    print(
        f"\nWrote {OUTPUT}  ({len(df):,} rows, {dead_pct:.1f}% dead_fish)",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
