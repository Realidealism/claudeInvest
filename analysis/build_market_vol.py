"""Build/refresh tw.market_vol — the daily cross-sectional median Parkinson-233 vol.

This is the denominator of the ScoreBoard low-volatility cell.  See
db/migrations/062_add_market_vol.sql for why the series exists and why it is the
cross-sectional median rather than the TAIEX index's own volatility.

Full rebuild is cheap (one pass over daily_prices), so there is no incremental mode:
recompute and upsert every date.  Safe to run daily.

Usage:
    python -m analysis.build_market_vol
"""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from analysis.volatility import PARKINSON_WINDOW, calculate_parkinson_vol
from db.connection import get_cursor


def main() -> None:
    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT p.stock_id, p.trade_date, p.high_price, p.low_price
            FROM tw.daily_prices p
            JOIN tw.stocks s ON s.stock_id = p.stock_id
            WHERE s.is_active = TRUE
              AND s.security_type = 'STOCK'
              AND s.market IN ('TWSE', 'TPEx')
              AND p.high_price IS NOT NULL
              AND p.low_price IS NOT NULL
              AND p.low_price > 0
            ORDER BY p.stock_id, p.trade_date
        """)
        rows = cur.fetchall()

    df = pd.DataFrame(rows)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    for c in ("high_price", "low_price"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    print(f"{len(df):,} price rows / {df['stock_id'].nunique():,} stocks", file=sys.stderr)

    vols = []
    for sid, g in df.groupby("stock_id", sort=False):
        g = g.sort_values("trade_date")
        vols.append(pd.DataFrame({
            "trade_date": g["trade_date"].to_numpy(),
            "vol": calculate_parkinson_vol(
                g["high_price"].to_numpy(), g["low_price"].to_numpy()
            ),
        }))
    v = pd.concat(vols, ignore_index=True)
    v = v[np.isfinite(v["vol"]) & (v["vol"] > 0)]

    daily = v.groupby("trade_date")["vol"].agg(["median", "count"]).reset_index()
    daily = daily[daily["count"] >= 100]
    print(f"{len(daily):,} dates with >=100 stocks "
          f"({daily['trade_date'].min().date()} .. {daily['trade_date'].max().date()})",
          file=sys.stderr)

    records = [
        (r["trade_date"].date(), float(r["median"]), int(r["count"]))
        for _, r in daily.iterrows()
    ]
    with get_cursor(commit=True) as cur:
        cur.executemany(
            "INSERT INTO tw.market_vol (trade_date, median_vol, n_stocks) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (trade_date) DO UPDATE SET "
            "median_vol = EXCLUDED.median_vol, n_stocks = EXCLUDED.n_stocks, "
            "updated_at = NOW()",
            records,
        )
    print(f"upserted {len(records):,} rows (window={PARKINSON_WINDOW})", file=sys.stderr)


if __name__ == "__main__":
    main()
