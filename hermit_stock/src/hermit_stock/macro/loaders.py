"""DB loaders for macro/breadth/sentiment series.

All series returned indexed by trade_date (datetime.date), float values.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
from psycopg2.extras import RealDictCursor

from ..data.adapters.db_adapter import _connect


def load_taiex(start: date | None = None, end: date | None = None) -> pd.Series:
    """TAIEX close-price series."""
    sql = """
        SELECT trade_date, close_price
        FROM tw.index_prices
        WHERE index_id = 'TAIEX'
          AND (%s::date IS NULL OR trade_date >= %s)
          AND (%s::date IS NULL OR trade_date <= %s)
        ORDER BY trade_date
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (start, start, end, end))
        rows = cur.fetchall()
    s = pd.Series(
        {r["trade_date"]: float(r["close_price"]) for r in rows if r["close_price"] is not None}
    )
    s.index = pd.to_datetime(s.index)
    s.name = "taiex_close"
    return s


def load_market_breadth_trend(start: date | None = None, end: date | None = None) -> pd.DataFrame:
    """Market-breadth trend codes (short_trend / medium_trend / long_trend).

    Trend codes: -2 = strong bear, -1 = bear, 0 = neutral, 1 = bull, 2 = strong bull.
    """
    sql = """
        SELECT trade_date, short_trend, medium_trend, long_trend
        FROM tw.market_breadth
        WHERE (%s::date IS NULL OR trade_date >= %s)
          AND (%s::date IS NULL OR trade_date <= %s)
        ORDER BY trade_date
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (start, start, end, end))
        rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df.set_index("trade_date").sort_index()


def load_foreign_net_aggregated(start: date | None = None, end: date | None = None) -> pd.Series:
    """Daily total foreign-investor net buy/sell across the whole market (NTD)."""
    sql = """
        SELECT trade_date, SUM(foreign_net)::BIGINT AS net
        FROM tw.daily_prices
        WHERE foreign_net IS NOT NULL
          AND (%s::date IS NULL OR trade_date >= %s)
          AND (%s::date IS NULL OR trade_date <= %s)
        GROUP BY trade_date
        ORDER BY trade_date
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (start, start, end, end))
        rows = cur.fetchall()
    s = pd.Series({r["trade_date"]: float(r["net"]) for r in rows if r["net"] is not None})
    s.index = pd.to_datetime(s.index)
    s.name = "foreign_net"
    return s


def load_margin_balance(start: date | None = None, end: date | None = None) -> pd.Series:
    """Daily total margin balance (融資餘額)."""
    sql = """
        SELECT trade_date, margin_balance
        FROM tw.margin_summary
        WHERE margin_balance IS NOT NULL
          AND (%s::date IS NULL OR trade_date >= %s)
          AND (%s::date IS NULL OR trade_date <= %s)
        ORDER BY trade_date
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (start, start, end, end))
        rows = cur.fetchall()
    s = pd.Series({r["trade_date"]: float(r["margin_balance"]) for r in rows})
    s.index = pd.to_datetime(s.index)
    s.name = "margin_balance"
    return s
