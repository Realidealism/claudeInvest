"""Read-only data access layer for the chip (集保大戶) model.

Every connection is opened read-only via psycopg2's set_session(readonly=True),
so any accidental INSERT/UPDATE/DELETE/DDL raises ReadOnlySqlTransaction before
touching the database. Reuses config.settings.DB_CONFIG — no DATABASE_URL and no
separate DB role (per project decision).
"""
from contextlib import contextmanager

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

from config.settings import DB_CONFIG

COMMON_STOCK_TYPE = "STOCK"   # tw.stocks.security_type for 普通股 (vs EQUITY_ETF / BOND_ETF)
BENCHMARK_INDEX_ID = "TAIEX"  # 加權指數, tw.index_prices


@contextmanager
def get_ro_cursor():
    """Yield a read-only RealDictCursor. Writes raise ReadOnlySqlTransaction."""
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        yield cur
    finally:
        cur.close()
        conn.close()


def load_distribution() -> pd.DataFrame:
    """Weekly tier columns used by the chip metrics.

    散戶 <10張 = t1..t3, 大戶 >800張 = t14..t15, 千張大戶人數 = t15_holders.
    """
    with get_ro_cursor() as cur:
        cur.execute(
            """
            SELECT stock_id, data_date,
                   t1_pct, t2_pct, t3_pct,
                   t14_pct, t15_pct, t15_holders
            FROM tw.shareholder_distribution
            ORDER BY stock_id, data_date
            """
        )
        return pd.DataFrame(cur.fetchall())


def load_common_universe() -> set[str]:
    """Common stocks only (security_type='STOCK'), excluding delisted."""
    with get_ro_cursor() as cur:
        cur.execute(
            """
            SELECT stock_id FROM tw.stocks
            WHERE security_type = %s AND delisted_date IS NULL
            """,
            (COMMON_STOCK_TYPE,),
        )
        return {r["stock_id"] for r in cur.fetchall()}


def load_prices(stock_ids, start, end) -> pd.DataFrame:
    """Daily close for the given stocks within [start, end]."""
    with get_ro_cursor() as cur:
        cur.execute(
            """
            SELECT stock_id, trade_date, close_price
            FROM tw.daily_prices
            WHERE stock_id = ANY(%s)
              AND trade_date BETWEEN %s AND %s
              AND close_price IS NOT NULL
            ORDER BY stock_id, trade_date
            """,
            (list(stock_ids), start, end),
        )
        return pd.DataFrame(cur.fetchall())


def load_benchmark(index_id, start, end) -> pd.DataFrame:
    """Index daily close within [start, end]."""
    with get_ro_cursor() as cur:
        cur.execute(
            """
            SELECT trade_date, close_price
            FROM tw.index_prices
            WHERE index_id = %s
              AND trade_date BETWEEN %s AND %s
              AND close_price IS NOT NULL
            ORDER BY trade_date
            """,
            (index_id, start, end),
        )
        return pd.DataFrame(cur.fetchall())


def shareholder_dates() -> list:
    """All distinct weekly snapshot dates, ascending."""
    with get_ro_cursor() as cur:
        cur.execute(
            "SELECT DISTINCT data_date FROM tw.shareholder_distribution ORDER BY data_date"
        )
        return [r["data_date"] for r in cur.fetchall()]
