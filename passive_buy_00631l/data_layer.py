"""Read-only data access for the 00631L passive-buy model.

Every connection is opened read-only (set_session(readonly=True)); reuses
config.settings.DB_CONFIG. Returns a single date-aligned pandas frame so the
signal/strategy/backtest layers never re-query.
"""
from __future__ import annotations

from contextlib import contextmanager

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

from config.settings import DB_CONFIG


@contextmanager
def _ro_cursor():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        yield cur
    finally:
        cur.close()
        conn.close()


def _split_adjust(close: np.ndarray) -> np.ndarray:
    """Back-adjust stock-split discontinuities (tw.daily_prices is unadjusted and
    has no split records). A split shows as a single-bar ratio far outside any
    real daily move (leveraged ETF max ≈ ±20%), so |ratio|<0.7 or >1.43 ⇒ split.
    Most-recent prices keep their scale; historical prices are scaled to match,
    preserving real wealth (more shares at lower price). The tiny market move on
    the split bar is folded into the factor — immaterial vs a 4×–23× split.
    """
    n = len(close)
    mult = np.ones(n)
    running = 1.0
    for i in range(n - 1, 0, -1):
        if close[i - 1] > 0:
            r = close[i] / close[i - 1]
            if r < 0.7 or r > 1.43:
                running *= r
        mult[i - 1] = running
    return close * mult


def _stock_close(cur, stock_id: str, start, end) -> pd.Series:
    cur.execute(
        """
        SELECT trade_date, close_price
        FROM tw.daily_prices
        WHERE stock_id = %s AND trade_date BETWEEN %s AND %s
        ORDER BY trade_date
        """,
        (stock_id, start, end),
    )
    df = pd.DataFrame(cur.fetchall())
    s = pd.Series(dtype="float64", name=stock_id)
    if not df.empty:
        s = pd.Series(
            df["close_price"].astype("float64").values,
            index=pd.to_datetime(df["trade_date"]),
            name=stock_id,
        )
    return s


def load_market_frame(cfg) -> pd.DataFrame:
    """Date-aligned frame on the 00631L trading calendar.

    Columns: target (00631L close), proxy (TAIEX close), c_close (0050 close),
    advance/decline/advance_limit/decline_limit, breadth net ratios,
    margin_balance. Close NULLs are forward-filled (handful of days).
    """
    start = cfg["backtest"]["start"]
    end = cfg["backtest"]["end"] or "2100-01-01"
    target = cfg["target"]
    proxy = cfg["signal_proxy"]
    c_sym = cfg["baseline_c_symbol"]

    with _ro_cursor() as cur:
        target_s = _stock_close(cur, target, start, end)
        c_s = _stock_close(cur, c_sym, start, end)

        cur.execute(
            """
            SELECT trade_date, close_price, change_pct,
                   advance, decline, advance_limit, decline_limit
            FROM tw.index_prices
            WHERE index_id = %s AND trade_date BETWEEN %s AND %s
            ORDER BY trade_date
            """,
            (proxy, start, end),
        )
        idx = pd.DataFrame(cur.fetchall())

        cur.execute(
            """
            SELECT trade_date, short_up, short_down, medium_up, medium_down,
                   long_up, long_down
            FROM tw.market_breadth
            WHERE trade_date BETWEEN %s AND %s
            ORDER BY trade_date
            """,
            (start, end),
        )
        breadth = pd.DataFrame(cur.fetchall())

        cur.execute(
            """
            SELECT trade_date, margin_balance
            FROM tw.margin_summary
            WHERE trade_date BETWEEN %s AND %s
            ORDER BY trade_date
            """,
            (start, end),
        )
        margin = pd.DataFrame(cur.fetchall())

    # Master index = 00631L trading days (we can only buy when it trades).
    frame = pd.DataFrame(index=target_s.index)
    frame["target"] = target_s
    frame["c_close"] = c_s.reindex(frame.index)

    if not idx.empty:
        idx.index = pd.to_datetime(idx["trade_date"])
        for col in ["close_price", "change_pct", "advance", "decline",
                    "advance_limit", "decline_limit"]:
            frame[col] = idx[col].astype("float64").reindex(frame.index)
    frame.rename(columns={"close_price": "proxy"}, inplace=True)

    if not breadth.empty:
        breadth.index = pd.to_datetime(breadth["trade_date"])
        for col in ["short_up", "short_down", "medium_up", "medium_down",
                    "long_up", "long_down"]:
            frame[col] = breadth[col].astype("float64").reindex(frame.index)

    if not margin.empty:
        margin.index = pd.to_datetime(margin["trade_date"])
        frame["margin_balance"] = (
            margin["margin_balance"].astype("float64").reindex(frame.index)
        )

    # Forward-fill the handful of NULL closes and any reindex gaps.
    frame["target"] = frame["target"].ffill()
    frame["c_close"] = frame["c_close"].ffill()
    frame["proxy"] = frame["proxy"].ffill()
    frame = frame.ffill()
    frame = frame.dropna(subset=["target", "proxy"])

    # Split-adjust the tradable stocks (TAIEX index has no splits).
    frame["target_raw"] = frame["target"]
    frame["target"] = _split_adjust(frame["target"].to_numpy(dtype=np.float64))
    frame["c_close"] = _split_adjust(frame["c_close"].to_numpy(dtype=np.float64))
    return frame


def load_cnn_fear_greed(start, end) -> pd.Series:
    """CNN Fear & Greed headline score (0-100). ~1 year history; report overlay only."""
    with _ro_cursor() as cur:
        cur.execute(
            """
            SELECT trade_date, score
            FROM tw.cnn_fear_greed
            WHERE trade_date BETWEEN %s AND %s AND score IS NOT NULL
            ORDER BY trade_date
            """,
            (start, end or "2100-01-01"),
        )
        df = pd.DataFrame(cur.fetchall())
    if df.empty:
        return pd.Series(dtype="float64", name="cnn")
    return pd.Series(
        df["score"].astype("float64").values,
        index=pd.to_datetime(df["trade_date"]),
        name="cnn",
    )
