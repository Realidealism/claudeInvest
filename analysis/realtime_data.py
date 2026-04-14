"""
Real-time stock data loader.

Mirrors backtest.data.load_stock_data, but appends today's still-forming
intraday bar from tw.intraday_quotes when daily_update.py has not yet
written today's row to tw.daily_prices.

The merge rule is intentionally clock-free: we trust trade_date as the
source of truth.

  - intraday.trade_date NOT IN daily.trade_date  -> append a forming bar
  - intraday.trade_date IS  IN daily.trade_date  -> skip (daily wins)
  - intraday row missing                          -> pure history (e.g. weekend)

Field mapping for the forming bar:

  intraday_quotes              ->  daily_prices shape
  ---------------------------------------------------
  open_price                   ->  open_price
  high_price                   ->  high_price
  low_price                    ->  low_price
  last_price                   ->  close_price   (latest matched price)
  total_volume                 ->  volume        (already shares, ×1000 in normalizer)
  total_value                  ->  turnover
  ref_price                    ->  ref_price     (from SinoPac pre-market;
                                                  falls back to previous close)

ref_price sourcing:
  tw.intraday_quotes.ref_price is populated by pre_market_update.py before
  09:00 from SinoPac Shioaji contracts (the authoritative 參考價 for each
  trading day). If that pre-market run hasn't happened — e.g. the first time
  the pipeline is started, or weekends — we fall back to the previous trading
  day's close, which is the closest approximation available.
"""

from __future__ import annotations

from db.connection import get_cursor
from backtest.data import (
    StockData,
    build_stock_data,
    fetch_stock_name,
    fetch_dividends,
)


def load_stock_data_live(stock_id: str) -> StockData:
    """
    Load full price history for `stock_id`, merge today's forming intraday
    bar if applicable, then run the same 6-module analysis pipeline as
    backtest.data.load_stock_data.
    """
    stock_name = fetch_stock_name(stock_id)

    rows = _fetch_all_daily_prices(stock_id)
    if not rows:
        raise ValueError(f"No daily_prices data for {stock_id}")

    intraday = _fetch_intraday_row(stock_id)
    rows = _merge_intraday_into_daily(rows, intraday)

    dates = [r["trade_date"] for r in rows]
    dividends = fetch_dividends(stock_id, dates)
    return build_stock_data(stock_id, stock_name, rows, dividends)


# ── DB fetchers ─────────────────────────────────────────────────────────────


def _fetch_all_daily_prices(stock_id: str) -> list[dict]:
    """All historical daily bars for a stock, oldest first."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT trade_date, open_price, high_price, low_price,
                   close_price, volume, turnover, ref_price
            FROM tw.daily_prices
            WHERE stock_id = %s
              AND close_price IS NOT NULL
            ORDER BY trade_date ASC
            """,
            (stock_id,),
        )
        return list(cur.fetchall())


def _fetch_intraday_row(stock_id: str) -> dict | None:
    """The current intraday snapshot row for a stock, or None."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT stock_id, trade_date,
                   open_price, high_price, low_price, last_price,
                   total_volume, total_value, ref_price
            FROM tw.intraday_quotes
            WHERE stock_id = %s
              AND trade_date IS NOT NULL
            """,
            (stock_id,),
        )
        return cur.fetchone()


# ── Pure merge / mapping (unit-tested without DB) ───────────────────────────


def _merge_intraday_into_daily(
    daily_rows: list[dict],
    intraday_row: dict | None,
) -> list[dict]:
    """
    Return daily_rows with one extra forming bar appended if-and-only-if the
    intraday row's trade_date is strictly newer than every existing daily
    row.

    Pure function: no DB, no clock.
    """
    if not daily_rows:
        return daily_rows
    if intraday_row is None or intraday_row.get("trade_date") is None:
        return daily_rows

    existing_dates = {r["trade_date"] for r in daily_rows}
    if intraday_row["trade_date"] in existing_dates:
        return daily_rows

    prev_close = float(daily_rows[-1]["close_price"])
    forming = _intraday_to_daily_shape(intraday_row, prev_close)
    return [*daily_rows, forming]


def _intraday_to_daily_shape(intraday_row: dict, prev_close: float) -> dict:
    """
    Project a tw.intraday_quotes row into the dict shape that build_stock_data
    expects (daily_prices column names).

    ref_price prefers the SinoPac pre-market value when present; otherwise
    falls back to the previous trading day's close.
    """
    ref = intraday_row.get("ref_price")
    ref_value = float(ref) if ref is not None else prev_close
    return {
        "trade_date":  intraday_row["trade_date"],
        "open_price":  intraday_row["open_price"],
        "high_price":  intraday_row["high_price"],
        "low_price":   intraday_row["low_price"],
        "close_price": intraday_row["last_price"],
        "volume":      intraday_row["total_volume"],
        "turnover":    intraday_row["total_value"],
        "ref_price":   ref_value,
    }
