"""
Data loading and analysis pipeline for backtesting.

Fetches OHLCV data from tw.daily_prices, runs all 6 analysis modules,
and bundles everything into a single StockData object.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
from numpy.typing import NDArray

from db.connection import get_cursor
from analysis.close import calculate_close, calc_sort_forming, CloseResult, SortResult
from analysis.volume import calculate_volume, VolumeResult
from analysis.candle import calculate_candle, CandleResult
from analysis.money import calculate_money, MoneyResult
from analysis.obv import calculate_obv, OBVResult
from analysis.wave import calculate_wave, WaveResult

F32 = np.float32
F32Array = NDArray[np.float32]

# Warmup days needed before backtest start (longest SMA period)
WARMUP_DAYS = 400


@dataclass
class DividendEvent:
    """A single dividend event for a stock."""
    ex_date: date
    day_index: int          # index into the data arrays
    cash_dividend: float    # per share (NTD)
    stock_dividend: float   # per 10 shares (Taiwan convention)


@dataclass
class StockData:
    """All raw + computed data for one stock, indexed by trading day."""
    stock_id: str
    stock_name: str
    dates: list[date]
    open: F32Array
    high: F32Array
    low: F32Array
    close: F32Array
    volume: F32Array
    turnover: F32Array
    ref_price: F32Array

    # Analysis results
    close_result: CloseResult
    volume_result: VolumeResult
    candle_result: CandleResult
    money_result: MoneyResult
    obv_result: OBVResult
    wave_result: WaveResult

    # Forming sort alignment (depends on close + volume)
    sort_forming: dict[str, SortResult]

    # Dividend events
    dividends: list[DividendEvent]

    @property
    def n(self) -> int:
        return len(self.dates)


def load_stock_data(
    stock_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> StockData:
    """
    Load stock data and run full analysis pipeline.

    If start_date is given, extra warmup days are fetched before it
    so all indicators are valid at start_date.
    """
    stock_name = fetch_stock_name(stock_id)

    rows = _fetch_prices(stock_id, start_date, end_date)
    if not rows:
        raise ValueError(f"No data found for {stock_id}")

    dates = [r["trade_date"] for r in rows]
    dividends = fetch_dividends(stock_id, dates)
    return build_stock_data(stock_id, stock_name, rows, dividends)


def build_stock_data(
    stock_id: str,
    stock_name: str,
    rows: list[dict],
    dividends: list["DividendEvent"],
) -> StockData:
    """
    Convert raw daily_prices-shaped rows into a StockData with all 6 analysis
    results populated.

    `rows` must be ordered ascending by trade_date and contain the keys
    trade_date, open_price, high_price, low_price, close_price, volume,
    turnover, ref_price (ref_price may be None).

    Realtime callers (analysis.realtime_data) reuse this with the last row
    being a forming intraday bar projected into the same shape.
    """
    if not rows:
        raise ValueError(f"build_stock_data: empty rows for {stock_id}")

    dates = [r["trade_date"] for r in rows]
    close = np.array([float(r["close_price"]) for r in rows], dtype=F32)
    high = np.array([float(r["high_price"]) for r in rows], dtype=F32)
    low = np.array([float(r["low_price"]) for r in rows], dtype=F32)
    open_ = np.array([float(r["open_price"]) for r in rows], dtype=F32)
    volume = np.array([float(r["volume"]) for r in rows], dtype=F32)
    turnover = np.array([float(r["turnover"]) for r in rows], dtype=F32)

    # ref_price for OBV: use ref_price column, fallback to previous close
    ref_price = np.zeros(len(rows), dtype=F32)
    for i, r in enumerate(rows):
        if r["ref_price"] is not None:
            ref_price[i] = float(r["ref_price"])
        elif i > 0:
            ref_price[i] = close[i - 1]
        else:
            ref_price[i] = close[i]

    close_result = calculate_close(close)
    volume_result = calculate_volume(volume)
    candle_result = calculate_candle(open_, high, low, close)
    money_result = calculate_money(turnover)
    obv_result = calculate_obv(close, ref_price, high, low, volume)
    wave_result = calculate_wave(
        open_, high, low, close,
        candle_result, close_result.bs,
        volume=volume,
    )
    sort_forming = calc_sort_forming(close_result, volume_result.volume_status)

    return StockData(
        stock_id=stock_id,
        stock_name=stock_name,
        dates=dates,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        turnover=turnover,
        ref_price=ref_price,
        close_result=close_result,
        volume_result=volume_result,
        candle_result=candle_result,
        money_result=money_result,
        obv_result=obv_result,
        wave_result=wave_result,
        sort_forming=sort_forming,
        dividends=dividends,
    )


def fetch_stock_name(stock_id: str) -> str:
    """Get stock name from tw.stocks."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT name FROM tw.stocks WHERE stock_id = %s",
            (stock_id,),
        )
        row = cur.fetchone()
    return row["name"] if row else stock_id


def _fetch_prices(
    stock_id: str,
    start_date: date | None,
    end_date: date | None,
) -> list[dict]:
    """Fetch OHLCV + ref_price from tw.daily_prices."""
    base_query = """
        SELECT trade_date, open_price, high_price, low_price,
               close_price, volume, turnover, ref_price
        FROM tw.daily_prices
        WHERE stock_id = %s
          AND close_price IS NOT NULL
    """
    params: list = [stock_id]

    if end_date:
        base_query += " AND trade_date <= %s"
        params.append(end_date)

    base_query += " ORDER BY trade_date ASC"

    with get_cursor(commit=False) as cur:
        cur.execute(base_query, params)
        all_rows = cur.fetchall()

    if start_date:
        # Find start index with warmup
        start_idx = 0
        for i, r in enumerate(all_rows):
            if r["trade_date"] >= start_date:
                start_idx = max(0, i - WARMUP_DAYS)
                break
        return all_rows[start_idx:]

    return all_rows


def fetch_dividends(
    stock_id: str,
    dates: list[date],
) -> list[DividendEvent]:
    """Fetch dividend events and map to day indices."""
    if not dates:
        return []

    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT ex_date, cash_dividend, stock_dividend
            FROM tw.dividends
            WHERE stock_id = %s
              AND ex_date BETWEEN %s AND %s
            ORDER BY ex_date ASC
            """,
            (stock_id, dates[0], dates[-1]),
        )
        rows = cur.fetchall()

    # Build date->index lookup
    date_to_idx = {d: i for i, d in enumerate(dates)}

    events = []
    for r in rows:
        idx = date_to_idx.get(r["ex_date"])
        if idx is not None:
            events.append(DividendEvent(
                ex_date=r["ex_date"],
                day_index=idx,
                cash_dividend=float(r["cash_dividend"] or 0),
                stock_dividend=float(r["stock_dividend"] or 0),
            ))
    return events
