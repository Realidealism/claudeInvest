"""
Data loading and analysis pipeline for backtesting.

Fetches OHLCV data from tw.daily_prices, runs all 6 analysis modules,
and bundles everything into a single StockData object.
"""

from __future__ import annotations

import os
import pickle
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
from numpy.typing import NDArray

from db.connection import get_cursor
from analysis.close import calculate_close, calc_sort_forming, CloseResult, SortResult
from analysis.volume import calculate_volume, VolumeResult
from analysis.candle import calculate_candle, CandleResult
from analysis.money import calculate_money, MoneyResult
from analysis.obv import calculate_obv_multi, OBVMultiResult
from analysis.wave import calculate_wave, WaveResult
from analysis.over_breakout import calculate_over_breakout, OverBreakoutResult
from analysis.market_state import calculate_market_state, MarketState
from analysis.macd import calculate_macd, MACDResult
from analysis.donchian import calculate_donchian, DonchianResult


@dataclass
class DonchianMulti:
    """Three-scope Donchian breakout signals (production: long-only scored).

    Long entry=233 / exit=144 (Fibonacci, golden-ratio exit).
    Window sweep on 89/123/144/233/377 picked 233 for best balance:
    H=60 全期 +0.023pp (alpha), H=60 空頭 +0.023pp (only positive in bear).
    short/medium computed but NOT added to ScoreBoard (subset trap).
    """
    short: DonchianResult   # entry=21 / exit=8
    medium: DonchianResult  # entry=55 / exit=21
    long: DonchianResult    # entry=233 / exit=144

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
    obv: OBVMultiResult
    wave_result: WaveResult
    over_breakout: OverBreakoutResult
    market_state: MarketState
    macd: MACDResult
    donchian: DonchianMulti

    # Forming sort alignment (depends on close + volume)
    sort_forming: dict[str, SortResult]

    # Dividend events
    dividends: list[DividendEvent]

    @property
    def n(self) -> int:
        return len(self.dates)


_INDEX_IDS = {"TAIEX", "TPEx"}


# ── Pickle cache for built StockData ─────────────────────────────────────────
# Profile shows 79% of load time is the analysis pipeline (build_stock_data),
# only 21% is DB IO — so caching just raw rows would barely help. We pickle the
# full StockData (numpy arrays + nested dataclasses) and key the file by the
# DB's latest trade_date, which auto-invalidates after daily_update.py.

CACHE_DIR = Path("data/stock_cache")
_DB_MAX_DATE: date | None = None  # session-scoped, queried once per process


def _get_db_max_date() -> date:
    """Return max(trade_date) across tw.daily_prices. Cached per process."""
    global _DB_MAX_DATE
    if _DB_MAX_DATE is None:
        with get_cursor(commit=False) as cur:
            cur.execute("SELECT MAX(trade_date) AS d FROM tw.daily_prices")
            row = cur.fetchone()
            _DB_MAX_DATE = row["d"]
    return _DB_MAX_DATE


def _cache_path(stock_id: str, db_date: date) -> Path:
    """Cache file is keyed by stock_id + DB latest trade_date."""
    return CACHE_DIR / f"{stock_id}_{db_date.isoformat()}.pkl"


def _try_load_cache(path: Path) -> StockData | None:
    """Read pickled StockData if file exists and is loadable."""
    if not path.exists():
        return None
    try:
        with path.open("rb") as f:
            return pickle.load(f)
    except Exception:
        # Stale schema, partial write, etc. — fall back to rebuild.
        return None


def _write_cache(path: Path, data: StockData) -> None:
    """Write pickle atomically (tmp + rename) so concurrent workers are safe."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp{os.getpid()}")
    with tmp.open("wb") as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(tmp, path)


def load_stock_data(
    stock_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
    use_cache: bool = False,
) -> StockData:
    """
    Load stock data and run full analysis pipeline.

    If start_date is given, extra warmup days are fetched before it
    so all indicators are valid at start_date.

    Supports index IDs ('TAIEX', 'TPEx') — fetches from tw.index_prices
    and uses turnover as the volume field.

    When ``use_cache=True`` and no date range is specified, looks up a
    pickled StockData under ``data/stock_cache/<stock_id>_<db_date>.pkl``
    and rebuilds + writes the cache on miss. Cache is keyed by the DB's
    latest trade_date so it auto-invalidates after fresh data is loaded.
    """
    if stock_id in _INDEX_IDS:
        return _load_index_data(stock_id, start_date, end_date)

    cache_eligible = use_cache and start_date is None and end_date is None
    cache_file: Path | None = None
    if cache_eligible:
        cache_file = _cache_path(stock_id, _get_db_max_date())
        cached = _try_load_cache(cache_file)
        if cached is not None:
            return cached

    stock_name = fetch_stock_name(stock_id)

    rows = _fetch_prices(stock_id, start_date, end_date)
    if not rows:
        raise ValueError(f"No data found for {stock_id}")

    dates = [r["trade_date"] for r in rows]
    dividends = fetch_dividends(stock_id, dates)
    data = build_stock_data(stock_id, stock_name, rows, dividends)

    if cache_file is not None:
        try:
            _write_cache(cache_file, data)
        except Exception:
            # Cache write failure should never break the run.
            pass

    return data


def _load_index_data(
    index_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> StockData:
    """Load index data from tw.index_prices, using turnover as volume."""
    rows = _fetch_index_prices(index_id, start_date, end_date)
    if not rows:
        raise ValueError(f"No data found for index {index_id}")

    # Drop rows with NULL turnover (incomplete intraday data for current day)
    rows = [r for r in rows if r["turnover"] is not None]
    if not rows:
        raise ValueError(f"No usable data for index {index_id}")

    # Map turnover → volume, set ref_price to None (fallback to prev close)
    for r in rows:
        r["volume"] = r["turnover"]
        r["ref_price"] = None

    return build_stock_data(index_id, index_id, rows, dividends=[])


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
    volume_result = calculate_volume(volume, open_=open_, close=close, high=high, low=low)
    candle_result = calculate_candle(open_, high, low, close)
    money_result = calculate_money(turnover)
    obv = calculate_obv_multi(close, ref_price, high, low, volume)
    wave_result = calculate_wave(
        open_, high, low, close,
        candle_result, close_result.bs,
        volume=volume,
    )
    over_breakout = calculate_over_breakout(
        high, low, close, candle_result, close_result,
    )
    market_state = calculate_market_state(dates)
    macd = calculate_macd(close)
    donchian = DonchianMulti(
        short=calculate_donchian(high, low, close, entry_length=21, exit_length=8),
        medium=calculate_donchian(high, low, close, entry_length=55, exit_length=21),
        long=calculate_donchian(high, low, close, entry_length=233, exit_length=144),
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
        obv=obv,
        wave_result=wave_result,
        over_breakout=over_breakout,
        market_state=market_state,
        macd=macd,
        donchian=donchian,
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


def _fetch_index_prices(
    index_id: str,
    start_date: date | None,
    end_date: date | None,
) -> list[dict]:
    """Fetch OHLCV from tw.index_prices."""
    base_query = """
        SELECT trade_date, open_price, high_price, low_price,
               close_price, turnover
        FROM tw.index_prices
        WHERE index_id = %s
          AND close_price IS NOT NULL
    """
    params: list = [index_id]

    if end_date:
        base_query += " AND trade_date <= %s"
        params.append(end_date)

    base_query += " ORDER BY trade_date ASC"

    with get_cursor(commit=False) as cur:
        cur.execute(base_query, params)
        all_rows = cur.fetchall()

    if start_date:
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
