"""Forward-adjusted close prices computed from raw events.

Inputs:
    prices: ascending list[DailyPrice] (unadjusted close)
    dividends: list of (ex_date, cash_dividend, stock_dividend)
                cash in NTD/share, stock as fraction-of-share (0.05 = 5% bonus)
    reductions: list of (effective_date, ratio)
                ratio = close_pre / close_post (pre-computed by scraper)

Forward-adjustment convention:
    adj_close[latest] == close[latest]
    adj_close[t < ex_date] = close[t] * Π factor_d  (for all event dates d > t)

Per-event factors:
    cash dividend D on date d:    factor = (close_prev - D) / close_prev
    stock dividend R on date d:   factor = 1 / (1 + R)
    reduction on date d:          factor = ratio  (already < 1 for share-cancellation)

When multiple events fall on the same ex_date (e.g. both cash and stock),
factors multiply.

This module is the chokepoint that bridges raw events to backtest NAV. Bugs
here are the largest single source of return-overestimation, so unit tests
hand-compute expected values against known TSMC dates.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import pandas as pd

from .models import DailyPrice


@dataclass(frozen=True)
class AdjustmentEvent:
    event_date: date
    factor: float
    kind: str  # 'cash' | 'stock' | 'reduction' | 'combined'


def build_events(
    prices: list[DailyPrice],
    dividends: list[tuple[date, Decimal, Decimal]],
    reductions: list[tuple[date, Decimal]],
) -> list[AdjustmentEvent]:
    """Translate raw inputs into per-date adjustment factors.

    Drops events that cannot be normalized (e.g. ex_date earlier than the first
    price observation, or zero/missing prev_close).
    """
    if not prices:
        return []
    sorted_prices = sorted(prices, key=lambda p: p.trade_date)
    price_dates = [p.trade_date for p in sorted_prices]
    close_by_date = {p.trade_date: float(p.close) for p in sorted_prices if p.close is not None}

    events: dict[date, float] = {}

    def _multiply(d: date, f: float) -> None:
        events[d] = events.get(d, 1.0) * f

    for ex_date, cash, stock in dividends:
        idx = bisect.bisect_left(price_dates, ex_date)
        if idx == 0:
            continue
        prev_date = price_dates[idx - 1]
        prev_close = close_by_date.get(prev_date)
        if prev_close is None or prev_close <= 0:
            continue
        if cash > 0:
            f_cash = (prev_close - float(cash)) / prev_close
            if f_cash > 0:
                _multiply(ex_date, f_cash)
        if stock > 0:
            f_stock = 1.0 / (1.0 + float(stock))
            _multiply(ex_date, f_stock)

    for eff_date, ratio in reductions:
        f = float(ratio)
        if f > 0:
            _multiply(eff_date, f)

    return [
        AdjustmentEvent(event_date=d, factor=events[d], kind="combined") for d in sorted(events)
    ]


def adjusted_close_series(
    prices: list[DailyPrice],
    dividends: list[tuple[date, Decimal, Decimal]],
    reductions: list[tuple[date, Decimal]],
) -> pd.Series:
    """Return a pd.Series of adjusted close prices, indexed by trade_date.

    If no events, this is identical to the raw close series.
    """
    if not prices:
        return pd.Series(dtype=float)

    sorted_prices = sorted(prices, key=lambda p: p.trade_date)
    events = build_events(sorted_prices, dividends, reductions)
    factor_by_date = {e.event_date: e.factor for e in events}

    out: dict[date, float] = {}
    cum = 1.0
    for p in reversed(sorted_prices):
        if p.close is None:
            continue
        out[p.trade_date] = float(p.close) * cum
        if p.trade_date in factor_by_date:
            cum *= factor_by_date[p.trade_date]

    s = pd.Series(out).sort_index()
    s.name = "adj_close"
    return s
