"""
Core backtest engine — iterates day by day, manages positions, records trades.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

import numpy as np

from backtest.trade import Trade, BacktestResult
from backtest.strategy import Strategy, Condition

if TYPE_CHECKING:
    from backtest.data import StockData

F32 = np.float32


class InsufficientDataError(ValueError):
    """Raised when stock data is too short for backtesting."""
    pass


@dataclass
class _Position:
    """Internal mutable position state."""
    side: str               # "long" or "short"
    entry_date: date
    entry_price: float
    entry_index: int
    entry_reasons: list[str]
    shares: float
    cash_dividends: float
    defense_price: float    # NaN if no trailing stop


def run_backtest(
    data: StockData,
    strategy: Strategy,
    capital: float = 1_000_000.0,
    shares_per_trade: int = 1000,
    start_index: int | None = None,
) -> BacktestResult:
    """
    Run a backtest on a single stock.

    Parameters
    ----------
    data : StockData with raw prices and analysis results
    strategy : Strategy with entry/exit conditions
    capital : initial cash
    shares_per_trade : fixed position size
    start_index : day index to start trading (default: 55)
    """
    MIN_DAYS = 13
    n = data.n
    if n < MIN_DAYS:
        raise InsufficientDataError(
            f"{data.stock_id} {data.stock_name} 資料僅 {n} 天，"
            f"低於最低要求 {MIN_DAYS} 天，無法進行回測"
        )

    if start_index is not None:
        si = min(start_index, n - 1)
    else:
        # Default: skip first 55 days (shortest reliable warmup)
        si = min(55, n - 1)

    # Per-day tracking arrays
    equity = np.full(n, capital, dtype=F32)
    position_side = np.zeros(n, dtype=np.int8)
    defense_arr = np.full(n, np.nan, dtype=F32)

    trades: list[Trade] = []
    pos: _Position | None = None
    cash = capital

    # Build dividend lookup: day_index -> DividendEvent
    div_map = {d.day_index: d for d in data.dividends}

    for i in range(si, n):
        price = float(data.close[i])

        # 1. Dividend adjustment
        if pos is not None and i in div_map:
            div = div_map[i]
            if div.cash_dividend > 0:
                pos.cash_dividends += pos.shares * div.cash_dividend
                cash += pos.shares * div.cash_dividend
            if div.stock_dividend > 0:
                pos.shares *= (1 + div.stock_dividend / 10)

        # 2. Exit check (before entry)
        if pos is not None:
            exit_reasons: list[str] = []

            # Trailing stop check
            if not np.isnan(pos.defense_price):
                if pos.side == "long" and price < pos.defense_price:
                    exit_reasons.append(f"停利防守 ({pos.defense_price:.2f})")
                elif pos.side == "short" and price > pos.defense_price:
                    exit_reasons.append(f"停利防守 ({pos.defense_price:.2f})")

            # Strategy exit conditions (OR logic)
            if not exit_reasons:
                conditions = (strategy.long_exit if pos.side == "long"
                              else strategy.short_exit)
                for cond in conditions:
                    if cond.evaluate(data, i):
                        exit_reasons.append(cond.name)

            if exit_reasons:
                # Close position
                trade = _close_position(pos, data, i, price, exit_reasons)
                trades.append(trade)
                if pos.side == "long":
                    cash += price * pos.shares
                else:
                    # Short: profit = (entry - exit) * shares
                    cash += (pos.entry_price - price) * pos.shares + pos.entry_price * pos.shares
                pos = None
            else:
                # Update trailing stop defense price
                if strategy.trailing_stop is not None:
                    new_defense = float(
                        strategy.trailing_stop.defense_source(data)[i]
                    )
                    if np.isnan(pos.defense_price):
                        pos.defense_price = new_defense
                    elif pos.side == "long":
                        pos.defense_price = max(pos.defense_price, new_defense)
                    else:
                        pos.defense_price = min(pos.defense_price, new_defense)

        # 3. Entry check (if flat)
        if pos is None:
            entry_reasons = _check_entry(data, i, strategy.long_entry)
            if entry_reasons:
                pos = _open_position(
                    "long", data, i, price, shares_per_trade,
                    entry_reasons, strategy,
                )
                cash -= price * pos.shares
            else:
                entry_reasons = _check_entry(data, i, strategy.short_entry)
                if entry_reasons:
                    pos = _open_position(
                        "short", data, i, price, shares_per_trade,
                        entry_reasons, strategy,
                    )
                    # Short: reserve entry cost from cash
                    cash -= price * pos.shares

        # 4. Record daily state
        if pos is not None:
            position_side[i] = 1 if pos.side == "long" else -1
            defense_arr[i] = pos.defense_price
            # Mark-to-market equity
            if pos.side == "long":
                unrealized = (price - pos.entry_price) * pos.shares
            else:
                unrealized = (pos.entry_price - price) * pos.shares
            equity[i] = F32(cash + price * pos.shares + pos.cash_dividends
                            if pos.side == "long"
                            else cash + unrealized + pos.entry_price * pos.shares)
        else:
            equity[i] = F32(cash)

    # Force-close any open position at end
    if pos is not None:
        price = float(data.close[n - 1])
        trade = _close_position(pos, data, n - 1, price, ["回測結束"])
        trades.append(trade)
        if pos.side == "long":
            cash += price * pos.shares
        else:
            cash += (pos.entry_price - price) * pos.shares + pos.entry_price * pos.shares

    # Fill equity for pre-start days
    for i in range(si):
        equity[i] = F32(capital)

    # Determine backtest date range
    start_date = data.dates[si] if si < n else data.dates[0]
    end_date = data.dates[-1]

    result = BacktestResult(
        stock_id=data.stock_id,
        strategy_name=strategy.name,
        start_date=start_date,
        end_date=end_date,
        initial_capital=capital,
        trades=trades,
        equity=equity,
        position_side=position_side,
        defense_price=defense_arr,
    )
    result.compute_stats()
    return result


def _check_entry(
    data: StockData,
    i: int,
    conditions: list[Condition],
) -> list[str]:
    """Check if ALL entry conditions are met. Return triggered names or []."""
    if not conditions:
        return []
    names = []
    for cond in conditions:
        if cond.evaluate(data, i):
            names.append(cond.name)
        else:
            return []  # AND logic: one false → skip
    return names


def _open_position(
    side: str,
    data: StockData,
    i: int,
    price: float,
    shares: int,
    entry_reasons: list[str],
    strategy: Strategy,
) -> _Position:
    """Create a new position."""
    defense = float('nan')
    if strategy.trailing_stop is not None:
        defense = float(strategy.trailing_stop.defense_source(data)[i])

    return _Position(
        side=side,
        entry_date=data.dates[i],
        entry_price=price,
        entry_index=i,
        entry_reasons=entry_reasons,
        shares=float(shares),
        cash_dividends=0.0,
        defense_price=defense,
    )


def _close_position(
    pos: _Position,
    data: StockData,
    i: int,
    price: float,
    exit_reasons: list[str],
) -> Trade:
    """Close a position and create a Trade record."""
    if pos.side == "long":
        pnl = (price - pos.entry_price) * pos.shares + pos.cash_dividends
    else:
        pnl = (pos.entry_price - price) * pos.shares - pos.cash_dividends

    cost = pos.entry_price * pos.shares
    pnl_pct = pnl / cost if cost > 0 else 0.0

    return Trade(
        direction=pos.side,
        entry_date=pos.entry_date,
        entry_price=pos.entry_price,
        entry_index=pos.entry_index,
        entry_reasons=pos.entry_reasons,
        exit_date=data.dates[i],
        exit_price=price,
        exit_index=i,
        exit_reasons=exit_reasons,
        shares=pos.shares,
        cash_dividends=pos.cash_dividends,
        pnl=pnl,
        pnl_pct=pnl_pct,
        holding_days=i - pos.entry_index,
    )
