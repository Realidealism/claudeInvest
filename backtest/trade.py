"""
Trade record and backtest result dataclasses.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import numpy as np
from numpy.typing import NDArray

F32Array = NDArray[np.float32]


@dataclass
class Trade:
    """One completed round-trip trade."""
    direction: str              # "long" or "short"
    entry_date: date
    entry_price: float
    entry_index: int
    entry_reasons: list[str]

    exit_date: date
    exit_price: float
    exit_index: int
    exit_reasons: list[str]

    shares: float               # may be fractional after stock dividends
    cash_dividends: float       # total cash dividends received during hold
    pnl: float                  # realized P&L including dividends
    pnl_pct: float              # percentage return
    holding_days: int           # trading days held


@dataclass
class BacktestResult:
    """Complete backtest output."""
    stock_id: str
    strategy_name: str
    start_date: date
    end_date: date
    initial_capital: float
    trades: list[Trade]

    # Per-day arrays (aligned with StockData arrays)
    equity: F32Array
    position_side: NDArray[np.int8]   # 0=flat, 1=long, -1=short
    defense_price: F32Array           # trailing stop price (NaN when flat)

    # Summary stats (computed after backtest)
    total_return: float = 0.0
    win_rate: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    max_drawdown: float = 0.0
    profit_factor: float = 0.0
    total_trades: int = 0
    sharpe_ratio: float = 0.0

    def compute_stats(self) -> None:
        """Populate summary stats from trades and equity curve."""
        self.total_trades = len(self.trades)
        if self.total_trades == 0:
            return

        wins = [t for t in self.trades if t.pnl > 0]
        losses = [t for t in self.trades if t.pnl <= 0]

        self.win_rate = len(wins) / self.total_trades
        self.avg_win = np.mean([t.pnl_pct for t in wins]) if wins else 0.0
        self.avg_loss = np.mean([t.pnl_pct for t in losses]) if losses else 0.0

        total_win = sum(t.pnl for t in wins)
        total_loss = abs(sum(t.pnl for t in losses))
        self.profit_factor = total_win / total_loss if total_loss > 0 else float('inf')

        self.total_return = float(
            (self.equity[-1] - self.initial_capital) / self.initial_capital
        )

        # Max drawdown from equity curve
        peak = np.maximum.accumulate(self.equity)
        drawdown = (self.equity - peak) / np.where(peak > 0, peak, 1)
        self.max_drawdown = float(np.min(drawdown))

        # Annualized Sharpe ratio (daily returns, 252 trading days)
        valid = self.equity[1:] != 0
        daily_returns = np.where(
            valid,
            self.equity[1:] / np.where(self.equity[:-1] != 0, self.equity[:-1], 1) - 1,
            0,
        )
        if len(daily_returns) > 1 and np.std(daily_returns) > 0:
            self.sharpe_ratio = float(
                np.mean(daily_returns) / np.std(daily_returns) * np.sqrt(252)
            )
