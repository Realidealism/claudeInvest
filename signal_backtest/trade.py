"""
Trade record dataclasses for signal-driven backtesting.

A Trade records one round-trip on a single side (long or short).
Defense price changes are tracked as a list of DefenseEvent so the
trailing-stop trajectory can be inspected after the fact.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date


# Reason constants (Chinese, user-facing)
REASON_ENTRY_INIT = "進場初始"
REASON_FLOOR = "保底"
REASON_EXIT_SIGNAL = "訊號出場"
REASON_EXIT_DEFENSE = "防守價觸發"
REASON_EXIT_END = "回測結束"

SIDE_LONG = "做多"
SIDE_SHORT = "做空"


@dataclass
class DefenseEvent:
    """One change to the defense (trailing-stop) price."""
    date: date
    price: float       # NaN allowed — when the source itself is NaN at entry
    reason: str        # REASON_ENTRY_INIT or REASON_TS_UPDATE


@dataclass
class Trade:
    """One completed signal round-trip."""
    stock_id: str
    stock_name: str
    side: str                          # SIDE_LONG / SIDE_SHORT
    entry_date: date
    entry_price: float
    defense_events: list[DefenseEvent]  # [0] is always REASON_ENTRY_INIT
    exit_date: date
    exit_price: float
    exit_reason: str                   # REASON_EXIT_*
    holding_days: int
    pnl_pct: float                     # signed pct return; short is reversed


@dataclass
class SideResult:
    """All trades for one (stock, side) combination."""
    stock_id: str
    stock_name: str
    side: str                          # SIDE_LONG / SIDE_SHORT
    start_date: date
    end_date: date
    trades: list[Trade] = field(default_factory=list)

    @property
    def n_trades(self) -> int:
        return len(self.trades)
