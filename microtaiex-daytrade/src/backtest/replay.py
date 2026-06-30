"""Replay historical bars through the live TradingEngine + SimBroker.

The same strategy/risk/position code runs as in live trading; only the bar
source differs (historical bars vs aggregated ticks). A realistic cost model is
applied to each closed round-trip: per-lot fixed fee both sides + transaction
tax on notional (NOT a stock percentage fee, per plan §8).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional, Sequence

from broker.sim import SimBroker
from broker.types import Bar
from core.engine import RoundTrip, TradingEngine
from position.state_machine import PositionStateMachine
from risk.risk_manager import RiskConfig, RiskManager
from strategy.base import Strategy


@dataclass
class CostModel:
    fee_per_lot: float = 20.0      # NT$ per lot per side
    tax_rate: float = 0.00002      # notional * rate, charged each side
    point_value: float = 10.0      # NT$ per index point (micro-Taiex)

    def round_trip_cost(self, entry_price: float, exit_price: float, lot: int) -> float:
        fees = self.fee_per_lot * lot * 2
        tax = (entry_price + exit_price) * self.point_value * lot * self.tax_rate
        return fees + tax


@dataclass
class BacktestResult:
    round_trips: List[RoundTrip] = field(default_factory=list)
    gross_pnl: float = 0.0   # NT$ before costs
    cost: float = 0.0        # NT$ total fees + tax
    net_pnl: float = 0.0     # NT$ after costs

    @property
    def n_trades(self) -> int:
        return len(self.round_trips)

    @property
    def win_rate(self) -> float:
        if not self.round_trips:
            return 0.0
        wins = sum(1 for rt in self.round_trips if rt.points > 0)
        return wins / len(self.round_trips)


def replay(
    bars: Sequence[Bar],
    strategy: Strategy,
    risk_cfg: Optional[RiskConfig] = None,
    cost: Optional[CostModel] = None,
    *,
    atr_period: int = 14,
    force_close_fn: Optional[Callable] = None,
    symbol: str = "TMFR1",
) -> BacktestResult:
    cost = cost or CostModel()
    broker = SimBroker(symbol=symbol)
    risk = RiskManager(risk_cfg or RiskConfig())
    engine = TradingEngine(
        broker,
        strategy,
        risk,
        PositionStateMachine(),
        atr_period=atr_period,
        force_close_fn=force_close_fn,
    )
    broker.set_on_trade(engine.on_trade)

    last_bar: Optional[Bar] = None
    last_date = None
    for bar in bars:
        # new trading day -> reset the daily loss cap / halt state
        if last_date is not None and bar.ts.date() != last_date:
            risk.reset_session()
        last_date = bar.ts.date()
        broker.set_mark_time(bar.ts)   # so sim fills are stamped with bar time
        engine.on_bar(bar)
        last_bar = bar

    # close any open position at end of data (session-end semantics)
    if last_bar is not None and not engine.position.is_flat():
        broker.set_mark_time(last_bar.ts)
        engine.flatten(last_bar.close, last_bar.ts)

    result = BacktestResult(round_trips=list(engine.round_trips))
    for rt in result.round_trips:
        gross = rt.points * cost.point_value * rt.lot
        c = cost.round_trip_cost(rt.entry_price or 0.0, rt.exit_price, rt.lot)
        result.gross_pnl += gross
        result.cost += c
    result.net_pnl = result.gross_pnl - result.cost
    return result
