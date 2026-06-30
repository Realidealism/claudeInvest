"""TradingEngine: wires bar -> strategy -> risk -> position -> broker.

Shared by live (bars arrive via EventBus) and backtest (bars replayed directly).
Per bar of the strategy timeframe, the engine, in order:
1. checks the ATR stop on any holding (protective exit),
2. on a session force-close bar, flattens and blocks new entries,
3. otherwise asks the strategy for a Signal and routes it through risk.

Fills are fed back via ``on_trade``; closed round-trips are recorded with their
realized points and reported to the risk manager's daily-loss accounting.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Optional

from broker.base import BrokerAdapter
from broker.types import OpenClose, OrderRequest, Side, Trade
from position.state_machine import PositionStateMachine, PosState
from risk.risk_manager import RiskManager
from strategy.base import Strategy, StrategyContext
from strategy.indicators import atr

ForceCloseFn = Callable[[datetime], bool]
OnEvent = Callable[[tuple], None]


@dataclass(frozen=True)
class RoundTrip:
    symbol: str
    side: Side                # entry side
    lot: int
    entry_ts: Optional[datetime]
    entry_price: Optional[float]
    exit_ts: datetime
    exit_price: float
    points: float             # realized index points (entry vs exit), per lot
    reason: str = ""          # entry signal name (e.g. pick / sell_flee)


def realized_points(side: Optional[Side], entry: Optional[float], exit_price: float) -> float:
    if side is None or entry is None:
        return 0.0
    return exit_price - entry if side is Side.BUY else entry - exit_price


class TradingEngine:
    def __init__(
        self,
        broker: BrokerAdapter,
        strategy: Strategy,
        risk: RiskManager,
        position: PositionStateMachine,
        *,
        atr_period: int = 14,
        force_close_fn: Optional[ForceCloseFn] = None,
        on_event: Optional[OnEvent] = None,
    ) -> None:
        self._broker = broker
        self._strategy = strategy
        self._risk = risk
        self._pos = position
        self._atr_period = atr_period
        self._force_close_fn = force_close_fn
        self._on_event = on_event
        self._ctx = StrategyContext()
        self._entry_trade: Optional[Trade] = None
        self._entry_reason: str = ""          # signal name of the open position
        self._pending_open_reason: str = ""   # reason of the last submitted entry
        self.fills: List[Trade] = []
        self.round_trips: List[RoundTrip] = []

    @property
    def position(self) -> PositionStateMachine:
        return self._pos

    def on_bar(self, bar) -> None:
        force_close = bool(self._force_close_fn(bar.ts)) if self._force_close_fn else False

        # Force-close is checked on EVERY bar, including the finer detail
        # timeframe (1m), so the narrow pre-close window is caught even when it
        # falls between strategy bars (e.g. 13:44 with a 5m strategy timeframe).
        # Detail bars do nothing else and never drive strategy/stop decisions.
        if bar.timeframe != self._strategy.timeframe:
            if force_close:
                fc = self._risk.force_close(self._pos, bar.close)
                if fc is not None:
                    self._submit(fc)
            return
        self._ctx.bars.append(bar)

        # 1) protective stop
        stop_order = self._risk.check_stop(bar, self._pos, atr(self._ctx.bars, self._atr_period))
        if stop_order is not None:
            self._submit(stop_order)
            return

        # 2) session force-close: flatten, no new entries
        if force_close:
            fc = self._risk.force_close(self._pos, bar.close)
            if fc is not None:
                self._submit(fc)
            return

        # 3) strategy signal
        signal = self._strategy.on_bar_close(bar, self._ctx)
        if signal is None:
            return
        order = self._risk.evaluate_signal(signal, self._pos, force_close=force_close)
        if order is not None:
            if order.open_close is OpenClose.OPEN:
                self._pending_open_reason = signal.reason
            self._submit(order)

    def flatten(self, price: float, ts: datetime) -> None:
        """Force-close any open position (e.g. at end of replay / session)."""
        fc = self._risk.force_close(self._pos, price)
        if fc is not None:
            self._submit(fc)

    def on_trade(self, trade: Trade) -> None:
        self.fills.append(trade)
        closing = self._pos.state is PosState.PENDING_EXIT
        entry_side = self._pos.side
        entry_price = self._pos.entry_price
        lot = self._pos.lot
        self._pos.on_trade(trade)

        if self._pos.is_holding() and not closing:
            self._entry_trade = trade
            self._entry_reason = self._pending_open_reason
        elif closing:
            points = realized_points(entry_side, entry_price, trade.price)
            self.round_trips.append(RoundTrip(
                symbol=trade.symbol,
                side=entry_side if entry_side is not None else trade.side,
                lot=lot,
                entry_ts=self._entry_trade.ts if self._entry_trade else None,
                entry_price=entry_price,
                exit_ts=trade.ts,
                exit_price=trade.price,
                points=points,
                reason=self._entry_reason,
            ))
            self._risk.register_trade_pnl_points(points)
            self._entry_trade = None
            if self._on_event:
                self._on_event(("close", self.round_trips[-1]))

    def _submit(self, order: OrderRequest) -> None:
        if not self._pos.on_order_submitted(order):
            return  # rejected: duplicate / wrong state
        result = self._broker.place_order(order)
        if self._on_event:
            reason = self._pending_open_reason if order.open_close is OpenClose.OPEN else ""
            self._on_event(("order", order, result, reason))
