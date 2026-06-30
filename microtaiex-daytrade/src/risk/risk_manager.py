"""Risk manager: the only path from Signal to OrderRequest.

Gates enforced (plan §6/§12):
- max_lots: never enter beyond the configured lot cap (fixed 1 by default).
- ATR stop loss: exit a holding when price runs the stop distance against entry.
- session force-close window: block new entries; only exits allowed.
- daily loss halt: after cumulative loss reaches the cap, block new entries for
  the rest of the session.

The manager is pure/deterministic (no clock, no threads): callers pass ``now``,
``atr`` and ``force_close`` so it stays unit-testable.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from broker.types import Bar, OpenClose, OrderRequest, Side
from position.state_machine import PositionStateMachine, PosState
from strategy.base import Signal, SignalType


@dataclass
class RiskConfig:
    max_lots: int = 1
    stop_loss_atr_mult: float = 2.0
    # Optional per-direction overrides for the Chandelier stop distance. None ->
    # fall back to stop_loss_atr_mult. Long-side typically wants a wider stop.
    long_stop_atr_mult: Optional[float] = None
    short_stop_atr_mult: Optional[float] = None
    # Exit "handicap": price must breach the defense line by buffer*ATR before
    # the stop fires, so a small poke through the line is tolerated (noise
    # filter). None/0 -> exact line. The ratcheting line itself is unaffected.
    stop_buffer_atr: Optional[float] = None
    # Daily loss cap in index points; reset each trading day (RiskManager.reset_session).
    # Default = no cap (inf) so backtests show raw strategy behavior; live sets a real value.
    max_daily_loss_points: float = float("inf")


class RiskManager:
    def __init__(self, cfg: Optional[RiskConfig] = None) -> None:
        self.cfg = cfg or RiskConfig()
        self.daily_loss_points = 0.0
        self.halted = False
        self._trail_extreme: Optional[float] = None  # favorable extreme since entry
        self._trail_stop: Optional[float] = None     # ratcheting protective line

    # ---- signal -> order ----
    def evaluate_signal(
        self,
        signal: Signal,
        pos: PositionStateMachine,
        force_close: bool = False,
    ) -> Optional[OrderRequest]:
        if signal.type is SignalType.FLAT:
            return self._exit_order(pos, signal.price)

        # No new entries while force-closing or halted.
        if force_close or self.halted:
            return None

        desired = Side.BUY if signal.type is SignalType.LONG else Side.SELL

        if pos.state is PosState.HOLDING:
            if pos.side is desired:
                return None                      # already in desired direction
            return self._exit_order(pos, signal.price)   # flip: exit first

        if pos.state is not PosState.FLAT:
            return None                          # pending: don't double-order

        return OrderRequest(
            symbol=signal.symbol,
            side=desired,
            lot=self.cfg.max_lots,
            price=signal.price,
            open_close=OpenClose.OPEN,
        )

    # ---- protective exits ----
    def check_stop(self, bar: Bar, pos: PositionStateMachine, atr: Optional[float]) -> Optional[OrderRequest]:
        """Chandelier trailing stop whose protective line only tightens, never loosens.

        long:  exit when close <= (highest high since entry) - mult*ATR
        short: exit when close >= (lowest low since entry)  + mult*ATR

        The line ratchets toward price (up for long, down for short); an ATR
        expansion can never widen the stop away from price once it is armed. A
        stop_buffer_atr handicap tolerates a small breach before firing.
        """
        if pos.state is not PosState.HOLDING:
            self._trail_extreme = None
            self._trail_stop = None
            return None
        # seed from entry price, then track the favorable extreme each bar
        if self._trail_extreme is None:
            self._trail_extreme = pos.entry_price if pos.entry_price is not None else (
                bar.high if pos.side is Side.BUY else bar.low)
        if pos.side is Side.BUY:
            self._trail_extreme = max(self._trail_extreme, bar.high)
        else:
            self._trail_extreme = min(self._trail_extreme, bar.low)

        if atr is None:
            return None  # still warming up; extreme is tracked, stop not yet armed
        if pos.side is Side.BUY and self.cfg.long_stop_atr_mult is not None:
            mult = self.cfg.long_stop_atr_mult
        elif pos.side is Side.SELL and self.cfg.short_stop_atr_mult is not None:
            mult = self.cfg.short_stop_atr_mult
        else:
            mult = self.cfg.stop_loss_atr_mult
        dist = atr * mult
        raw = self._trail_extreme - dist if pos.side is Side.BUY else self._trail_extreme + dist
        # ratchet: only move the line toward price, never away from it
        if self._trail_stop is None:
            self._trail_stop = raw
        elif pos.side is Side.BUY:
            self._trail_stop = max(self._trail_stop, raw)
        else:
            self._trail_stop = min(self._trail_stop, raw)

        buf = atr * self.cfg.stop_buffer_atr if self.cfg.stop_buffer_atr else 0.0
        if pos.side is Side.BUY and bar.close <= self._trail_stop - buf:
            return self._exit_order(pos, bar.close)
        if pos.side is Side.SELL and bar.close >= self._trail_stop + buf:
            return self._exit_order(pos, bar.close)
        return None

    def force_close(self, pos: PositionStateMachine, price: float) -> Optional[OrderRequest]:
        return self._exit_order(pos, price)

    # ---- pnl / halt accounting ----
    def register_trade_pnl_points(self, points: float) -> None:
        """Record realized PnL (in index points) of a closed round-trip."""
        if points < 0:
            self.daily_loss_points += -points
            if self.daily_loss_points >= self.cfg.max_daily_loss_points:
                self.halted = True

    def reset_session(self) -> None:
        self.daily_loss_points = 0.0
        self.halted = False
        self._trail_extreme = None
        self._trail_stop = None

    # ---- internals ----
    def _exit_order(self, pos: PositionStateMachine, price: float) -> Optional[OrderRequest]:
        if pos.state is not PosState.HOLDING or pos.side is None:
            return None
        exit_side = Side.SELL if pos.side is Side.BUY else Side.BUY
        return OrderRequest(
            symbol=pos.symbol or "",
            side=exit_side,
            lot=pos.lot,
            price=price,
            open_close=OpenClose.COVER,
        )
