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
from strategy.measured_move import impulse_box, target_1to1
from strategy.signals_masha import exhaustion
from strategy.timing import is_timewave_bar, no_direction

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
        self._pending_open_defense: Optional[float] = None  # entry stop line
        self._pending_open_bar: Optional[tuple] = None      # (high, low) of entry bar
        self._pending_open_target: Optional[float] = None   # 麻紗 1:1 滿足點
        self._last_defense: Optional[float] = None  # last-notified ratchet level
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
        # Zero-volume bars are gap-fill placeholders (無量補空) or genuine no-trade
        # buckets: no trade happened, so they carry no price action and must not
        # feed ATR, ratchet the trailing stop, or drive strategy signals. Letting
        # them through walks the Chandelier stop up on phantom bars (a run of flat
        # fillers collapses ATR -> stop distance shrinks -> line ratchets up). They
        # are still allowed to trigger the session force-close (their one legit use).
        if bar.volume == 0:
            if force_close:
                fc = self._risk.force_close(self._pos, bar.close)
                if fc is not None:
                    self._submit(fc)
            return
        self._ctx.bars.append(bar)
        atr_val = atr(self._ctx.bars, self._atr_period)

        # 1) protective stop (Chandelier or 麻紗 exit module)
        stop_order = self._risk.check_stop(bar, self._pos, atr_val)
        if stop_order is not None:
            self._submit(stop_order)
            return
        self._notify_defense_raise(atr_val)

        # 1b) 麻紗 力竭 exit (§2.4c): bar-pattern reversal against the position
        cfg = self._risk.cfg
        if (cfg.stop_mode == "masha" and cfg.masha_use_exhaustion
                and self._pos.is_holding()):
            side = "long" if self._pos.side is Side.BUY else "short"
            if exhaustion(self._ctx.bars, side):
                self._submit(self._risk.force_close(self._pos, bar.close))
                return

        # 1c) 麻紗 時間波 exit (§8.3): 時間點到 + 沒方向 → 有單先出
        if (cfg.stop_mode == "masha" and cfg.masha_timewave_exit
                and self._pos.is_holding() and is_timewave_bar(bar.ts)
                and no_direction(self._ctx.bars, cfg.masha_timewave_doji_ratio)):
            self._submit(self._risk.force_close(self._pos, bar.close))
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
                self._pending_open_bar = (bar.high, bar.low)   # 麻紗 entry-bar stop
                self._pending_open_target = None
                if cfg.stop_mode == "masha":
                    # §4.7 defense = entry-bar low/high, capped by the 20-pt 出村
                    # stop (§9.1) — whichever line is closer to entry fires first.
                    cap = cfg.max_loss_points_per_trade
                    if order.side is Side.BUY:
                        self._pending_open_defense = (bar.low if cap is None
                                                      else max(bar.low, order.price - cap))
                    else:
                        self._pending_open_defense = (bar.high if cap is None
                                                      else min(bar.high, order.price + cap))
                    if cfg.masha_use_target:
                        box = impulse_box(self._ctx.bars)
                        if box is not None:
                            side = "long" if order.side is Side.BUY else "short"
                            self._pending_open_target = target_1to1(side, order.price, box)
                else:
                    self._pending_open_defense = self._risk.initial_effective_stop(
                        order.side, order.price, atr_val)
            self._submit(order)

    def _notify_defense_raise(self, atr_val) -> None:
        """Emit a ('defense', ...) event when the *binding* stop (effective_stop:
        tighter of per-trade cap and Chandelier) ratchets >= 1 pt favorably. Using
        the effective stop, not the bare Chandelier line, keeps the notification on
        the line that actually protects the trade."""
        if not self._on_event or not self._pos.is_holding():
            return
        cur = self._risk.effective_stop(self._pos, atr_val)
        if cur is None:
            return
        if self._last_defense is None:
            self._last_defense = cur
            return
        is_long = self._pos.side is Side.BUY
        raised = (cur - self._last_defense >= 1.0) if is_long else (self._last_defense - cur >= 1.0)
        if raised:
            self._on_event(("defense", self._pos.side, cur,
                            self._pos.entry_price, self._pos.symbol))
            self._last_defense = cur

    def preload(self, bars) -> None:
        """Seed ctx.bars with historical closed bars for warmup (ATR / 軌道 channel
        armed immediately after a restart). Pure state fill — no stop/strategy/order
        runs, so it never trades the past. Skips bars not newer than the last seen."""
        last_ts = self._ctx.bars[-1].ts if self._ctx.bars else None
        for b in bars:
            if last_ts is None or b.ts > last_ts:
                self._ctx.bars.append(b)
                last_ts = b.ts

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
            self._last_defense = self._pending_open_defense
            if self._pending_open_bar is not None:
                self._risk.set_entry_bar(*self._pending_open_bar)   # 麻紗 §4.7
                self._risk.set_target(self._pending_open_target)    # 麻紗 §7.1
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
            self._last_defense = None
            if self._on_event:
                self._on_event(("close", self.round_trips[-1]))

    def _submit(self, order: OrderRequest) -> None:
        if not self._pos.on_order_submitted(order):
            return  # rejected: duplicate / wrong state
        result = self._broker.place_order(order)
        if self._on_event:
            if order.open_close is OpenClose.OPEN:
                self._on_event(("order", order, result,
                                self._pending_open_reason, self._pending_open_defense))
            else:
                self._on_event(("order", order, result, ""))
