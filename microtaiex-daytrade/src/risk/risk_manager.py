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
    # 麻紗 出村標準 (§9.1) — index points, None = off:
    #   max_loss_points_per_trade: hard per-trade stop (單筆≤20), ATR-independent.
    #   daily_profit_target_points: halt new entries once +N reached (單日正≥50 鎖利停手).
    max_loss_points_per_trade: Optional[float] = None
    daily_profit_target_points: Optional[float] = None
    # Stop system: "chandelier" (ATR trailing) or "masha" (entry-bar high/low, §4.7).
    stop_mode: str = "chandelier"
    # 麻紗 exit module (§2.4, only when stop_mode="masha"):
    #   masha_trail_enabled: turn on 移動停利 ratchet (default off — backtest shows
    #     the tight candle-trail whipsaws winners; plain entry-bar stop rides better).
    #   masha_trail_mode: 移動停利 line — "low"=紅K底 (looser) / "mid"=紅K一半 (tighter).
    #   masha_use_target: exit at the 1:1 滿足點 (§7.1) set by the engine at entry.
    #   masha_use_exhaustion: exit on a 力竭 reversal (engine-checked, §2.4c).
    masha_trail_enabled: bool = False
    masha_trail_mode: str = "low"
    masha_use_target: bool = False
    masha_use_exhaustion: bool = False
    # §8.3 時間波 有單/沒單: at a 時間波 point, if holding and the bar shows 沒方向
    # (doji-like), 先出 (exit) — claims to cut washes ~50%. Engine-checked, opt-in.
    masha_timewave_exit: bool = False
    masha_timewave_doji_ratio: float = 0.3
    # 麻紗 dynamic per-trade stop (§9.1 evolution): when set, the per-trade cap
    # is clamp(mult*ATR, lo, hi) instead of the fixed max_loss_points_per_trade —
    # it tightens when calm, widens when volatile. 0.5xATR(21) beats the fixed 20
    # on the track backtest (PF 2.61 vs 2.31, all segments). None = fixed cap.
    # Falls back to the fixed cap while ATR is still warming up.
    masha_loss_atr_mult: Optional[float] = None
    masha_loss_atr_lo: float = 12.0
    masha_loss_atr_hi: float = 40.0


class RiskManager:
    def __init__(self, cfg: Optional[RiskConfig] = None) -> None:
        self.cfg = cfg or RiskConfig()
        self.daily_loss_points = 0.0
        self.daily_profit_points = 0.0
        self.halted = False
        self._trail_extreme: Optional[float] = None  # favorable extreme since entry
        self._trail_stop: Optional[float] = None     # ratcheting protective line
        self._entry_bar_hi: Optional[float] = None   # 麻紗 entry-bar high (§4.7)
        self._entry_bar_lo: Optional[float] = None   # 麻紗 entry-bar low
        self._masha_trail: Optional[float] = None    # 麻紗 移動停利 line (§2.4b)
        self._target: Optional[float] = None         # 麻紗 1:1 滿足點 (§7.1)

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
            self._entry_bar_hi = None
            self._entry_bar_lo = None
            self._masha_trail = None
            self._target = None
            return None

        # §9.1 per-trade point cap. Dynamic (clamp(mult*ATR, lo, hi)) when
        # configured — adapts the stop to current volatility — else the fixed cap;
        # dynamic falls back to the fixed cap while ATR is still warming up.
        cap = self._trade_cap(atr)
        if cap is not None and pos.entry_price is not None:
            loss = (pos.entry_price - bar.close if pos.side is Side.BUY
                    else bar.close - pos.entry_price)
            if loss >= cap:
                return self._exit_order(pos, bar.close)

        # 麻紗 exit (§4.7 進場K高低 seed + §2.4b 移動停利 ratchet + §7.1 滿足點)
        if self.cfg.stop_mode == "masha":
            is_long = pos.side is Side.BUY
            if self._masha_trail is None:
                self._masha_trail = self._entry_bar_lo if is_long else self._entry_bar_hi
            if self.cfg.masha_trail_enabled:               # 移動停利 ratchet (opt-in)
                mid = (bar.high + bar.low) / 2.0
                if is_long and bar.close > bar.open:       # 紅K → trail up
                    cand = bar.low if self.cfg.masha_trail_mode == "low" else mid
                    self._masha_trail = cand if self._masha_trail is None else max(self._masha_trail, cand)
                elif (not is_long) and bar.close < bar.open:   # 黑K → trail down
                    cand = bar.high if self.cfg.masha_trail_mode == "low" else mid
                    self._masha_trail = cand if self._masha_trail is None else min(self._masha_trail, cand)
            if is_long and self._masha_trail is not None and bar.close < self._masha_trail:
                return self._exit_order(pos, bar.close)
            if (not is_long) and self._masha_trail is not None and bar.close > self._masha_trail:
                return self._exit_order(pos, bar.close)
            if self.cfg.masha_use_target and self._target is not None:
                if is_long and bar.high >= self._target:
                    return self._exit_order(pos, self._target)
                if (not is_long) and bar.low <= self._target:
                    return self._exit_order(pos, self._target)
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

    def set_entry_bar(self, high: float, low: float) -> None:
        """Record the entry bar's high/low for the 麻紗 entry-bar stop (§4.7)."""
        self._entry_bar_hi = high
        self._entry_bar_lo = low

    def set_target(self, price: Optional[float]) -> None:
        """Set the 麻紗 1:1 滿足點 target (§7.1) for this position."""
        self._target = price

    def initial_defense(self, side: Side, entry_price: float,
                        atr: Optional[float]) -> Optional[float]:
        """The Chandelier stop line at entry (entry -/+ mult*ATR), using the
        same per-side mult as check_stop. None while ATR is warming up."""
        if atr is None:
            return None
        if side is Side.BUY and self.cfg.long_stop_atr_mult is not None:
            mult = self.cfg.long_stop_atr_mult
        elif side is Side.SELL and self.cfg.short_stop_atr_mult is not None:
            mult = self.cfg.short_stop_atr_mult
        else:
            mult = self.cfg.stop_loss_atr_mult
        dist = atr * mult
        return entry_price - dist if side is Side.BUY else entry_price + dist

    def current_defense(self) -> Optional[float]:
        """The live ratcheting Chandelier stop line (None until armed). NOTE: this
        is only the loose trailing backstop — for the line that actually fires,
        use effective_stop (the per-trade point cap usually dominates it)."""
        return self._trail_stop

    def _trade_cap(self, atr: Optional[float]) -> Optional[float]:
        """Per-trade point cap in force: dynamic clamp(mult*ATR, lo, hi) when
        configured, else the fixed cap; falls back to the fixed cap while ATR is
        still warming up. Single source of truth shared by check_stop and the
        reported effective stop so they can never diverge. None if no cap set."""
        cap = self.cfg.max_loss_points_per_trade
        if self.cfg.masha_loss_atr_mult is not None and atr is not None:
            cap = max(self.cfg.masha_loss_atr_lo,
                      min(self.cfg.masha_loss_atr_hi, self.cfg.masha_loss_atr_mult * atr))
        return cap

    def effective_stop(self, pos: PositionStateMachine, atr: Optional[float]) -> Optional[float]:
        """The price a holding would actually exit on: the tighter (closer to
        price) of the per-trade cap line (entry -/+ cap) and the Chandelier
        trailing line. Reporting the bare Chandelier line is misleading when a
        tight per-trade cap dominates it. Chandelier mode only; None when flat or
        unarmed. (麻紗 mode keeps its own entry-bar defense — reports nothing here.)"""
        if self.cfg.stop_mode == "masha":
            return None
        if pos.state is not PosState.HOLDING or pos.entry_price is None:
            return None
        is_long = pos.side is Side.BUY
        lines = []
        cap = self._trade_cap(atr)
        if cap is not None:
            lines.append(pos.entry_price - cap if is_long else pos.entry_price + cap)
        if self._trail_stop is not None:
            buf = atr * self.cfg.stop_buffer_atr if (self.cfg.stop_buffer_atr and atr) else 0.0
            lines.append(self._trail_stop - buf if is_long else self._trail_stop + buf)
        if not lines:
            return None
        return max(lines) if is_long else min(lines)

    def initial_effective_stop(self, side: Side, entry_price: float,
                               atr: Optional[float]) -> Optional[float]:
        """The binding stop at entry (before the trail arms): tighter of the
        per-trade cap line and the initial Chandelier line. Matches what check_stop
        will fire on for the first bars of the trade."""
        is_long = side is Side.BUY
        lines = []
        cap = self._trade_cap(atr)
        if cap is not None:
            lines.append(entry_price - cap if is_long else entry_price + cap)
        ch = self.initial_defense(side, entry_price, atr)
        if ch is not None:
            lines.append(ch)
        if not lines:
            return None
        return max(lines) if is_long else min(lines)

    # ---- pnl / halt accounting ----
    def register_trade_pnl_points(self, points: float) -> None:
        """Record realized PnL (points) of a closed round-trip; halt on 出村 caps."""
        if points < 0:
            self.daily_loss_points += -points
            if self.daily_loss_points >= self.cfg.max_daily_loss_points:
                self.halted = True   # 單日負損益達上限 → 當日停手
        else:
            self.daily_profit_points += points
            tgt = self.cfg.daily_profit_target_points
            if tgt is not None and self.daily_profit_points >= tgt:
                self.halted = True   # 單日正損益達標 → 鎖利停手

    def reset_session(self) -> None:
        self.daily_loss_points = 0.0
        self.daily_profit_points = 0.0
        self.halted = False
        self._trail_extreme = None
        self._trail_stop = None
        self._entry_bar_hi = None
        self._entry_bar_lo = None

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
