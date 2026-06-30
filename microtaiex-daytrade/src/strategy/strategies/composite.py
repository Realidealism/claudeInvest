"""Unified six-signal day-trade strategy (single-lot).

Mirrors the daily signal factory's unified position machine, collapsed onto one
instrument / one lot:

  long_entry  = pick OR buy OR sell_flee
  short_entry = touch OR sell OR buy_flee

Each bar the six price-pattern signals are evaluated; the winning net direction
is emitted as a Signal and the engine's position machine (evaluate_signal) opens,
holds, or flips one lot. Flee signals are dual-role: e.g. buy_flee (bull trap) is
both the long's exit and the short's entry — emitting SHORT makes the engine flip.

Conflict arbitration when both sides fire on the same bar: priority
flee(3) > reversal(2) > momentum(1); on a tie the bar is skipped (no action).

Optional daily-state gate: with ``daily_self_gate``, the strategy aggregates its
own intraday bars into TAIFEX trading-day bars (night session 15:00 leads the next
day), runs the same composite on that daily series to get a directional state
(long/short/flat), and hard-gates intraday entries by the last COMPLETED day's
state (prior-day, no look-ahead): daily-long -> intraday longs only, daily-short
-> shorts only. Self-contained — no external data needed, so it runs live.
"""
from __future__ import annotations

from datetime import date, time, timedelta
from typing import Optional

from broker.types import Bar
from strategy.base import Signal, SignalType, Strategy, StrategyContext
from strategy.signals import pick, touch, buy, sell, buy_flee, sell_flee

# name -> priority. priority: flee 3 > reversal 2 > momentum 1.
_LONG = {"pick": 2, "buy": 1, "sell_flee": 3}
_SHORT = {"touch": 2, "sell": 1, "buy_flee": 3}

_NIGHT_OPEN = time(15, 0)   # TAIFEX night session leads the next trading day


class CompositeStrategy(Strategy):
    def __init__(self, timeframe: str = "5m", lookback: int = 20,
                 breakout_lb: int = 20, flee_lb: int = 10,
                 enable: Optional[set[str]] = None,
                 daily_self_gate: bool = False) -> None:
        self.timeframe = timeframe
        self.lookback = lookback
        self.breakout_lb = breakout_lb
        self.flee_lb = flee_lb
        self.enable = enable or set(_LONG) | set(_SHORT)
        # Self-contained daily-state hard gate (see module docstring): bull day
        # -> longs only, bear day -> shorts only. False = no gate (both sides).
        self.daily_self_gate = daily_self_gate
        if daily_self_gate:
            self._dcomp = CompositeStrategy(timeframe=timeframe, lookback=lookback,
                                            breakout_lb=breakout_lb, flee_lb=flee_lb)
            self._dctx = StrategyContext()
            self._dstate = 0
            self._cur_day: Optional[date] = None
            self._acc: Optional[list] = None   # [open, high, low, close, vol, ts]

    def _trade_day(self, bar: Bar) -> date:
        d = bar.ts.date()
        return d + timedelta(days=1) if bar.ts.time() >= _NIGHT_OPEN else d

    def _update_daily(self, bar: Bar) -> None:
        """Aggregate into trading-day bars; on a day change finalize the prior
        day and advance the daily directional state (prior-day, no look-ahead)."""
        td = self._trade_day(bar)
        if self._cur_day is not None and td != self._cur_day and self._acc is not None:
            o, h, l, c, v, ts = self._acc
            db = Bar(bar.symbol, ts, o, h, l, c, v, self.timeframe)
            self._dctx.bars.append(db)
            sig = self._dcomp.on_bar_close(db, self._dctx)
            if sig is not None:
                self._dstate = 1 if sig.type is SignalType.LONG else -1
            self._acc = None
        self._cur_day = td
        if self._acc is None:
            self._acc = [bar.open, bar.high, bar.low, bar.close, bar.volume, bar.ts]
        else:
            self._acc[1] = max(self._acc[1], bar.high)
            self._acc[2] = min(self._acc[2], bar.low)
            self._acc[3] = bar.close
            self._acc[4] += bar.volume
            self._acc[5] = bar.ts

    def _fired(self, bars) -> dict[str, bool]:
        f = {}
        if "pick" in self.enable:
            f["pick"] = pick(bars, self.lookback)
        if "touch" in self.enable:
            f["touch"] = touch(bars, self.lookback)
        if "buy" in self.enable:
            f["buy"] = buy(bars, self.breakout_lb)
        if "sell" in self.enable:
            f["sell"] = sell(bars, self.breakout_lb)
        if "buy_flee" in self.enable:
            f["buy_flee"] = buy_flee(bars, self.flee_lb)
        if "sell_flee" in self.enable:
            f["sell_flee"] = sell_flee(bars, self.flee_lb)
        return f

    def on_bar_close(self, bar: Bar, ctx: StrategyContext) -> Optional[Signal]:
        if self.daily_self_gate:
            self._update_daily(bar)

        f = self._fired(ctx.bars)
        long_hits = [(n, p) for n, p in _LONG.items() if f.get(n)]
        short_hits = [(n, p) for n, p in _SHORT.items() if f.get(n)]

        # daily-state hard gate: bull day -> longs only, bear day -> shorts only
        if self.daily_self_gate:
            if self._dstate > 0:
                short_hits = []
            elif self._dstate < 0:
                long_hits = []

        long_score = max((p for _, p in long_hits), default=0)
        short_score = max((p for _, p in short_hits), default=0)

        if long_score > short_score:
            win = max(long_hits, key=lambda x: x[1])[0]
            return Signal(bar.symbol, SignalType.LONG, bar.ts, bar.close, reason=win)
        if short_score > long_score:
            win = max(short_hits, key=lambda x: x[1])[0]
            return Signal(bar.symbol, SignalType.SHORT, bar.ts, bar.close, reason=win)
        return None  # flat or unresolved conflict -> no action
