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

Optional daily-trend gate: with ``tx_status_path`` pointing at the daily signal
factory's TX unified-state file (frontend/public/data/futures_tx.json), intraday
entries are hard-gated by that state — factory long -> intraday longs only,
factory short -> shorts only, flat -> both. This reuses the sophisticated
factory position machine (entry + trailing defense), not a naive self-computed
state, so it matches the 大台 signal the user sees.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from broker.types import Bar
from strategy.base import Signal, SignalType, Strategy, StrategyContext
from strategy.signals import (pick, touch, buy, sell, buy_flee, sell_flee,
                              vwap, vwap_s, orb, orb_s)

# name -> priority. priority: flee 3 > reversal 2 > momentum 1.
_LONG = {"pick": 2, "buy": 1, "sell_flee": 3, "vwap": 2, "orb": 1}
_SHORT = {"touch": 2, "sell": 1, "buy_flee": 3, "vwap_s": 2, "orb_s": 1}
_FN = {"pick": pick, "touch": touch, "buy": buy, "sell": sell,
       "buy_flee": buy_flee, "sell_flee": sell_flee,
       "vwap": vwap, "vwap_s": vwap_s, "orb": orb, "orb_s": orb_s}
_LB = {"pick": "lookback", "touch": "lookback", "buy": "breakout_lb",
       "sell": "breakout_lb", "buy_flee": "flee_lb", "sell_flee": "flee_lb"}
# Default = the original 6 price-pattern signals; vwap/orb are opt-in via enable.
_DEFAULT = {"pick", "touch", "buy", "sell", "buy_flee", "sell_flee"}
_REGIME = {"long": 1, "short": -1, "flat": 0}


class CompositeStrategy(Strategy):
    def __init__(self, timeframe: str = "5m", lookback: int = 20,
                 breakout_lb: int = 20, flee_lb: int = 10,
                 enable: Optional[set[str]] = None,
                 tx_status_path: Optional[str] = None) -> None:
        self.timeframe = timeframe
        self.lookback = lookback
        self.breakout_lb = breakout_lb
        self.flee_lb = flee_lb
        self.enable = enable or set(_DEFAULT)
        # Daily-trend gate: the daily factory's TX unified state, read from this
        # JSON. None = no gate. long -> longs only, short -> shorts only, flat -> both.
        self.tx_status_path = tx_status_path
        self._tx_cache: tuple[int, Optional[float], float] = (0, None, 0.0)  # (regime, 多單防守, mtime)

    def _load_tx(self) -> tuple[int, Optional[float]]:
        """(regime, long_defense) from the factory's TX status file. regime:
        +1 long / -1 short / 0 flat|missing. long_defense: the current 多單
        protective line (position.defense_price) when state==long, else None.
        Cached on mtime; unreadable/missing -> (0, None) (fail-safe, no gate)."""
        if not self.tx_status_path:
            return 0, None
        try:
            mtime = Path(self.tx_status_path).stat().st_mtime
            if mtime != self._tx_cache[2]:
                state = json.loads(Path(self.tx_status_path).read_text(encoding="utf-8"))
                regime = _REGIME.get(state.get("state"), 0)
                dfn = (state.get("position") or {}).get("defense_price") if regime > 0 else None
                self._tx_cache = (regime, float(dfn) if dfn is not None else None, mtime)
            return self._tx_cache[0], self._tx_cache[1]
        except (OSError, ValueError, TypeError):
            return 0, None

    def _tx_regime(self) -> int:
        return self._load_tx()[0]

    def _fired(self, bars) -> dict[str, bool]:
        lbmap = {"lookback": self.lookback, "breakout_lb": self.breakout_lb,
                 "flee_lb": self.flee_lb}
        f = {}
        for name in self.enable:
            fn = _FN.get(name)
            if fn is None:
                continue
            lbkey = _LB.get(name)
            f[name] = fn(bars, lbmap[lbkey]) if lbkey else fn(bars)
        return f

    def on_bar_close(self, bar: Bar, ctx: StrategyContext) -> Optional[Signal]:
        f = self._fired(ctx.bars)
        long_hits = [(n, p) for n, p in _LONG.items() if f.get(n)]
        short_hits = [(n, p) for n, p in _SHORT.items() if f.get(n)]

        # daily-trend hard gate: factory long -> longs only, short -> shorts only
        if self.tx_status_path:
            regime, long_defense = self._load_tx()
            if regime > 0:
                short_hits = []
                # 多單防守價被跌破 → 當根不做多. Per-bar (re-evaluates every bar,
                # no latch) and applies day + night: the frozen overnight state
                # keeps its 多單防守 line, so an intraday breach still blocks longs.
                if long_defense is not None and bar.close < long_defense:
                    long_hits = []
            elif regime < 0:
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
