"""麻紗 day-trade strategy (MASHA_FUTURES_SPEC, M0+M1 subset).

Entry pipeline per bar (5m):
  1. Time gates (§8, entry-only): in_tradeable_window + not is_excluded_bar.
  2. Trend gate (§2.1): trend_gate="tx" (daily factory TX state, live overlay) /
     "track" (軌道線 direction) / "both" / None. NOTE: the 軌道線 *direction* gate
     backtests destructive (M1 signals are counter-trend reversals); 軌道線's real
     edge is the *location* signal `track` below, not a direction filter.
  3. Signals (§3.4, §3.6, §4-§6): track (軌道線 §3.4 rail entry — engulf/doji at
     the trend rail, the M4 winner & default primary: PF 2.31/+3494 vs M1 1.26,
     rail_tol 40, triggers engulf+doji), gift (§3.6 送錢盤法, opt-in — too rare in
     5m data), sop, island, doji, redblack, engulf. Priority track/gift(5) >
     sop(4) > island(3) > doji/redblack(2) > engulf(1); tie = skip.

Exit-side 麻紗 modules (§8.3 時間波先出 etc.) are engine/risk-checked and default
off — like the M2/M3 exits, backtests show mechanical exits whipsaw this
instrument, so they stay opt-in.

Exits are NOT emitted here — they run in the risk layer: the 麻紗 entry-bar stop
(§4.7, stop_mode="masha"), the 出村 point caps (§9.1), and session force-close.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from broker.types import Bar
from strategy.base import Signal, SignalType, Strategy, StrategyContext
from strategy.signals_masha import (engulf, redblack_lazy, doji_flip,
                                     island_reversal)
from strategy.sop import sop_signal
from strategy.timing import in_tradeable_window, is_excluded_bar
from strategy.trendline import channel, rail_entry, gift_play

# §0 priority: 軌道線 > SOP > 撐壓 > 型態. track/gift are both 軌道 (highest).
_PRIO = {"track": 5, "gift": 5, "sop": 4, "island": 3, "doji": 2, "redblack": 2, "engulf": 1}
_REGIME = {"long": 1, "short": -1, "flat": 0}


class MashaStrategy(Strategy):
    def __init__(self, timeframe: str = "5m", zone_lb: int = 8, tol: float = 0.0,
                 enable: Optional[set[str]] = None,
                 tx_status_path: Optional[str] = None,
                 entry_window: bool = True, exclude_settlement: bool = True,
                 sop_k: int = 2, level_tol: float = 20.0, retest_tol: float = 0.0,
                 sop_window: int = 10, sop_hist: int = 60,
                 trend_gate: str = "tx", track_k: int = 2, track_lookback: int = 40,
                 track_tol: float = 30.0, track_min_touches: int = 2,
                 rail_tol: float = 40.0,
                 rail_triggers: tuple[str, ...] = ("engulf", "doji"),
                 rail_hold: bool = False) -> None:
        self.timeframe = timeframe
        self.zone_lb = zone_lb
        self.tol = tol
        # Primary entry = 軌道線 rail (track, §3.4) — the M4 winner (PF ~2.1 vs the
        # M1 mix 1.26). M1/M2/M3 signals stay available via explicit `enable`.
        self.enable = enable or {"track"}
        self.sop_k = sop_k
        self.level_tol = level_tol
        self.retest_tol = retest_tol
        self.sop_window = sop_window
        self.sop_hist = sop_hist
        self.tx_status_path = tx_status_path
        self.entry_window = entry_window
        self.exclude_settlement = exclude_settlement
        # §2.1 trend gate source: "tx" (daily factory), "track" (軌道線 §3),
        # "both" (require agreement), None (no gate).
        self.trend_gate = trend_gate
        self.track_k = track_k
        self.track_lookback = track_lookback
        self.track_tol = track_tol
        self.track_min_touches = track_min_touches
        self.rail_tol = rail_tol
        self.rail_triggers = rail_triggers
        self.rail_hold = rail_hold
        self._tx_cache: tuple[int, float] = (0, 0.0)

    def _tx_regime(self) -> int:
        if not self.tx_status_path:
            return 0
        try:
            mtime = Path(self.tx_status_path).stat().st_mtime
            if mtime != self._tx_cache[1]:
                state = json.loads(Path(self.tx_status_path).read_text(encoding="utf-8"))
                self._tx_cache = (_REGIME.get(state.get("state"), 0), mtime)
            return self._tx_cache[0]
        except (OSError, ValueError):
            return 0

    def _track_regime(self, bars) -> int:
        """§3.4 軌道線方向 → gate: up → +1 (只做多), down → -1 (只做空)."""
        direction, _, _ = channel(bars, self.track_k, self.track_lookback,
                                  self.track_tol, self.track_min_touches)
        return 1 if direction == "up" else -1 if direction == "down" else 0

    def _regime(self, bars) -> int:
        if self.trend_gate == "track":
            return self._track_regime(bars)
        if self.trend_gate == "both":
            tx, tr = self._tx_regime(), self._track_regime(bars)
            return tx if tx == tr else 0     # agree → gate; disagree → suppress both
        if self.trend_gate == "tx":
            return self._tx_regime()
        return 0                             # None → ungated

    def _eval(self, name: str, bars) -> Optional[str]:
        if name == "track":
            return rail_entry(bars, self.track_k, self.track_lookback,
                              self.track_tol, self.track_min_touches, self.rail_tol,
                              self.rail_triggers, self.rail_hold)
        if name == "gift":
            return gift_play(bars, self.track_k, self.track_lookback,
                             self.track_tol, self.track_min_touches)
        if name == "sop":
            return sop_signal(bars, self.sop_k, self.level_tol,
                              self.retest_tol, self.sop_window, self.sop_hist)
        if name == "engulf":
            return engulf(bars)
        if name == "redblack":
            return redblack_lazy(bars, self.zone_lb, self.tol)
        if name == "doji":
            return doji_flip(bars)
        if name == "island":
            return island_reversal(bars)
        return None

    def on_bar_close(self, bar: Bar, ctx: StrategyContext) -> Optional[Signal]:
        # 1. entry-only time gates (§8)
        if self.entry_window and not in_tradeable_window(bar.ts):
            return None
        if self.exclude_settlement and is_excluded_bar(bar.ts):
            return None

        # 3. directional signals
        fired = [(d, name) for name in self.enable
                 if (d := self._eval(name, ctx.bars)) is not None]
        if not fired:
            return None

        # 2. trend gate (§2.1): suppress off-side entries (tx / 軌道線 / both)
        regime = self._regime(ctx.bars)
        if regime > 0:
            fired = [f for f in fired if f[0] == "long"]
        elif regime < 0:
            fired = [f for f in fired if f[0] == "short"]
        if not fired:
            return None

        long_p = max((_PRIO[n] for d, n in fired if d == "long"), default=0)
        short_p = max((_PRIO[n] for d, n in fired if d == "short"), default=0)
        if long_p > short_p:
            win = max((n for d, n in fired if d == "long"), key=lambda n: _PRIO[n])
            return Signal(bar.symbol, SignalType.LONG, bar.ts, bar.close, reason=win)
        if short_p > long_p:
            win = max((n for d, n in fired if d == "short"), key=lambda n: _PRIO[n])
            return Signal(bar.symbol, SignalType.SHORT, bar.ts, bar.close, reason=win)
        return None  # unresolved conflict
