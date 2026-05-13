"""OBV-based diagnostic signal factories.

One pure cross signal per Fibonacci scope (short / medium / long).
Used to validate edge of each OBV scope in isolation — not part of the
production 6-signal rotation.

  long_entry  = obv.{scope}.signal_up
  long_exit   = obv.{scope}.signal_down
  short_entry = obv.{scope}.signal_down
  short_exit  = obv.{scope}.signal_up
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from signal_backtest.signal import SignalSet, SignalSpec

if TYPE_CHECKING:
    from backtest.data import StockData


def _obv_scope_signal(name: str, scope_attr: str):
    def factory(data: "StockData") -> SignalSpec:
        o = getattr(data.obv, scope_attr)
        return SignalSpec(
            name=name,
            signals=SignalSet(
                long_entry=o.signal_up,
                long_exit=o.signal_down,
                short_entry=o.signal_down,
                short_exit=o.signal_up,
            ),
        )
    return factory


obv_short_signal = _obv_scope_signal("obv_short", "short")
obv_medium_signal = _obv_scope_signal("obv_medium", "medium")
obv_long_signal = _obv_scope_signal("obv_long", "long")
