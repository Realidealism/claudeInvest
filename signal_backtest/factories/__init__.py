"""Signal factory package — exposes the six factories for the registry."""

from signal_backtest.factories.pick_touch import pick_signal, touch_signal
from signal_backtest.factories.buy_sell import buy_signal, sell_signal
from signal_backtest.factories.flee import buy_flee_factory, sell_flee_factory

__all__ = [
    "pick_signal",
    "touch_signal",
    "buy_signal",
    "sell_signal",
    "buy_flee_factory",
    "sell_flee_factory",
]
