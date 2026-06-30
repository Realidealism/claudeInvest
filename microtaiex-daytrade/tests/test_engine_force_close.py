from datetime import datetime, timedelta

from broker.sim import SimBroker
from broker.types import Bar
from core import clock
from core.engine import TradingEngine
from position.state_machine import PositionStateMachine
from risk.risk_manager import RiskManager
from strategy.strategies.ma_cross import MaCrossStrategy

# Monotonic rise: MaCross opens LONG and never crosses back down -> stays holding.
RISING = [5, 4, 3, 2, 3, 6, 9, 12, 15, 18]


def _engine():
    broker = SimBroker("TMFR1")
    engine = TradingEngine(
        broker, MaCrossStrategy(fast=2, slow=3),
        RiskManager(), PositionStateMachine(),
        force_close_fn=clock.should_force_close,
    )
    broker.set_on_trade(engine.on_trade)
    return broker, engine


def _open_long(broker, engine):
    base = datetime(2024, 12, 18, 9, 0)   # well inside the day session
    for i, c in enumerate(RISING):
        b = Bar("TMFR1", base + timedelta(minutes=5 * i), c, c + 1, c - 1, c, 1, "5m")
        broker.set_mark_time(b.ts)
        engine.on_bar(b)
    assert engine.position.is_holding()


def test_detail_bar_force_closes_in_preclose_window():
    """A 1m detail bar at 13:44 flattens the position even though no 5m strategy
    bar lands on the 1-minute force-close boundary (regression for carry-across)."""
    broker, engine = _engine()
    _open_long(broker, engine)

    fc_bar = Bar("TMFR1", datetime(2024, 12, 18, 13, 44), 18, 18, 18, 18, 1, "1m")
    broker.set_mark_time(fc_bar.ts)
    engine.on_bar(fc_bar)

    assert engine.position.is_flat()
    assert len(engine.round_trips) == 1


def test_detail_bar_outside_window_does_not_trade():
    """A 1m detail bar in-session (not force-close) leaves the position untouched."""
    broker, engine = _engine()
    _open_long(broker, engine)

    mid = Bar("TMFR1", datetime(2024, 12, 18, 11, 0), 18, 18, 18, 18, 1, "1m")
    broker.set_mark_time(mid.ts)
    engine.on_bar(mid)

    assert engine.position.is_holding()
    assert len(engine.round_trips) == 0
