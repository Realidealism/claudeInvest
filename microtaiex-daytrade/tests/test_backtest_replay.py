from datetime import datetime, timedelta

from broker.sim import SimBroker
from broker.types import Bar
from core.engine import TradingEngine
from core.event_bus import EventBus
from data.feed import EVENT_BAR, EVENT_TRADE
from position.state_machine import PositionStateMachine
from risk.risk_manager import RiskManager
from strategy.strategies.ma_cross import MaCrossStrategy
from backtest.replay import CostModel, replay


# LONG entry at close=6 (bar i5); rises monotonically -> flattened at last close=18.
RISING = [5, 4, 3, 2, 3, 6, 9, 12, 15, 18]
# LONG entry at close=6 (bar i5); SHORT cross-down exit at close=6 (bar i9).
CROSS = [5, 4, 3, 2, 3, 6, 9, 12, 9, 6]


def _bars(closes):
    base = datetime(2024, 12, 18, 9, 0)
    return [
        Bar("TMFR1", base + timedelta(minutes=5 * i), c, c + 1, c - 1, c, 1, "5m")
        for i, c in enumerate(closes)
    ]


def test_replay_round_trip_and_costs():
    res = replay(_bars(RISING), MaCrossStrategy(fast=2, slow=3))
    assert res.n_trades == 1
    rt = res.round_trips[0]
    assert (rt.entry_price, rt.exit_price, rt.points) == (6, 18, 12)
    assert res.gross_pnl == 120.0                    # 12 pts * 10 NT$/pt * 1 lot
    # cost = fees(20*1*2) + tax((6+18)*10*1*0.00002)
    assert abs(res.cost - (40.0 + 24 * 10 * 0.00002)) < 1e-9
    assert res.net_pnl > 0
    assert res.win_rate == 1.0


def test_custom_cost_model():
    res = replay(_bars(RISING), MaCrossStrategy(fast=2, slow=3),
                 cost=CostModel(fee_per_lot=5.0, tax_rate=0.0, point_value=10.0))
    assert res.cost == 10.0                           # 5 * 1 * 2 sides, no tax
    assert res.net_pnl == 110.0                        # 120 gross - 10 cost


def test_opposite_signal_closes_position():
    res = replay(_bars(CROSS), MaCrossStrategy(fast=2, slow=3))
    assert res.n_trades == 1
    assert res.round_trips[0].exit_price == 6          # closed by SHORT cross-down


def test_live_bus_matches_replay():
    """Same strategy code, same fills: paced EventBus path == synchronous replay."""
    bars = _bars(RISING)
    bt = replay(bars, MaCrossStrategy(fast=2, slow=3))

    broker = SimBroker("TMFR1")
    engine = TradingEngine(broker, MaCrossStrategy(fast=2, slow=3),
                           RiskManager(), PositionStateMachine())
    bus = EventBus()
    bus.subscribe(EVENT_BAR, engine.on_bar)
    bus.subscribe(EVENT_TRADE, engine.on_trade)
    broker.set_on_trade(lambda t: bus.publish(EVENT_TRADE, t))
    bus.start()
    for b in bars:
        broker.set_mark_time(b.ts)
        bus.publish(EVENT_BAR, b)
        bus.wait_idle()                               # let the fill settle before next bar
    if not engine.position.is_flat():
        broker.set_mark_time(bars[-1].ts)
        engine.flatten(bars[-1].close, bars[-1].ts)
        bus.wait_idle()
    bus.stop()

    assert [(r.entry_price, r.exit_price, r.points) for r in engine.round_trips] == \
           [(r.entry_price, r.exit_price, r.points) for r in bt.round_trips]
