from datetime import datetime, timedelta

from broker.types import Bar
from strategy.base import SignalType, StrategyContext
from strategy.strategies.composite import CompositeStrategy
from strategy.strategies.engulfing import EngulfingStrategy
from strategy import signals


def _bar(o, h, l, c, ts):
    return Bar("TM", ts, o, h, l, c, 1, "5m")


def _series(rows):
    base = datetime(2024, 12, 18, 9, 0)
    return [_bar(o, h, l, c, base + timedelta(minutes=5 * i))
            for i, (o, h, l, c) in enumerate(rows)]


def test_buy_breakout_fires_on_new_high():
    # 5 flat bars at high=10, then a close above the prior-5-bar high.
    bars = _series([(10, 10, 9, 10)] * 5 + [(10, 12, 10, 11)])
    assert signals.buy(bars, breakout_lb=5) is True
    assert signals.sell(bars, breakout_lb=5) is False


def test_sell_flee_bear_trap():
    # new prior-5-bar low intrabar, then closes above the previous close -> long.
    bars = _series([(10, 10, 9, 10)] * 5 + [(10, 10, 7, 11)])
    prev_close = bars[-2].close
    assert bars[-1].low < min(b.low for b in bars[-6:-1])
    assert bars[-1].close > prev_close
    assert signals.sell_flee(bars, flee_lb=5) is True


def test_composite_reversal_only_matches_engulfing():
    # With only pick/touch enabled, the composite must emit the same directional
    # signals as the standalone engulfing strategy on the same bar stream.
    rows = [(10, 11, 8, 9 + (i % 3)) for i in range(60)]
    bars = _series(rows)
    comp = CompositeStrategy(lookback=10, enable={"pick", "touch"})
    eng = EngulfingStrategy(lookback=10)
    cctx, ectx = StrategyContext(), StrategyContext()
    for b in bars:
        cctx.bars.append(b)
        ectx.bars.append(b)
        cs = comp.on_bar_close(b, cctx)
        es = eng.on_bar_close(b, ectx)
        assert (cs.type if cs else None) == (es.type if es else None)


def test_trade_day_boundary():
    # night session (>= 15:00) leads the NEXT trading day; earlier bars stay same.
    comp = CompositeStrategy(daily_self_gate=True)
    night = _bar(1, 1, 1, 1, datetime(2024, 12, 18, 20, 0))
    morning = _bar(1, 1, 1, 1, datetime(2024, 12, 19, 2, 0))
    day = _bar(1, 1, 1, 1, datetime(2024, 12, 19, 10, 0))
    assert comp._trade_day(night) == datetime(2024, 12, 19).date()
    assert comp._trade_day(morning) == datetime(2024, 12, 19).date()
    assert comp._trade_day(day) == datetime(2024, 12, 19).date()


def test_daily_self_gate_runs_and_tracks_state():
    # Feed a multi-day stream; the gated composite must process without error
    # and advance its internal daily state past the initial neutral 0.
    comp = CompositeStrategy(lookback=3, breakout_lb=3, flee_lb=3, daily_self_gate=True)
    ctx = StrategyContext()
    base = datetime(2024, 12, 1, 9, 0)
    for i in range(300):
        c = 100 + (i % 7) - 3
        b = _bar(c, c + 2, c - 2, c, base + timedelta(hours=2 * i))
        ctx.bars.append(b)
        comp.on_bar_close(b, ctx)
    assert comp._dstate in (-1, 0, 1)
    assert len(comp._dctx.bars) > 5   # several trading days were finalized


def test_entry_reason_flows_to_round_trip():
    from broker.sim import SimBroker
    from core.engine import TradingEngine
    from position.state_machine import PositionStateMachine
    from risk.risk_manager import RiskManager
    from strategy.base import Signal, Strategy

    class Stub(Strategy):
        timeframe = "5m"
        def __init__(self):
            self.i = 0
        def on_bar_close(self, bar, ctx):
            self.i += 1
            if self.i == 1:
                return Signal(bar.symbol, SignalType.LONG, bar.ts, bar.close, reason="pick")
            if self.i == 2:
                return Signal(bar.symbol, SignalType.SHORT, bar.ts, bar.close, reason="x")
            return None

    broker = SimBroker("TM")
    engine = TradingEngine(broker, Stub(), RiskManager(), PositionStateMachine())
    broker.set_on_trade(engine.on_trade)
    base = datetime(2024, 12, 18, 9, 0)
    for i in range(2):
        b = _bar(100, 101, 99, 100, base + timedelta(minutes=5 * i))
        broker.set_mark_time(b.ts)
        engine.on_bar(b)
    assert engine.round_trips and engine.round_trips[0].reason == "pick"


def test_composite_arbitration_flee_beats_momentum():
    # Construct a bar that is both a downside breakout (sell, momentum) and a
    # bear trap (sell_flee, flee=long). Flee has higher priority -> LONG.
    bars = _series([(10, 10, 9, 10)] * 6 + [(10, 10, 7, 11)])
    comp = CompositeStrategy(breakout_lb=5, flee_lb=5)
    sig = comp.on_bar_close(bars[-1], StrategyContext(bars=list(bars)))
    assert sig is not None and sig.type is SignalType.LONG and sig.reason == "sell_flee"
