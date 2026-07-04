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


def test_tx_status_gate(tmp_path):
    import json
    p = tmp_path / "futures_tx.json"

    def regime(state):
        p.write_text(json.dumps({"state": state}), encoding="utf-8")
        return CompositeStrategy(tx_status_path=str(p))._tx_regime()

    assert regime("long") == 1
    assert regime("short") == -1
    assert regime("flat") == 0
    # missing file -> 0 (fail-safe, no gate)
    assert CompositeStrategy(tx_status_path=str(tmp_path / "nope.json"))._tx_regime() == 0
    # no path -> 0
    assert CompositeStrategy()._tx_regime() == 0


def test_tx_gate_suppresses_offside(tmp_path):
    import json
    p = tmp_path / "futures_tx.json"
    p.write_text(json.dumps({"state": "short"}), encoding="utf-8")
    # factory short -> long entries suppressed; a pick (long) must not fire.
    comp = CompositeStrategy(lookback=3, enable={"pick"}, tx_status_path=str(p))
    ctx = StrategyContext()
    # a bullish-engulfing-at-low pattern that would normally emit LONG
    rows = [(10, 11, 8, 9)] * 4 + [(9, 9, 6, 6), (6, 11, 6, 10)]
    for o, h, l, c in rows:
        b = _bar(o, h, l, c, datetime(2024, 12, 18, 9, 0))
        ctx.bars.append(b)
        sig = comp.on_bar_close(b, ctx)
    assert sig is None   # long gated out under a short regime


def test_tx_gate_long_defense_breach_suppresses_long(tmp_path):
    import json
    p = tmp_path / "futures_tx.json"
    # a bear-trap bar (sell_flee -> LONG); close = 11.
    bars = _series([(10, 10, 9, 10)] * 6 + [(10, 10, 7, 11)])

    def sig_for(defense):
        p.write_text(json.dumps({"state": "long",
                                 "position": {"side": "long", "defense_price": defense}}),
                     encoding="utf-8")
        comp = CompositeStrategy(breakout_lb=5, flee_lb=5, tx_status_path=str(p))
        return comp.on_bar_close(bars[-1], StrategyContext(bars=list(bars)))

    # close 11 < defense 100 -> 多單防守跌破, long suppressed
    assert sig_for(100.0) is None
    # close 11 >= defense 5 -> long allowed to fire
    s = sig_for(5.0)
    assert s is not None and s.type is SignalType.LONG


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
