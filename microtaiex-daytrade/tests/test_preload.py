from datetime import datetime, timedelta

from broker.sim import SimBroker
from broker.types import Bar
from core.engine import TradingEngine
from position.state_machine import PositionStateMachine
from risk.risk_manager import RiskConfig, RiskManager
from strategy.strategies.masha import MashaStrategy


def _bars(n, start_price=1000.0):
    t0 = datetime(2026, 7, 1, 9, 0)
    return [Bar("TM0000", t0 + timedelta(minutes=5 * i),
                start_price + i, start_price + i + 2, start_price + i - 2,
                start_price + i + 1, 100, "5m") for i in range(n)]


def _engine():
    sim = SimBroker(symbol="TM0000")
    eng = TradingEngine(sim, MashaStrategy(trend_gate=None),
                        RiskManager(RiskConfig(stop_mode="masha")),
                        PositionStateMachine(), atr_period=21)
    sim.set_on_trade(eng.on_trade)
    return eng


def test_preload_fills_ctx_without_trading():
    eng = _engine()
    eng.preload(_bars(50))
    assert len(eng._ctx.bars) == 50
    assert eng.round_trips == []            # pure state fill — no orders on the past
    assert eng.position.state.value == "flat"


def test_preload_skips_non_newer_bars():
    eng = _engine()
    bars = _bars(50)
    eng.preload(bars)
    eng.preload(bars)                       # same bars again → all older-or-equal
    assert len(eng._ctx.bars) == 50         # deduped by ts, no duplicates appended
    eng.preload(_bars(60)[50:])             # 10 genuinely newer bars
    assert len(eng._ctx.bars) == 60
