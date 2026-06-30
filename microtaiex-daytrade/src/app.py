"""Entry point: wire the full live pipeline and run it.

Live architecture (used for both real broker and the `sim` demo):

    broker.on_tick -> EventBus("tick") -> Feed -> BarAggregator
        -> EventBus("bar") -> TradingEngine.on_bar -> risk -> broker.place_order
    broker.on_trade -> EventBus("trade") -> TradingEngine.on_trade

All decision logic runs on the single EventBus consumer thread. The live Capital
SKCOM broker additionally needs 32-bit Python + comtypes (not available here),
so ``main`` defaults to the `sim` source feeding synthetic ticks.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Tuple

from broker.base import BrokerAdapter
from broker.factory import make_broker
from broker.types import Tick
from core import clock
from core.engine import TradingEngine
from core.event_bus import EventBus
from data.bar_aggregator import BarAggregator
from data.feed import EVENT_BAR, EVENT_TRADE, Feed
from position.state_machine import PositionStateMachine
from risk.risk_manager import RiskConfig, RiskManager
from strategy.strategies.engulfing import EngulfingStrategy
from strategy.strategies.ma_cross import MaCrossStrategy

log = logging.getLogger(__name__)


def _make_strategy(strat_cfg: dict, timeframe: str):
    name = strat_cfg.get("name", "ma_cross")
    if name == "engulfing":
        return EngulfingStrategy(lookback=int(strat_cfg.get("lookback", 10)), timeframe=timeframe)
    return MaCrossStrategy(
        fast=int(strat_cfg.get("fast", 5)),
        slow=int(strat_cfg.get("slow", 20)),
        timeframe=timeframe,
    )


def load_config(path: str) -> dict:
    import yaml  # lazy: only needed for live config, not the sim demo
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    return yaml.safe_load(os.path.expandvars(raw))


def build_pipeline(cfg: dict, broker: BrokerAdapter) -> Tuple[EventBus, TradingEngine, BarAggregator]:
    """Wire the full live pipeline around an already-built broker."""
    strat_tf = cfg.get("strategy", {}).get("timeframe", "5m")
    risk_cfg = cfg.get("risk", {})

    bus = EventBus()
    agg = BarAggregator(timeframes=("1m", strat_tf))
    Feed(broker, bus, agg)

    strat_cfg = cfg.get("strategy", {})
    strategy = _make_strategy(strat_cfg, strat_tf)
    risk = RiskManager(RiskConfig(
        max_lots=int(risk_cfg.get("max_lots", 1)),
        stop_loss_atr_mult=float(risk_cfg.get("stop_loss_atr_mult", 2.0)),
        max_daily_loss_points=float(risk_cfg.get("max_daily_loss_points", 10)),
    ))
    engine = TradingEngine(
        broker, strategy, risk, PositionStateMachine(),
        force_close_fn=clock.should_force_close,
    )

    bus.subscribe(EVENT_BAR, engine.on_bar)
    bus.subscribe(EVENT_TRADE, engine.on_trade)
    broker.set_on_trade(lambda t: bus.publish(EVENT_TRADE, t))
    return bus, engine, agg


def _synthetic_ticks(symbol: str):
    """~60 min of ticks (uptrend then downtrend) -> ~12 5m bars, one round trip."""
    base = datetime(2024, 12, 18, 9, 0, 0)
    prices = list(range(100, 160)) + list(range(160, 100, -1))   # 120 ticks, 30s apart
    for i, p in enumerate(prices):
        yield Tick(symbol=symbol, ts=base + timedelta(seconds=i * 30), price=float(p), volume=1)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    symbol = "TMFR1"
    broker = make_broker({"name": "sim", "symbol": symbol})
    cfg = {"strategy": {"timeframe": "5m", "fast": 2, "slow": 3}, "risk": {"max_lots": 1}}
    bus, engine, agg = build_pipeline(cfg, broker)

    bus.start()
    broker.connect()
    broker.subscribe(symbol)
    # Real-time pacing: drain the cascade (tick -> bar -> order -> fill) before the
    # next tick, mirroring live timing where fills settle long before the next bar.
    last_tick = None
    for tick in _synthetic_ticks(symbol):
        broker.feed_tick(tick)
        bus.wait_idle()
        last_tick = tick
    agg.reset()
    bus.wait_idle()
    # session-end: flatten any open position (live: clock.should_force_close does this)
    if last_tick is not None and not engine.position.is_flat():
        broker.set_mark_time(last_tick.ts)
        engine.flatten(last_tick.price, last_tick.ts)
        bus.wait_idle()
    bus.stop()
    broker.disconnect()

    print(f"\nRound-trips: {len(engine.round_trips)}")
    for rt in engine.round_trips:
        print(f"  {rt.side.value:5} entry={rt.entry_price} exit={rt.exit_price} points={rt.points:+.1f}")


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent))
    main()
