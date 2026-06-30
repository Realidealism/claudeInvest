"""Feed: wire broker ticks -> EventBus -> bar aggregator -> EventBus bars.

Broker tick callbacks run on the pump/socket thread and only ``publish`` a
"tick" event. The EventBus consumer thread then drives the aggregator, so all
aggregation and downstream strategy/risk run serialized on one thread.
"""
from __future__ import annotations

from broker.base import BrokerAdapter
from broker.types import Bar, Tick
from core.event_bus import EventBus
from data.bar_aggregator import BarAggregator

EVENT_TICK = "tick"
EVENT_BAR = "bar"
EVENT_TRADE = "trade"


class Feed:
    def __init__(self, broker: BrokerAdapter, bus: EventBus, aggregator: BarAggregator) -> None:
        self._bus = bus
        self._agg = aggregator
        aggregator.set_on_bar_close(lambda bar: bus.publish(EVENT_BAR, bar))
        broker.set_on_tick(lambda tick: bus.publish(EVENT_TICK, tick))
        bus.subscribe(EVENT_TICK, self._on_tick_event)

    def _on_tick_event(self, tick: Tick) -> None:
        self._agg.on_tick(tick)
