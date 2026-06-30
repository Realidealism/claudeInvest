"""Demo: SimBroker -> Feed -> EventBus -> BarAggregator, printing aggregated K.

Run: python demo_pipeline.py
Feeds synthetic ticks and prints the 1m / 5m bars the pipeline produces, so you
can eyeball boundary alignment and that a 5m bar equals five 1m bars.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from broker.sim import SimBroker          # noqa: E402
from broker.types import Tick             # noqa: E402
from core.event_bus import EventBus       # noqa: E402
from data.bar_aggregator import BarAggregator  # noqa: E402
from data.feed import EVENT_BAR, Feed     # noqa: E402


def main() -> None:
    bus = EventBus()
    agg = BarAggregator(timeframes=("1m", "5m"))
    broker = SimBroker(symbol="TMFR1")
    Feed(broker, bus, agg)

    bus.subscribe(EVENT_BAR, lambda bar: print(
        f"  [{bar.timeframe}] {bar.ts:%H:%M}  "
        f"O={bar.open:>6} H={bar.high:>6} L={bar.low:>6} C={bar.close:>6} V={bar.volume}"
    ))
    bus.start()

    # synthetic ticks over ~6 minutes, one every 20s
    base = datetime(2024, 12, 18, 9, 0, 0)
    prices = [100, 102, 101, 103, 99, 104, 100, 106, 102, 105,
              101, 107, 103, 99, 108, 104, 102, 110]
    print("Feeding ticks...")
    for i, p in enumerate(prices):
        broker.feed_tick(Tick(symbol="TMFR1", ts=base + timedelta(seconds=i * 20),
                              price=p, volume=1))
    bus.wait_idle()   # let the consumer drain all ticks before flushing
    agg.reset()       # flush the last open bars (session-close semantics)

    bus.wait_idle()
    bus.stop()
    print("Done.")


if __name__ == "__main__":
    main()
