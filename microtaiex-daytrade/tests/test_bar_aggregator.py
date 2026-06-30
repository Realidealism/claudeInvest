from datetime import datetime

from broker.types import Tick
from data.bar_aggregator import BarAggregator


def _tick(hh, mm, ss, price, vol=1):
    return Tick(symbol="TMFR1", ts=datetime(2024, 12, 18, hh, mm, ss), price=price, volume=vol)


def _collect(timeframes):
    bars = []
    agg = BarAggregator(timeframes=timeframes, on_bar_close=bars.append)
    return agg, bars


def test_1m_boundary_and_ohlcv():
    agg, bars = _collect(("1m",))
    agg.on_tick(_tick(9, 0, 10, 100, 1))
    agg.on_tick(_tick(9, 0, 30, 105, 2))
    agg.on_tick(_tick(9, 0, 50, 98, 1))
    agg.on_tick(_tick(9, 1, 5, 103, 1))   # crosses into 9:01 -> 9:00 bar closes
    assert len(bars) == 1
    b = bars[0]
    assert b.ts == datetime(2024, 12, 18, 9, 1, 0)   # right edge
    assert (b.open, b.high, b.low, b.close, b.volume) == (100, 105, 98, 98, 4)
    assert b.timeframe == "1m"


def test_first_bar_open():
    agg, bars = _collect(("1m",))
    agg.on_tick(_tick(9, 0, 10, 100, 1))
    agg.reset()
    assert len(bars) == 1 and bars[0].open == 100


def test_gap_fill_zero_volume():
    agg, bars = _collect(("1m",))
    agg.on_tick(_tick(9, 0, 10, 100, 1))
    agg.on_tick(_tick(9, 3, 5, 110, 1))   # skips 9:01 and 9:02
    # real 9:00 bar + two zero-volume fillers
    assert len(bars) == 3
    assert bars[0].volume == 1 and bars[0].ts == datetime(2024, 12, 18, 9, 1)
    assert bars[1].volume == 0 and bars[1].ts == datetime(2024, 12, 18, 9, 2)
    assert bars[2].volume == 0 and bars[2].ts == datetime(2024, 12, 18, 9, 3)
    assert bars[1].close == bars[0].close == 100   # carried forward


def test_5m_equals_five_1m():
    agg = BarAggregator(timeframes=("1m", "5m"))
    by_tf = {"1m": [], "5m": []}
    agg.set_on_bar_close(lambda bar: by_tf[bar.timeframe].append(bar))
    # ticks spanning 9:00:00 .. 9:05:30, one per ~40s
    prices = [100, 102, 99, 105, 101, 103, 98, 104, 100, 106]
    for i, p in enumerate(prices):
        agg.on_tick(_tick(9, (i * 40) // 60, (i * 40) % 60, p, 1))
    agg.reset()
    # first 5m window = 9:00..9:05 (ts 9:05)
    five = next(b for b in by_tf["5m"] if b.ts == datetime(2024, 12, 18, 9, 5))
    ones = [b for b in by_tf["1m"] if b.ts <= datetime(2024, 12, 18, 9, 5)]
    assert five.open == ones[0].open
    assert five.high == max(b.high for b in ones)
    assert five.low == min(b.low for b in ones)
    assert five.close == ones[-1].close
    assert five.volume == sum(b.volume for b in ones)
