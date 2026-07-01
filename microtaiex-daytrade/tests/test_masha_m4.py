from datetime import datetime

from datetime import time
from broker.types import Bar
from strategy import timing as tm
from strategy import trendline as tl


def _b(o, h, l, c):
    return Bar("TM", datetime(2024, 12, 16, 9, 0), o, h, l, c, 1, "5m")


def test_fit_line():
    line = tl._fit_line([(0, 0.0), (1, 2.0), (2, 4.0)])
    assert line is not None
    slope, intercept = line
    assert abs(slope - 2.0) < 1e-9 and abs(intercept) < 1e-9


def _wave(base, step, n):
    """Channel bars: every even bar is a swing LOW on line L(c)=base+step*c,
    every odd bar a swing HIGH on the parallel line L(c)+40 (c = cycle = i//2).
    |step| < 20 keeps each pivot a genuine local extreme vs its neighbours."""
    bars = []
    for i in range(n):
        lvl = base + step * (i // 2)
        if i % 2 == 0:
            bars.append(_b(lvl + 1, lvl + 3, lvl, lvl + 2))          # swing low @ lvl
        else:
            bars.append(_b(lvl + 25, lvl + 40, lvl + 20, lvl + 30))  # swing high @ lvl+40
    return bars


def test_channel_up():
    bars = _wave(1000.0, 6.0, 40)          # rising base → up channel
    direction, lower, upper = tl.channel(bars, k=1, lookback=40, tol=20.0, min_touches=3)
    assert direction == "up"
    assert lower is not None and lower > 1000.0


def test_channel_down():
    bars = _wave(1300.0, -6.0, 40)         # falling base → down channel
    direction, lower, upper = tl.channel(bars, k=1, lookback=40, tol=20.0, min_touches=3)
    assert direction == "down"
    assert upper is not None


def test_channel_range_when_flat():
    flat = [_b(1000, 1005, 995, 1000)] * 40   # no slope → range
    direction, lower, upper = tl.channel(flat, k=1, lookback=40, tol=5.0, min_touches=3)
    assert direction == "range" and lower is None and upper is None


def test_rail_entry_long_at_lower_rail():
    bars = _wave(1000.0, 6.0, 40)             # up channel
    _, lower, _ = tl.channel(bars, k=1, lookback=40, tol=20.0, min_touches=3)
    # append a black bar then a bullish engulf sitting on the lower rail
    lo = lower
    bars = bars + [_b(lo + 4, lo + 5, lo - 1, lo + 1),      # black
                   _b(lo, lo + 8, lo - 2, lo + 6)]          # bullish engulf @ rail
    assert tl.rail_entry(bars, k=1, lookback=42, tol=20.0,
                         min_touches=3, rail_tol=15.0) == "long"


def test_rail_entry_none_when_far_from_rail():
    bars = _wave(1000.0, 6.0, 40)
    _, lower, _ = tl.channel(bars, k=1, lookback=40, tol=20.0, min_touches=3)
    far = lower + 500                          # engulf far above the rail → no entry
    bars = bars + [_b(far + 4, far + 5, far - 1, far + 1),
                   _b(far, far + 8, far - 2, far + 6)]
    assert tl.rail_entry(bars, k=1, lookback=42, tol=20.0,
                         min_touches=3, rail_tol=15.0) is None


# ── §8.3 時間波 有單/沒單 ────────────────────────────────────────────────

def _dt(h, m):
    return datetime(2024, 12, 16, h, m)


def test_is_timewave_bar():
    assert tm.is_timewave_bar(_dt(9, 40)) is True
    assert tm.is_timewave_bar(_dt(9, 25)) is False


def test_no_direction():
    doji = [Bar("TM", _dt(9, 40), 100, 105, 95, 100.5, 1, "5m")]   # tiny body
    trend = [Bar("TM", _dt(9, 40), 100, 106, 99, 105.5, 1, "5m")]  # big body
    assert tm.no_direction(doji, doji_ratio=0.3) is True
    assert tm.no_direction(trend, doji_ratio=0.3) is False
