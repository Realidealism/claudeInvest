from datetime import datetime

from broker.types import Bar
from strategy.indicators import atr, ema, sma, true_range, vwap


def _bar(h, l, c, v):
    return Bar("TMFR1", datetime(2024, 12, 18, 9, 0), c, h, l, c, v, "1m")


def test_sma():
    assert sma([1, 2, 3, 4], 2) == 3.5
    assert sma([1], 2) is None


def test_ema():
    assert ema([2, 4, 6], 3) == 4.0          # seed only
    assert ema([2, 4, 6, 8], 3) == 6.0       # seed 4 -> 8*0.5+4*0.5
    assert ema([1, 2], 3) is None


def test_true_range():
    assert true_range(9, 11, 9) == 2
    assert true_range(5, 11, 9) == 6         # gap from prev close dominates


def test_atr():
    bars = [_bar(10, 8, 9, 1), _bar(11, 9, 10, 1), _bar(12, 10, 11, 2)]
    assert atr(bars, 2) == 2.0                 # constant TR=2 -> Wilder == 2.0
    assert atr(bars, 3) == 2.0                 # exactly n bars -> seeded value
    assert atr(bars, 4) is None                # fewer than n bars


def test_atr_wilder_recursion():
    # TR series = [2, 10, 2, 2]; Wilder ATR(2): seed (2+10)/2=6 -> 4 -> 3.0.
    # A simple trailing mean of the last 2 TRs would give 2.0, so this pins Wilder.
    bars = [_bar(10, 8, 9, 1), _bar(19, 17, 18, 1), _bar(20, 18, 19, 1), _bar(21, 19, 20, 1)]
    assert atr(bars, 2) == 3.0


def test_vwap():
    bars = [_bar(10, 8, 9, 1), _bar(11, 9, 10, 1), _bar(12, 10, 11, 2)]
    assert vwap(bars) == 10.25
    assert vwap([_bar(10, 8, 9, 0)]) is None  # zero volume
