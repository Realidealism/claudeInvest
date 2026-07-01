from datetime import datetime

from broker.types import Bar
from strategy import sop


def _b(o, h, l, c):
    return Bar("TM", datetime(2024, 12, 16, 9, 0), o, h, l, c, 1, "5m")


def test_pivots():
    bars = [_b(9, 10, 8, 9), _b(9, 10, 8, 9), _b(13, 15, 13, 14),
            _b(9, 10, 8, 9), _b(9, 10, 8, 9)]
    highs, lows = sop.pivots(bars, k=2)
    assert highs == [15.0] and lows == []


def test_sr_levels_cluster():
    flat = [_b(9, 10, 8, 9)] * 2
    bars = [*flat, _b(13, 15.0, 13, 14), *flat, _b(13, 15.2, 13, 14), *flat]
    levels = sop.sr_levels(bars, k=2, tol=1.0, min_touches=2)
    assert 15.1 in levels   # two swing highs (15.0, 15.2) cluster (flat lows may add 8.0)


def test_check_sop_long():
    bars = [
        _b(99, 99, 98, 99),          # context below level 100
        _b(99, 103, 99, 102),        # step1: close 102 > 100 (站上)
        _b(102, 102, 99.5, 101),     # step2: 回踩 (low 99.5 ≤ 100, close 101 ≥ 100)
        _b(100, 100, 98, 98.5),      # black bar (prev for engulf)
        _b(98, 104, 97, 103),        # step3: bullish engulf → SOP long
    ]
    assert sop.check_sop(bars, level=100.0, direction="long") is True
    assert sop.check_sop(bars, level=100.0, direction="short") is False


def test_sop_signal_none_without_levels():
    flat = [_b(99, 99.2, 98.8, 99)] * 12       # no clustered pivots → no S/R levels
    assert sop.sop_signal(flat) is None
