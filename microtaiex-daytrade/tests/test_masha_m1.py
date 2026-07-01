from datetime import datetime, timedelta

from broker.types import Bar
from strategy.base import SignalType, StrategyContext
from strategy import signals_masha as sm
from strategy.strategies.masha import MashaStrategy


def _b(o, h, l, c, ts=None):
    return Bar("TM", ts or datetime(2024, 12, 16, 9, 0), o, h, l, c, 1, "5m")


# ── §4.2 吞噬 ───────────────────────────────────────────────────────────

def test_engulf_and_strength():
    long_bars = [_b(10, 10, 7, 8), _b(7, 12, 6, 11)]     # black then red, wick-inclusive
    assert sm.engulf(long_bars) == "long"
    assert sm.engulf_strength(long_bars) == 1.0
    short_bars = [_b(8, 11, 8, 10), _b(11, 12, 6, 7)]
    assert sm.engulf(short_bars) == "short"
    # body-only engulf (does not cover prev wicks) → 0.8
    body_only = [_b(10, 15, 5, 8), _b(7.5, 11, 6.5, 10.5)]
    assert sm.engulf(body_only) == "long" and sm.engulf_strength(body_only) == 0.8


# ── §5.5 紅黑交錯懶人法 ─────────────────────────────────────────────────

def test_redblack_zone_and_lazy():
    zone = [_b(8, 10, 8, 10), _b(10, 10, 8, 8), _b(8.5, 9.5, 8.5, 9.5), _b(9.5, 9.5, 8.5, 8.5)]
    assert sm.redblack_zone(zone) == (10.0, 8.0)
    up = [*zone, _b(9, 12, 9, 11)]          # breaks above zone hi 10 → long
    assert sm.redblack_lazy(up) == "long"
    dn = [*zone, _b(9, 9, 6, 7)]            # breaks below zone lo 8 → short
    assert sm.redblack_lazy(dn) == "short"
    inside = [*zone, _b(9, 9.6, 8.4, 9)]   # stays inside → None
    assert sm.redblack_lazy(inside) is None


# ── §6 型態 ─────────────────────────────────────────────────────────────

def test_doji_flip():
    bars = [_b(10, 11, 9, 10.05), _b(9, 12, 9, 11.5)]    # doji then close>doji high, red
    assert sm.doji_flip(bars) == "long"


def test_island_reversal():
    bars = [_b(9, 10, 8, 9.5), _b(11.5, 13, 11, 12), _b(10, 10.5, 9, 9.5)]  # gap up then gap down
    assert sm.island_reversal(bars) == "short"


def test_four_hand():
    bars = [_b(10, 11.1, 9.9, 11), _b(11, 11.1, 9.9, 10),
            _b(10, 11.1, 9.9, 11), _b(11, 11.1, 9.9, 10)]
    assert sm.four_hand(bars) is True


# ── MashaStrategy integration ──────────────────────────────────────────

def test_masha_emits_and_time_gates():
    base = datetime(2024, 12, 16, 9, 0)      # Monday, in day entry window
    rows = [(8, 10, 8, 10), (10, 10, 8, 8), (8.5, 9.5, 8.5, 9.5),
            (9.5, 9.5, 8.5, 8.5), (9, 12, 9, 11)]   # zone + upside breakout
    ms = MashaStrategy(enable={"redblack"})
    ctx = StrategyContext()
    sig = None
    for i, (o, h, l, c) in enumerate(rows):
        b = _b(o, h, l, c, base + timedelta(minutes=5 * i))
        ctx.bars.append(b)
        sig = ms.on_bar_close(b, ctx)
    assert sig is not None and sig.type is SignalType.LONG and sig.reason == "redblack"

    # off-window (12:00): same breakout bar produces no entry
    off = _b(9, 12, 9, 11, datetime(2024, 12, 16, 12, 0))
    ctx2 = StrategyContext(bars=[_b(*r, base + timedelta(minutes=5 * i)) for i, r in enumerate(rows[:-1])] + [off])
    assert ms.on_bar_close(off, ctx2) is None
