from datetime import datetime, timedelta

from broker.types import Bar
from strategy.base import SignalType, StrategyContext
from strategy.strategies.engulfing import EngulfingStrategy


def _drive(strat, ohlc):
    ctx = StrategyContext()
    base = datetime(2024, 12, 18, 9, 0)
    out = []
    for i, (o, h, l, c) in enumerate(ohlc):
        bar = Bar("TMF00", base + timedelta(minutes=5 * i), o, h, l, c, 10, strat.timeframe)
        ctx.bars.append(bar)
        out.append(strat.on_bar_close(bar, ctx))
    return out


# 3 descending black bars (prior), then a bullish engulfing making a new low.
BULLISH_AT_LOW = [
    (62, 63, 60, 61),
    (61, 62, 58, 59),
    (59, 60, 56, 57),
    (57, 57.5, 53, 54),   # prev: black
    (53, 59, 52, 58),     # curr: red, engulfs prev, low 52 < prior min low 56
]

# same pattern but the pattern low does NOT break the prior lows -> no signal
BULLISH_NOT_AT_LOW = [
    (62, 63, 50, 61),     # prior low 50 (very low)
    (61, 62, 58, 59),
    (59, 60, 57, 57.5),
    (57.5, 58, 56, 56.5),  # prev black
    (56.5, 60, 56, 59),    # curr red engulf but low 56 > prior min 50 -> not relative low
]

BEARISH_AT_HIGH = [
    (40, 42, 39, 41),
    (41, 44, 40, 43),
    (43, 46, 42, 45),
    (45, 49, 44, 48),     # prev: red
    (48, 50, 43, 44),     # curr: black, engulfs prev, high 50 > prior max high 46
]


def test_bullish_engulfing_at_low_goes_long():
    sigs = _drive(EngulfingStrategy(lookback=3), BULLISH_AT_LOW)
    assert sigs[-1] is not None and sigs[-1].type is SignalType.LONG


def test_engulfing_not_at_relative_low_no_signal():
    sigs = _drive(EngulfingStrategy(lookback=3), BULLISH_NOT_AT_LOW)
    assert sigs[-1] is None


def test_bearish_engulfing_at_high_goes_short():
    sigs = _drive(EngulfingStrategy(lookback=3), BEARISH_AT_HIGH)
    assert sigs[-1] is not None and sigs[-1].type is SignalType.SHORT


def test_warmup_no_signal():
    sigs = _drive(EngulfingStrategy(lookback=10), BULLISH_AT_LOW)
    assert all(s is None for s in sigs)   # not enough bars for lookback=10


# 10 rising bars (closes 100..109) then a bearish engulfing at the high (-> SHORT).
# That SHORT is counter-trend (close above the rising SMA), so trend_sma filters it.
_UPTREND_THEN_BEARISH = (
    [(c - 1, c + 1, c - 1, c) for c in range(100, 110)] +   # 10 rising red bars
    [(109, 114, 108, 113),                                  # prev: red
     (113, 115, 107, 108)]                                  # curr: bearish engulfing at high
)


def test_trend_filter_blocks_counter_trend():
    no_filter = _drive(EngulfingStrategy(lookback=10), _UPTREND_THEN_BEARISH)
    assert no_filter[-1] is not None and no_filter[-1].type is SignalType.SHORT
    # with trend filter: a SHORT while price is above the SMA is counter-trend -> dropped
    filtered = _drive(EngulfingStrategy(lookback=10, trend_sma=10), _UPTREND_THEN_BEARISH)
    assert filtered[-1] is None
