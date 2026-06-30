"""Pure indicator functions over price/bar sequences.

All return ``None`` when there is insufficient data, so callers can short-circuit
during warmup. Sequences are oldest..newest.
"""
from __future__ import annotations

from typing import List, Optional, Sequence

from broker.types import Bar


def sma(values: Sequence[float], n: int) -> Optional[float]:
    if n <= 0 or len(values) < n:
        return None
    return sum(values[-n:]) / n


def ema(values: Sequence[float], n: int) -> Optional[float]:
    if n <= 0 or len(values) < n:
        return None
    k = 2.0 / (n + 1)
    e = sum(values[:n]) / n  # seed with SMA of the first n
    for v in values[n:]:
        e = v * k + e * (1 - k)
    return e


def true_range(prev_close: float, high: float, low: float) -> float:
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def atr(bars: Sequence[Bar], n: int) -> Optional[float]:
    """Wilder's ATR (RMA of True Range), returning the latest value.

    Seeds at bar n-1 with SMA(TR, n), then applies Wilder's recursion, matching
    analysis.chandelier (everget Pine v6 port). TR[0] uses high-low only (no
    prior close). Returns None until there are at least n bars.
    """
    if n <= 0 or len(bars) < n:
        return None
    trs: List[float] = [bars[0].high - bars[0].low]
    trs.extend(
        true_range(bars[i - 1].close, bars[i].high, bars[i].low)
        for i in range(1, len(bars))
    )
    a = sum(trs[:n]) / n                       # seed: SMA of the first n TRs
    for i in range(n, len(trs)):               # Wilder RMA recursion
        a = (a * (n - 1) + trs[i]) / n
    return a


def vwap(bars: Sequence[Bar]) -> Optional[float]:
    total_vol = sum(b.volume for b in bars)
    if total_vol == 0:
        return None
    pv = sum(((b.high + b.low + b.close) / 3.0) * b.volume for b in bars)
    return pv / total_vol
