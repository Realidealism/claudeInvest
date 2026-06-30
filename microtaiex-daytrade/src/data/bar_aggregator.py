"""Aggregate ticks into multi-timeframe bars (tick -> 1m -> 5m).

Each timeframe is built independently off the same tick stream, so a 5m bar's
OHLCV equals the aggregation of its five 1m bars by construction. Bars are
labeled by their right edge (close timestamp).

Gap handling within a session: missing buckets are backfilled as zero-volume
bars carrying the previous close ("無量補空"). Across a session boundary the
caller invokes ``reset()`` (flush + clear) so no backfill spans the gap.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, List, Optional, Sequence

from broker.types import Bar, Tick

OnBarClose = Callable[[Bar], None]


@dataclass
class _Building:
    bucket_start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


def _bucket_start(ts: datetime, minutes: int) -> datetime:
    discard = timedelta(
        minutes=ts.minute % minutes,
        seconds=ts.second,
        microseconds=ts.microsecond,
    )
    return ts - discard


class BarAggregator:
    def __init__(
        self,
        timeframes: Sequence[str] = ("1m", "5m"),
        on_bar_close: Optional[OnBarClose] = None,
        fill_gaps: bool = True,
    ) -> None:
        self._tfs = {tf: self._tf_minutes(tf) for tf in timeframes}
        self._states: dict[str, Optional[_Building]] = {tf: None for tf in timeframes}
        self._cb = on_bar_close
        self._fill_gaps = fill_gaps
        self._symbol: Optional[str] = None

    @staticmethod
    def _tf_minutes(tf: str) -> int:
        if not tf.endswith("m"):
            raise ValueError(f"unsupported timeframe: {tf}")
        return int(tf[:-1])

    def set_on_bar_close(self, cb: OnBarClose) -> None:
        self._cb = cb

    def on_tick(self, tick: Tick) -> None:
        self._symbol = tick.symbol
        for tf, minutes in self._tfs.items():
            self._update(tf, minutes, tick)

    def reset(self) -> None:
        """Flush all open bars and clear state (call at session boundary)."""
        for tf in list(self._states):
            st = self._states[tf]
            if st is not None:
                self._emit(tf, st)
                self._states[tf] = None

    # ---- internals ----
    def _update(self, tf: str, minutes: int, tick: Tick) -> None:
        st = self._states[tf]
        bstart = _bucket_start(tick.ts, minutes)
        if st is None:
            self._states[tf] = self._new(bstart, tick)
            return
        if bstart == st.bucket_start:
            st.high = max(st.high, tick.price)
            st.low = min(st.low, tick.price)
            st.close = tick.price
            st.volume += tick.volume
            return
        # crossed into a new bucket: close current, backfill gap, open new
        self._emit(tf, st)
        if self._fill_gaps:
            self._fill(tf, minutes, st, bstart)
        self._states[tf] = self._new(bstart, tick)

    @staticmethod
    def _new(bucket_start: datetime, tick: Tick) -> _Building:
        return _Building(
            bucket_start=bucket_start,
            open=tick.price,
            high=tick.price,
            low=tick.price,
            close=tick.price,
            volume=tick.volume,
        )

    def _fill(self, tf: str, minutes: int, prev: _Building, next_start: datetime) -> None:
        step = timedelta(minutes=minutes)
        cursor = prev.bucket_start + step
        while cursor < next_start:
            filler = _Building(
                bucket_start=cursor,
                open=prev.close,
                high=prev.close,
                low=prev.close,
                close=prev.close,
                volume=0,
            )
            self._emit(tf, filler)
            cursor += step

    def _emit(self, tf: str, st: _Building) -> None:
        if self._cb is None:
            return
        minutes = self._tfs[tf]
        bar = Bar(
            symbol=self._symbol or "",
            ts=st.bucket_start + timedelta(minutes=minutes),
            open=st.open,
            high=st.high,
            low=st.low,
            close=st.close,
            volume=st.volume,
            timeframe=tf,
        )
        self._cb(bar)
