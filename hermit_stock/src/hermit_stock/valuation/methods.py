"""Target-price methods + buy/sell decision per design §8.4 / §8.6."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd

from .bands import Band
from .selector import ValuationMethod

Decision = Literal["BUY", "SELL", "HOLD"]


@dataclass(frozen=True)
class ValuationSnapshot:
    method: ValuationMethod
    current_close: float
    current_multiple: float | None  # PE / PB / PS at the latest day
    per_share_metric: float | None  # eps_ttm / bvps / sps_ttm
    band: Band
    band_position: str | None  # e.g. "−1σ ~ mean"
    target_mean: float | None
    target_minus_1sd: float | None
    target_plus_1sd: float | None
    upside_mean: float | None
    upside_lower: float | None
    upside_upper: float | None
    decision: Decision


def _safe_div(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or b == 0:
        return None
    return a / b


def make_snapshot(
    daily: pd.DataFrame,
    method: ValuationMethod,
    band: Band,
    *,
    buy_threshold: float = 0.20,
    sell_threshold: float = 0.0,
) -> ValuationSnapshot | None:
    """Build a snapshot of the latest day in `daily`. Returns None if empty."""
    if daily.empty:
        return None
    last_row = daily.iloc[-1]
    close = float(last_row["close"])
    col = method.lower()  # "pe" / "pb" / "ps"
    cur_multiple = last_row.get(col)
    cur_multiple = float(cur_multiple) if pd.notna(cur_multiple) else None
    per_share = _safe_div(close, cur_multiple)

    band_pos = band.classify(cur_multiple)

    target_mean: float | None = None
    target_minus: float | None = None
    target_plus: float | None = None
    upside_mean: float | None = None
    upside_lower: float | None = None
    upside_upper: float | None = None
    if per_share is not None and band.mean is not None:
        target_mean = per_share * band.mean
        if band.minus_1sd is not None:
            target_minus = per_share * band.minus_1sd
            upside_lower = (target_minus - close) / close
        if band.plus_1sd is not None:
            target_plus = per_share * band.plus_1sd
            upside_upper = (target_plus - close) / close
        upside_mean = (target_mean - close) / close

    decision: Decision
    if upside_mean is None:
        decision = "HOLD"
    elif upside_mean >= buy_threshold:
        decision = "BUY"
    elif upside_mean <= sell_threshold:
        decision = "SELL"
    else:
        decision = "HOLD"

    return ValuationSnapshot(
        method=method,
        current_close=close,
        current_multiple=cur_multiple,
        per_share_metric=per_share,
        band=band,
        band_position=band_pos,
        target_mean=target_mean,
        target_minus_1sd=target_minus,
        target_plus_1sd=target_plus,
        upside_mean=upside_mean,
        upside_lower=upside_lower,
        upside_upper=upside_upper,
        decision=decision,
    )
