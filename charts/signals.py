"""
Signal marker generator — converts analysis results into chart markers.

Maps boolean signal arrays from analysis modules (candle, volume, close)
into SignalMarker lists that candlestick.py can overlay on the chart.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
from numpy.typing import NDArray

from charts.candlestick import SignalMarker


# Signal definitions: (analysis_path, display_config)
# analysis_path: dot-separated path into the analysis result dataclass
# display_config: visual properties for the marker

@dataclass
class SignalDef:
    """Definition of a signal type that can be toggled on/off."""
    key: str                # unique identifier
    label: str              # display name (Chinese)
    category: str           # grouping category
    attr_path: str          # dot-separated path on the result object
    source: str             # which analysis module: 'candle', 'volume', 'close'
    position: str           # 'above' (high+offset), 'below' (low-offset), 'on' (close, no offset)
    symbol: str             # plotly marker symbol
    color: str              # marker color
    size: int = 10          # marker size


# All available signal definitions
SIGNAL_DEFS: list[SignalDef] = [
    # -- Candle signals --
    SignalDef("jump", "跳空上漲", "candle", "jump", "candle", "below", "triangle-up", "#ff9800", 11),
    SignalDef("squat", "跳空下跌", "candle", "squat", "candle", "above", "triangle-down", "#9c27b0", 11),
    SignalDef("short_hl", "短峰", "candle", "hl.short_hl", "candle", "above", "diamond", "#ffeb3b", 8),
    SignalDef("medium_hl", "中峰", "candle", "hl.medium_hl", "candle", "above", "diamond", "#ff9800", 10),
    SignalDef("long_hl", "長峰", "candle", "hl.long_hl", "candle", "above", "diamond", "#f44336", 12),
    SignalDef("red_long", "紅長棒", "candle", "stick_length.red_long", "candle", "below", "arrow-up", "#ef5350", 12),
    SignalDef("black_long", "黑長棒", "candle", "stick_length.black_long", "candle", "above", "arrow-down", "#26a69a", 12),
    SignalDef("upper_shadow", "上影線", "candle", "shadow.upper", "candle", "above", "arrow-bar-down", "#ba68c8", 11),
    SignalDef("lower_shadow", "下影線", "candle", "shadow.lower", "candle", "below", "arrow-bar-up", "#4fc3f7", 11),

    # -- Candle trigger/creep --
    SignalDef("trigger_high1", "觸高1", "trigger", "trigger_high1", "candle", "above", "star-triangle-up", "#ff5722", 9),
    SignalDef("trigger_low1", "觸低1", "trigger", "trigger_low1", "candle", "below", "star-triangle-down", "#00bcd4", 9),
    SignalDef("trigger_high2", "觸高2", "trigger", "trigger_high2", "candle", "above", "star-triangle-up", "#e64a19", 10),
    SignalDef("trigger_low2", "觸低2", "trigger", "trigger_low2", "candle", "below", "star-triangle-down", "#0097a7", 10),
    SignalDef("trigger_high3", "觸高3", "trigger", "trigger_high3", "candle", "above", "star-triangle-up", "#bf360c", 11),
    SignalDef("trigger_low3", "觸低3", "trigger", "trigger_low3", "candle", "below", "star-triangle-down", "#006064", 11),
    SignalDef("creep_high1", "爬高1", "creep", "creep_high1", "candle", "above", "circle", "#ffab91", 7),
    SignalDef("creep_low1", "爬低1", "creep", "creep_low1", "candle", "below", "circle", "#80deea", 7),

    # -- Volume signals --
    SignalDef("vol_burst", "量爆", "volume", "burst", "volume", "below", "triangle-up", "#ffc107", 12),
    SignalDef("vol_sleep", "量窒息", "volume", "sleep", "volume", "above", "x", "#607d8b", 10),
    SignalDef("vol_flood", "量洪", "volume", "flood", "volume", "below", "hexagram", "#e91e63", 13),
    SignalDef("dead_fish", "死魚", "volume", "dead_fish", "money", "above", "x-thin", "#b0bec5", 10),

    # -- Bollinger band breakouts (period 21) --
    SignalDef("boll_break_u1", "破布林上1", "bollinger", "boll.21.close_gt_u1", "close", "above", "triangle-up-open", "#ff8a65", 9),
    SignalDef("boll_break_u2", "破布林上2", "bollinger", "boll.21.close_gt_u2", "close", "above", "triangle-up", "#ff5722", 11),
    SignalDef("boll_break_d1", "破布林下1", "bollinger", "boll.21.close_lt_d1", "close", "below", "triangle-down-open", "#4dd0e1", 9),
    SignalDef("boll_break_d2", "破布林下2", "bollinger", "boll.21.close_lt_d2", "close", "below", "triangle-down", "#0097a7", 11),

    # -- Knot (均線糾結) breakouts (medium timeframe) --
    SignalDef("knot_break_up", "糾結突破上", "knot", "knot.medium.break_up", "close", "below", "star", "#ffd54f", 13),
    SignalDef("knot_break_down", "糾結突破下", "knot", "knot.medium.break_down", "close", "above", "star", "#7e57c2", 13),

    # -- OBV buy/sell (markers placed on K-bar at close price) --
    SignalDef("obv_buy", "OBV買進", "obv", "signal_up", "obv", "on", "arrow-up", "#ff1744", 13),
    SignalDef("obv_sell", "OBV賣出", "obv", "signal_down", "obv", "on", "arrow-down", "#00e676", 13),
]


def get_signal_categories() -> dict[str, list[SignalDef]]:
    """Group signal definitions by category for UI display."""
    cats: dict[str, list[SignalDef]] = {}
    for sd in SIGNAL_DEFS:
        cats.setdefault(sd.category, []).append(sd)
    return cats


def _resolve_attr(obj: object, path: str) -> object:
    """Resolve a dot-separated path. Numeric/string segments fall back to
    dict-key lookup when attribute access fails, so paths like
    'boll.21.close_gt_u1' or 'knot.medium.break_up' work."""
    for part in path.split("."):
        try:
            obj = getattr(obj, part)
        except AttributeError:
            key = int(part) if part.lstrip("-").isdigit() else part
            obj = obj[key]
    return obj


def generate_markers(
    dates: list[str],
    high: np.ndarray,
    low: np.ndarray,
    analysis_results: dict[str, object],
    enabled_signals: list[str],
    offset_pct: float = 0.015,
    close: Optional[np.ndarray] = None,
) -> list[SignalMarker]:
    """
    Generate signal markers from analysis results.

    Parameters
    ----------
    dates : list of date strings
    high, low : price arrays for marker positioning
    analysis_results : dict mapping source name to analysis result object
        e.g. {'candle': CandleResult, 'volume': VolumeResult, 'close': CloseResult}
    enabled_signals : list of signal keys to display
    offset_pct : percentage offset from high/low for marker placement

    Returns
    -------
    List of SignalMarker objects ready for chart overlay.
    """
    markers: list[SignalMarker] = []
    price_range = np.max(high) - np.min(low)
    offset = price_range * offset_pct

    enabled_set = set(enabled_signals)
    defs_by_key = {sd.key: sd for sd in SIGNAL_DEFS}

    for key in enabled_signals:
        sd = defs_by_key.get(key)
        if sd is None:
            continue

        result_obj = analysis_results.get(sd.source)
        if result_obj is None:
            continue

        try:
            flags = _resolve_attr(result_obj, sd.attr_path)
        except AttributeError:
            continue

        if not isinstance(flags, np.ndarray):
            continue

        indices = np.where(flags)[0]
        for idx in indices:
            if sd.position == "above":
                price = float(high[idx]) + offset
            elif sd.position == "on" and close is not None:
                price = float(close[idx])
            else:
                price = float(low[idx]) - offset

            markers.append(SignalMarker(
                date=dates[idx],
                price=price,
                symbol=sd.symbol,
                color=sd.color,
                label=sd.label,
                size=sd.size,
            ))

    return markers
