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
    position: str           # 'above' (use high) or 'below' (use low)
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
]


def get_signal_categories() -> dict[str, list[SignalDef]]:
    """Group signal definitions by category for UI display."""
    cats: dict[str, list[SignalDef]] = {}
    for sd in SIGNAL_DEFS:
        cats.setdefault(sd.category, []).append(sd)
    return cats


def _resolve_attr(obj: object, path: str) -> object:
    """Resolve a dot-separated attribute path on an object."""
    for part in path.split("."):
        obj = getattr(obj, part)
    return obj


def generate_markers(
    dates: list[str],
    high: np.ndarray,
    low: np.ndarray,
    analysis_results: dict[str, object],
    enabled_signals: list[str],
    offset_pct: float = 0.015,
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
