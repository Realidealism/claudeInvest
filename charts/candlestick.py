"""
Interactive candlestick chart builder using Plotly.

Builds a two-panel figure (price + volume) with drawing tools enabled,
and accepts signal markers to overlay on the chart.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from charts.signals import SignalDef

import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots


@dataclass
class SignalMarker:
    """A single signal marker to display on the chart."""
    date: str               # trade date string
    price: float            # y-position (typically high or low)
    symbol: str             # plotly marker symbol: 'triangle-up', 'triangle-down', etc.
    color: str              # marker color
    label: str              # signal name for legend/hover
    size: int = 12          # marker size


def build_candlestick_figure(
    dates: list[str],
    open_: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    sub_value: np.ndarray,
    is_index: bool = False,
    signals: Optional[list[SignalMarker]] = None,
    all_signal_defs: Optional[list] = None,
    enabled_signals: Optional[set[str]] = None,
    sma_lines: Optional[dict[str, np.ndarray]] = None,
    enabled_ma: Optional[set[str]] = None,
    wave_lines: Optional[list[dict]] = None,
    wave_visible: Optional[set[str]] = None,
    title: str = "",
) -> go.Figure:
    """
    Build an interactive candlestick chart with volume/turnover subplot.

    Parameters
    ----------
    dates : list of date strings (oldest first)
    open_, high, low, close : price arrays
    sub_value : volume (shares) for stocks, turnover (NTD) for indices
    is_index : if True, sub_value is turnover displayed in 億元
    signals : optional list of SignalMarker to overlay
    sma_lines : optional dict of {label: values} for MA overlay lines
    wave_lines : optional list of wave line dicts with keys:
        dates, prices, name, color
    title : chart title

    Returns
    -------
    Plotly Figure with drawing tools enabled.
    """
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.75, 0.25],
    )

    # -- Candlestick --
    fig.add_trace(
        go.Candlestick(
            x=dates,
            open=open_,
            high=high,
            low=low,
            close=close,
            increasing_line_color="#ef5350",   # red for up (TW convention)
            decreasing_line_color="#26a69a",   # green for down
            increasing_fillcolor="#ef5350",
            decreasing_fillcolor="#26a69a",
            name="K Line",
        ),
        row=1, col=1,
    )

    # -- MA lines (always add all 10 slots for stable trace count) --
    _ma_periods = ["3", "5", "8", "13", "21", "34", "55", "89", "144", "233"]
    _ma_colors = [
        "#ff9800", "#2196f3", "#9c27b0", "#4caf50",
        "#f44336", "#00bcd4", "#795548", "#607d8b", "#e91e63", "#cddc39",
    ]
    _enabled_ma = enabled_ma or set()
    if not sma_lines:
        sma_lines = {}
    for i, p in enumerate(_ma_periods):
        label = f"MA{p}"
        values = sma_lines.get(label)
        has_data = values is not None
        vis = has_data and p in _enabled_ma
        fig.add_trace(
            go.Scatter(
                x=dates if has_data else [],
                y=values if has_data else [],
                mode="lines",
                name=label,
                visible=vis,
                showlegend=vis,
                line=dict(width=1, color=_ma_colors[i]),
                hovertemplate=f"{label}: %{{y:.2f}}<extra></extra>",
            ),
            row=1, col=1,
        )

    # -- Sub chart bars (volume or turnover) --
    bar_colors = np.where(
        close >= open_,
        "rgba(239, 83, 80, 0.7)",    # red
        "rgba(38, 166, 154, 0.7)",   # green
    )
    if is_index:
        bar_y = sub_value / 1e8  # convert NTD -> 億元
        bar_name = "Turnover"
        bar_hover = "成交金額: %{y:,.1f} 億<extra></extra>"
    else:
        bar_y = sub_value
        bar_name = "Volume"
        bar_hover = "成交量: %{y:,.0f}<extra></extra>"
    fig.add_trace(
        go.Bar(
            x=dates,
            y=bar_y,
            marker_color=bar_colors.tolist(),
            name=bar_name,
            showlegend=False,
            hovertemplate=bar_hover,
        ),
        row=2, col=1,
    )

    # -- Signal markers (always add ALL defined signals for stable trace count) --
    _enabled_sigs = enabled_signals or set()
    signals_by_label: dict[str, list[SignalMarker]] = {}
    if signals:
        for s in signals:
            signals_by_label.setdefault(s.label, []).append(s)
    if all_signal_defs:
        for sd in all_signal_defs:
            markers_for_key = signals_by_label.get(sd.label, [])
            has_data = len(markers_for_key) > 0
            vis = has_data and sd.key in _enabled_sigs
            fig.add_trace(
                go.Scatter(
                    x=[m.date for m in markers_for_key] if has_data else [],
                    y=[m.price for m in markers_for_key] if has_data else [],
                    mode="markers",
                    name=f"sig_{sd.key}",
                    visible=vis,
                    showlegend=False,
                    marker=dict(
                        symbol=sd.symbol,
                        size=sd.size,
                        color=sd.color,
                        line=dict(width=1, color="white"),
                    ),
                    hovertemplate=f"{sd.label}<br>%{{x}}<br>%{{y:.2f}}<extra></extra>",
                ),
                row=1, col=1,
            )

    # -- Wave lines (always add 4 slots for stable trace count) --
    _wave_slots = [
        ("Wave", "wave", "#ffd54f", True),
        ("浪瀑(多)", "wf", "#ef5350", False),
        ("浪瀑(空)", "wf", "#26a69a", False),
        ("浪溝", "wf", "#9e9e9e", False),
    ]
    _wv = wave_visible or set()
    wave_data = {}
    if wave_lines:
        for wl in wave_lines:
            wave_data[wl["name"]] = wl
    for wname, wgroup, wcolor, is_line_type in _wave_slots:
        wl = wave_data.get(wname)
        has_data = wl is not None and len(wl["dates"]) > 0
        vis = has_data and wgroup in _wv
        fig.add_trace(
            go.Scatter(
                x=wl["dates"] if has_data else [],
                y=wl["prices"] if has_data else [],
                mode="lines+markers" if is_line_type else "markers",
                name=wname,
                visible=vis,
                showlegend=False,
                line=dict(width=1.5, color=wcolor, dash="dot") if is_line_type else None,
                marker=dict(
                    size=4 if is_line_type else 10,
                    color=wcolor,
                    symbol="circle" if is_line_type else "diamond",
                    line=dict(width=1, color=wcolor) if not is_line_type else None,
                ),
                hovertemplate="%{y:.2f}<extra>" + wname + "</extra>",
            ),
            row=1, col=1,
        )

    # -- Layout --
    fig.update_layout(
        title=dict(text=title, x=0.5),
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        height=700,
        autosize=True,
        margin=dict(l=80, r=40, t=80, b=30, autoexpand=False),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=10, color="#eee"),
            bgcolor="rgba(0,0,0,0)",
        ),
        dragmode="pan",
        hovermode="x unified",
    )

    # Enable drawing tools in the modebar
    fig.update_layout(
        newshape=dict(
            line_color="#ffd54f",
            line_width=1.5,
            opacity=0.8,
        ),
    )

    n = len(dates)
    fig.update_xaxes(
        type="category",
        categoryorder="array",
        categoryarray=dates,
        range=[-0.5, n - 0.5] if n > 0 else None,
        autorange=False if n > 0 else None,
        row=1, col=1,
    )
    fig.update_xaxes(
        type="category",
        categoryorder="array",
        categoryarray=dates,
        range=[-0.5, n - 0.5] if n > 0 else None,
        autorange=False if n > 0 else None,
        row=2, col=1,
    )
    # Lock y-axis ranges to prevent autorange recalculation when
    # signal markers or wave traces change visibility
    if len(high) > 0:
        y_min, y_max = float(np.min(low)), float(np.max(high))
        pad = (y_max - y_min) * 0.06
        fig.update_yaxes(title_text="Price", tickformat=",", automargin=False,
                         range=[y_min - pad, y_max + pad], autorange=False,
                         row=1, col=1)
    else:
        fig.update_yaxes(title_text="Price", tickformat=",", automargin=False, row=1, col=1)

    if len(sub_value) > 0:
        sub_max = float(np.max(bar_y))
        fig.update_yaxes(title_text="成交金額 (億)" if is_index else "Volume",
                         tickformat=",", automargin=False,
                         range=[0, sub_max * 1.1], autorange=False,
                         row=2, col=1)
    else:
        fig.update_yaxes(title_text="成交金額 (億)" if is_index else "Volume",
                         tickformat=",", automargin=False, row=2, col=1)

    return fig



# Drawing tool config for Dash / standalone usage
DRAWING_BUTTONS = [
    "drawline",
    "drawrect",
    "drawcircle",
    "eraseshape",
]
