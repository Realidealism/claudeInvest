"""
Interactive candlestick chart builder using Plotly.

Builds a two-panel figure (price + volume) with drawing tools enabled,
and accepts signal markers to overlay on the chart.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

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
    sma_lines: Optional[dict[str, np.ndarray]] = None,
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

    # -- MA lines --
    if sma_lines:
        ma_colors = [
            "#ff9800", "#2196f3", "#9c27b0", "#4caf50",
            "#f44336", "#00bcd4", "#795548", "#607d8b",
        ]
        for i, (label, values) in enumerate(sma_lines.items()):
            color = ma_colors[i % len(ma_colors)]
            fig.add_trace(
                go.Scatter(
                    x=dates,
                    y=values,
                    mode="lines",
                    name=label,
                    line=dict(width=1, color=color),
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

    # -- Signal markers --
    if signals:
        _add_signal_markers(fig, signals)

    # -- Wave lines (visibility controlled by wave_visible set) --
    _wave_group = {"Wave": "wave", "浪瀑(多)": "wf", "浪瀑(空)": "wf", "浪溝": "wf"}
    _wv = wave_visible or set()
    if wave_lines:
        for wl in wave_lines:
            is_line = len(wl["dates"]) > 1 and wl["name"] == "Wave"
            group = _wave_group.get(wl["name"], "")
            vis = group in _wv
            fig.add_trace(
                go.Scatter(
                    x=wl["dates"],
                    y=wl["prices"],
                    mode="lines+markers" if is_line else "markers",
                    name=wl["name"],
                    visible=vis,
                    showlegend=False,
                    line=dict(width=1.5, color=wl["color"], dash="dot") if is_line else None,
                    marker=dict(
                        size=4 if is_line else 10,
                        color=wl["color"],
                        symbol="circle" if is_line else "diamond",
                        line=dict(width=1, color="white") if not is_line else None,
                    ),
                    hovertemplate="%{y:.2f}<extra>" + wl["name"] + "</extra>",
                ),
                row=1, col=1,
            )

    # -- Layout --
    fig.update_layout(
        title=dict(text=title, x=0.5),
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        uirevision="keep",   # preserve zoom/pan across figure rebuilds
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

    fig.update_xaxes(
        type="category",
        categoryorder="array",
        categoryarray=dates,
        row=1, col=1,
    )
    fig.update_xaxes(
        type="category",
        categoryorder="array",
        categoryarray=dates,
        row=2, col=1,
    )
    fig.update_yaxes(title_text="Price", tickformat=",", automargin=False, row=1, col=1)
    fig.update_yaxes(title_text="成交金額 (億)" if is_index else "Volume",
                     tickformat=",", automargin=False, row=2, col=1)

    return fig


def _add_signal_markers(fig: go.Figure, signals: list[SignalMarker]) -> None:
    """Group signals by label and add as scatter traces."""
    grouped: dict[str, list[SignalMarker]] = {}
    for s in signals:
        grouped.setdefault(s.label, []).append(s)

    for label, markers in grouped.items():
        fig.add_trace(
            go.Scatter(
                x=[m.date for m in markers],
                y=[m.price for m in markers],
                mode="markers",
                name=label,
                marker=dict(
                    symbol=markers[0].symbol,
                    size=markers[0].size,
                    color=markers[0].color,
                    line=dict(width=1, color="white"),
                ),
                hovertemplate=f"{label}<br>%{{x}}<br>%{{y:.2f}}<extra></extra>",
            ),
            row=1, col=1,
        )


# Drawing tool config for Dash / standalone usage
DRAWING_BUTTONS = [
    "drawline",
    "drawrect",
    "drawcircle",
    "eraseshape",
]
