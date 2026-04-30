"""
Interactive Plotly chart for backtest results.

Shows candlesticks with:
  - Defense lines only during active positions (long defense / short defense)
  - Batched entry/exit markers with trade info on hover
  - Volume subplot
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

if TYPE_CHECKING:
    from backtest.data import StockData
    from backtest.trade import BacktestResult


def plot_backtest_interactive(
    data: "StockData",
    result: "BacktestResult",
    save_path: str | None = None,
) -> go.Figure:
    """
    Build an interactive backtest chart.

    Defense lines are segmented: only visible during active positions.
    Entry/exit points are batched into 4 traces (long entry, short entry,
    win exit, loss exit).
    """
    dates = [d.strftime("%Y-%m-%d") for d in data.dates]

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
            open=data.open,
            high=data.high,
            low=data.low,
            close=data.close,
            increasing_line_color="#ef5350",
            decreasing_line_color="#26a69a",
            increasing_fillcolor="#ef5350",
            decreasing_fillcolor="#26a69a",
            name="K Line",
        ),
        row=1, col=1,
    )

    # -- Defense lines segmented by position --
    long_defense = np.where(result.position_side == 1, result.defense_price, np.nan)
    fig.add_trace(
        go.Scatter(
            x=dates, y=long_defense,
            mode="lines",
            name="多方防守",
            line=dict(width=2, color="#26a69a", dash="dash"),
            hovertemplate="多方防守: %{y:.2f}<extra></extra>",
            connectgaps=False,
        ),
        row=1, col=1,
    )

    short_defense = np.where(result.position_side == -1, result.defense_price, np.nan)
    fig.add_trace(
        go.Scatter(
            x=dates, y=short_defense,
            mode="lines",
            name="空方防守",
            line=dict(width=2, color="#ef5350", dash="dash"),
            hovertemplate="空方防守: %{y:.2f}<extra></extra>",
            connectgaps=False,
        ),
        row=1, col=1,
    )

    # -- Batched entry/exit markers --
    # Group into 4 categories
    groups = {
        "long_entry": {"x": [], "y": [], "text": [], "symbol": "triangle-up",
                       "color": "#ef5350", "size": 11, "name": "做多進場"},
        "short_entry": {"x": [], "y": [], "text": [], "symbol": "triangle-down",
                        "color": "#26a69a", "size": 11, "name": "做空進場"},
        "win_exit": {"x": [], "y": [], "text": [], "symbol": "x",
                     "color": "#ffd54f", "size": 10, "name": "獲利出場"},
        "loss_exit": {"x": [], "y": [], "text": [], "symbol": "x",
                      "color": "#9e9e9e", "size": 10, "name": "虧損出場"},
    }

    for t in result.trades:
        is_long = t.direction == "long"
        pnl_sign = "+" if t.pnl_pct > 0 else ""
        side_label = "做多" if is_long else "做空"

        # Entry
        key = "long_entry" if is_long else "short_entry"
        groups[key]["x"].append(t.entry_date.strftime("%Y-%m-%d"))
        groups[key]["y"].append(t.entry_price)
        groups[key]["text"].append(
            f"{side_label}進場<br>原因: {', '.join(t.entry_reasons)}"
        )

        # Exit
        key = "win_exit" if t.pnl > 0 else "loss_exit"
        groups[key]["x"].append(t.exit_date.strftime("%Y-%m-%d"))
        groups[key]["y"].append(t.exit_price)
        groups[key]["text"].append(
            f"{side_label}出場<br>"
            f"原因: {', '.join(t.exit_reasons)}<br>"
            f"損益: {pnl_sign}{t.pnl_pct:.1%}"
        )

    for g in groups.values():
        fig.add_trace(
            go.Scatter(
                x=g["x"], y=g["y"],
                mode="markers",
                name=g["name"],
                marker=dict(
                    symbol=g["symbol"],
                    size=g["size"],
                    color=g["color"],
                    line=dict(width=1, color="white"),
                ),
                customdata=g["text"],
                hovertemplate="%{customdata}<br>%{x}<br>價格: %{y:.2f}<extra></extra>",
            ),
            row=1, col=1,
        )

    # -- Volume bars --
    bar_colors = np.where(
        data.close >= data.open,
        "rgba(239, 83, 80, 0.7)",
        "rgba(38, 166, 154, 0.7)",
    )
    fig.add_trace(
        go.Bar(
            x=dates, y=data.volume,
            marker_color=bar_colors.tolist(),
            name="Volume",
            showlegend=False,
            hovertemplate="成交量: %{y:,.0f}<extra></extra>",
        ),
        row=2, col=1,
    )

    # -- Layout --
    title = (
        f"{data.stock_id} {data.stock_name} — {result.strategy_name} "
        f"| 交易 {result.total_trades} 筆 "
        f"| 勝率 {result.win_rate:.1%} "
        f"| PF {result.profit_factor:.2f} "
        f"| 報酬 {result.total_return:.1%}"
    )
    fig.update_layout(
        title=dict(text=title, x=0.5, font=dict(size=14)),
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        height=700,
        autosize=True,
        margin=dict(l=80, r=40, t=60, b=30),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="left", x=0,
            font=dict(size=11, color="#eee"),
        ),
        dragmode="pan",
        hovermode="x unified",
    )

    for row in [1, 2]:
        fig.update_xaxes(
            type="category",
            categoryorder="array",
            categoryarray=dates,
            row=row, col=1,
        )

    fig.update_yaxes(title_text="Price", tickformat=",", row=1, col=1)
    fig.update_yaxes(title_text="Volume", tickformat=",", row=2, col=1)

    if save_path:
        fig.write_html(save_path)

    return fig
