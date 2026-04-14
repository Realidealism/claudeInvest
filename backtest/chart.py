"""
Matplotlib chart output for backtest results.

Two-panel layout:
  - Upper: price + entry/exit markers + defense price + optional SMA
  - Lower: equity curve + drawdown shading
"""

from __future__ import annotations

import numpy as np
import matplotlib
matplotlib.use("Agg")  # non-interactive backend by default

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.gridspec import GridSpec

from backtest.trade import BacktestResult
from backtest.data import StockData


# Chinese font fallback for labels
plt.rcParams["font.sans-serif"] = ["Microsoft JhengHei", "SimHei", "Arial"]
plt.rcParams["axes.unicode_minus"] = False


def plot_backtest(
    data: StockData,
    result: BacktestResult,
    save_path: str | None = None,
    show_sma: int | None = 21,
    show_interactive: bool = False,
) -> None:
    """
    Generate a two-panel backtest chart.

    Parameters
    ----------
    data : StockData
    result : BacktestResult
    save_path : path to save PNG (if None and show_interactive, display window)
    show_sma : SMA period to overlay on price chart (None to skip)
    show_interactive : if True and no save_path, show interactive window
    """
    # Find backtest range indices
    si = 0
    for i, d in enumerate(data.dates):
        if d >= result.start_date:
            si = i
            break
    ei = data.n

    dates = data.dates[si:ei]
    close = data.close[si:ei]
    equity = result.equity[si:ei]
    defense = result.defense_price[si:ei]
    pos_side = result.position_side[si:ei]

    fig = plt.figure(figsize=(16, 10))
    gs = GridSpec(3, 1, height_ratios=[2, 1, 0.05], hspace=0.15)
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1], sharex=ax1)

    # ── Upper panel: Price ──────────────────────────────────────────────
    ax1.plot(dates, close, color="gray", linewidth=0.8, label="收盤價", zorder=1)

    # Optional SMA overlay
    if show_sma and show_sma in data.close_result.ma.sma:
        sma_line = data.close_result.ma.sma[show_sma][si:ei]
        ax1.plot(dates, sma_line, color="blue", linewidth=0.6,
                 alpha=0.5, label=f"SMA{show_sma}")

    # Defense price line (only where position is open)
    in_position = pos_side != 0
    defense_visible = np.where(in_position, defense, np.nan)
    ax1.plot(dates, defense_visible, color="orange", linewidth=1.2,
             linestyle="--", label="防守價", zorder=2)

    # Entry/exit markers
    for t in result.trades:
        # Adjust indices relative to display range
        e_idx = t.entry_index - si
        x_idx = t.exit_index - si
        if e_idx < 0 or x_idx < 0:
            continue

        if t.direction == "long":
            ax1.scatter(dates[e_idx], t.entry_price, marker="^", color="green",
                        s=80, zorder=5, label="做多進場" if t == result.trades[0] else "")
            ax1.scatter(dates[x_idx], t.exit_price, marker="x", color="red",
                        s=80, zorder=5, label="做多出場" if t == result.trades[0] else "")
        else:
            ax1.scatter(dates[e_idx], t.entry_price, marker="v", color="red",
                        s=80, zorder=5, label="做空進場" if t == result.trades[0] else "")
            ax1.scatter(dates[x_idx], t.exit_price, marker="x", color="green",
                        s=80, zorder=5, label="做空出場" if t == result.trades[0] else "")

    ax1.set_title(f"{data.stock_id} {data.stock_name} — {result.strategy_name}",
                  fontsize=14)
    ax1.set_ylabel("價格")
    ax1.legend(loc="upper left", fontsize=8)
    ax1.grid(True, alpha=0.3)

    # ── Lower panel: Equity curve ───────────────────────────────────────
    ax2.plot(dates, equity, color="steelblue", linewidth=1.0, label="權益曲線")

    # Drawdown shading
    peak = np.maximum.accumulate(equity)
    ax2.fill_between(dates, equity, peak, where=(equity < peak),
                     color="salmon", alpha=0.3, label="回撤")

    ax2.set_ylabel("權益")
    ax2.set_xlabel("日期")
    ax2.legend(loc="upper left", fontsize=8)
    ax2.grid(True, alpha=0.3)

    # Date formatting
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    plt.setp(ax1.get_xticklabels(), visible=False)
    fig.autofmt_xdate(rotation=45)

    plt.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"圖表已儲存: {save_path}")
    elif show_interactive:
        matplotlib.use("TkAgg")
        plt.show()

    plt.close(fig)
