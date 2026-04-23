"""
Lightweight flood-reference backtest overlay for the Dash chart.

Computes position state, segmented defense lines, and entry/exit markers
directly from VolumeResult — no dependency on backtest.engine.
"""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray

F32 = np.float32


def compute_flood_overlay(
    close: NDArray[np.float32],
    flood_above: NDArray[np.bool_],
    flood_below: NDArray[np.bool_],
    flood_high: NDArray[np.float32],
    flood_low: NDArray[np.float32],
    dates: list[str],
    skip: int = 55,
) -> dict:
    """
    Simulate flood-reference strategy and return overlay data.

    Returns dict with keys:
        long_defense  — float array (NaN when not long)
        short_defense — float array (NaN when not short)
        markers       — dict of 4 groups, each with x/y/text lists
    """
    n = len(close)
    long_def = np.full(n, np.nan, dtype=F32)
    short_def = np.full(n, np.nan, dtype=F32)

    markers = {
        "long_entry": {"x": [], "y": [], "text": []},
        "short_entry": {"x": [], "y": [], "text": []},
        "win_exit": {"x": [], "y": [], "text": []},
        "loss_exit": {"x": [], "y": [], "text": []},
    }

    # State machine
    side = 0        # 0=flat, 1=long, -1=short
    entry_price = 0.0
    defense = float("nan")

    for i in range(skip, n):
        price = float(close[i])

        # Exit check
        if side == 1:
            exited = False
            reason = ""
            if not np.isnan(defense) and price < defense:
                reason = f"停利防守 ({defense:.0f})"
                exited = True
            elif flood_below[i]:
                reason = "跌破洪量低"
                exited = True
            if exited:
                pnl_pct = (price - entry_price) / entry_price
                _add_exit(markers, dates[i], price, "做多", reason, pnl_pct)
                side = 0

        elif side == -1:
            exited = False
            reason = ""
            if not np.isnan(defense) and price > defense:
                reason = f"停利防守 ({defense:.0f})"
                exited = True
            elif flood_above[i]:
                reason = "站上洪量高"
                exited = True
            if exited:
                pnl_pct = (entry_price - price) / entry_price
                _add_exit(markers, dates[i], price, "做空", reason, pnl_pct)
                side = 0

        # Update defense if still in position
        if side == 1:
            new_def = float(flood_low[i])
            if new_def > 0:
                defense = max(defense, new_def) if not np.isnan(defense) else new_def
            long_def[i] = defense
        elif side == -1:
            new_def = float(flood_high[i])
            if new_def > 0:
                defense = min(defense, new_def) if not np.isnan(defense) else new_def
            short_def[i] = defense

        # Entry check (if flat after exit)
        if side == 0:
            if flood_above[i]:
                side = 1
                entry_price = price
                defense = float(flood_low[i]) if flood_low[i] > 0 else float("nan")
                long_def[i] = defense
                markers["long_entry"]["x"].append(dates[i])
                markers["long_entry"]["y"].append(price)
                markers["long_entry"]["text"].append("做多進場<br>站上洪量高")
            elif flood_below[i]:
                side = -1
                entry_price = price
                defense = float(flood_high[i]) if flood_high[i] > 0 else float("nan")
                short_def[i] = defense
                markers["short_entry"]["x"].append(dates[i])
                markers["short_entry"]["y"].append(price)
                markers["short_entry"]["text"].append("做空進場<br>跌破洪量低")

    return {
        "long_defense": long_def,
        "short_defense": short_def,
        "markers": markers,
    }


def _add_exit(markers, date, price, side_label, reason, pnl_pct):
    pnl_sign = "+" if pnl_pct > 0 else ""
    key = "win_exit" if pnl_pct > 0 else "loss_exit"
    markers[key]["x"].append(date)
    markers[key]["y"].append(price)
    markers[key]["text"].append(
        f"{side_label}出場<br>原因: {reason}<br>損益: {pnl_sign}{pnl_pct:.1%}"
    )
