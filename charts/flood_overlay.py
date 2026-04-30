"""
Lightweight flood-reference backtest overlay for the Dash chart.

Computes position state, segmented defense lines, and entry/exit markers
directly from VolumeResult — no dependency on backtest.engine.

Supports tier 1/2/3:
  - Entry condition uses tier k (above_tier[k] / below_tier[k])
  - Exit condition and defense both use tier 1 (consistent with strategy)
"""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray

F32 = np.float32


def compute_flood_overlay_tier(
    close: NDArray[np.float32],
    above_entry: NDArray[np.bool_],       # tier k entry signal
    below_entry: NDArray[np.bool_],       # tier k entry signal
    flood_above_t1: NDArray[np.bool_],    # tier 1 (for exit)
    flood_below_t1: NDArray[np.bool_],    # tier 1 (for exit)
    flood_high_t1: NDArray[np.float32],   # tier 1 (for defense)
    flood_low_t1: NDArray[np.float32],    # tier 1 (for defense)
    dates: list[str],
    tier: int,
    skip: int = 55,
) -> dict:
    """
    Simulate flood-reference strategy at a given tier.
    Entry uses tier k, exit and defense use tier 1.
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

    side = 0
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
            elif flood_below_t1[i]:
                reason = "跌破洪量低"
                exited = True
            if exited:
                pnl_pct = (price - entry_price) / entry_price
                _add_exit(markers, dates[i], price, "做多", reason, pnl_pct, tier)
                side = 0
        elif side == -1:
            exited = False
            reason = ""
            if not np.isnan(defense) and price > defense:
                reason = f"停利防守 ({defense:.0f})"
                exited = True
            elif flood_above_t1[i]:
                reason = "站上洪量高"
                exited = True
            if exited:
                pnl_pct = (entry_price - price) / entry_price
                _add_exit(markers, dates[i], price, "做空", reason, pnl_pct, tier)
                side = 0

        # Update defense (always tier 1)
        if side == 1:
            new_def = float(flood_low_t1[i])
            if new_def > 0:
                defense = max(defense, new_def) if not np.isnan(defense) else new_def
            long_def[i] = defense
        elif side == -1:
            new_def = float(flood_high_t1[i])
            if new_def > 0:
                defense = min(defense, new_def) if not np.isnan(defense) else new_def
            short_def[i] = defense

        # Entry check (if flat)
        if side == 0:
            if above_entry[i]:
                side = 1
                entry_price = price
                defense = float(flood_low_t1[i]) if flood_low_t1[i] > 0 else float("nan")
                long_def[i] = defense
                markers["long_entry"]["x"].append(dates[i])
                markers["long_entry"]["y"].append(price)
                markers["long_entry"]["text"].append(f"做多進場<br>站上{tier}階洪")
            elif below_entry[i]:
                side = -1
                entry_price = price
                defense = float(flood_high_t1[i]) if flood_high_t1[i] > 0 else float("nan")
                short_def[i] = defense
                markers["short_entry"]["x"].append(dates[i])
                markers["short_entry"]["y"].append(price)
                markers["short_entry"]["text"].append(f"做空進場<br>跌破{tier}階洪")

    return {
        "long_defense": long_def,
        "short_defense": short_def,
        "markers": markers,
    }


def compute_flood_overlay_all_tiers(
    close, vol_r, dates, skip: int = 55, tiers: tuple = (1, 2, 3),
    dedupe_to_highest: bool = True,
) -> dict[int, dict]:
    """
    Compute overlay for each tier. Returns {tier: overlay_dict}.

    If dedupe_to_highest is True (default), defense lines at overlapping days
    show only the highest active tier (tier 3 takes priority over tier 2 over
    tier 1). Markers are kept as-is.
    """
    result = {}
    for k in tiers:
        result[k] = compute_flood_overlay_tier(
            close=close,
            above_entry=vol_r.above_tier[k],
            below_entry=vol_r.below_tier[k],
            flood_above_t1=vol_r.above_tier[1],
            flood_below_t1=vol_r.below_tier[1],
            flood_high_t1=vol_r.flood_high_tier[1],
            flood_low_t1=vol_r.flood_low_tier[1],
            dates=dates,
            tier=k,
            skip=skip,
        )

    if dedupe_to_highest and len(tiers) > 1:
        sorted_tiers = sorted(tiers, reverse=True)   # [3, 2, 1]

        # Defense lines: keep highest active tier per day
        for side_key in ("long_defense", "short_defense"):
            n = len(result[sorted_tiers[0]][side_key])
            for i in range(n):
                highest = None
                for t in sorted_tiers:
                    if not np.isnan(result[t][side_key][i]):
                        highest = t
                        break
                if highest is not None:
                    for t in sorted_tiers:
                        if t != highest:
                            result[t][side_key][i] = np.nan

        # Markers: for each (date, marker_type), keep only highest tier's marker
        marker_keys = ("long_entry", "short_entry", "win_exit", "loss_exit")
        taken: dict[tuple, int] = {}  # (date, mkey) -> highest tier seen
        for t in sorted_tiers:
            for mkey in marker_keys:
                mg = result[t]["markers"][mkey]
                keep_idx = []
                for idx, d in enumerate(mg["x"]):
                    key = (d, mkey)
                    if key not in taken:
                        taken[key] = t
                        keep_idx.append(idx)
                # Rebuild group with kept indices
                mg["x"] = [mg["x"][i] for i in keep_idx]
                mg["y"] = [mg["y"][i] for i in keep_idx]
                mg["text"] = [mg["text"][i] for i in keep_idx]

    return result


def _add_exit(markers, date, price, side_label, reason, pnl_pct, tier):
    pnl_sign = "+" if pnl_pct > 0 else ""
    key = "win_exit" if pnl_pct > 0 else "loss_exit"
    markers[key]["x"].append(date)
    markers[key]["y"].append(price)
    markers[key]["text"].append(
        f"{side_label}出場 ({tier}階)<br>原因: {reason}<br>損益: {pnl_sign}{pnl_pct:.1%}"
    )


# Legacy single-tier API (tier 1 equivalent)
def compute_flood_overlay(
    close, flood_above, flood_below, flood_high, flood_low, dates, skip: int = 55,
) -> dict:
    return compute_flood_overlay_tier(
        close, flood_above, flood_below,
        flood_above, flood_below, flood_high, flood_low,
        dates, tier=1, skip=skip,
    )
