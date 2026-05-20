"""
Single-side backtest engine.

For one (stock, side) combination, walks day by day:
  1. If holding: defense-price check first, then signal-exit check.
     Defense price only moves in the favorable direction (long up, short down).
  2. If flat: signal-entry check.
  3. At end of data, force-close any open position.

Defense price is purely rule-driven: each DefenseRule has a trigger
(bool array) and a source (price array). On trigger days, the source
value is considered for an update — committed only if favorable.
Initial defense at entry is NaN (no defense until first rule fires).

No capital simulation — only entry/exit prices and pct return.
Long and short are run independently; positions never flip.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
from numpy.typing import NDArray

from analysis.indicators import rolling_highest, rolling_lowest
from signal_backtest.trade import (
    DefenseEvent,
    Trade,
    SideResult,
    SIDE_LONG,
    SIDE_SHORT,
    REASON_ENTRY_INIT,
    REASON_FLOOR,
    REASON_EXIT_SIGNAL,
    REASON_EXIT_DEFENSE,
    REASON_EXIT_END,
    REASON_EXIT_GAP,
)


# Single-bar close-to-close jump above this threshold is treated as a
# corporate-action artifact (TW daily limit is +/-10%, so >15% in one
# bar is impossible without a reverse split / capital reduction / merger
# adjustment that the raw daily_prices table doesn't apply).
GAP_JUMP_THRESHOLD = 0.15
from signal_backtest.signal import DefenseRule, TierConfig


def _compute_last_boundary(close: NDArray[np.float32]) -> NDArray[np.int32]:
    """For each bar i, the most recent index j<=i where close[j] gapped
    > GAP_JUMP_THRESHOLD vs close[j-1]. -1 if no boundary yet seen.

    Used by rolling_lowest/highest_safe to truncate lookback so windows
    never straddle a corporate-action / regime-shift bar.
    """
    n = len(close)
    last = np.full(n, -1, dtype=np.int32)
    if n < 2:
        return last
    prev = close[:-1]
    safe_prev = np.where(prev > 0, prev, 1.0)
    bar_gap = np.abs(close[1:] / safe_prev - 1.0)
    boundary = bar_gap > GAP_JUMP_THRESHOLD
    cur = -1
    for i in range(1, n):
        if boundary[i - 1]:
            cur = i
        last[i] = cur
    return last


def rolling_lowest_safe(
    arr: NDArray[np.float32],
    period: int,
    last_boundary: NDArray[np.int32],
) -> NDArray[np.float32]:
    """rolling_lowest but lookback start clamped at most-recent boundary.

    Equivalent to rolling_lowest(arr, period) when no boundary exists in
    the window. When a boundary at index b is within [i-period+1, i],
    the window starts at b (the new regime's first bar) instead.
    """
    n = len(arr)
    out = np.full(n, np.nan, dtype=arr.dtype)
    for i in range(n):
        start = max(0, i - period + 1)
        b = int(last_boundary[i])
        if b > start:
            start = b
        out[i] = arr[start:i + 1].min()
    return out


def rolling_highest_safe(
    arr: NDArray[np.float32],
    period: int,
    last_boundary: NDArray[np.int32],
) -> NDArray[np.float32]:
    """Mirror of rolling_lowest_safe for short-side defense."""
    n = len(arr)
    out = np.full(n, np.nan, dtype=arr.dtype)
    for i in range(n):
        start = max(0, i - period + 1)
        b = int(last_boundary[i])
        if b > start:
            start = b
        out[i] = arr[start:i + 1].max()
    return out

if TYPE_CHECKING:
    from backtest.data import StockData

BoolArray = NDArray[np.bool_]
F32Array = NDArray[np.float32]


# Default skip — same as backtest/engine.py: longest reliable warmup
DEFAULT_START_INDEX = 55


class InsufficientDataError(ValueError):
    """Raised when stock data is too short to backtest."""


def run_side_backtest(
    data: "StockData",
    side: str,                                  # "long" or "short"
    entry: BoolArray,
    exit_: BoolArray,
    defense_rules: list[DefenseRule] | None = None,
    start_index: int = DEFAULT_START_INDEX,
    floor_period: int = 13,
    initial_period: int = 5,
) -> SideResult:
    """Run one side of a signal backtest on a single stock."""
    if side not in ("long", "short"):
        raise ValueError(f"side must be 'long' or 'short', got {side!r}")

    n = data.n
    if n < 13:
        raise InsufficientDataError(
            f"{data.stock_id} {data.stock_name} 資料僅 {n} 天，無法回測"
        )

    si = min(start_index, n - 1)
    side_label = SIDE_LONG if side == "long" else SIDE_SHORT
    is_long = side == "long"

    rules = defense_rules or []

    # Pre-computed rolling extremes on intraday H/L:
    #   initial_arr — entry-day default defense (5-bar)
    #   floor_arr   — daily floor ratchet (floor_period bars, default 13)
    # Use _safe variants so the lookback window never straddles a
    # corporate-action / regime-shift bar (>15% close-to-close gap).
    last_boundary = _compute_last_boundary(data.close)
    if is_long:
        initial_arr = rolling_lowest_safe(data.low, initial_period, last_boundary)
        floor_arr = rolling_lowest_safe(data.low, floor_period, last_boundary)
    else:
        initial_arr = rolling_highest_safe(data.high, initial_period, last_boundary)
        floor_arr = rolling_highest_safe(data.high, floor_period, last_boundary)

    trades: list[Trade] = []

    # Open position state (None if flat)
    pos_entry_date = None
    pos_entry_price = 0.0
    pos_entry_index = 0
    pos_defense_price = float("nan")
    pos_defense_events: list[DefenseEvent] = []

    def _try_update_defense(
        i: int,
        candidate: float,
        reason: str,
        cur_def: float,
    ) -> tuple[float, DefenseEvent | None]:
        """Return (new_defense_price, event_or_none).

        Update only if candidate is favorable: higher for long, lower for short.
        Initial NaN defense accepts any non-NaN candidate.
        """
        if np.isnan(candidate):
            return cur_def, None
        if np.isnan(cur_def):
            ev = DefenseEvent(date=data.dates[i], price=candidate, reason=reason)
            return candidate, ev
        if is_long and candidate > cur_def:
            ev = DefenseEvent(date=data.dates[i], price=candidate, reason=reason)
            return candidate, ev
        if (not is_long) and candidate < cur_def:
            ev = DefenseEvent(date=data.dates[i], price=candidate, reason=reason)
            return candidate, ev
        return cur_def, None

    for i in range(si, n):
        price = float(data.close[i])

        # ── 0. Abnormal-gap detection (corporate-action artifact) ──────
        # If today's close jumped > 15% from yesterday's, the raw price is
        # almost certainly tainted by a reverse split / capital reduction
        # that daily_prices doesn't adjust for. Force-close any open
        # position at yesterday's close (the last reliable price) and skip
        # entry on this bar.
        gap_artifact = False
        if i > 0:
            prev_close = float(data.close[i - 1])
            if prev_close > 0:
                if abs(price / prev_close - 1.0) > GAP_JUMP_THRESHOLD:
                    gap_artifact = True

        if pos_entry_date is not None and gap_artifact:
            trades.append(_make_trade(
                data=data,
                side_label=side_label,
                entry_date=pos_entry_date,
                entry_price=pos_entry_price,
                entry_index=pos_entry_index,
                defense_events=pos_defense_events,
                exit_index=i - 1,
                exit_price=prev_close,
                exit_reason=REASON_EXIT_GAP,
                is_long=is_long,
            ))
            pos_entry_date = None
            pos_defense_events = []
            continue

        if gap_artifact:
            # Flat — just skip this bar so we don't enter at a tainted price
            continue

        if pos_entry_date is not None:
            # ── 1. Trailing-stop trigger ───────────────────────────────
            exit_reason: str | None = None
            if not np.isnan(pos_defense_price):
                if is_long and price < pos_defense_price:
                    exit_reason = REASON_EXIT_DEFENSE
                elif (not is_long) and price > pos_defense_price:
                    exit_reason = REASON_EXIT_DEFENSE

            # ── 2. Signal exit ─────────────────────────────────────────
            if exit_reason is None and bool(exit_[i]):
                exit_reason = REASON_EXIT_SIGNAL

            if exit_reason is not None:
                trades.append(_make_trade(
                    data=data,
                    side_label=side_label,
                    entry_date=pos_entry_date,
                    entry_price=pos_entry_price,
                    entry_index=pos_entry_index,
                    defense_events=pos_defense_events,
                    exit_index=i,
                    exit_price=price,
                    exit_reason=exit_reason,
                    is_long=is_long,
                ))
                pos_entry_date = None
                pos_defense_events = []
            else:
                # ── 3a. Floor ratchet (daily 13-day extreme) ───────────
                new_def, ev = _try_update_defense(
                    i, float(floor_arr[i]), REASON_FLOOR, pos_defense_price,
                )
                if ev is not None:
                    pos_defense_price = new_def
                    pos_defense_events.append(ev)

                # ── 3b. Rule-driven defense updates ────────────────────
                for rule in rules:
                    if not bool(rule.trigger[i]):
                        continue
                    new_def, ev = _try_update_defense(
                        i,
                        float(rule.source[i]),
                        rule.name,
                        pos_defense_price,
                    )
                    if ev is not None:
                        pos_defense_price = new_def
                        pos_defense_events.append(ev)

        # ── 4. Entry check (if flat) ───────────────────────────────────
        if pos_entry_date is None and bool(entry[i]):
            pos_entry_date = data.dates[i]
            pos_entry_price = price
            pos_entry_index = i
            initial_def = float(initial_arr[i])
            pos_defense_price = initial_def
            pos_defense_events = [DefenseEvent(
                date=data.dates[i],
                price=initial_def,
                reason=REASON_ENTRY_INIT,
            )]

    # ── 5. Force-close at end ──────────────────────────────────────────
    if pos_entry_date is not None:
        last_i = n - 1
        last_price = float(data.close[last_i])
        trades.append(_make_trade(
            data=data,
            side_label=side_label,
            entry_date=pos_entry_date,
            entry_price=pos_entry_price,
            entry_index=pos_entry_index,
            defense_events=pos_defense_events,
            exit_index=last_i,
            exit_price=last_price,
            exit_reason=REASON_EXIT_END,
            is_long=is_long,
        ))

    return SideResult(
        stock_id=data.stock_id,
        stock_name=data.stock_name,
        side=side_label,
        start_date=data.dates[si],
        end_date=data.dates[-1],
        trades=trades,
    )


def run_side_backtest_tiered(
    data: "StockData",
    side: str,
    tiers: list[TierConfig],
    exit_: BoolArray,
    start_index: int = DEFAULT_START_INDEX,
    floor_period: int = 13,
    temp_strict_days: int = 5,
    initial_period: int = 5,
) -> SideResult:
    """Dynamic-tier backtest with strict→loose ordering.

    tiers: list[0] = strictest, list[-1] = loosest.

    State machine while holding:
      current_tier_idx — only ratchets UP (toward looser idx); set on entry,
        upgraded when a looser tier's entry signal fires during holding.
      temp_strict_tier_idx — when a stricter tier's entry fires, this is set
        and that tier's defense rules are active for temp_strict_days bars,
        then it expires and rules revert to current_tier_idx's rules.

    Exit (exit_) is global, applies regardless of active tier.
    """
    if side not in ("long", "short"):
        raise ValueError(f"side must be 'long' or 'short', got {side!r}")
    if not tiers:
        raise ValueError("tiers must be non-empty")

    n = data.n
    if n < 13:
        raise InsufficientDataError(
            f"{data.stock_id} {data.stock_name} 資料僅 {n} 天，無法回測"
        )

    si = min(start_index, n - 1)
    side_label = SIDE_LONG if side == "long" else SIDE_SHORT
    is_long = side == "long"

    last_boundary = _compute_last_boundary(data.close)
    if is_long:
        initial_arr = rolling_lowest_safe(data.low, initial_period, last_boundary)
        floor_arr = rolling_lowest_safe(data.low, floor_period, last_boundary)
    else:
        initial_arr = rolling_highest_safe(data.high, initial_period, last_boundary)
        floor_arr = rolling_highest_safe(data.high, floor_period, last_boundary)

    trades: list[Trade] = []

    pos_entry_date = None
    pos_entry_price = 0.0
    pos_entry_index = 0
    pos_defense_price = float("nan")
    pos_defense_events: list[DefenseEvent] = []
    current_tier_idx = -1       # -1 = flat
    temp_strict_idx = -1        # -1 = no temporary strict
    temp_strict_until = -1      # bar index when temp expires

    def _try_update_defense(
        i: int,
        candidate: float,
        reason: str,
        cur_def: float,
    ) -> tuple[float, DefenseEvent | None]:
        if np.isnan(candidate):
            return cur_def, None
        if np.isnan(cur_def):
            ev = DefenseEvent(date=data.dates[i], price=candidate, reason=reason)
            return candidate, ev
        if is_long and candidate > cur_def:
            ev = DefenseEvent(date=data.dates[i], price=candidate, reason=reason)
            return candidate, ev
        if (not is_long) and candidate < cur_def:
            ev = DefenseEvent(date=data.dates[i], price=candidate, reason=reason)
            return candidate, ev
        return cur_def, None

    for i in range(si, n):
        price = float(data.close[i])

        # Abnormal-gap detection
        gap_artifact = False
        if i > 0:
            prev_close = float(data.close[i - 1])
            if prev_close > 0:
                if abs(price / prev_close - 1.0) > GAP_JUMP_THRESHOLD:
                    gap_artifact = True

        if pos_entry_date is not None and gap_artifact:
            trades.append(_make_trade(
                data=data,
                side_label=side_label,
                entry_date=pos_entry_date,
                entry_price=pos_entry_price,
                entry_index=pos_entry_index,
                defense_events=pos_defense_events,
                exit_index=i - 1,
                exit_price=prev_close,
                exit_reason=REASON_EXIT_GAP,
                is_long=is_long,
            ))
            pos_entry_date = None
            pos_defense_events = []
            current_tier_idx = -1
            temp_strict_idx = -1
            continue

        if gap_artifact:
            continue

        if pos_entry_date is not None:
            # Tier state machine: detect tier signal fires this bar
            firing_indices = [
                idx for idx, tier in enumerate(tiers) if bool(tier.entry[i])
            ]
            for idx in firing_indices:
                if idx > current_tier_idx:
                    # Looser tier — upgrade current
                    current_tier_idx = idx
                elif idx < current_tier_idx:
                    # Stricter tier — set temporary strict (use strictest if multi)
                    if temp_strict_idx < 0 or idx < temp_strict_idx:
                        temp_strict_idx = idx
                    temp_strict_until = i + temp_strict_days

            # Expire temp_strict if past expiry or if current upgraded past it
            if temp_strict_idx >= 0:
                if i > temp_strict_until or temp_strict_idx >= current_tier_idx:
                    temp_strict_idx = -1

            # Check exits
            exit_reason: str | None = None
            if not np.isnan(pos_defense_price):
                if is_long and price < pos_defense_price:
                    exit_reason = REASON_EXIT_DEFENSE
                elif (not is_long) and price > pos_defense_price:
                    exit_reason = REASON_EXIT_DEFENSE

            if exit_reason is None and bool(exit_[i]):
                exit_reason = REASON_EXIT_SIGNAL

            if exit_reason is not None:
                trades.append(_make_trade(
                    data=data,
                    side_label=side_label,
                    entry_date=pos_entry_date,
                    entry_price=pos_entry_price,
                    entry_index=pos_entry_index,
                    defense_events=pos_defense_events,
                    exit_index=i,
                    exit_price=price,
                    exit_reason=exit_reason,
                    is_long=is_long,
                ))
                pos_entry_date = None
                pos_defense_events = []
                current_tier_idx = -1
                temp_strict_idx = -1
            else:
                # Floor ratchet
                new_def, ev = _try_update_defense(
                    i, float(floor_arr[i]), REASON_FLOOR, pos_defense_price,
                )
                if ev is not None:
                    pos_defense_price = new_def
                    pos_defense_events.append(ev)

                # Pick active tier's defense rules: temp_strict overrides current
                active_idx = temp_strict_idx if temp_strict_idx >= 0 else current_tier_idx
                active_tier = tiers[active_idx]
                if active_tier.defense_rules:
                    for rule in active_tier.defense_rules:
                        if not bool(rule.trigger[i]):
                            continue
                        new_def, ev = _try_update_defense(
                            i,
                            float(rule.source[i]),
                            rule.name,
                            pos_defense_price,
                        )
                        if ev is not None:
                            pos_defense_price = new_def
                            pos_defense_events.append(ev)

        # Entry check — first firing tier (strict→loose order = list[0] first)
        if pos_entry_date is None:
            for idx, tier in enumerate(tiers):
                if bool(tier.entry[i]):
                    pos_entry_date = data.dates[i]
                    pos_entry_price = price
                    pos_entry_index = i
                    current_tier_idx = idx
                    temp_strict_idx = -1
                    initial_def = float(initial_arr[i])
                    pos_defense_price = initial_def
                    pos_defense_events = [DefenseEvent(
                        date=data.dates[i],
                        price=initial_def,
                        reason=f"{REASON_ENTRY_INIT}[{tier.name}]",
                    )]
                    break

    if pos_entry_date is not None:
        last_i = n - 1
        last_price = float(data.close[last_i])
        trades.append(_make_trade(
            data=data,
            side_label=side_label,
            entry_date=pos_entry_date,
            entry_price=pos_entry_price,
            entry_index=pos_entry_index,
            defense_events=pos_defense_events,
            exit_index=last_i,
            exit_price=last_price,
            exit_reason=REASON_EXIT_END,
            is_long=is_long,
        ))

    return SideResult(
        stock_id=data.stock_id,
        stock_name=data.stock_name,
        side=side_label,
        start_date=data.dates[si],
        end_date=data.dates[-1],
        trades=trades,
    )


def _make_trade(
    *,
    data: "StockData",
    side_label: str,
    entry_date,
    entry_price: float,
    entry_index: int,
    defense_events: list[DefenseEvent],
    exit_index: int,
    exit_price: float,
    exit_reason: str,
    is_long: bool,
) -> Trade:
    if entry_price > 0:
        if is_long:
            pnl_pct = exit_price / entry_price - 1.0
        else:
            pnl_pct = entry_price / exit_price - 1.0
    else:
        pnl_pct = 0.0

    return Trade(
        stock_id=data.stock_id,
        stock_name=data.stock_name,
        side=side_label,
        entry_date=entry_date,
        entry_price=entry_price,
        defense_events=list(defense_events),
        exit_date=data.dates[exit_index],
        exit_price=exit_price,
        exit_reason=exit_reason,
        holding_days=exit_index - entry_index,
        pnl_pct=pnl_pct,
    )
