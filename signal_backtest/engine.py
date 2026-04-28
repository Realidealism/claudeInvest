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
)
from signal_backtest.signal import DefenseRule

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
    #   floor_arr   — daily floor ratchet (13-bar)
    if is_long:
        initial_arr = rolling_lowest(data.low, 5)
        floor_arr = rolling_lowest(data.low, 13)
    else:
        initial_arr = rolling_highest(data.high, 5)
        floor_arr = rolling_highest(data.high, 13)

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
