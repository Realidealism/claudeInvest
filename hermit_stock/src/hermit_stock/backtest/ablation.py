"""Rule-ablation harness: 16 variants (8 drop-one + 8 only-one).

Two families per the user-confirmed scope:

  Variant B "drop F_i" (1..8): re-run the backtest using 7 rules (excluding F_i).
                               max raw score = 7.
  Variant A "only F_i" (1..8): re-run with a single rule. Top-K is selected
                               from tickers passing F_i, ranked by full 8-rule
                               score as tiebreaker.

Engine.select_top_k already supports `enabled_rules`. We keep the same
min_score_floor=3 for drop-one (still meaningful; missing one rule can still
have ≥3 of the rest); for only-one we relax floor to 1 (must pass the rule).
"""

from __future__ import annotations

from dataclasses import replace

import pandas as pd

from ..data.models import MonthlyRevenue, QuarterlyReport, StockMeta
from .engine import BacktestConfig, BacktestResult, run_backtest

ALL_RULES: tuple[str, ...] = ("F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8")


def make_drop_one_configs(base: BacktestConfig) -> list[BacktestConfig]:
    out: list[BacktestConfig] = []
    for rule in ALL_RULES:
        enabled = frozenset(r for r in ALL_RULES if r != rule)
        out.append(
            replace(
                base,
                enabled_rules=enabled,
                label=f"drop_{rule}",
            )
        )
    return out


def make_only_one_configs(
    base: BacktestConfig, *, min_score_floor: int = 1
) -> list[BacktestConfig]:
    out: list[BacktestConfig] = []
    for rule in ALL_RULES:
        out.append(
            replace(
                base,
                enabled_rules=frozenset({rule}),
                min_score_floor=min_score_floor,
                label=f"only_{rule}",
            )
        )
    return out


def run_ablation_suite(
    base: BacktestConfig,
    *,
    metas: list[StockMeta],
    quarterly_by_ticker: dict[str, list[QuarterlyReport]],
    monthly_by_ticker: dict[str, list[MonthlyRevenue]],
    adj_close: pd.DataFrame,
    drop_one: bool = True,
    only_one: bool = True,
) -> dict[str, BacktestResult]:
    """Run main + 16 ablation backtests. Returns {label: BacktestResult}."""
    configs: list[BacktestConfig] = [base]
    if drop_one:
        configs.extend(make_drop_one_configs(base))
    if only_one:
        configs.extend(make_only_one_configs(base))

    out: dict[str, BacktestResult] = {}
    for cfg in configs:
        out[cfg.label] = run_backtest(
            cfg,
            metas=metas,
            quarterly_by_ticker=quarterly_by_ticker,
            monthly_by_ticker=monthly_by_ticker,
            adj_close=adj_close,
        )
    return out
