"""Main backtest engine: drives screener → portfolio → mark-to-market.

Hot path: adjusted close prices are precomputed into a single DataFrame
(index=trade_date, columns=ticker, values=adj_close). Everything else is
keyed off that table.

Lookahead defense: at each rebalance date d, the screener filters with
publish_date <= d (data/as_of.py). The rebalance itself executes at d's
adjusted close. So the only data leak vector would be if adj_close[d]
incorporated information unavailable at d — and adj_close is built from
events that, by definition, happened on or before d.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import pandas as pd

from ..data.adjusted_price import adjusted_close_series
from ..data.as_of import filter_monthly, filter_quarterly
from ..data.models import DailyPrice, MonthlyRevenue, QuarterlyReport, StockMeta
from ..scoring.rules import RuleResult, Thresholds, evaluate_all
from .calendar import rebalance_dates
from .portfolio import Portfolio


@dataclass
class BacktestConfig:
    start: date
    end: date
    top_k: int = 10
    min_score_floor: int = 3
    initial_cash: float = 1_000_000.0
    enabled_rules: frozenset[str] = frozenset({"F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"})
    gate_rules: frozenset[str] = frozenset()  # must-all-pass; excluded from score
    thresholds: Thresholds | None = None  # None = defaults
    macro_filter: bool = False  # enable Bear-regime top_k reduction
    label: str = "main"  # for ablation reporting


@dataclass
class BacktestResult:
    config: BacktestConfig
    nav: pd.Series  # date -> NAV (float)
    portfolio: Portfolio
    rebalance_dates_used: list[date] = field(default_factory=list)


def build_adj_close_table(
    quarterly_by_ticker: dict[str, list[QuarterlyReport]],
    prices_by_ticker: dict[str, list[DailyPrice]],
    dividends_by_ticker: dict[str, list],
    reductions_by_ticker: dict[str, list],
) -> pd.DataFrame:
    """Wide DataFrame: index = trade_date, columns = ticker, values = adj_close."""
    series_by_ticker: dict[str, pd.Series] = {}
    for ticker, prices in prices_by_ticker.items():
        if not prices:
            continue
        s = adjusted_close_series(
            prices,
            dividends_by_ticker.get(ticker, []),
            reductions_by_ticker.get(ticker, []),
        )
        if not s.empty:
            series_by_ticker[ticker] = s
    if not series_by_ticker:
        return pd.DataFrame()
    df = pd.DataFrame(series_by_ticker)
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    # Forward-fill missing closes so single-ticker halts and market-wide
    # closure days (e.g. typhoon half-days) don't zero out our holdings.
    # We only forward-fill, never back-fill, to keep lookahead-safe.
    df = df.ffill()
    return df


def select_top_k(
    rebalance_date: date,
    metas: list[StockMeta],
    quarterly_by_ticker: dict[str, list[QuarterlyReport]],
    monthly_by_ticker: dict[str, list[MonthlyRevenue]],
    *,
    enabled_rules: frozenset[str],
    top_k: int,
    min_score_floor: int,
    gate_rules: frozenset[str] = frozenset(),
    thresholds: Thresholds | None = None,
) -> list[tuple[str, int, list[RuleResult]]]:
    """Run scoring at `rebalance_date`, filter & sort, return up to `top_k`.

    `gate_rules` must all pass=True for a ticker to be considered. They are
    excluded from the score count (so they don't dilute the signal — they're
    a binary qualification).

    Scoring rules = enabled_rules \\ gate_rules.
    Tiebreak: total raw score (across all 8) descending; then ticker ascending.
    """
    scoring_rules = enabled_rules - gate_rules
    out: list[tuple[str, int, int, list[RuleResult]]] = []
    for meta in metas:
        ticker = meta.ticker
        qs = filter_quarterly(quarterly_by_ticker.get(ticker, []), rebalance_date)
        ms = filter_monthly(monthly_by_ticker.get(ticker, []), rebalance_date)
        if not qs and not ms:
            continue
        results = evaluate_all(qs, ms, thresholds)
        if gate_rules:
            by_code = {r.code: r for r in results}
            if any(by_code.get(g) is None or by_code[g].passed is not True for g in gate_rules):
                continue
        score_enabled = sum(1 for r in results if r.code in scoring_rules and r.passed is True)
        score_full = sum(1 for r in results if r.passed is True)
        if score_enabled < min_score_floor:
            continue
        out.append((ticker, score_enabled, score_full, results))

    out.sort(key=lambda x: (-x[1], -x[2], x[0]))
    return [(t, sf, res) for (t, _, sf, res) in out[:top_k]]


def run_backtest(
    config: BacktestConfig,
    *,
    metas: list[StockMeta],
    quarterly_by_ticker: dict[str, list[QuarterlyReport]],
    monthly_by_ticker: dict[str, list[MonthlyRevenue]],
    adj_close: pd.DataFrame,
    regime_series: pd.Series | None = None,
) -> BacktestResult:
    """Pure-function backtest. Caller pre-loads all data.

    `regime_series`: per-day Bull/Neutral/Bear labels, used only when
    `config.macro_filter=True`. In Bear days the rebalance halves top_k
    (rounded up) to concentrate into the highest-conviction names.
    """
    if adj_close.empty:
        raise ValueError("adj_close table is empty")
    trading_days = [d.date() for d in adj_close.index]
    trading_days_in_range = [d for d in trading_days if config.start <= d <= config.end]
    if not trading_days_in_range:
        raise ValueError("no trading days in range")

    rd = rebalance_dates(config.start, config.end, trading_days)
    if not rd:
        raise ValueError("no rebalance dates in range")

    portfolio = Portfolio(cash=config.initial_cash)

    rd_set = set(rd)
    for d in trading_days_in_range:
        row = adj_close.loc[pd.Timestamp(d)]
        prices = _row_to_dict(row if isinstance(row, pd.Series) else row.iloc[0])
        if d in rd_set:
            effective_top_k = config.top_k
            if config.macro_filter and regime_series is not None:
                ts = pd.Timestamp(d)
                idx = regime_series.index[regime_series.index <= ts]
                if len(idx) > 0:
                    cur_regime = str(regime_series.loc[idx[-1]])
                    if cur_regime == "Bear":
                        effective_top_k = (config.top_k + 1) // 2  # halve, round up
            picks = select_top_k(
                d,
                metas,
                quarterly_by_ticker,
                monthly_by_ticker,
                enabled_rules=config.enabled_rules,
                top_k=effective_top_k,
                min_score_floor=config.min_score_floor,
                gate_rules=config.gate_rules,
                thresholds=config.thresholds,
            )
            target_tickers = [t for (t, _s, _r) in picks]
            portfolio.rebalance_equal_weight(d, target_tickers, prices)
        portfolio.mark_to_market(d, prices)

    nav_series = pd.Series(portfolio.nav_history).sort_index()
    nav_series.index = pd.to_datetime(nav_series.index)
    return BacktestResult(
        config=config,
        nav=nav_series,
        portfolio=portfolio,
        rebalance_dates_used=rd,
    )


def _row_to_dict(row: pd.Series) -> dict[str, float]:
    """Turn a wide-row of adj_close into {ticker: float}, dropping NaNs."""
    return {str(t): float(v) for t, v in row.items() if pd.notna(v) and v > 0}
