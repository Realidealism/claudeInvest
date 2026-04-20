"""
Signal-driven backtest for fund/ETF holdings cross-reference strategies.

Entry signals: quarterly_to_monthly_top10, quarterly_dormant_etf_active,
               dual_track_entry, multi_fund_consensus, consensus_formation
Exit signals:  heavy_position_reduction, core_exit

Uses yfinance for price data and TAIEX (^TWII) as benchmark.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, timedelta

import numpy as np

from db.connection import get_cursor

ENTRY_SIGNALS = {
    "quarterly_to_monthly_top10",
    "quarterly_dormant_etf_active",
    "dual_track_entry",
    "multi_fund_consensus",
    "consensus_formation",
}
EXIT_SIGNALS = {
    "heavy_position_reduction",
    "core_exit",
}
# Accumulation signals: not entry/exit, but tracked for evidence
ACCUM_SIGNALS = {
    "consecutive_accumulation",
    "dual_track_accumulation",
    "consensus_formation",
}

TRADING_DAYS_PER_YEAR = 252


@dataclass
class SignalTrade:
    """One completed round-trip trade triggered by strategy signals."""
    ticker: str
    ticker_name: str
    entry_signal: str
    entry_period: str
    entry_date: date
    entry_price: float
    exit_signal: str | None
    exit_period: str | None
    exit_date: date | None
    exit_price: float | None
    return_pct: float | None
    holding_days: int | None


@dataclass
class SignalBacktestResult:
    """Aggregate backtest metrics for signal-driven strategies."""
    trades: list[SignalTrade] = field(default_factory=list)
    open_positions: list[SignalTrade] = field(default_factory=list)

    # Portfolio-level metrics
    total_return: float = 0.0
    win_rate: float = 0.0
    avg_return: float = 0.0
    avg_holding_days: float = 0.0
    max_drawdown: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    alpha: float = 0.0
    beta: float = 0.0
    information_ratio: float = 0.0
    kelly_criterion: float = 0.0


def _period_to_date(trigger_period: str) -> date:
    """Convert trigger_period to the earliest actionable date.

    SITCA monthly/quarterly reports are published around the 10th business
    day of the FOLLOWING month. Use the 15th of month+1 as a conservative
    proxy to avoid look-ahead bias.
    """
    p = trigger_period.rstrip("MQ")
    y, m = int(p[:4]), int(p[4:])
    # Report for period YYYYMM is available in month+1
    m += 1
    if m > 12:
        m, y = 1, y + 1
    return date(y, m, 15)


def _fetch_prices(tickers: list[str], start: date, end: date,
                   cur) -> dict[str, dict[date, float]]:
    """Fetch close prices from tw.daily_prices."""
    if not tickers:
        return {}

    cur.execute("""
        SELECT stock_id, trade_date, close_price
        FROM tw.daily_prices
        WHERE stock_id = ANY(%s)
          AND trade_date BETWEEN %s AND %s
          AND close_price IS NOT NULL
        ORDER BY stock_id, trade_date
    """, (tickers, start, end))

    result: dict[str, dict[date, float]] = {}
    for r in cur.fetchall():
        result.setdefault(r["stock_id"], {})[r["trade_date"]] = float(r["close_price"])
    return result


def _fetch_benchmark(start: date, end: date, cur) -> dict[date, float]:
    """Fetch TAIEX close prices from tw.index_prices."""
    cur.execute("""
        SELECT trade_date, close_price
        FROM tw.index_prices
        WHERE index_id = 'TAIEX'
          AND trade_date BETWEEN %s AND %s
          AND close_price IS NOT NULL
        ORDER BY trade_date
    """, (start, end))
    return {r["trade_date"]: float(r["close_price"]) for r in cur.fetchall()}


def _next_trading_date(prices: dict[date, float], target: date) -> date | None:
    """Find the first trading date on or after target."""
    sorted_dates = sorted(prices.keys())
    for d in sorted_dates:
        if d >= target:
            return d
    return None


def _build_trades(signals: list[dict]) -> tuple[list[SignalTrade], list[SignalTrade]]:
    """Match entry/exit signals into trades.

    Returns (completed_trades, open_positions).
    """
    # Group signals by ticker, sorted by period
    by_ticker: dict[str, list[dict]] = {}
    for s in signals:
        by_ticker.setdefault(s["ticker"], []).append(s)

    for ticker in by_ticker:
        by_ticker[ticker].sort(key=lambda s: s["trigger_period"])

    completed = []
    still_open = []

    for ticker, sigs in by_ticker.items():
        position = None  # current open position

        for s in sigs:
            st = s["signal_type"]

            if st in ENTRY_SIGNALS and position is None:
                position = SignalTrade(
                    ticker=ticker,
                    ticker_name=s["ticker_name"],
                    entry_signal=st,
                    entry_period=s["trigger_period"],
                    entry_date=_period_to_date(s["trigger_period"]),
                    entry_price=0,
                    exit_signal=None, exit_period=None,
                    exit_date=None, exit_price=None,
                    return_pct=None, holding_days=None,
                )

            elif st in EXIT_SIGNALS and position is not None:
                # Skip exit in the same period as entry (0-day round trip)
                if s["trigger_period"] == position.entry_period:
                    continue
                position.exit_signal = st
                position.exit_period = s["trigger_period"]
                position.exit_date = _period_to_date(s["trigger_period"])
                completed.append(position)
                position = None

        if position is not None:
            still_open.append(position)

    return completed, still_open


def _fill_prices(trades: list[SignalTrade], prices: dict[str, dict[date, float]]):
    """Fill entry/exit prices from price data."""
    for t in trades:
        ticker_prices = prices.get(t.ticker, {})
        if not ticker_prices:
            continue

        entry_d = _next_trading_date(ticker_prices, t.entry_date)
        if entry_d:
            t.entry_date = entry_d
            t.entry_price = ticker_prices[entry_d]

        if t.exit_date:
            exit_d = _next_trading_date(ticker_prices, t.exit_date)
            if exit_d:
                t.exit_date = exit_d
                t.exit_price = ticker_prices[exit_d]

        if t.entry_price and t.exit_price:
            t.return_pct = (t.exit_price - t.entry_price) / t.entry_price
            t.holding_days = (t.exit_date - t.entry_date).days


def _compute_metrics(result: SignalBacktestResult,
                     benchmark_prices: dict[date, float]):
    """Compute portfolio-level metrics from completed trades."""
    trades = result.trades
    closed = [t for t in trades if t.return_pct is not None]

    if not closed:
        return

    returns = [t.return_pct for t in closed]
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]

    result.total_return = float(np.prod([1 + r for r in returns]) - 1)
    result.win_rate = len(wins) / len(returns)
    result.avg_return = float(np.mean(returns))
    result.avg_holding_days = float(np.mean([t.holding_days for t in closed]))

    # Sharpe ratio (annualized, using per-trade returns)
    if len(returns) > 1:
        avg_hd = result.avg_holding_days or 1
        trades_per_year = TRADING_DAYS_PER_YEAR / avg_hd
        std = float(np.std(returns, ddof=1))
        if std > 0:
            result.sharpe_ratio = result.avg_return / std * math.sqrt(trades_per_year)

    # Sortino ratio (downside deviation only)
    downside = [min(r, 0) for r in returns]
    downside_std = float(np.std(downside, ddof=1)) if len(downside) > 1 else 0
    if downside_std > 0:
        avg_hd = result.avg_holding_days or 1
        trades_per_year = TRADING_DAYS_PER_YEAR / avg_hd
        result.sortino_ratio = result.avg_return / downside_std * math.sqrt(trades_per_year)

    # Max drawdown from cumulative returns
    cum = np.cumprod([1 + r for r in returns])
    peak = np.maximum.accumulate(cum)
    dd = (cum - peak) / peak
    result.max_drawdown = float(np.min(dd))

    # Kelly criterion: f* = (p * b - q) / b
    # p = win rate, q = 1-p, b = avg_win / avg_loss
    if wins and losses:
        avg_win = float(np.mean(wins))
        avg_loss = abs(float(np.mean(losses)))
        if avg_loss > 0:
            b = avg_win / avg_loss
            p = result.win_rate
            result.kelly_criterion = (p * b - (1 - p)) / b

    # Alpha / Beta / Information Ratio vs benchmark
    if not benchmark_prices:
        return

    bench_returns = []
    trade_returns = []
    for t in closed:
        if t.entry_date in benchmark_prices and t.exit_date in benchmark_prices:
            br = (benchmark_prices[t.exit_date] - benchmark_prices[t.entry_date]) / benchmark_prices[t.entry_date]
            bench_returns.append(br)
            trade_returns.append(t.return_pct)

    if len(bench_returns) >= 2:
        tr = np.array(trade_returns)
        br = np.array(bench_returns)

        cov = np.cov(tr, br)
        var_bench = cov[1, 1]
        if var_bench > 0:
            result.beta = float(cov[0, 1] / var_bench)
        result.alpha = float(np.mean(tr) - result.beta * np.mean(br))

        excess = tr - br
        excess_std = float(np.std(excess, ddof=1))
        if excess_std > 0:
            avg_hd = result.avg_holding_days or 1
            trades_per_year = TRADING_DAYS_PER_YEAR / avg_hd
            result.information_ratio = float(np.mean(excess)) / excess_std * math.sqrt(trades_per_year)


def run_signal_backtest() -> SignalBacktestResult:
    """Load all signals from DB, build trades, fetch prices, compute metrics."""
    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT signal_type, ticker, ticker_name, funds,
                   trigger_date, trigger_period, weight_change
            FROM tw.signals
            ORDER BY trigger_period, ticker
        """)
        signals = [dict(r) for r in cur.fetchall()]

        if not signals:
            print("No signals in DB.")
            return SignalBacktestResult()

        print(f"Loaded {len(signals)} signals.")

        # Build trades from signal sequence
        completed, still_open = _build_trades(signals)
        print(f"Completed trades: {len(completed)}, Open positions: {len(still_open)}")

        if not completed:
            return SignalBacktestResult(open_positions=still_open)

        # Determine date range
        all_dates = [t.entry_date for t in completed]
        all_dates += [t.exit_date for t in completed if t.exit_date]
        start = min(all_dates) - timedelta(days=7)
        end = max(all_dates) + timedelta(days=7)

        # Fetch prices from DB
        all_tickers = list({t.ticker for t in completed + still_open})
        print(f"Fetching prices for {len(all_tickers)} tickers from DB ...")
        prices = _fetch_prices(all_tickers, start, end, cur)

        print("Fetching TAIEX benchmark from DB ...")
        benchmark_prices = _fetch_benchmark(start, end, cur)

    # Fill prices and compute returns
    _fill_prices(completed, prices)
    _fill_prices(still_open, prices)

    result = SignalBacktestResult(
        trades=completed,
        open_positions=still_open,
    )
    _compute_metrics(result, benchmark_prices)

    return result


def save_backtest_results(result: SignalBacktestResult):
    """Persist completed trades to tw.signal_backtest_results."""
    with get_cursor() as cur:
        for t in result.trades:
            if t.return_pct is None:
                continue
            cur.execute("""
                INSERT INTO tw.signal_backtest_results
                    (ticker, ticker_name, entry_signal, entry_period,
                     entry_date, entry_price, exit_signal, exit_period,
                     exit_date, exit_price, return_pct, holding_days)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, entry_period, entry_signal) DO UPDATE SET
                    exit_signal = EXCLUDED.exit_signal,
                    exit_period = EXCLUDED.exit_period,
                    exit_date = EXCLUDED.exit_date,
                    exit_price = EXCLUDED.exit_price,
                    return_pct = EXCLUDED.return_pct,
                    holding_days = EXCLUDED.holding_days
            """, (t.ticker, t.ticker_name, t.entry_signal, t.entry_period,
                  t.entry_date, t.entry_price, t.exit_signal, t.exit_period,
                  t.exit_date, t.exit_price, t.return_pct, t.holding_days))
    print(f"Saved {len([t for t in result.trades if t.return_pct is not None])} trades to DB.")
