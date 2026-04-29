"""Performance metrics: cumret / annret / vol / Sharpe / MDD / win-rate / α / β."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

TRADING_DAYS_PER_YEAR = 252


@dataclass(frozen=True)
class Metrics:
    cumulative_return: float
    annual_return: float
    annual_volatility: float
    sharpe: float
    max_drawdown: float
    win_rate_daily: float
    alpha: float | None  # annualized
    beta: float | None
    benchmark_cumret: float | None


def _daily_returns(nav: pd.Series) -> pd.Series:
    return nav.pct_change().dropna()


def cumulative_return(nav: pd.Series) -> float:
    if len(nav) < 2:
        return 0.0
    last = float(nav.iloc[-1])
    first = float(nav.iloc[0])
    if first == 0:
        return 0.0
    return last / first - 1.0


def annualize(daily_returns: pd.Series) -> tuple[float, float]:
    """Return (annual_return, annual_volatility) from a daily-return series."""
    if daily_returns.empty:
        return 0.0, 0.0
    n = len(daily_returns)
    cum = float((1.0 + daily_returns).prod())  # type: ignore[arg-type]
    if cum <= 0 or n == 0:
        return -1.0, 0.0
    years = n / TRADING_DAYS_PER_YEAR
    ann_ret = cum ** (1.0 / years) - 1.0 if years > 0 else 0.0
    ann_vol = float(daily_returns.std(ddof=0)) * (TRADING_DAYS_PER_YEAR**0.5)
    return ann_ret, ann_vol


def sharpe_ratio(daily_returns: pd.Series, rf_daily: float = 0.0) -> float:
    if daily_returns.empty:
        return 0.0
    excess = daily_returns - rf_daily
    std = float(excess.std(ddof=0))
    if std == 0:
        return 0.0
    return float(excess.mean()) / std * (TRADING_DAYS_PER_YEAR**0.5)


def max_drawdown(nav: pd.Series) -> float:
    if nav.empty:
        return 0.0
    running_max = nav.cummax()
    dd = nav / running_max - 1.0
    return float(dd.min())


def daily_win_rate(daily_returns: pd.Series) -> float:
    if daily_returns.empty:
        return 0.0
    wins = (daily_returns > 0).sum()
    return float(wins) / float(len(daily_returns))


def alpha_beta(
    portfolio_returns: pd.Series, benchmark_returns: pd.Series, rf_daily: float = 0.0
) -> tuple[float, float]:
    """OLS alpha (annualized) and beta against benchmark daily returns."""
    if portfolio_returns.empty or benchmark_returns.empty:
        return 0.0, 0.0
    df = pd.concat([portfolio_returns, benchmark_returns], axis=1, join="inner")
    df.columns = ["p", "b"]
    df = df.dropna()
    if len(df) < 30:
        return 0.0, 0.0
    p = df["p"].to_numpy() - rf_daily
    b = df["b"].to_numpy() - rf_daily
    var_b = float(np.var(b, ddof=0))
    if var_b == 0:
        return 0.0, 0.0
    cov_pb = float(np.cov(p, b, ddof=0)[0, 1])
    beta = cov_pb / var_b
    alpha_daily = float(np.mean(p) - beta * np.mean(b))
    base = 1.0 + alpha_daily
    if base <= 0:
        alpha_ann = -1.0
    else:
        # exp(n*ln(x)) with overflow guard for extreme alpha
        log_term = TRADING_DAYS_PER_YEAR * np.log(base)
        if log_term > 700:  # exp(700) is near float max
            alpha_ann = float("inf")
        elif log_term < -700:
            alpha_ann = -1.0
        else:
            alpha_ann = float(np.exp(log_term)) - 1.0
    return alpha_ann, beta


def compute_metrics(nav: pd.Series, benchmark_nav: pd.Series | None = None) -> Metrics:
    rets = _daily_returns(nav)
    cum = cumulative_return(nav)
    ann_ret, ann_vol = annualize(rets)
    sh = sharpe_ratio(rets)
    mdd = max_drawdown(nav)
    win = daily_win_rate(rets)

    alpha = beta = None
    bench_cum: float | None = None
    if benchmark_nav is not None and not benchmark_nav.empty:
        bench_rets = _daily_returns(benchmark_nav)
        # Align to portfolio dates
        aligned = rets.index.intersection(bench_rets.index)
        if len(aligned) >= 30:
            alpha, beta = alpha_beta(rets.loc[aligned], bench_rets.loc[aligned])
            bench_cum = cumulative_return(benchmark_nav.loc[aligned[0] : aligned[-1]])

    return Metrics(
        cumulative_return=cum,
        annual_return=ann_ret,
        annual_volatility=ann_vol,
        sharpe=sh,
        max_drawdown=mdd,
        win_rate_daily=win,
        alpha=alpha,
        beta=beta,
        benchmark_cumret=bench_cum,
    )
