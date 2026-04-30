"""Performance metrics tests with hand-computed expected values."""

from __future__ import annotations

import numpy as np
import pandas as pd

from hermit_stock.backtest.metrics import (
    annualize,
    compute_metrics,
    cumulative_return,
    daily_win_rate,
    max_drawdown,
    sharpe_ratio,
)


def _flat_nav(start: float = 100.0, days: int = 252) -> pd.Series:
    idx = pd.date_range("2020-01-01", periods=days, freq="D")
    return pd.Series([start] * days, index=idx)


def _linear_growth_nav(start: float = 100.0, daily: float = 0.001, days: int = 252) -> pd.Series:
    idx = pd.date_range("2020-01-01", periods=days, freq="D")
    return pd.Series([start * (1 + daily) ** i for i in range(days)], index=idx)


def test_cumulative_return_flat_is_zero() -> None:
    nav = _flat_nav()
    assert cumulative_return(nav) == 0.0


def test_cumulative_return_doubled() -> None:
    idx = pd.date_range("2020-01-01", periods=2, freq="D")
    nav = pd.Series([100.0, 200.0], index=idx)
    assert abs(cumulative_return(nav) - 1.0) < 1e-9


def test_annualize_compounds_correctly() -> None:
    nav = _linear_growth_nav(daily=0.001, days=252)
    rets = nav.pct_change().dropna()
    ann_ret, ann_vol = annualize(rets)
    # 0.001/day for 252 days → (1.001)**252 - 1 ≈ 0.288
    expected = (1.001) ** 252 - 1
    assert abs(ann_ret - expected) < 0.01


def test_sharpe_zero_for_zero_excess() -> None:
    rets = pd.Series([0.001] * 252)
    sh = sharpe_ratio(rets, rf_daily=0.001)
    assert sh == 0.0


def test_max_drawdown_no_drawdown() -> None:
    nav = _linear_growth_nav(daily=0.001)
    assert max_drawdown(nav) == 0.0


def test_max_drawdown_50_percent() -> None:
    idx = pd.date_range("2020-01-01", periods=4, freq="D")
    nav = pd.Series([100.0, 200.0, 100.0, 100.0], index=idx)
    # peak 200, low 100 → drawdown 50%
    assert abs(max_drawdown(nav) - (-0.5)) < 1e-9


def test_daily_win_rate() -> None:
    rets = pd.Series([0.01, -0.01, 0.01, 0.01, -0.005])
    assert abs(daily_win_rate(rets) - 0.6) < 1e-9


def test_compute_metrics_with_benchmark() -> None:
    np.random.seed(0)
    idx = pd.date_range("2020-01-01", periods=300, freq="D")
    bench_rets = np.random.normal(0.0005, 0.01, 300)
    bench_nav = pd.Series((1 + bench_rets).cumprod() * 100, index=idx)
    # Portfolio = bench * 1.5 + alpha
    port_rets = 1.5 * bench_rets + 0.0003
    port_nav = pd.Series((1 + port_rets).cumprod() * 100, index=idx)

    m = compute_metrics(port_nav, bench_nav)
    # Beta should be ≈ 1.5
    assert m.beta is not None and 1.3 < m.beta < 1.7
    assert m.alpha is not None
    assert m.benchmark_cumret is not None
    assert m.cumulative_return > 0
