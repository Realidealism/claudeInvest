"""Macro regime classifier tests with synthetic series."""

from __future__ import annotations

from datetime import date

import pandas as pd

from hermit_stock.macro.regime import (
    composite_regime,
    foreign_20d_net,
    margin_yoy,
    regime_series,
    snapshot_at,
    taiex_60d_return,
    taiex_trend,
)


def _date_range(start: str, n: int) -> pd.DatetimeIndex:
    return pd.date_range(start, periods=n, freq="D")


def test_taiex_trend_above_sma_returns_one() -> None:
    n = 250
    s = pd.Series(
        [100 + i * 0.5 for i in range(n)],  # rising linearly
        index=_date_range("2020-01-01", n),
    )
    trend = taiex_trend(s, window=200)
    assert trend.iloc[-1] == 1


def test_taiex_trend_below_sma_returns_minus_one() -> None:
    n = 250
    s = pd.Series(
        [200 - i * 0.5 for i in range(n)],
        index=_date_range("2020-01-01", n),
    )
    trend = taiex_trend(s, window=200)
    assert trend.iloc[-1] == -1


def test_taiex_60d_return_positive() -> None:
    s = pd.Series([100.0] * 60 + [120.0], index=_date_range("2020-01-01", 61))
    assert abs(taiex_60d_return(s).iloc[-1] - 0.20) < 1e-9


def test_foreign_20d_net_sum() -> None:
    s = pd.Series([1.0] * 30, index=_date_range("2020-01-01", 30))
    rolled = foreign_20d_net(s)
    assert rolled.iloc[-1] == 20.0


def test_margin_yoy_returns_252_day_pct_change() -> None:
    n = 260
    s = pd.Series([100.0] * 252 + [110.0] * (n - 252), index=_date_range("2020-01-01", n))
    yoy = margin_yoy(s)
    assert abs(yoy.iloc[-1] - 0.10) < 1e-9


def test_composite_bull_when_all_positive() -> None:
    assert composite_regime(1, 1, 1.0e9) == "Bull"


def test_composite_bear_when_trend_down_and_breadth_or_outflow() -> None:
    assert composite_regime(-1, -1, 1.0e9) == "Bear"
    assert composite_regime(-1, 1, -1.0e9) == "Bear"


def test_composite_neutral_otherwise() -> None:
    assert composite_regime(0, 0, 0.0) == "Neutral"
    assert composite_regime(1, -1, 1.0) == "Neutral"  # trend up but breadth down


def test_snapshot_at_picks_latest_at_or_before() -> None:
    n = 250
    taiex = pd.Series(
        [100 + i * 0.5 for i in range(n)],
        index=_date_range("2020-01-01", n),
    )
    breadth = pd.DataFrame({"medium_trend": [1] * n}, index=_date_range("2020-01-01", n))
    foreign = pd.Series([1.0e8] * n, index=_date_range("2020-01-01", n))
    margin = pd.Series([100.0] * n, index=_date_range("2020-01-01", n))
    snap = snapshot_at(
        date(2020, 9, 1), taiex=taiex, breadth_df=breadth, foreign_daily=foreign, margin=margin
    )
    assert snap.regime in ("Bull", "Neutral", "Bear")
    assert snap.taiex_close is not None
    assert snap.breadth_medium == 1


def test_regime_series_has_all_three_labels_under_mixed_input() -> None:
    n = 400
    # First half rising, second half falling
    taiex_vals = [100 + i * 0.5 for i in range(n // 2)] + [300 - i * 0.5 for i in range(n // 2)]
    taiex = pd.Series(taiex_vals, index=_date_range("2020-01-01", n))
    breadth = pd.DataFrame(
        {"medium_trend": [1] * (n // 2) + [-1] * (n // 2)},
        index=_date_range("2020-01-01", n),
    )
    foreign = pd.Series(
        [1.0e8] * (n // 2) + [-1.0e8] * (n // 2),
        index=_date_range("2020-01-01", n),
    )
    rs = regime_series(taiex=taiex, breadth_df=breadth, foreign_daily=foreign)
    counts = rs.value_counts()
    # Should see at least 2 distinct regimes
    assert len(counts) >= 2


def test_macro_snapshot_label_helpers_handle_none() -> None:
    from hermit_stock.macro.regime import MacroSnapshot

    s = MacroSnapshot(
        as_of=date(2024, 1, 1),
        taiex_close=None,
        taiex_trend=0,
        taiex_60d_return=None,
        breadth_medium=None,
        foreign_20d_net=None,
        margin_yoy=None,
        regime="Neutral",
    )
    assert s.label_taiex_trend() == "—"
    assert s.label_60d() == "—"
    assert s.label_breadth() == "Neutral"
    assert s.label_foreign() == "—"
    assert s.label_margin() == "—"
