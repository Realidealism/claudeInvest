"""Unit tests for valuation: multiples / bands / selector / methods + lookahead."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pandas as pd
import pytest

from hermit_stock.data.models import DailyPrice
from hermit_stock.data.publish_date import quarter_publish_date
from hermit_stock.valuation.bands import Band, rolling_band
from hermit_stock.valuation.methods import make_snapshot
from hermit_stock.valuation.multiples import daily_multiples, quarter_multiples
from hermit_stock.valuation.selector import select_valuation_method
from tests.fixtures.synthetic import make_quarter


def _price(d: date, close: Decimal) -> DailyPrice:
    return DailyPrice(ticker="TEST", trade_date=d, source="db", close=close)


# --- multiples ---


def test_quarter_multiples_eps_ttm_is_sum_of_last_four() -> None:
    qs = [make_quarter(2024, q, eps=Decimal("1.5")) for q in (1, 2, 3, 4)]
    qm = quarter_multiples(qs)
    assert qm[3].eps_ttm == Decimal(6)
    assert qm[2].eps_ttm is None  # only 3 quarters available


def test_daily_multiples_uses_latest_published_quarter() -> None:
    qs = [
        make_quarter(
            2023,
            q,
            eps=Decimal("1.0"),
            revenue=Decimal(1000),
            book_value_per_share=Decimal(20),
            shares_outstanding=Decimal(100),
        )
        for q in (1, 2, 3, 4)
    ]
    qs.append(
        make_quarter(
            2024,
            1,
            eps=Decimal("2.0"),
            revenue=Decimal(1500),
            book_value_per_share=Decimal(22),
            shares_outstanding=Decimal(100),
        )
    )
    # 2023Q4 publishes 2024-03-31; 2024Q1 publishes 2024-05-15
    prices = [
        _price(date(2024, 4, 1), Decimal(100)),  # uses 2023Q4
        _price(date(2024, 5, 15), Decimal(100)),  # uses 2024Q1
    ]
    df = daily_multiples(qs, prices)
    assert df.loc[pd.Timestamp("2024-04-01"), "pe"] == pytest.approx(100 / 4.0)  # eps_ttm 4*1.0
    # On 2024-05-15, 2024Q1 is just published. eps_ttm = 1+1+1+2 = 5
    assert df.loc[pd.Timestamp("2024-05-15"), "pe"] == pytest.approx(100 / 5.0)


def test_daily_multiples_strict_publish_date_lookahead_safe() -> None:
    """Price one day BEFORE 2024Q1 publishes must use 2023Q4 metrics, not 2024Q1."""
    qs = [
        make_quarter(
            2023,
            q,
            eps=Decimal("1.0"),
            revenue=Decimal(1000),
            book_value_per_share=Decimal(20),
            shares_outstanding=Decimal(100),
        )
        for q in (1, 2, 3, 4)
    ]
    qs.append(
        make_quarter(
            2024,
            1,
            eps=Decimal("99.0"),
            revenue=Decimal(99999),
            book_value_per_share=Decimal(99),
            shares_outstanding=Decimal(100),
        )
    )
    pub = quarter_publish_date(2024, 1)
    day_before = _price(pub - timedelta(days=1), Decimal(100))
    df = daily_multiples(qs, [day_before])
    # Must use 2023Q4 metrics: eps_ttm = 4.0
    assert df.iloc[0]["pe"] == pytest.approx(100 / 4.0)


# --- bands ---


def test_band_from_series_computes_mean_and_std() -> None:
    s = pd.Series([10.0] * 100)
    b = Band.from_series(s)
    assert b.mean == 10.0
    assert b.std == 0.0


def test_band_returns_none_when_too_few_points() -> None:
    s = pd.Series([10.0] * 10)
    b = Band.from_series(s)
    assert b.mean is None
    assert b.n_obs == 10


def test_band_classify() -> None:
    b = Band(
        mean=10.0, std=2.0, minus_2sd=6.0, minus_1sd=8.0, plus_1sd=12.0, plus_2sd=14.0, n_obs=100
    )
    assert b.classify(5.0) == "< −2σ"
    assert b.classify(7.0) == "−2σ ~ −1σ"
    assert b.classify(9.0) == "−1σ ~ mean"
    assert b.classify(11.0) == "mean ~ +1σ"
    assert b.classify(13.0) == "+1σ ~ +2σ"
    assert b.classify(15.0) == "> +2σ"
    assert b.classify(None) is None


def test_rolling_band_window_filter() -> None:
    idx = pd.date_range("2020-01-01", periods=2000, freq="D")
    df = pd.DataFrame({"pe": list(range(2000))}, index=idx)
    b = rolling_band(df, "pe", window_years=5)
    # last 5 years ≈ 1826 days, so n_obs is about that
    assert b.n_obs >= 1500


# --- selector ---


def test_selector_picks_pe_for_profitable_company() -> None:
    qs = []
    for year in (2021, 2022, 2023):
        for q in (1, 2, 3, 4):
            qs.append(make_quarter(year, q, net_income=Decimal(100), revenue=Decimal(1000)))
    method = select_valuation_method(qs, value_source="operations")
    assert method == "PE"


def test_selector_picks_pb_for_assets_oriented_industry() -> None:
    qs = []
    for year in (2021, 2022, 2023):
        for q in (1, 2, 3, 4):
            qs.append(make_quarter(year, q, net_income=Decimal(100), revenue=Decimal(1000)))
    method = select_valuation_method(qs, value_source="assets")
    assert method == "PB"


def test_selector_picks_ps_for_high_growth_unprofitable() -> None:
    qs = []
    base = 100
    for year in (2021, 2022, 2023, 2024):
        for q in (1, 2, 3, 4):
            qs.append(make_quarter(year, q, net_income=Decimal(-10), revenue=Decimal(base)))
        base = int(base * 1.6)
    method = select_valuation_method(qs)
    assert method == "PS"


def test_selector_falls_back_to_pb_when_unprofitable_and_low_growth() -> None:
    qs = []
    for year in (2021, 2022, 2023, 2024):
        for q in (1, 2, 3, 4):
            qs.append(make_quarter(year, q, net_income=Decimal(-10), revenue=Decimal(100)))
    method = select_valuation_method(qs)
    assert method == "PB"


# --- methods (snapshot + decision) ---


def test_make_snapshot_buy_when_upside_above_threshold() -> None:
    idx = pd.date_range("2020-01-01", periods=400, freq="D")
    df = pd.DataFrame({"close": [100.0] * 400, "pe": [10.0] * 400}, index=idx)
    band = Band(
        mean=15.0, std=1.0, minus_2sd=13.0, minus_1sd=14.0, plus_1sd=16.0, plus_2sd=17.0, n_obs=400
    )
    snap = make_snapshot(df, "PE", band)
    assert snap is not None
    # eps_ttm = 100 / 10 = 10. target_mean = 10*15 = 150. upside = 50% -> BUY
    assert snap.decision == "BUY"
    assert snap.upside_mean == pytest.approx(0.5)


def test_make_snapshot_sell_when_overvalued() -> None:
    idx = pd.date_range("2020-01-01", periods=400, freq="D")
    df = pd.DataFrame({"close": [200.0] * 400, "pe": [40.0] * 400}, index=idx)
    band = Band(
        mean=15.0, std=1.0, minus_2sd=13.0, minus_1sd=14.0, plus_1sd=16.0, plus_2sd=17.0, n_obs=400
    )
    snap = make_snapshot(df, "PE", band)
    assert snap is not None
    assert snap.decision == "SELL"


def test_make_snapshot_hold_when_neutral() -> None:
    idx = pd.date_range("2020-01-01", periods=400, freq="D")
    df = pd.DataFrame({"close": [100.0] * 400, "pe": [13.0] * 400}, index=idx)
    band = Band(
        mean=15.0, std=1.0, minus_2sd=13.0, minus_1sd=14.0, plus_1sd=16.0, plus_2sd=17.0, n_obs=400
    )
    snap = make_snapshot(df, "PE", band)
    assert snap is not None
    # eps_ttm ≈ 7.69, target_mean ≈ 115.4, upside ≈ 15.4% -> HOLD
    assert snap.decision == "HOLD"


def test_make_snapshot_with_forward_eps_uses_it_for_upside() -> None:
    idx = pd.date_range("2020-01-01", periods=400, freq="D")
    # Trailing PE = 20 (close 100 / ttm_eps 5)
    df = pd.DataFrame({"close": [100.0] * 400, "pe": [20.0] * 400}, index=idx)
    band = Band(
        mean=15.0,
        std=1.0,
        minus_2sd=13.0,
        minus_1sd=14.0,
        plus_1sd=16.0,
        plus_2sd=17.0,
        n_obs=400,
    )
    # Forward EPS = 10 (50% growth from ttm 5)
    snap = make_snapshot(df, "PE", band, forward_eps=10.0)
    assert snap is not None
    assert snap.forward_eps == 10.0
    # forward_pe = 100 / 10 = 10
    assert snap.forward_pe == pytest.approx(10.0)
    # target_mean uses forward_eps × historical mean = 10 × 15 = 150
    # upside = (150 - 100) / 100 = 0.5
    assert snap.upside_mean == pytest.approx(0.5)
    assert snap.decision == "BUY"


def test_make_snapshot_forward_only_active_for_pe_method() -> None:
    idx = pd.date_range("2020-01-01", periods=400, freq="D")
    df = pd.DataFrame({"close": [100.0] * 400, "pb": [2.0] * 400}, index=idx)
    band = Band(
        mean=1.5,
        std=0.1,
        minus_2sd=1.3,
        minus_1sd=1.4,
        plus_1sd=1.6,
        plus_2sd=1.7,
        n_obs=400,
    )
    # forward_eps should be ignored for PB method
    snap = make_snapshot(df, "PB", band, forward_eps=99.0)
    assert snap is not None
    assert snap.forward_eps is None
    assert snap.forward_pe is None


def test_make_snapshot_returns_none_for_empty_frame() -> None:
    snap = make_snapshot(
        pd.DataFrame(columns=["close", "pe"]), "PE", Band(None, None, None, None, None, None, 0)
    )
    assert snap is None
