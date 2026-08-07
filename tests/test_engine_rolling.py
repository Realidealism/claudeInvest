"""Equivalence tests for the boundary-clamped rolling extremes.

These feed every defense price in every backtest, so a discrepancy would not
announce itself -- it would quietly shift entry and exit levels and change
which version looks better. The vectorised implementation is therefore
checked against the original per-bar loop, kept verbatim below as the
reference, over a large spread of shapes rather than a few hand-picked cases.
"""
import numpy as np
import pytest

from signal_backtest.engine import (
    _compute_last_boundary,
    rolling_highest_safe,
    rolling_lowest_safe,
)


def _reference(arr, period, last_boundary, lowest):
    """The original implementation, unchanged."""
    n = len(arr)
    out = np.full(n, np.nan, dtype=arr.dtype)
    for i in range(n):
        start = max(0, i - period + 1)
        b = int(last_boundary[i])
        if b > start:
            start = b
        window = arr[start:i + 1]
        out[i] = window.min() if lowest else window.max()
    return out


def _boundaries_from(flags, n):
    """Build a last_boundary array the same way _compute_last_boundary does."""
    last = np.full(n, -1, dtype=np.int32)
    cur = -1
    for i in range(1, n):
        if flags[i - 1]:
            cur = i
        last[i] = cur
    return last


@pytest.mark.parametrize("lowest", [True, False])
@pytest.mark.parametrize("period", [1, 2, 3, 5, 8, 21, 55])
@pytest.mark.parametrize("n", [0, 1, 2, 7, 60, 500])
@pytest.mark.parametrize("gap_rate", [0.0, 0.002, 0.05, 0.5])
def test_matches_the_per_bar_reference(lowest, period, n, gap_rate):
    rng = np.random.default_rng(hash((lowest, period, n, int(gap_rate * 1000))) % 2**32)
    arr = (rng.random(n) * 100).astype(np.float32)
    flags = rng.random(max(n - 1, 0)) < gap_rate
    last_boundary = _boundaries_from(flags, n)

    fn = rolling_lowest_safe if lowest else rolling_highest_safe
    got = fn(arr, period, last_boundary)
    want = _reference(arr, period, last_boundary, lowest)

    assert got.shape == want.shape
    assert got.dtype == arr.dtype
    if n:
        np.testing.assert_array_equal(got, want)


@pytest.mark.parametrize("lowest", [True, False])
def test_adjacent_boundaries_let_the_later_one_win(lowest):
    """Two gaps inside one window is where an ordering mistake would show."""
    n = 40
    arr = np.arange(n, dtype=np.float32)[::-1].copy()   # strictly decreasing
    flags = np.zeros(n - 1, dtype=bool)
    flags[9] = flags[11] = flags[12] = True             # boundaries at 10, 12, 13
    last_boundary = _boundaries_from(flags, n)

    fn = rolling_lowest_safe if lowest else rolling_highest_safe
    np.testing.assert_array_equal(
        fn(arr, 21, last_boundary),
        _reference(arr, 21, last_boundary, lowest),
    )


@pytest.mark.parametrize("lowest", [True, False])
def test_boundary_on_the_final_bar(lowest):
    n = 30
    arr = (np.arange(n, dtype=np.float32) % 7)
    flags = np.zeros(n - 1, dtype=bool)
    flags[-1] = True                                     # boundary at n-1
    last_boundary = _boundaries_from(flags, n)

    fn = rolling_lowest_safe if lowest else rolling_highest_safe
    np.testing.assert_array_equal(
        fn(arr, 8, last_boundary),
        _reference(arr, 8, last_boundary, lowest),
    )


@pytest.mark.parametrize("lowest", [True, False])
def test_period_longer_than_the_series(lowest):
    n = 5
    arr = np.array([3, 1, 4, 1, 5], dtype=np.float32)
    last_boundary = np.full(n, -1, dtype=np.int32)
    fn = rolling_lowest_safe if lowest else rolling_highest_safe
    np.testing.assert_array_equal(
        fn(arr, 99, last_boundary),
        _reference(arr, 99, last_boundary, lowest),
    )


def test_against_boundaries_derived_from_real_price_shapes():
    """Uses _compute_last_boundary itself rather than a synthetic flag array."""
    rng = np.random.default_rng(7)
    close = np.cumprod(1 + rng.normal(0, 0.02, 800)).astype(np.float32) * 100
    close[200] *= 1.4          # reverse-split style jump
    close[201:] *= 1.4
    close[600] *= 0.6
    close[601:] *= 0.6
    last_boundary = _compute_last_boundary(close)
    assert (last_boundary >= 0).any(), "expected the synthetic gaps to register"

    low = close * 0.99
    high = close * 1.01
    for period in (8, 21):
        np.testing.assert_array_equal(
            rolling_lowest_safe(low, period, last_boundary),
            _reference(low, period, last_boundary, True),
        )
        np.testing.assert_array_equal(
            rolling_highest_safe(high, period, last_boundary),
            _reference(high, period, last_boundary, False),
        )
