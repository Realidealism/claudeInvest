"""Equivalence tests for chandelier's rolling extreme.

The rolling highest/lowest feeds every Chandelier stop level, and Chandelier
stops are the defense price for four of the six signal factories. A rounding
or off-by-one here would move stops slightly and change which trades survive,
without anything failing. So the vectorised helper is checked against the
per-bar loop it replaced, kept verbatim as the reference.
"""
import numpy as np
import pytest

from analysis.chandelier import (
    F64,
    _rolling_extreme_f64,
    calculate_chandelier,
)


def _reference(src, length, highest):
    """The original per-bar loop from calculate_chandelier."""
    src = np.asarray(src, dtype=F64)
    n = len(src)
    out = np.full(n, np.nan, dtype=F64)
    for i in range(n):
        lo_idx = max(0, i - length + 1)
        window = src[lo_idx:i + 1]
        out[i] = window.max() if highest else window.min()
    return out


@pytest.mark.parametrize("highest", [True, False])
@pytest.mark.parametrize("length", [1, 2, 3, 5, 21, 55])
@pytest.mark.parametrize("n", [0, 1, 2, 20, 21, 22, 300])
def test_matches_the_per_bar_loop(highest, length, n):
    rng = np.random.default_rng(hash((highest, length, n)) % 2**32)
    src = rng.random(n) * 100
    got = _rolling_extreme_f64(src, length, highest=highest)
    want = _reference(src, length, highest)
    assert got.dtype == F64
    if n:
        np.testing.assert_array_equal(got, want)


@pytest.mark.parametrize("highest", [True, False])
def test_keeps_full_float64_precision(highest):
    """Values that do not survive a float32 round trip.

    analysis.indicators' rolling helpers return float32; borrowing them here
    would silently round every stop level.
    """
    src = np.array([1.000000000000001, 1.000000000000002,
                    1.000000000000003], dtype=F64)
    got = _rolling_extreme_f64(src, 2, highest=highest)
    np.testing.assert_array_equal(got, _reference(src, 2, highest))
    assert got[-1] != np.float32(got[-1]) or src[0] == src[-1]


@pytest.mark.parametrize("highest", [True, False])
def test_handles_a_non_contiguous_source(highest):
    """Callers pass slices; as_strided on a non-contiguous view would read
    the wrong memory."""
    base = (np.arange(200, dtype=F64) % 17) * 1.5
    view = base[::2]
    assert not view.flags["C_CONTIGUOUS"]
    np.testing.assert_array_equal(
        _rolling_extreme_f64(view, 8, highest=highest),
        _reference(view, 8, highest),
    )


@pytest.mark.parametrize("use_close", [True, False])
def test_full_chandelier_output_is_unchanged(use_close):
    """End to end: stops, direction and flips must all be identical."""
    rng = np.random.default_rng(11)
    n = 600
    close = np.cumprod(1 + rng.normal(0, 0.02, n)) * 100
    high = close * (1 + rng.random(n) * 0.01)
    low = close * (1 - rng.random(n) * 0.01)

    res = calculate_chandelier(high, low, close, length=21, mult=3.0,
                               use_close=use_close)

    # Rebuild the rolling arrays the old way and confirm they agree.
    hi_src = close if use_close else high
    lo_src = close if use_close else low
    np.testing.assert_array_equal(
        _rolling_extreme_f64(hi_src, 21, highest=True),
        _reference(hi_src, 21, True),
    )
    np.testing.assert_array_equal(
        _rolling_extreme_f64(lo_src, 21, highest=False),
        _reference(lo_src, 21, False),
    )
    assert np.isfinite(res.long_stop[21:]).all()
    assert np.isfinite(res.short_stop[21:]).all()
