"""Tests for the scalar clamp used by the continuous ScoreBoard cells.

It replaced np.clip, which costs about 2.4us on a Python scalar and was 77%
of _zscore_close_vs_sma. Two cells route their final value through this, so
any divergence from np.clip would shift scores, and scores drive pct, which
drives every gate threshold the signals are tuned against.
"""
import math

import numpy as np
import pytest

from analysis.score import _clip


@pytest.mark.parametrize("x", [
    0.0, -0.0, 1.0, -1.0, 9.999999, -9.999999, 10.0, -10.0,
    10.0000001, -10.0000001, 1e300, -1e300, 1e-300, -1e-300,
    3.0, -7.5, 0.1, -0.1,
])
def test_matches_np_clip(x):
    assert _clip(x, -10.0, 10.0) == float(np.clip(x, -10.0, 10.0))


@pytest.mark.parametrize("lo,hi", [(-10.0, 10.0), (-3.0, 3.0), (0.0, 1.0), (-1.5, 2.5)])
def test_matches_np_clip_across_bounds(lo, hi):
    rng = np.random.default_rng(3)
    for x in rng.normal(0, 5, 500):
        assert _clip(float(x), lo, hi) == float(np.clip(float(x), lo, hi))


def test_nan_propagates_like_np_clip():
    """Every comparison against NaN is False, so both branches must fall
    through -- the same thing np.clip does."""
    got = _clip(float("nan"), -10.0, 10.0)
    want = float(np.clip(float("nan"), -10.0, 10.0))
    assert math.isnan(got) and math.isnan(want)


@pytest.mark.parametrize("x,expected", [
    (float("inf"), 10.0),
    (float("-inf"), -10.0),
])
def test_infinities_clamp(x, expected):
    assert _clip(x, -10.0, 10.0) == expected == float(np.clip(x, -10.0, 10.0))


def test_returns_a_plain_float():
    """np.clip returned a numpy scalar that callers wrapped in float()."""
    assert type(_clip(1.5, -10.0, 10.0)) is float
    assert type(_clip(99.0, -10.0, 10.0)) is float
