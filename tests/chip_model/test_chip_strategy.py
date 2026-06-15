"""M3 verification: consensus-rank rule correctness + determinism + universe."""
from datetime import date

import pandas as pd

from chip_model.strategy import Rule, generate_signals, pick_for_date

W = date(2025, 2, 1)


def _metrics():
    """Single week; consensus order is A > B > C > D across all 3 dimensions."""
    rows = []
    for sid, big, ret, hld in [
        ("A", 5.0, 5.0, 5.0),
        ("B", 4.0, 4.0, 4.0),
        ("C", 3.0, 3.0, 3.0),
        ("D", 1.0, 1.0, 1.0),
    ]:
        rows.append({"stock_id": sid, "data_date": W, "ratio": 50.0,
                     "d_big": big, "d_retail": ret, "d_holders": hld})
    return pd.DataFrame(rows)


def test_consensus_top_n():
    sig = generate_signals(_metrics(), Rule(top_n=2), {"A", "B", "C", "D"})
    assert list(sig["stock_id"]) == ["A", "B"]


def test_universe_filter():
    sig = generate_signals(_metrics(), Rule(top_n=10), {"A", "C"})
    assert set(sig["stock_id"]) == {"A", "C"}


def test_drops_incomplete_dimensions():
    m = _metrics()
    m.loc[m["stock_id"] == "A", "d_holders"] = None  # A missing a dimension
    sig = generate_signals(m, Rule(top_n=10), {"A", "B", "C", "D"})
    assert "A" not in set(sig["stock_id"])


def test_consensus_beats_single_outlier():
    """A wins 2 of 3 dims; B leads only one. Consensus should rank A above B."""
    m = pd.DataFrame([
        {"stock_id": "A", "data_date": W, "ratio": 50.0,
         "d_big": 9.0, "d_retail": 9.0, "d_holders": 0.0},
        {"stock_id": "B", "data_date": W, "ratio": 50.0,
         "d_big": 0.0, "d_retail": 0.0, "d_holders": 9.0},
    ])
    sig = generate_signals(m, Rule(top_n=1), {"A", "B"})
    assert list(sig["stock_id"]) == ["A"]


def test_deterministic():
    a = generate_signals(_metrics(), Rule(top_n=3), {"A", "B", "C", "D"})
    b = generate_signals(_metrics(), Rule(top_n=3), {"A", "B", "C", "D"})
    pd.testing.assert_frame_equal(a, b)


def test_pick_for_date():
    sig = generate_signals(_metrics(), Rule(top_n=3), {"A", "B", "C"})
    assert set(pick_for_date(sig, W)["data_date"]) == {W}
