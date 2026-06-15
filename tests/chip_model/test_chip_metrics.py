"""M2 verification: golden ratio for 2330 + window-delta math."""
from datetime import date, timedelta

import pandas as pd

from chip_model.db_access import get_ro_cursor
from chip_model.metrics import WINDOW_WEEKS, compute_metrics


def test_metrics_golden_2330():
    """2330 latest-week ratio must match the official t15_pct."""
    with get_ro_cursor() as cur:
        cur.execute(
            "SELECT t15_pct FROM tw.shareholder_distribution "
            "WHERE stock_id = '2330' ORDER BY data_date DESC LIMIT 1"
        )
        official = float(cur.fetchone()["t15_pct"])

    m = compute_metrics()
    latest = m[m["stock_id"] == "2330"].sort_values("data_date").iloc[-1]
    assert abs(latest["ratio"] - official) < 1e-6


def _synthetic():
    """One stock, WINDOW_WEEKS+1 weeks, linearly changing tiers."""
    weeks = [date(2025, 1, 1) + timedelta(weeks=i) for i in range(WINDOW_WEEKS + 1)]
    rows = []
    for i, wk in enumerate(weeks):
        rows.append({
            "stock_id": "X", "data_date": wk,
            "t1_pct": 10.0 - i, "t2_pct": 5.0, "t3_pct": 5.0,      # retail falling 1/wk
            "t14_pct": 2.0, "t15_pct": 40.0 + 2 * i,               # big rising 2/wk
            "t15_holders": 100 + 3 * i,                            # holders +3/wk
        })
    return pd.DataFrame(rows)


def test_window_deltas():
    m = compute_metrics(_synthetic()).sort_values("data_date")
    last = m.iloc[-1]
    k = WINDOW_WEEKS
    # big = t14+t15 rose 2/wk over k weeks
    assert last["d_big"] == 2.0 * k
    # retail = t1+t2+t3 fell 1/wk; d_retail is the *decrease* (positive)
    assert last["d_retail"] == 1.0 * k
    # holders rose 3/wk
    assert last["d_holders"] == 3.0 * k
    # earlier weeks have no full window -> NaN
    assert m.iloc[0][["d_big", "d_retail", "d_holders"]].isna().all()


def test_deterministic():
    a = compute_metrics(_synthetic())
    b = compute_metrics(_synthetic())
    pd.testing.assert_frame_equal(a, b)
