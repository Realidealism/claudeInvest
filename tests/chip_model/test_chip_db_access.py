"""M1 verification: read-only enforcement + tier-sum integrity.

Run from project root: python -m pytest tests/chip_model/

Tier-sum reality (see DATA_MAP.md): sum(t1..t16_shares) == t17_shares holds
EXACTLY for the median common stock and within 0.01% for ~99% of rows, but a
~1% tail is off by up to ~0.76% because TDCC's 差異數調整 (t16) does not always
fully close. Non-common securities (ETFs) can be off by up to ~40%. The model
uses the official t15_pct / t17_shares directly, so this residual is a data
sanity gate only, not a correctness dependency.
"""
import numpy as np
import psycopg2
import pytest

from chip_model.db_access import COMMON_STOCK_TYPE, get_ro_cursor

_TIER_SUM = " + ".join(f"COALESCE(t{i}_shares, 0)" for i in range(1, 17))


def test_readonly_rejects_write():
    """A write inside a read-only session must raise; no row is ever inserted."""
    with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction):
        with get_ro_cursor() as cur:
            cur.execute(
                "INSERT INTO tw.shareholder_distribution (stock_id, data_date) "
                "VALUES ('ROPROBE', DATE '1900-01-01')"
            )


def test_tier_sum_matches_total():
    """sum(t1..t16_shares) reconciles to t17_shares for the common-stock universe.

    Asserts the realistic invariant: exact for the median, within 0.01% for the
    bulk, with only a small bounded tail. t16 (差異數調整) is INCLUDED.
    """
    with get_ro_cursor() as cur:
        cur.execute(
            f"""
            SELECT abs(({_TIER_SUM}) - sd.t17_shares)::numeric / sd.t17_shares AS rel
            FROM tw.shareholder_distribution sd
            JOIN tw.stocks s ON s.stock_id = sd.stock_id
            WHERE sd.t17_shares > 0 AND s.security_type = %s
            """,
            (COMMON_STOCK_TYPE,),
        )
        rel = np.array([float(r["rel"]) for r in cur.fetchall()])

    assert rel.size > 1000, "common-stock universe unexpectedly small"
    assert np.median(rel) == 0.0                       # median reconciles exactly
    assert (rel < 1e-4).mean() >= 0.98                 # ~99% within 0.01%
    assert np.percentile(rel, 99) < 1e-3               # 99th pct within 0.1%
    assert rel.max() < 0.02                            # bounded tail (<2%)
