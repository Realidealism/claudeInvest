"""M1 verification: read-only enforcement + tier-sum integrity.

Run from project root: python -m pytest tests/chip_model/

Tier-sum reality (see DATA_MAP.md): sum(t1..t16_shares) == t17_shares holds
EXACTLY for the median common stock and within 0.01% for ~99% of rows, but a
~1% tail is off by up to ~0.76% because TDCC's 差異數調整 (t16) does not always
fully close. Non-common securities (ETFs) can be off by up to ~40%. The model
uses the official t15_pct / t17_shares directly, so this residual is a data
sanity gate only, not a correctness dependency.
"""
from datetime import date

import numpy as np
import psycopg2
import pytest

from chip_model.db_access import COMMON_STOCK_TYPE, get_ro_cursor

_TIER_SUM = " + ".join(f"COALESCE(t{i}_shares, 0)" for i in range(1, 17))

# Two rows in the 2026-07-03 snapshot are corrupt: their tier sum exceeds
# t17_shares by 8.48% (4440) and 4.42% (6624). Both stocks reconcile exactly
# on every other snapshot before and after, so this is a bad scrape of that
# one weekly file rather than anything about those stocks.
#
# It cannot be repaired: TDCC's endpoint only serves the current snapshot, so
# re-running the scraper for that date fetches the latest week instead.
#
# Excluded by identity rather than by loosening the bound, so the invariant
# keeps its full strength for every other row and a NEW corruption still
# fails the test. test_known_corrupt_rows_still_corrupt below tracks them.
_KNOWN_CORRUPT = {("4440", date(2026, 7, 3)), ("6624", date(2026, 7, 3))}


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
            SELECT sd.stock_id, sd.data_date,
                   abs(({_TIER_SUM}) - sd.t17_shares)::numeric / sd.t17_shares AS rel
            FROM tw.shareholder_distribution sd
            JOIN tw.stocks s ON s.stock_id = sd.stock_id
            WHERE sd.t17_shares > 0 AND s.security_type = %s
            """,
            (COMMON_STOCK_TYPE,),
        )
        rows = [r for r in cur.fetchall()
                if (r["stock_id"], r["data_date"]) not in _KNOWN_CORRUPT]

    rel = np.array([float(r["rel"]) for r in rows])
    assert rel.size > 1000, "common-stock universe unexpectedly small"
    assert np.median(rel) == 0.0                       # median reconciles exactly
    assert (rel < 1e-4).mean() >= 0.98                 # ~99% within 0.01%
    assert np.percentile(rel, 99) < 1e-3               # 99th pct within 0.1%

    worst = max(rows, key=lambda r: float(r["rel"]))
    assert float(worst["rel"]) < 0.02, (               # bounded tail (<2%)
        f"new corrupt row: {worst['stock_id']} on {worst['data_date']} "
        f"is off by {float(worst['rel']):.4%}"
    )


def test_known_corrupt_rows_are_still_the_only_exception():
    """Tracks the excluded rows so the exclusion cannot outlive the problem.

    Written as a plain assertion rather than xfail: an xfail swallows *any*
    exception, so a broken query inside one still reports as expected-failure
    and proves nothing. This fails loudly if the data is ever repaired, which
    is the signal to delete _KNOWN_CORRUPT and its exclusion above.
    """
    with get_ro_cursor() as cur:
        cur.execute(
            f"""
            SELECT stock_id, data_date,
                   abs(({_TIER_SUM}) - t17_shares)::numeric / t17_shares AS rel
            FROM tw.shareholder_distribution
            WHERE t17_shares > 0
            """
        )
        still_bad = {
            (r["stock_id"], r["data_date"])
            for r in cur.fetchall()
            if float(r["rel"]) >= 0.02
        }

    repaired = _KNOWN_CORRUPT - still_bad
    assert not repaired, (
        f"{sorted(repaired)} now reconcile — drop them from _KNOWN_CORRUPT "
        f"and from the exclusion in test_tier_sum_matches_total"
    )
