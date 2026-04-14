"""
Test the SinoPac snapshot fallback normalizer.

Pure unit tests only — no network, no DB. Verifies that _normalize_snapshot
produces the same dict shape that store.upsert_quotes expects.
"""

from types import SimpleNamespace

from intraday.sinopac_snapshot import _normalize_snapshot


def _fake_snap(code="2330", open=1975.0, high=2000.0, low=1970.0, close=2000.0,
               volume=196, total_volume=28504, total_amount=56641325000,
               change_price=45.0, change_rate=2.3):
    return SimpleNamespace(
        code=code,
        open=open,
        high=high,
        low=low,
        close=close,
        volume=volume,
        total_volume=total_volume,
        total_amount=total_amount,
        change_price=change_price,
        change_rate=change_rate,
    )


def test_normalize_happy_path():
    row = _normalize_snapshot(_fake_snap())
    ok = (
        row is not None
        and row["stock_id"]     == "2330"
        and row["open_price"]   == 1975.0
        and row["high_price"]   == 2000.0
        and row["low_price"]    == 1970.0
        and row["last_price"]   == 2000.0
        and row["total_volume"] == 28504 * 1000  # lots → shares
        and row["total_value"]  == 56641325000
        and row["change_price"] == 45.0
        and row["change_pct"]   == 2.3
        and row["limit_up"]     is None   # don't overwrite pre-market values
        and row["limit_down"]   is None
    )
    print(f"  normalize_happy_path: {'PASS' if ok else 'FAIL'}")
    if not ok:
        print(f"    got: {row}")
    return ok


def test_normalize_drops_zero_close():
    row = _normalize_snapshot(_fake_snap(close=0.0))
    ok = row is None
    print(f"  normalize_drops_zero_close: {'PASS' if ok else 'FAIL'}")
    return ok


def test_normalize_drops_blank_code():
    row = _normalize_snapshot(_fake_snap(code=""))
    ok = row is None
    print(f"  normalize_drops_blank_code: {'PASS' if ok else 'FAIL'}")
    return ok


def test_normalize_volume_lots_to_shares():
    """total_volume in Shioaji is lots; we multiply by 1000."""
    row = _normalize_snapshot(_fake_snap(total_volume=100))
    ok = row is not None and row["total_volume"] == 100_000
    print(f"  normalize_volume_lots_to_shares: {'PASS' if ok else 'FAIL'}")
    return ok


def test_normalize_zero_volume():
    row = _normalize_snapshot(_fake_snap(total_volume=0))
    ok = row is not None and row["total_volume"] is None
    print(f"  normalize_zero_volume: {'PASS' if ok else 'FAIL'}")
    return ok


# ── Runner ──────────────────────────────────────────────────────────────────


def main() -> int:
    results = []
    print("== sinopac_snapshot unit tests ==")
    results.append(("normalize_happy_path",        test_normalize_happy_path()))
    results.append(("normalize_drops_zero_close",  test_normalize_drops_zero_close()))
    results.append(("normalize_drops_blank_code",  test_normalize_drops_blank_code()))
    results.append(("normalize_vol_lots_to_shares", test_normalize_volume_lots_to_shares()))
    results.append(("normalize_zero_volume",        test_normalize_zero_volume()))

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    all_pass = True
    for name, ok in results:
        status = "PASS" if ok else "FAIL"
        if not ok:
            all_pass = False
        print(f"  {name:<35s}: {status}")
    print()
    print("All tests PASSED." if all_pass else "Some tests FAILED!")
    return 0 if all_pass else 1


if __name__ == "__main__":
    exit(main())
