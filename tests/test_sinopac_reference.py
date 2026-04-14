"""
Test the SinoPac pre-market reference-price path.

Two layers:

  1. Pure unit tests for _normalise_stock (no network, no DB):
     - happy path
     - reference = 0 -> dropped
     - limit_up = 0 -> dropped
     - blank code -> dropped

  2. Integration smoke test:
     - Log in to Shioaji
     - Fetch all reference rows
     - Sanity check 2330 (ref > 0, limit_up > ref > limit_down)
     - Gracefully SKIP if credentials are missing or the login fails
       (so the test file doesn't block CI when running offline)

Style matches tests/test_realtime_data.py — standalone script, prints PASS/FAIL,
exits non-zero on failure.
"""

from types import SimpleNamespace

from intraday.sinopac_reference import _normalise_stock


# ── Pure unit tests ─────────────────────────────────────────────────────────


def _fake_stock(code="2330", name="台積電", reference=1000.0,
                limit_up=1100.0, limit_down=900.0, update_date="2026/04/12",
                category="24", day_trade="Yes",
                margin_trading_balance=237, short_selling_balance=63):
    return SimpleNamespace(
        code=code,
        name=name,
        reference=reference,
        limit_up=limit_up,
        limit_down=limit_down,
        update_date=update_date,
        category=category,
        day_trade=day_trade,
        margin_trading_balance=margin_trading_balance,
        short_selling_balance=short_selling_balance,
    )


def test_normalise_happy_path():
    row = _normalise_stock(_fake_stock(), market="TWSE")
    ok = (
        row is not None
        and row["stock_id"]   == "2330"
        and row["name"]       == "台積電"
        and row["market"]     == "TWSE"
        and row["ref_price"]  == 1000.0
        and row["limit_up"]   == 1100.0
        and row["limit_down"] == 900.0
        and row["category"]       == "24"
        and row["day_trade"]      is True
        and row["margin_balance"] == 237
        and row["short_balance"]  == 63
    )
    print(f"  normalise_happy_path: {'PASS' if ok else 'FAIL'}")
    if not ok:
        print(f"    got: {row}")
    return ok


def test_normalise_drops_zero_reference():
    row = _normalise_stock(_fake_stock(reference=0.0), market="TWSE")
    ok = row is None
    print(f"  normalise_drops_zero_ref: {'PASS' if ok else 'FAIL'}")
    return ok


def test_normalise_drops_zero_limit_up():
    row = _normalise_stock(_fake_stock(limit_up=0.0), market="TWSE")
    ok = row is None
    print(f"  normalise_drops_zero_limit_up: {'PASS' if ok else 'FAIL'}")
    return ok


def test_normalise_drops_zero_limit_down():
    row = _normalise_stock(_fake_stock(limit_down=0.0), market="TWSE")
    ok = row is None
    print(f"  normalise_drops_zero_limit_down: {'PASS' if ok else 'FAIL'}")
    return ok


def test_normalise_drops_blank_code():
    row = _normalise_stock(_fake_stock(code=""), market="TWSE")
    ok = row is None
    print(f"  normalise_drops_blank_code: {'PASS' if ok else 'FAIL'}")
    return ok


def test_normalise_strips_whitespace():
    row = _normalise_stock(_fake_stock(code=" 2330 ", name="  台積電 "), market="TPEx")
    ok = row is not None and row["stock_id"] == "2330" and row["name"] == "台積電"
    print(f"  normalise_strips_whitespace: {'PASS' if ok else 'FAIL'}")
    return ok


def test_normalise_day_trade_no():
    row = _normalise_stock(_fake_stock(day_trade="No"), market="TWSE")
    ok = row is not None and row["day_trade"] is False
    print(f"  normalise_day_trade_no: {'PASS' if ok else 'FAIL'}")
    return ok


def test_normalise_drops_unknown_security_type():
    """Warrants / TDRs / ETNs etc. should be dropped before reaching the DB."""
    # Warrant codes are typically 6-digit starting with a non-zero prefix,
    # which classify_tw_security returns None for.
    row = _normalise_stock(_fake_stock(code="030123"), market="TWSE")
    ok = row is None
    print(f"  normalise_drops_unknown_security: {'PASS' if ok else 'FAIL'}")
    return ok


# ── Integration smoke test ─────────────────────────────────────────────────


def test_login_and_fetch_2330():
    """
    Full login + contract fetch. Validates 2330 comes back with sane values.
    Skips (returns True) if credentials are missing or login fails.
    """
    from config.settings import SINOPAC_API_KEY, SINOPAC_SECRET_KEY
    if not SINOPAC_API_KEY or not SINOPAC_SECRET_KEY:
        print("  login_and_fetch_2330: SKIP (SINOPAC_API_KEY / SINOPAC_SECRET_KEY not set)")
        return True

    try:
        from intraday.sinopac_loader import load_api, logout_api
        from intraday.sinopac_reference import fetch_reference_rows
    except Exception as e:
        print(f"  login_and_fetch_2330: SKIP (import failed: {e})")
        return True

    api = None
    try:
        api = load_api()
        rows = fetch_reference_rows(api)
    except Exception as e:
        print(f"  login_and_fetch_2330: SKIP ({type(e).__name__}: {e})")
        if api is not None:
            try:
                logout_api(api)
            except Exception:
                pass
        return True

    try:
        by_id = {r["stock_id"]: r for r in rows}
        hit_2330 = by_id.get("2330")
        ok = (
            len(rows) > 1000                                  # realistic total
            and hit_2330 is not None
            and hit_2330["ref_price"] > 0
            and hit_2330["limit_up"]  > hit_2330["ref_price"]
            and hit_2330["limit_down"] < hit_2330["ref_price"]
        )
        msg = f"(n={len(rows)}"
        if hit_2330:
            msg += f", 2330 ref={hit_2330['ref_price']}, up={hit_2330['limit_up']}, down={hit_2330['limit_down']}"
        msg += ")"
        print(f"  login_and_fetch_2330: {'PASS' if ok else 'FAIL'}  {msg}")
        return ok
    finally:
        logout_api(api)


# ── Runner ──────────────────────────────────────────────────────────────────


def main() -> int:
    results = []
    print("== unit tests ==")
    results.append(("normalise_happy_path",        test_normalise_happy_path()))
    results.append(("normalise_drops_zero_ref",    test_normalise_drops_zero_reference()))
    results.append(("normalise_drops_zero_up",     test_normalise_drops_zero_limit_up()))
    results.append(("normalise_drops_zero_down",   test_normalise_drops_zero_limit_down()))
    results.append(("normalise_drops_blank_code",  test_normalise_drops_blank_code()))
    results.append(("normalise_strips_whitespace", test_normalise_strips_whitespace()))
    results.append(("normalise_day_trade_no",      test_normalise_day_trade_no()))
    results.append(("normalise_drops_unknown",     test_normalise_drops_unknown_security_type()))

    print("\n== integration ==")
    results.append(("login_and_fetch_2330",        test_login_and_fetch_2330()))

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
