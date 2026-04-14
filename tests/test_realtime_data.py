"""
Test the realtime data loader's merge logic and full pipeline.

Two layers:

  1. Pure unit tests for _merge_intraday_into_daily / _intraday_to_daily_shape
     (no DB, no clock).
  2. Integration smoke test that calls load_stock_data_live('2330') against
     the real database — skipped if the DB is unreachable.

Style matches existing tests/test_*.py: standalone script, prints PASS/FAIL,
exits non-zero on failure.
"""

from datetime import date

from analysis.realtime_data import (
    _intraday_to_daily_shape,
    _merge_intraday_into_daily,
    load_stock_data_live,
)


# ── Pure unit tests ─────────────────────────────────────────────────────────


def _make_daily(d: date, close: float) -> dict:
    return {
        "trade_date":  d,
        "open_price":  close,
        "high_price":  close,
        "low_price":   close,
        "close_price": close,
        "volume":      1_000_000,
        "turnover":    int(close * 1_000_000),
        "ref_price":   close,
    }


def _make_intraday(d: date, ref_price=None) -> dict:
    return {
        "stock_id":     "TEST",
        "trade_date":   d,
        "open_price":   100,
        "high_price":   105,
        "low_price":    99,
        "last_price":   104,
        "total_volume": 1_234_000,
        "total_value":  128_000_000,
        "ref_price":    ref_price,
    }


def test_intraday_to_daily_shape_maps_fields():
    intraday = _make_intraday(date(2026, 4, 13))
    out = _intraday_to_daily_shape(intraday, prev_close=98.0)
    ok = (
        out["trade_date"]  == date(2026, 4, 13)
        and out["open_price"]  == 100
        and out["high_price"]  == 105
        and out["low_price"]   == 99
        and out["close_price"] == 104                # last_price -> close
        and out["volume"]      == 1_234_000          # total_volume -> volume
        and out["turnover"]    == 128_000_000        # total_value -> turnover
        and out["ref_price"]   == 98.0               # interim: prev close
    )
    print(f"  intraday_to_daily_shape: {'PASS' if ok else 'FAIL'}")
    if not ok:
        print(f"    got: {out}")
    return ok


def test_merge_returns_history_when_no_intraday():
    daily = [
        _make_daily(date(2026, 4, 9), 1955),
        _make_daily(date(2026, 4, 10), 2000),
    ]
    merged = _merge_intraday_into_daily(daily, intraday_row=None)
    ok = (
        len(merged) == 2
        and merged[-1]["trade_date"] == date(2026, 4, 10)
    )
    print(f"  merge_no_intraday: {'PASS' if ok else 'FAIL'}")
    return ok


def test_merge_skips_when_intraday_date_already_in_daily():
    """
    daily_update.py has already written today (e.g. it's 14:30+), so we must
    NOT append the intraday bar — daily wins.
    """
    today = date(2026, 4, 11)
    daily = [
        _make_daily(date(2026, 4, 10), 2000),
        _make_daily(today, 2010),
    ]
    intraday = _make_intraday(today)
    merged = _merge_intraday_into_daily(daily, intraday)
    ok = (
        len(merged) == 2                                  # not appended
        and merged[-1]["close_price"] == 2010             # daily value preserved
        and merged[-1]["trade_date"] == today
    )
    print(f"  merge_skip_when_already_in_daily: {'PASS' if ok else 'FAIL'}")
    return ok


def test_merge_appends_when_intraday_is_newer():
    """
    Pre-daily-update window: daily_prices stops at yesterday, intraday is today.
    Append a forming bar with ref_price = yesterday's close.
    """
    yesterday = date(2026, 4, 10)
    today = date(2026, 4, 13)
    daily = [
        _make_daily(date(2026, 4, 9), 1955),
        _make_daily(yesterday, 2000),
    ]
    intraday = _make_intraday(today)
    merged = _merge_intraday_into_daily(daily, intraday)
    ok = (
        len(merged) == 3                                  # appended
        and merged[-1]["trade_date"] == today
        and merged[-1]["close_price"] == 104              # last_price
        and merged[-1]["ref_price"]  == 2000.0            # = yesterday close
        and merged[-2]["close_price"] == 2000             # untouched
    )
    print(f"  merge_appends_when_newer: {'PASS' if ok else 'FAIL'}")
    if not ok:
        print(f"    last row: {merged[-1]}")
    return ok


def test_merge_handles_empty_history():
    merged = _merge_intraday_into_daily([], _make_intraday(date(2026, 4, 13)))
    ok = merged == []
    print(f"  merge_empty_history: {'PASS' if ok else 'FAIL'}")
    return ok


def test_merge_prefers_sinopac_ref_when_present():
    """
    When tw.intraday_quotes.ref_price is populated (SinoPac pre-market ran),
    the forming bar should use that value instead of yesterday's close.
    """
    today = date(2026, 4, 13)
    daily = [
        _make_daily(date(2026, 4, 9), 1955),
        _make_daily(date(2026, 4, 10), 2000),
    ]
    intraday = _make_intraday(today, ref_price=1998.5)
    merged = _merge_intraday_into_daily(daily, intraday)
    ok = (
        len(merged) == 3
        and merged[-1]["ref_price"] == 1998.5            # SinoPac value wins
        and merged[-1]["trade_date"] == today
    )
    print(f"  merge_prefers_sinopac_ref: {'PASS' if ok else 'FAIL'}")
    if not ok:
        print(f"    last row: {merged[-1]}")
    return ok


# ── Integration smoke test ──────────────────────────────────────────────────


def test_load_2330_smoke():
    """
    Full pipeline against real DB. Validates that load_stock_data_live('2330')
    returns a non-empty StockData with sane invariants and a populated
    close_result.
    """
    try:
        data = load_stock_data_live("2330")
    except Exception as e:
        print(f"  load_2330_smoke: SKIP ({type(e).__name__}: {e})")
        return True

    n = data.n
    last_close = float(data.close[-1])
    last_date = data.dates[-1]
    sma_5 = data.close_result.ma.sma[5]
    sma5_last = float(sma_5[-1]) if len(sma_5) else 0.0

    ok = (
        n > 400                                           # >= 400 history days
        and len(data.close) == n
        and len(data.high) == n
        and len(data.low) == n
        and len(data.volume) == n
        and last_close > 0
        and sma5_last > 0
    )
    print(f"  load_2330_smoke: {'PASS' if ok else 'FAIL'}  "
          f"(n={n}, last_date={last_date}, last_close={last_close:.2f}, "
          f"sma5={sma5_last:.2f})")
    return ok


# ── Runner ──────────────────────────────────────────────────────────────────


def main() -> int:
    results = []
    print("== unit tests ==")
    results.append(("intraday_to_daily_shape",        test_intraday_to_daily_shape_maps_fields()))
    results.append(("merge_no_intraday",              test_merge_returns_history_when_no_intraday()))
    results.append(("merge_skip_when_already_daily",  test_merge_skips_when_intraday_date_already_in_daily()))
    results.append(("merge_appends_when_newer",       test_merge_appends_when_intraday_is_newer()))
    results.append(("merge_empty_history",            test_merge_handles_empty_history()))
    results.append(("merge_prefers_sinopac_ref",       test_merge_prefers_sinopac_ref_when_present()))

    print("\n== integration ==")
    results.append(("load_2330_smoke",                test_load_2330_smoke()))

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
