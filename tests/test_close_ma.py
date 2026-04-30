"""
Test close-price MA (moving average) indicators against database data.

Uses 2330 (TSMC) as the test target, fetching ~60 days from tw.daily_prices.
Verifies SMA, Pre-SMA, EMA calculations by comparing with manually
computed expected values.
"""

import numpy as np
from decimal import Decimal
from db.connection import get_cursor
from analysis.close import calculate_close

STOCK_ID = "2330"
DAYS = 60


def fetch_data() -> dict:
    """Fetch OHLCV data for 2330, oldest first."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT trade_date, open_price, high_price, low_price,
                   close_price, volume, turnover
            FROM tw.daily_prices
            WHERE stock_id = %s
              AND close_price IS NOT NULL
            ORDER BY trade_date ASC
            """,
            (STOCK_ID,),
        )
        rows = cur.fetchall()

    # Take last DAYS rows (so we have enough history)
    rows = rows[-DAYS:]

    dates = [r["trade_date"] for r in rows]
    close = np.array([float(r["close_price"]) for r in rows], dtype=np.float32)
    high = np.array([float(r["high_price"]) for r in rows], dtype=np.float32)
    low = np.array([float(r["low_price"]) for r in rows], dtype=np.float32)
    open_ = np.array([float(r["open_price"]) for r in rows], dtype=np.float32)
    volume = np.array([float(r["volume"]) for r in rows], dtype=np.float32)
    turnover = np.array([float(r["turnover"]) for r in rows], dtype=np.float32)

    return {
        "dates": dates,
        "close": close,
        "high": high,
        "low": low,
        "open": open_,
        "volume": volume,
        "turnover": turnover,
    }


def manual_sma(data: np.ndarray, period: int) -> np.ndarray:
    """Reference SMA: simple loop-based implementation."""
    n = len(data)
    out = np.zeros(n, dtype=np.float32)
    for i in range(period - 1, n):
        out[i] = np.mean(data[i - period + 1 : i + 1])
    return out


def manual_ema(data: np.ndarray, period: int) -> np.ndarray:
    """Reference EMA: simple loop-based implementation."""
    n = len(data)
    out = np.zeros(n, dtype=np.float32)
    k = 2.0 / (period + 1)
    out[0] = data[0]
    for i in range(1, n):
        out[i] = data[i] * k + out[i - 1] * (1 - k)
    return out


def test_sma(result, close):
    """Test SMA for all periods against manual reference."""
    print("=" * 60)
    print("SMA Test")
    print("=" * 60)

    n = len(close)
    all_pass = True

    for period in result.ma.sma:
        sma_arr = result.ma.sma[period]
        ref = manual_sma(close, period)

        # Only compare where both should have valid data
        start = period - 1
        if start >= n:
            print(f"  SMA({period:>3d}): SKIP (not enough data, need {period}, have {n})")
            continue

        valid = slice(start, n)
        max_diff = float(np.max(np.abs(sma_arr[valid] - ref[valid])))
        ok = max_diff < 0.01  # tolerance for float32

        status = "PASS" if ok else "FAIL"
        if not ok:
            all_pass = False

        # Show last 3 values
        last = min(3, n - start)
        calc_vals = [f"{sma_arr[n - i]:.2f}" for i in range(1, last + 1)]
        ref_vals = [f"{ref[n - i]:.2f}" for i in range(1, last + 1)]

        print(f"  SMA({period:>3d}): {status}  max_diff={max_diff:.4f}  "
              f"last={','.join(calc_vals)}  ref={','.join(ref_vals)}")

    return all_pass


def test_ema(result, close):
    """Test EMA for all periods against manual reference."""
    print()
    print("=" * 60)
    print("EMA Test")
    print("=" * 60)

    n = len(close)
    all_pass = True

    for period in result.ema:
        ema_arr = result.ema[period]
        ref = manual_ema(close, period)

        max_diff = float(np.max(np.abs(ema_arr - ref)))
        ok = max_diff < 0.01

        status = "PASS" if ok else "FAIL"
        if not ok:
            all_pass = False

        last_calc = [f"{ema_arr[n - i]:.2f}" for i in range(1, 4)]
        last_ref = [f"{ref[n - i]:.2f}" for i in range(1, 4)]

        print(f"  EMA({period:>3d}): {status}  max_diff={max_diff:.4f}  "
              f"last={','.join(last_calc)}  ref={','.join(last_ref)}")

    return all_pass


def test_close_on_ma(result, close):
    """Test CloseOnMA (close >= SMA) flag."""
    print()
    print("=" * 60)
    print("Close On MA Test")
    print("=" * 60)

    n = len(close)
    all_pass = True

    for period in [3, 5, 8, 13, 21, 34, 55]:
        flag = result.ma.close_on_ma[period]
        sma_arr = result.ma.sma[period]

        ref = close >= sma_arr
        mismatch = int(np.sum(flag != ref))
        ok = mismatch == 0

        status = "PASS" if ok else "FAIL"
        if not ok:
            all_pass = False

        # Show last day's values
        print(f"  CloseOnMA({period:>3d}): {status}  mismatches={mismatch}  "
              f"last: close={close[-1]:.2f} sma={sma_arr[-1]:.2f} flag={flag[-1]}")

    return all_pass


def test_bias(result, close):
    """Test bias ratio: (close - sma) / sma."""
    print()
    print("=" * 60)
    print("Bias Ratio Test")
    print("=" * 60)

    n = len(close)
    all_pass = True

    for period in [3, 5, 8, 13, 21]:
        bias_arr = result.ma.bias[period]
        sma_arr = result.ma.sma[period]

        ref = np.where(sma_arr != 0, (close - sma_arr) / sma_arr, 0).astype(np.float32)
        max_diff = float(np.max(np.abs(bias_arr - ref)))
        ok = max_diff < 1e-5

        status = "PASS" if ok else "FAIL"
        if not ok:
            all_pass = False

        print(f"  Bias({period:>3d}): {status}  max_diff={max_diff:.6f}  "
              f"last: bias={bias_arr[-1]:.4f} ref={ref[-1]:.4f}")

    return all_pass


def test_sort(result):
    """Test trend sort (up/down alignment)."""
    print()
    print("=" * 60)
    print("Trend Sort Test")
    print("=" * 60)

    all_pass = True
    sma_d = result.ma.sma

    # Normal sort: short=(3,8,21), medium=(5,13,34), long=(8,21,55)
    checks = {
        "short": (3, 8, 21),
        "medium": (5, 13, 34),
        "long": (8, 21, 55),
    }

    for label, (p1, p2, p3) in checks.items():
        sort_r = result.ma.sort_normal[label]
        ref_up = (sma_d[p1] > sma_d[p2]) & (sma_d[p2] > sma_d[p3])
        ref_down = (sma_d[p1] < sma_d[p2]) & (sma_d[p2] < sma_d[p3])

        up_ok = np.array_equal(sort_r.up, ref_up)
        down_ok = np.array_equal(sort_r.down, ref_down)
        ok = up_ok and down_ok

        status = "PASS" if ok else "FAIL"
        if not ok:
            all_pass = False

        print(f"  Sort Normal {label:>6s} ({p1},{p2},{p3}): {status}  "
              f"last: up={sort_r.up[-1]} down={sort_r.down[-1]}")

    return all_pass


def test_data_sanity(data):
    """Basic sanity check on fetched data."""
    print("=" * 60)
    print(f"Data: {STOCK_ID} ({len(data['dates'])} days)")
    print(f"  Period: {data['dates'][0]} ~ {data['dates'][-1]}")
    print(f"  Close range: {data['close'].min():.2f} ~ {data['close'].max():.2f}")
    print("=" * 60)
    print()


def main():
    data = fetch_data()
    test_data_sanity(data)

    close = data["close"]
    result = calculate_close(close)

    results = []
    results.append(("SMA", test_sma(result, close)))
    results.append(("EMA", test_ema(result, close)))
    results.append(("CloseOnMA", test_close_on_ma(result, close)))
    results.append(("Bias", test_bias(result, close)))
    results.append(("Sort", test_sort(result)))

    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    all_pass = True
    for name, ok in results:
        status = "PASS" if ok else "FAIL"
        if not ok:
            all_pass = False
        print(f"  {name:<15s}: {status}")

    print()
    if all_pass:
        print("All tests PASSED.")
    else:
        print("Some tests FAILED!")

    return 0 if all_pass else 1


if __name__ == "__main__":
    exit(main())
