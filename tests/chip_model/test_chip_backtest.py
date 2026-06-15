"""M4 verification: look-ahead safety + return math (synthetic prices)."""
from datetime import date, timedelta

import pandas as pd

import chip_model.backtest as bt
from chip_model.backtest import _close_after, run_backtest


def test_close_after_strict_vs_nonstrict():
    dates = [date(2025, 1, d) for d in (1, 8, 15)]
    closes = [10.0, 11.0, 12.0]
    # strict: first date > target
    assert _close_after(dates, closes, date(2025, 1, 8), strict=True) == (date(2025, 1, 15), 12.0)
    # non-strict: first date >= target
    assert _close_after(dates, closes, date(2025, 1, 8), strict=False) == (date(2025, 1, 8), 11.0)
    # past the end
    assert _close_after(dates, closes, date(2025, 2, 1), strict=False) == (None, None)


def _daily(stock_id, start, days, price_fn):
    rows = []
    for i in range(days):
        d = start + timedelta(days=i)
        if d.weekday() < 5:  # trading days only (Mon-Fri)
            rows.append({"stock_id": stock_id, "trade_date": d,
                         "close_price": price_fn(i)})
    return rows


def test_no_lookahead_and_returns(monkeypatch):
    snap = date(2025, 1, 3)  # a Friday snapshot
    signals = pd.DataFrame(
        {"stock_id": ["X"], "data_date": [snap],
         "ratio": [60.0], "ratio_chg": [2.0], "consec_up": [3]}
    )

    # Stock X: flat 100 then steps up; benchmark flat 1000.
    px = pd.DataFrame(_daily("X", snap - timedelta(days=5), 120,
                             lambda i: 100.0 + i))
    bench = pd.DataFrame(
        [{"trade_date": r["trade_date"], "close_price": 1000.0}
         for r in _daily("IDX", snap - timedelta(days=5), 120, lambda i: 0)]
    )

    monkeypatch.setattr(bt, "load_prices", lambda sids, s, e: px)
    monkeypatch.setattr(bt, "load_benchmark", lambda idx, s, e: bench)

    out = run_backtest(signals, horizons=(4, 8))
    trades = out["trades"]
    assert len(trades) == 1
    t = trades.iloc[0]

    # Look-ahead safety: entry strictly AFTER the snapshot date.
    assert t["entry_date"] > snap

    # Benchmark flat -> excess return equals raw return.
    for h in (4, 8):
        assert t[f"ret_{h}w"] is not None
        assert abs(t[f"excess_{h}w"] - t[f"ret_{h}w"]) < 1e-9

    # Return sign: prices strictly rising -> positive return.
    assert t["ret_4w"] > 0
    s = out["summary"]["horizons"][4]
    assert s["win_rate"] == 1.0


def test_empty_signals():
    out = run_backtest(pd.DataFrame(
        {"stock_id": [], "data_date": []}
    ))
    assert out["trades"].empty
    assert out["summary"]["n_trades"] == 0
