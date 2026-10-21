"""Total-return accounting on raw prices (user ruling 2026-10-21, plan A):
signals and fills stay raw; corporate actions inside (entry, exit] go into
the trade's return through the backtest.adjust cumulative factor."""
import argparse
import types

import numpy as np
import pytest

import signal_backtest.signal  # noqa: F401  -- loads the factories in their usual order
from signal_backtest.engine import run_side_backtest, run_side_backtest_tiered
from signal_backtest.signal import TierConfig
from tests.test_short_constraints import DATES, N, _bars


def _rf(*events):
    """Cumulative factor with an event of factor f on bar k (bars before k)."""
    rf = np.ones(N)
    for k, f in events:
        rf[:k] *= f
    return rf


def _data(close=None, rf=None):
    close = np.asarray(close if close is not None else np.full(N, 100.0), dtype=np.float32)
    return types.SimpleNamespace(
        n=N, stock_id="9999", stock_name="test", dates=DATES, close=close,
        low=close - 1, high=close + 1, return_factor=rf,
        volume_result=types.SimpleNamespace(flood=np.zeros(N, dtype=bool)))


def _run(side, rf, close=None, entry=2, exit_=7):
    res = run_side_backtest(_data(close, rf), side, _bars(entry), _bars(exit_), [], start_index=1)
    return res.trades


def test_long_held_over_an_ex_date_earns_the_dividend():
    (t,) = _run("long", _rf((5, 0.95)))
    assert t.pnl_pct == pytest.approx(1 / 0.95 - 1)


def test_short_held_over_an_ex_date_pays_the_dividend():
    (t,) = _run("short", _rf((5, 0.95)))
    assert t.pnl_pct == pytest.approx(0.95 - 1)


def test_event_on_the_exit_bar_counts_and_on_the_entry_bar_does_not():
    (t,) = _run("long", _rf((7, 0.9)))
    assert t.pnl_pct == pytest.approx(1 / 0.9 - 1)
    (t,) = _run("long", _rf((2, 0.9)))
    assert t.pnl_pct == 0.0
    (t,) = _run("long", _rf((3, 0.9)))        # ex-date the bar after the entry
    assert t.pnl_pct == pytest.approx(1 / 0.9 - 1)


def test_no_event_inside_is_bit_identical_to_raw():
    rng = np.random.default_rng(3)
    close = (100 * np.exp(np.cumsum(rng.normal(0, 0.01, N)))).astype(np.float32)
    for side in ("long", "short"):
        (raw,) = _run(side, None, close)
        (tr,) = _run(side, _rf((12, 0.9), (1, 0.8)), close)
        assert tr.pnl_pct == raw.pnl_pct


def test_gap_exit_fills_on_the_bar_before_and_leaves_its_event_out():
    close = np.full(N, 100.0)
    close[6:] = 150.0                          # a >15% jump: exit at bar 5's close
    (t,) = _run("long", _rf((6, 1 / 1.5)), close)
    assert t.exit_date == DATES[5] and t.pnl_pct == 0.0
    (t,) = _run("long", _rf((6, 1 / 1.5), (4, 0.95)), close)
    assert t.pnl_pct == pytest.approx(1 / 0.95 - 1)


def test_tiered_engine_uses_the_same_accounting():
    tiers = [TierConfig(name="t", entry=_bars(2))]
    res = run_side_backtest_tiered(_data(rf=_rf((5, 0.95))), "long", tiers, exit_=_bars(7), start_index=1)
    (t,) = res.trades
    assert t.pnl_pct == pytest.approx(1 / 0.95 - 1)


# ── batch / CLI ──────────────────────────────────────────────────────────────

def _worker(monkeypatch, rows_dates, total_return=True):
    from backtest import adjust, data as bdata
    from signal_backtest import batch
    from signal_backtest import signal as sig
    from signal_backtest.signal import SignalSet, SignalSpec
    d = _data()
    monkeypatch.setattr(batch, "load_stock_data", lambda sid, use_cache=False, adjusted=False: d)
    monkeypatch.setattr(bdata, "_fetch_prices", lambda sid, s, e: [{"trade_date": x} for x in rows_dates])
    monkeypatch.setattr(adjust, "adjustment", lambda sid, rows: (_rf((5, 0.95)), np.ones(N), []))
    spec = SignalSpec(name="t", signals=SignalSet(long_entry=_bars(2), long_exit=_bars(7),
                                                  short_entry=_bars(), short_exit=_bars()))
    monkeypatch.setitem(sig.SIGNAL_FACTORIES, "t_long", lambda x: spec)
    _, trades, _, skipped = batch._process_one_stock("9999", ["t_long"], 1, False, None, None, False,
                                                     total_return)
    return trades["t_long"], skipped["t_long"]


def test_batch_worker_sets_the_factor(monkeypatch):
    trades, skipped = _worker(monkeypatch, DATES)
    assert skipped == []
    assert [round(t.pnl_pct, 6) for t in trades] == [round(1 / 0.95 - 1, 6)]
    trades, _ = _worker(monkeypatch, DATES, total_return=False)
    assert [t.pnl_pct for t in trades] == [0.0]


def test_batch_worker_fails_the_run_when_rows_differ(monkeypatch):
    with pytest.raises(RuntimeError, match="rows differ"):
        _worker(monkeypatch, DATES[:-1])


def test_run_batch_multi_refuses_both_modes(tmp_path, monkeypatch):
    from signal_backtest import batch

    def ran(*a, **k):
        raise AssertionError("run_batch_multi went on to run")
    monkeypatch.setattr(batch, "fetch_listed_stocks", ran)
    monkeypatch.setattr(batch, "fetch_stocks_by_id", ran)
    with pytest.raises(ValueError):
        batch.run_batch_multi({"pick": None}, tmp_path, adjusted=True, total_return=True)


def test_cli_defaults_to_the_baseline_and_marks_nothing(tmp_path, monkeypatch):
    from tests.test_holdout import _cli
    from signal_backtest import batch
    seen = _cli(monkeypatch, tmp_path)
    assert seen["total_return"] is True and seen["adjusted"] is False
    assert seen["short_constraints"] == batch.BASELINE_SHORT_CONSTRAINTS == "margin_futures"
    for m in (batch.RAW_RETURN_MARKER, batch.SHORT_CONSTRAINTS_MARKER, batch.ADJUSTED_PRICES_MARKER):
        assert not (tmp_path / m).exists(), m


def test_cli_raw_return_marks_and_a_baseline_run_clears(tmp_path, monkeypatch):
    from tests.test_holdout import _cli
    from signal_backtest import batch
    seen = _cli(monkeypatch, tmp_path, "--raw-return")
    assert seen["total_return"] is False
    assert (tmp_path / batch.RAW_RETURN_MARKER).exists()
    _cli(monkeypatch, tmp_path)
    assert not (tmp_path / batch.RAW_RETURN_MARKER).exists()


def test_cli_adjusted_prices_run_without_the_factor(tmp_path, monkeypatch):
    from tests.test_holdout import _cli
    seen = _cli(monkeypatch, tmp_path, "--adjusted-prices")
    assert seen["adjusted"] is True and seen["total_return"] is False


def test_cli_refuses_raw_with_adjusted_and_leaves_the_marks(tmp_path, monkeypatch):
    from tests.test_holdout import _cli
    from signal_backtest import batch
    _cli(monkeypatch, tmp_path, "--short-constraints", "margin")
    with pytest.raises(SystemExit) as e:
        _cli(monkeypatch, tmp_path, "--raw-return", "--adjusted-prices")
    assert e.value.code == 1
    assert (tmp_path / batch.SHORT_CONSTRAINTS_MARKER).exists()


def test_single_signal_run_is_marked_as_not_the_baseline(tmp_path, monkeypatch):
    import sys
    from signal_backtest import __main__ as cli, batch

    def stop(**kw):
        raise RuntimeError("stop after the marks")
    monkeypatch.setattr(cli, "run_batch", stop)
    monkeypatch.setattr(sys, "argv", ["x", "--signal", "pick", "--output", str(tmp_path)])
    with pytest.raises(RuntimeError, match="stop after the marks"):
        cli.main()
    assert (tmp_path / batch.RAW_RETURN_MARKER).exists()
    assert (tmp_path / batch.SHORT_CONSTRAINTS_MARKER).read_text(encoding="utf-8").strip() == "off"


def test_versions_add_refuses_a_raw_return_dir(tmp_path, monkeypatch):
    from signal_backtest import _versions as versions, batch
    (tmp_path / batch.RAW_RETURN_MARKER).write_text("raw return", encoding="utf-8")
    monkeypatch.setattr(versions, "_connect", lambda: pytest.fail("opened the version DB"))
    with pytest.raises(SystemExit) as e:
        versions.cmd_add(argparse.Namespace(scan_dir=str(tmp_path), version_id="v999",
                                            summary="x", snapshot=None))
    assert e.value.code == 1
