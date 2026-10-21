"""A sweep runs as the v370 baseline does (total returns, the margin_futures
short constraints), so its GRID[0] reproduces the version."""
import argparse
import types

import signal_backtest.signal  # noqa: F401  -- loads the factories in their usual order
from signal_backtest import batch, sweep
from tests.test_short_constraints import DATES, _bars
from tests.test_total_return import _data, _rf


def _worker(monkeypatch, side, short_events, futures_days, total_return):
    d = _data()
    seen = {}
    monkeypatch.setattr(sweep, "load_stock_data", lambda sid, use_cache=False: d)

    def settings(sid, data, ev, fut, tr):
        seen["settings"] = (ev, fut, tr)
        data.return_factor = _rf((5, 0.95)) if tr else None
        return (_bars(7), _bars(), _bars()) if ev is not None else (None, None, None)
    monkeypatch.setattr(sweep, "apply_settings", settings)
    monkeypatch.setattr(sweep, "DEFAULT_START_INDEX", 1)
    sweep._WORKER_SPEC = types.SimpleNamespace(
        GRID=[(1,)], build=lambda data, p: dict(side=side, entry=_bars(2), exit_=_bars(9), defense_rules=[]))
    out = sweep._process_one_stock(("9999", "x", False, short_events, futures_days, total_return))
    return out[0], seen


def test_worker_puts_the_constraints_on_the_short_side(monkeypatch):
    rows, seen = _worker(monkeypatch, "short", [(DATES[7], None)], set(), True)
    assert seen["settings"] == ([(DATES[7], None)], set(), True)
    assert [(r["出場日期"], r["出場原因"]) for r in rows] == [(DATES[7], "強制回補")]
    assert abs(rows[0]["報酬率"] - (0.95 - 1)) < 1e-12      # the short pays the dividend


def test_worker_leaves_the_long_side_unconstrained_but_total_return(monkeypatch):
    rows, _ = _worker(monkeypatch, "long", [(DATES[7], None)], set(), True)
    assert [(r["出場日期"], r["出場原因"]) for r in rows] == [(DATES[9], "訊號出場")]
    assert abs(rows[0]["報酬率"] - (1 / 0.95 - 1)) < 1e-12


def test_run_sweep_defaults_to_the_baseline(monkeypatch, tmp_path):
    seen = {}
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sweep, "_load_spec", lambda p: types.SimpleNamespace(NAME="t", SIGNAL="pick", GRID=[(1,)]))
    monkeypatch.setattr(sweep, "_init_worker", lambda p: None)
    monkeypatch.setattr(sweep, "fetch_listed_stocks", lambda: [("1101", "x")])

    def inputs(mode):
        seen["mode"] = mode
        return {"1101": [(DATES[3], None)]}, {"1101": {DATES[2]}}
    monkeypatch.setattr(sweep, "load_short_inputs", inputs)

    def worker(args):
        seen["args"] = args
        return {}
    monkeypatch.setattr(sweep, "_process_one_stock", worker)
    sweep.run_sweep("spec.py", workers=1, use_cache=False, no_archive=True)
    assert seen["mode"] == batch.BASELINE_SHORT_CONSTRAINTS
    assert seen["args"] == ("1101", "x", False, [(DATES[3], None)], {DATES[2]}, True)


def test_only_a_baseline_sweep_is_archived():
    ok = sweep._archivable
    base = dict(short_constraints="margin_futures", raw_return=False, no_archive=False)
    assert ok(argparse.Namespace(**base))
    assert not ok(argparse.Namespace(**dict(base, raw_return=True)))
    assert not ok(argparse.Namespace(**dict(base, short_constraints="off")))
    assert ok(argparse.Namespace(**dict(base, short_constraints="off", no_archive=True)))
