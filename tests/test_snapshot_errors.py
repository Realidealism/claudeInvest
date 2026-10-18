"""Per-stock exceptions in the daily and intraday snapshots.

Each step of a stock's evaluation used to swallow its exception, so a step
broken for every stock published "no signal, no position today". The worker
now reports each one, and a run that collected any raises before it writes.
Both modules share the shape; every case runs against both.

No database, no network: the loader, the ScoreBoard, the signal conditions,
the unified factories, the backtest, the pool and the saves are stand-ins.
"""
import sys
import types
from datetime import date

import numpy as np
import pytest

import analysis.daily_snapshot as ds
import analysis.intraday_snapshot as isn
from analysis.snapshot_errors import PerStockErrors, summarize
from daily_update_guards import no_side_effects  # noqa: F401

DAY = date(2026, 10, 16)

# (module, name of its loader, how to call its worker for one stock)
MODULES = [
    pytest.param(ds, "load_stock_data", lambda sid: ds._eval_stock((sid, DAY, DAY)), id="daily"),
    pytest.param(isn, "load_stock_data_intraday", lambda sid: isn._eval_stock(sid), id="intraday"),
]


def stock(n=100):
    return types.SimpleNamespace(
        n=n, dates=[DAY] * n, turnover=np.ones(n),
        volume_result=types.SimpleNamespace(volume_status=np.zeros(n, dtype=int)))


class _Board:
    def __init__(self, broken_bars=()):
        self.broken_bars = broken_bars

    def evaluate(self, data, idx):
        if data.n - 1 - idx in self.broken_bars:
            raise ZeroDivisionError("board")
        pct = types.SimpleNamespace(pct=10.0)
        return types.SimpleNamespace(total=types.SimpleNamespace(long=pct, short=pct))


def boom(*_args, **_kwargs):
    raise ValueError("boom")


def setup(monkeypatch, mod, loader, *, load=None, board=None, signal=None,
          factory=None, backtest=None):
    """Healthy stand-ins, with one piece optionally broken."""
    monkeypatch.setattr(mod, loader, load or (lambda *a, **k: stock()))
    monkeypatch.setattr(mod, "_BOARD", board or _Board())
    monkeypatch.setattr(mod, "post_break_blackout", lambda data: np.zeros(data.n, dtype=bool))
    signals = [(name, lambda data: np.zeros(data.n, dtype=bool))
               for name in ("pick", "touch", "buy", "sell", "buy_flee", "sell_flee")]
    if signal:
        signals = [(name, boom if name == signal else fn) for name, fn in signals]
    monkeypatch.setattr(mod, "SIGNALS", signals)
    spec = types.SimpleNamespace(
        long_tiers=[1], short_tiers=[1], long_floor_period=8, short_floor_period=8,
        signals=types.SimpleNamespace(long_exit=None, short_exit=None))
    monkeypatch.setattr(mod, "unified_long_factory", boom if factory == "long" else (lambda d: spec))
    monkeypatch.setattr(mod, "unified_short_factory", boom if factory == "short" else (lambda d: spec))
    monkeypatch.setattr(mod, "run_side_backtest_tiered",
                        (lambda data, side, *a, **k: boom() if side == backtest
                         else types.SimpleNamespace(trades=[])))
    if hasattr(mod, "_VOLUME_SCALE"):
        monkeypatch.setattr(mod, "_VOLUME_SCALE", 1.0)


@pytest.mark.parametrize("mod, loader, call", MODULES)
def test_healthy_stock_has_no_errors(monkeypatch, mod, loader, call):
    setup(monkeypatch, mod, loader)
    result, errors = call("2330")
    assert result is not None and result["sid"] == "2330" and errors == []


@pytest.mark.parametrize("mod, loader, call", MODULES)
@pytest.mark.parametrize("broken, site, kept", [
    ({"signal": "buy"}, "signal buy", True),
    ({"factory": "short"}, "unified short", True),
    ({"backtest": "long"}, "backtest long", True),
    ({"load": boom}, "load", False),
    ({"board": _Board(broken_bars=(0,))}, "score", False),
    ({"board": _Board(broken_bars=(2,))}, "score_d2", True),
    ({"board": _Board(broken_bars=(1,))}, "score_d1", True),
    ({"board": _Board(broken_bars=(3,))}, "score_d3", True),
    ({"factory": "long"}, "unified long", True),
    ({"backtest": "short"}, "backtest short", True),
])
def test_every_catch_site_is_reported(monkeypatch, mod, loader, call, broken, site, kept):
    setup(monkeypatch, mod, loader, **broken)
    result, errors = call("2330")
    assert [(s, sid) for s, sid, _ in errors] == [(site, "2330")]
    assert errors[0][2].startswith(("ValueError", "ZeroDivisionError"))
    assert (result is not None) is kept


@pytest.mark.parametrize("mod, loader, call", MODULES)
def test_short_history_is_unusable_not_an_error(monkeypatch, mod, loader, call):
    setup(monkeypatch, mod, loader, load=lambda *a, **k: stock(n=59))
    assert call("4169") == (None, [])


@pytest.mark.parametrize("mod, loader, call", MODULES)
def test_unavailable_price_is_skipped_not_an_error(monkeypatch, mod, loader, call):
    # Emerging-board stocks (db.price_availability) are left out by design.
    from db.price_availability import PriceUnavailable

    def load(*a, **k):
        raise PriceUnavailable("1260")
    setup(monkeypatch, mod, loader, load=load)
    assert call("1260") == ({"_unavailable": "1260"}, [])


# ── run(): nothing is written once any stock raised ────────────────────────

class _SyncPool:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def imap_unordered(self, fn, items, chunksize=1):
        return map(fn, items)


def _run_stand_ins(monkeypatch, mod, worker):
    saves = []
    monkeypatch.setattr(mod, "Pool", _SyncPool, raising=False)
    import multiprocessing
    monkeypatch.setattr(multiprocessing, "Pool", _SyncPool)
    monkeypatch.setattr(mod, "_eval_stock", worker)
    monkeypatch.setattr(mod, "_load_active_stock_ids", lambda d: ["1101", "2330"])
    monkeypatch.setattr(mod, "_load_prev_score_ranks", lambda *a: {})
    for name in ("_save_score_side", "_save_signal_fires", "_save_open_positions"):
        monkeypatch.setattr(mod, name, lambda *a, name=name: saves.append(name) or (
            {"long": 0, "short": 0, "exited_long": 0, "exited_short": 0}
            if name == "_save_open_positions" else {s: 0 for s, _ in mod.SIGNALS}
            if name == "_save_signal_fires" else 0))
    monkeypatch.setitem(sys.modules, "analysis.tx_status", types.SimpleNamespace(
        compute_and_save_tx_status=lambda **k: None))
    if mod is ds:
        for check in ("_check_market_breadth_fresh", "_check_market_vol_fresh",
                      "_check_liquidity_fresh"):
            monkeypatch.setattr(ds, check, lambda d: None)
    else:
        monkeypatch.setattr(isn, "_compute_volume_scale", lambda now, final_pass=False: 1.0)
        monkeypatch.setattr(isn, "compute_live_breadth", lambda *a, **k: saves.append("breadth") or {
            k: 0 for k in ("total", "short_up", "short_down", "medium_up", "medium_down",
                           "long_up", "long_down")})
        monkeypatch.setattr(isn, "_save_breadth_sidecar", lambda *a: saves.append("sidecar"))
    return saves


def _result(sid):
    return {"sid": sid, "lp": 1.0, "sp": 1.0, "tv": 1.0, "fires": [], "open_positions": [],
            "lp_d1": None, "sp_d1": None, "lp_d2": None, "sp_d2": None,
            "lp_d3": None, "sp_d3": None, "vol_status": 0}


def _call_run(mod):
    return mod.run(DAY) if mod is ds else mod.run()


@pytest.mark.parametrize("mod", [ds, isn], ids=["daily", "intraday"])
def test_run_with_a_failed_stock_writes_nothing(monkeypatch, mod):
    def worker(item):
        sid = item[0] if isinstance(item, tuple) else item
        return (_result(sid), [("signal buy", sid, "ValueError: boom")] if sid == "2330" else [])

    saves = _run_stand_ins(monkeypatch, mod, worker)
    with pytest.raises(PerStockErrors) as raised:
        _call_run(mod)
    assert saves == []
    assert raised.value.errors == [("signal buy", "2330", "ValueError: boom")]
    assert "signal buy 1 檔" in str(raised.value)


@pytest.mark.parametrize("mod", [ds, isn], ids=["daily", "intraday"])
def test_run_with_unavailable_stocks_still_writes_the_rest(monkeypatch, mod):
    from contextlib import contextmanager
    import analysis.market_thermometer as mt

    def worker(item):
        sid = item[0] if isinstance(item, tuple) else item
        return ({"_unavailable": sid}, []) if sid == "2330" else (_result(sid), [])

    saves = _run_stand_ins(monkeypatch, mod, worker)
    if mod is isn:
        @contextmanager
        def get_cursor(commit=True):
            yield object()
        monkeypatch.setattr(isn, "get_cursor", get_cursor)
        monkeypatch.setattr(mt, "build_intraday_stance", lambda cur, scale, now: None)
    assert _call_run(mod)["stocks_evaluated"] == 1
    assert "_save_open_positions" in saves


def test_daily_run_without_errors_writes(monkeypatch):
    saves = _run_stand_ins(monkeypatch, ds, lambda item: (_result(item[0]), []))
    assert ds.run(DAY)["stocks_evaluated"] == 2
    assert saves == ["_save_score_side", "_save_score_side", "_save_signal_fires",
                     "_save_open_positions"]


def test_intraday_run_without_errors_writes(monkeypatch):
    from contextlib import contextmanager
    import analysis.market_thermometer as mt
    saves = _run_stand_ins(monkeypatch, isn, lambda sid: (_result(sid), []))

    @contextmanager
    def get_cursor(commit=True):
        yield object()
    monkeypatch.setattr(isn, "get_cursor", get_cursor)
    monkeypatch.setattr(mt, "build_intraday_stance", lambda cur, scale, now: None)
    assert isn.run()["stocks_evaluated"] == 2
    assert saves == ["breadth", "_save_score_side", "_save_score_side", "_save_signal_fires",
                     "_save_open_positions", "sidecar"]


def test_log_of_a_step_broken_for_every_stock_is_bounded(monkeypatch, capsys):
    # A step broken for every stock prints a bounded log, not a line per stock.
    def worker(item):
        sid = item[0] if isinstance(item, tuple) else item
        return None, [("load", sid, "ValueError: x")]

    _run_stand_ins(monkeypatch, ds, worker)
    monkeypatch.setattr(ds, "_load_active_stock_ids", lambda d: [str(1000 + i) for i in range(80)])
    with pytest.raises(PerStockErrors):
        ds.run(DAY)
    out = capsys.readouterr().out
    assert out.count("[ERROR]") == 51 and "and 30 more" in out


# ── the summary ────────────────────────────────────────────────────────────

def test_summary_groups_by_site_and_stays_short():
    errors = [("signal buy", f"{1000 + i}", "ValueError: boom") for i in range(800)]
    errors += [("load", "9999", "ValueError: No data found for 9999"),
               ("score_d1", "1000", "ZeroDivisionError: x"),
               ("backtest long", "1001", "IndexError: y")]
    line = summarize(errors)
    assert line.startswith("逐檔例外 803 處（801 檔）：signal buy 800 檔（ValueError: boom）")
    assert "另 1 個位置" in line and "\n" not in line and len(line) < 300


def test_multi_line_messages_are_folded_onto_one_line():
    # The nightly summary keeps the traceback's last three lines; a message
    # spanning lines would push the summary's head out.
    from analysis.snapshot_errors import describe
    msg = describe(ValueError("operands could not be broadcast\nshapes (3,) (4,)"))
    assert msg == "ValueError: operands could not be broadcast shapes (3,) (4,)"
    assert "\n" not in str(PerStockErrors([("signal buy", "2330", msg)]))


def test_the_error_survives_pickling():
    import pickle
    err = PerStockErrors([("load", "2330", "ValueError: x")])
    back = pickle.loads(pickle.dumps(err))
    assert back.errors == err.errors and str(back) == str(err)
