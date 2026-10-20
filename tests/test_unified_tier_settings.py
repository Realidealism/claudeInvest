"""unified_long / unified_short carry the exits and initial defenses their
tiers' own signals adopted (user ruling 2026-10-20): give-back on pick and
sell_flee, 3-bar initial defense on sell_flee, 3-bar flood-tight initial
defense on buy_flee; nothing on buy, touch or sell."""
import types

import numpy as np
import pytest

import signal_backtest.signal  # noqa: F401  -- loads the factories in their usual order
from signal_backtest.factories import unified

N = 10


@pytest.fixture
def data(monkeypatch):
    marks = {}

    def arr(name):
        a = np.zeros(N, dtype=bool)
        marks[id(a)] = name
        return a

    for fn in ("pick_condition", "buy_condition", "sell_flee_signal", "buy_flee_signal",
               "touch_condition", "sell_condition", "post_break_blackout"):
        monkeypatch.setattr(unified, fn, lambda d, _n=fn: arr(_n))
    giveback = arr("giveback")
    monkeypatch.setattr(unified, "_transient_giveback_exit", lambda d: giveback)
    spec = types.SimpleNamespace(long_defense=["L"], short_defense=["S"])
    for fn in ("pick_signal", "buy_signal", "sell_flee_factory",
               "touch_signal", "sell_signal", "buy_flee_factory"):
        monkeypatch.setattr(unified, fn, lambda d: spec)
    d = types.SimpleNamespace(n=N, money_result=types.SimpleNamespace(dead=np.zeros(N, dtype=bool)))
    d.giveback = giveback
    return d


def test_unified_long_tiers(data):
    tiers = {t.name: t for t in unified.unified_long_factory(data).long_tiers}
    assert list(tiers) == ["pick", "sell_flee", "buy"]
    assert tiers["pick"].exit is data.giveback and tiers["sell_flee"].exit is data.giveback
    assert tiers["buy"].exit is None
    assert [tiers[k].exit_reason for k in tiers] == ["漲多回吐出場", "漲多回吐出場", "訊號出場"]
    assert [tiers[k].initial_period for k in tiers] == [None, 3, None]
    assert [tiers[k].flood_tight_init for k in tiers] == [None, None, None]


def test_unified_short_tiers(data):
    tiers = {t.name: t for t in unified.unified_short_factory(data).short_tiers}
    assert list(tiers) == ["buy_flee", "touch", "sell"]
    assert all(t.exit is None for t in tiers.values())
    assert [tiers[k].initial_period for k in tiers] == [3, None, None]
    assert [tiers[k].flood_tight_init for k in tiers] == [True, None, None]
