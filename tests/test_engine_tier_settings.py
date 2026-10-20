"""Tier-level exit and initial-defense settings in run_side_backtest_tiered
(user ruling 2026-10-20: unified carries the exits and initial defenses its
tiers' own signals adopted). A tier's exit counts while that tier is the
active one -- the temporary strict tier if one is set, else the current
tier -- the same tier whose defense rules apply."""
import types
from datetime import date, timedelta

import numpy as np

from signal_backtest.engine import run_side_backtest_tiered
from signal_backtest.signal import TierConfig
from signal_backtest.trade import REASON_EXIT_END, REASON_EXIT_GIVEBACK, REASON_EXIT_SIGNAL

N = 30


def _data(low=None, flood=None):
    close = np.full(N, 100.0, dtype=np.float32)
    low = np.asarray(low if low is not None else close - 1, dtype=np.float32)
    return types.SimpleNamespace(
        n=N, stock_id="9999", stock_name="test",
        dates=[date(2024, 1, 1) + timedelta(days=i) for i in range(N)],
        close=close, low=low, high=close + 1,
        volume_result=types.SimpleNamespace(
            flood=np.zeros(N, dtype=bool) if flood is None else np.asarray(flood)))


def _bars(*idx):
    a = np.zeros(N, dtype=bool)
    a[list(idx)] = True
    return a


def _tiers(entries, exits=(None, None, None), periods=(None, None, None),
           floods=(None, None, None)):
    names = ("pick", "sell_flee", "buy")
    return [TierConfig(name=names[k], entry=entries[k], exit=exits[k],
                       initial_period=periods[k], flood_tight_init=floods[k])
            for k in range(3)]


def run(tiers, exit_=None, **kw):
    r = run_side_backtest_tiered(_data(**kw.pop("data", {})), "long", tiers,
                                 exit_=_bars() if exit_ is None else exit_,
                                 start_index=1, **kw)
    return [(t.entry_date.day - 1, (t.exit_date - date(2024, 1, 1)).days, t.exit_reason)
            for t in r.trades]


NONE = _bars()


def test_tier_exit_fires_while_its_tier_is_current():
    trades = run(_tiers([_bars(2), NONE, NONE], exits=(_bars(6), None, None)))
    assert trades == [(2, 6, REASON_EXIT_SIGNAL)]


def test_tier_exit_stops_once_upgraded_to_a_looser_tier():
    trades = run(_tiers([_bars(2), NONE, _bars(4)], exits=(_bars(6), None, None)))
    assert trades == [(2, N - 1, REASON_EXIT_END)]


def test_tier_exit_fires_while_temporarily_strict():
    # held as buy from bar 2; pick fires on bar 5: pick is active through bar 10
    entries = [_bars(5), NONE, _bars(2)]
    assert run(_tiers(entries, exits=(_bars(7), None, None))) == [(2, 7, REASON_EXIT_SIGNAL)]
    assert run(_tiers(entries, exits=(_bars(10), None, None))) == [(2, 10, REASON_EXIT_SIGNAL)]
    assert run(_tiers(entries, exits=(_bars(11), None, None))) == [(2, N - 1, REASON_EXIT_END)]


def test_tier_exit_on_the_bar_its_tier_turns_active():
    entries = [_bars(5), NONE, _bars(2)]
    trades = run(_tiers(entries, exits=(_bars(5), None, None)))
    # out on bar 5, then pick's entry that bar opens a new position, as the
    # engine re-enters on an exit bar
    assert trades == [(2, 5, REASON_EXIT_SIGNAL), (5, N - 1, REASON_EXIT_END)]


def test_another_tiers_exit_does_not_count():
    trades = run(_tiers([_bars(2), NONE, NONE], exits=(None, _bars(6), _bars(7))))
    assert trades == [(2, N - 1, REASON_EXIT_END)]


def test_global_exit_still_applies():
    trades = run(_tiers([NONE, NONE, _bars(2)]), exit_=_bars(8))
    assert trades == [(2, 8, REASON_EXIT_SIGNAL)]


def test_tier_exit_records_the_tiers_reason():
    # user ruling 2026-10-20: the push tells a give-back from a flee reversal;
    # the global exit wins when both fire on the bar
    def tiers(exit_bar):
        t = _tiers([_bars(2), NONE, NONE], exits=(_bars(exit_bar), None, None))
        t[0].exit_reason = REASON_EXIT_GIVEBACK
        return t
    assert run(tiers(6)) == [(2, 6, REASON_EXIT_GIVEBACK)]
    assert run(tiers(6), exit_=_bars(6)) == [(2, 6, REASON_EXIT_SIGNAL)]
    assert run(tiers(9), exit_=_bars(6)) == [(2, 6, REASON_EXIT_SIGNAL)]


def _initial(tiers, **kw):
    r = run_side_backtest_tiered(_data(**kw.pop("data", {})), "long", tiers,
                                 exit_=_bars(), start_index=1, **kw)
    return r.trades[0].defense_events[0].price


RISING_LOW = [90.0 + i for i in range(N)]     # lowest of the last k lows = low[i-k+1]


def test_entry_tier_initial_period():
    # entry on bar 5 by sell_flee (period 3): lowest of bars 3..5 = 93
    tiers = _tiers([NONE, _bars(5), NONE], periods=(None, 3, None))
    assert _initial(tiers, data={"low": RISING_LOW}) == 93.0
    # pick enters instead: its period is unset, the function's 5 applies (91)
    tiers = _tiers([_bars(5), _bars(5), NONE], periods=(None, 3, None))
    assert _initial(tiers, data={"low": RISING_LOW}) == 91.0


def test_unset_period_falls_back_to_the_function_argument():
    tiers = _tiers([_bars(5), NONE, NONE])
    assert _initial(tiers, data={"low": RISING_LOW}, initial_period=3) == 93.0


def test_entry_tier_flood_tight_init():
    flood = _bars(5)
    tiers = _tiers([_bars(5), NONE, NONE], floods=(True, None, None))
    assert _initial(tiers, data={"low": RISING_LOW, "flood": flood}) == 95.0   # the bar's low
    tiers = _tiers([_bars(5), NONE, NONE])
    assert _initial(tiers, data={"low": RISING_LOW, "flood": flood}) == 91.0
    # the function-level flag still works for tiers that leave it unset
    assert _initial(tiers, data={"low": RISING_LOW, "flood": flood}, flood_tight_init=True) == 95.0
    # a tier can switch it off
    tiers = _tiers([_bars(5), NONE, NONE], floods=(False, None, None))
    assert _initial(tiers, data={"low": RISING_LOW, "flood": flood}, flood_tight_init=True) == 91.0


def test_a_looser_tier_firing_on_the_entry_bar_counts_for_the_exit():
    # pick and buy both fire on bar 2: the entry is pick's, but buy was
    # reached, so pick's exit does not apply (user ruling 2026-10-20)
    trades = run(_tiers([_bars(2), NONE, _bars(2)], exits=(_bars(6), None, None)))
    assert trades == [(2, N - 1, REASON_EXIT_END)]
    # sell_flee alongside pick: sell_flee carries the exit, so it still fires
    trades = run(_tiers([_bars(2), _bars(2), NONE], exits=(None, _bars(6), None)))
    assert trades == [(2, 6, REASON_EXIT_SIGNAL)]
    # the entry still takes pick's initial defense
    tiers = _tiers([_bars(5), NONE, _bars(5)], periods=(3, None, None))
    assert _initial(tiers, data={"low": RISING_LOW}) == 93.0


def test_short_side_initial_defense_uses_highs():
    high = np.array([110.0 - i for i in range(N)], dtype=np.float32)   # falling highs
    data = _data()
    data.high = high
    data.volume_result.flood = _bars(5)
    tiers = _tiers([_bars(5), NONE, NONE], periods=(3, None, None))

    def first_defense(tiers, **kw):
        r = run_side_backtest_tiered(data, "short", tiers, exit_=_bars(), start_index=1, **kw)
        return r.trades[0].defense_events[0].price

    assert first_defense(tiers) == 107.0                         # highest of bars 3..5
    assert first_defense(_tiers([_bars(5), NONE, NONE])) == 109.0  # 5 bars: bar 1
    tiers = _tiers([_bars(5), NONE, NONE], periods=(3, None, None), floods=(True, None, None))
    assert first_defense(tiers) == 105.0                         # the flood bar's high
