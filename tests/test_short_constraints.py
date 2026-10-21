"""Margin short-sale constraints (user ruling 2026-10-20): a short is bought
back at the close of the 融券最後回補日, and none opens while 停券."""
import types
from datetime import date, timedelta

import numpy as np

import signal_backtest.signal  # noqa: F401  -- loads the factories in their usual order
from signal_backtest import short_constraints as sc
from signal_backtest.engine import run_side_backtest, run_side_backtest_tiered
from signal_backtest.signal import TierConfig
from signal_backtest.trade import (REASON_EXIT_DEFENSE, REASON_EXIT_END,
                                   REASON_EXIT_FORCED_COVER, REASON_EXIT_SIGNAL)

N = 30
D0 = date(2024, 1, 1)
DATES = [D0 + timedelta(days=i) for i in range(N)]


def _bars(*idx):
    a = np.zeros(N, dtype=bool)
    a[list(idx)] = True
    return a


def _data(close=None):
    close = np.asarray(close if close is not None else np.full(N, 100.0), dtype=np.float32)
    flat = np.full(N, 100.0, dtype=np.float32)
    return types.SimpleNamespace(
        n=N, stock_id="9999", stock_name="test", dates=DATES,
        close=close, low=flat - 1, high=flat + 1,
        volume_result=types.SimpleNamespace(flood=np.zeros(N, dtype=bool)))


def _trades(result):
    return [((t.entry_date - D0).days, (t.exit_date - D0).days, t.exit_reason) for t in result.trades]


# ── masks ────────────────────────────────────────────────────────────────

def test_mask_on_the_cover_bar_and_its_window():
    force, block = sc.masks(DATES, [(DATES[5], DATES[8])])
    assert np.flatnonzero(force).tolist() == [5]
    assert np.flatnonzero(block).tolist() == [5, 6, 7, 8]


def test_mask_cover_date_without_a_bar_takes_the_bar_after():
    # a market closure moves the cover one day later; never earlier, which
    # would act on an announcement not yet made
    dates = DATES[:5] + DATES[7:]            # no bars on days 5 and 6
    force, block = sc.masks(dates, [(DATES[6], DATES[8])])
    assert [dates[k] for k in np.flatnonzero(force)] == [DATES[7]]
    assert [dates[k] for k in np.flatnonzero(block)] == [DATES[7], DATES[8]]


def test_mask_halt_past_the_window_covers_and_blocks_on_resumption():
    dates = DATES[:5] + DATES[20:]           # halted from day 5 to day 19
    force, block = sc.masks(dates, [(DATES[6], DATES[8])])
    assert [dates[k] for k in np.flatnonzero(force)] == [DATES[20]]
    assert [dates[k] for k in np.flatnonzero(block)] == [DATES[20]]


def test_no_reentry_on_a_moved_cover_bar():
    dates = DATES[:5] + DATES[7:]
    force, block = sc.masks(dates, [(DATES[6], None)])
    data = _data()
    data.dates, data.n = dates, len(dates)
    for name in ("close", "low", "high"):
        setattr(data, name, getattr(data, name)[:len(dates)])
    data.volume_result.flood = data.volume_result.flood[:len(dates)]
    entry = np.zeros(len(dates), dtype=bool)
    entry[[2, 5]] = True                     # bar 5 is day 7, the moved cover bar
    r = run_side_backtest(data, "short", entry, np.zeros(len(dates), dtype=bool), [],
                          start_index=1, force_exit=force, entry_block=block)
    assert [(t.entry_date, t.exit_date, t.exit_reason) for t in r.trades] == \
        [(DATES[2], DATES[7], REASON_EXIT_FORCED_COVER)]


def test_mask_without_an_end_blocks_four_bars():
    force, block = sc.masks(DATES, [(DATES[5], None)])
    assert np.flatnonzero(block).tolist() == [5, 6, 7, 8]


def test_mask_ignores_events_after_the_last_bar():
    force, block = sc.masks(DATES, [(DATES[-1] + timedelta(days=3), None)])
    assert not force.any() and not block.any()


def test_load_calendar_reads_official_rows_without_extraordinary_meetings(monkeypatch):
    seen = {}

    class Cur:
        def execute(self, sql, params):
            seen["sql"], seen["params"] = sql, params

        def fetchall(self):
            return [{"stock_id": "1101", "last_cover_date": DATES[5], "suspension_end": None}]

    class Ctx:
        def __enter__(self):
            return Cur()

        def __exit__(self, *a):
            return False

    import db.connection
    monkeypatch.setattr(db.connection, "get_cursor", lambda commit=True: Ctx())
    assert sc.load_calendar() == {"1101": [(DATES[5], None)]}
    sources, excluded = seen["params"]
    assert sorted(sources) == ["BFI84U", "BFI84U2", "tpex_term", "tpex_term_hist"]
    assert excluded == ["臨時會"]


# ── engine ───────────────────────────────────────────────────────────────

def plain(entry, exit_=None, close=None, **kw):
    return _trades(run_side_backtest(_data(close), "short", entry,
                                     _bars() if exit_ is None else exit_, [],
                                     start_index=1, **kw))


def tiered(entries, tier_exit=None, exit_=None, **kw):
    tiers = [TierConfig(name=n, entry=e) for n, e in zip(("buy_flee", "touch", "sell"), entries)]
    tiers[0].exit, tiers[0].exit_reason = tier_exit, "漲多回吐出場"
    return _trades(run_side_backtest_tiered(_data(), "short", tiers,
                                            exit_=_bars() if exit_ is None else exit_,
                                            start_index=1, **kw))


NONE = _bars()


def test_forced_cover_closes_on_the_cover_bar():
    assert plain(_bars(2), force_exit=_bars(7)) == [(2, 7, REASON_EXIT_FORCED_COVER)]
    assert tiered([NONE, NONE, _bars(2)], force_exit=_bars(7)) == [(2, 7, REASON_EXIT_FORCED_COVER)]


def test_other_exits_on_the_cover_bar_keep_their_reason():
    close = np.full(N, 100.0)
    close[7] = 102.0                         # above the 101 defense
    assert plain(_bars(2), close=close, force_exit=_bars(7)) == [(2, 7, REASON_EXIT_DEFENSE)]
    assert plain(_bars(2), exit_=_bars(7), force_exit=_bars(7)) == [(2, 7, REASON_EXIT_SIGNAL)]
    assert tiered([NONE, NONE, _bars(2)], exit_=_bars(7), force_exit=_bars(7)) == \
        [(2, 7, REASON_EXIT_SIGNAL)]
    # a tier's own exit on the cover bar keeps its reason too
    assert tiered([_bars(2), NONE, NONE], tier_exit=_bars(7), force_exit=_bars(7)) == \
        [(2, 7, "漲多回吐出場")]


def test_no_entry_while_blocked():
    assert plain(_bars(2), entry_block=_bars(2)) == []
    assert plain(_bars(2, 4), entry_block=_bars(2, 3)) == [(4, N - 1, REASON_EXIT_END)]
    assert tiered([NONE, NONE, _bars(2, 4)], entry_block=_bars(2, 3)) == [(4, N - 1, REASON_EXIT_END)]


def test_cover_and_window_together_reopen_after_the_window():
    force, block = sc.masks(DATES, [(DATES[7], DATES[9])])
    entry = _bars(2, 8, 10)
    assert plain(entry, force_exit=force, entry_block=block) == \
        [(2, 7, REASON_EXIT_FORCED_COVER), (10, N - 1, REASON_EXIT_END)]


def test_block_does_not_stop_tier_changes_while_holding():
    # held as buy_flee from bar 2; sell (looser) fires on blocked bar 4, so
    # buy_flee's own exit on bar 6 no longer counts
    trades = tiered([_bars(2), NONE, _bars(4)], tier_exit=_bars(6), entry_block=_bars(4))
    assert trades == [(2, N - 1, REASON_EXIT_END)]


def test_cli_marks_and_clears_the_dir(tmp_path, monkeypatch):
    from tests.test_holdout import _cli
    from signal_backtest import batch
    seen = _cli(monkeypatch, tmp_path, "--short-constraints", "margin")
    assert seen["short_constraints"] == "margin"
    assert (tmp_path / batch.SHORT_CONSTRAINTS_MARKER).read_text(encoding="utf-8").strip() == "margin"
    seen = _cli(monkeypatch, tmp_path)
    assert seen["short_constraints"] == "margin_futures"   # the baseline: no mark
    assert not (tmp_path / batch.SHORT_CONSTRAINTS_MARKER).exists()
    _cli(monkeypatch, tmp_path, "--short-constraints", "off")
    assert (tmp_path / batch.SHORT_CONSTRAINTS_MARKER).read_text(encoding="utf-8").strip() == "off"


def test_versions_add_refuses_a_constrained_dir(tmp_path, monkeypatch):
    import argparse
    import pytest
    from signal_backtest import _versions as versions, batch
    (tmp_path / batch.SHORT_CONSTRAINTS_MARKER).write_text("margin\n", encoding="utf-8")
    monkeypatch.setattr(versions, "_connect", lambda: pytest.fail("opened the version DB"))
    with pytest.raises(SystemExit) as e:
        versions.cmd_add(argparse.Namespace(scan_dir=str(tmp_path), version_id="v999",
                                            summary="x", snapshot=None))
    assert e.value.code == 1


def test_availability_file_round_trip_matches_bar_dates(tmp_path):
    # written as map_codes.py writes it; a type mismatch (Timestamp vs date)
    # would silently exempt nothing and make the futures arm equal margin
    import pandas as pd
    df = pd.DataFrame({"trade_date": pd.to_datetime([DATES[3], DATES[4]]), "stock_id": ["2330", "2330"],
                       "avail": [True, True]})
    df["trade_date"] = df["trade_date"].dt.date
    df.loc[2] = [DATES[5], "2330", False]
    df.to_parquet(tmp_path / "a.parquet", index=False)
    got = sc.load_futures_availability(tmp_path / "a.parquet")
    assert got == {"2330": {DATES[3], DATES[4]}}
    assert np.flatnonzero(sc.futures_exempt(DATES, got["2330"])).tolist() == [3, 4]


def test_futures_exempt_marks_usable_days():
    assert np.flatnonzero(sc.futures_exempt(DATES, {DATES[3], DATES[4]})).tolist() == [3, 4]


def test_position_opened_on_a_futures_bar_is_never_force_covered():
    force = _bars(7)
    assert plain(_bars(2), force_exit=force, cover_exempt=_bars(2)) == [(2, N - 1, REASON_EXIT_END)]
    # exempt on the cover bar only: the position went in on margin
    assert plain(_bars(2), force_exit=force, cover_exempt=_bars(7)) == [(2, 7, REASON_EXIT_FORCED_COVER)]
    assert tiered([NONE, NONE, _bars(2)], force_exit=force, cover_exempt=_bars(2)) == \
        [(2, N - 1, REASON_EXIT_END)]
    assert tiered([NONE, NONE, _bars(2)], force_exit=force, cover_exempt=_bars(7)) == \
        [(2, 7, REASON_EXIT_FORCED_COVER)]


def _worker(monkeypatch, futures_days):
    """batch._process_one_stock on a synthetic stock: one short signal that
    enters on bars 2 and 7, a cover date on bar 7 (停券 bars 7-9)."""
    from signal_backtest import batch
    from signal_backtest import signal as sig
    from signal_backtest.signal import SignalSet, SignalSpec
    monkeypatch.setattr(batch, "load_stock_data", lambda sid, use_cache=False, adjusted=False: _data())
    spec = SignalSpec(name="t", signals=SignalSet(long_entry=_bars(), long_exit=_bars(),
                                                  short_entry=_bars(2, 7), short_exit=_bars()))
    monkeypatch.setitem(sig.SIGNAL_FACTORIES, "t_short", lambda d: spec)
    _, trades, _, _ = batch._process_one_stock("9999", ["t_short"], 1, False,
                                               [(DATES[7], DATES[9])], futures_days)
    return [((t.entry_date - D0).days, (t.exit_date - D0).days, t.exit_reason) for t in trades["t_short"]]


def test_batch_margin_arm(monkeypatch):
    assert _worker(monkeypatch, None) == [(2, 7, REASON_EXIT_FORCED_COVER)]


def test_batch_futures_arm(monkeypatch):
    # usable on the entry bar: no cover
    assert _worker(monkeypatch, {DATES[2]}) == [(2, N - 1, REASON_EXIT_END)]
    # usable only on bar 7: covered there, and bar 7's entry goes through futures
    assert _worker(monkeypatch, {DATES[7]}) == [(2, 7, REASON_EXIT_FORCED_COVER),
                                                 (7, N - 1, REASON_EXIT_END)]
    # usable nowhere: the margin arm
    assert _worker(monkeypatch, set()) == [(2, 7, REASON_EXIT_FORCED_COVER)]


def test_unset_or_empty_masks_change_nothing():
    entry, exit_ = _bars(2, 12), _bars(9)
    empty = dict(force_exit=_bars(), entry_block=_bars())
    assert plain(entry, exit_) == plain(entry, exit_, **empty) == \
        [(2, 9, REASON_EXIT_SIGNAL), (12, N - 1, REASON_EXIT_END)]
    entries = [NONE, _bars(2), _bars(12)]
    assert tiered(entries, exit_=exit_) == tiered(entries, exit_=exit_, **empty) == \
        [(2, 9, REASON_EXIT_SIGNAL), (12, N - 1, REASON_EXIT_END)]
