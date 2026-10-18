"""Open positions that used to drop out of the nightly table unrecorded.

The nightly snapshot reran every stock over a rolling 900-day window and only
over stocks with money_level >= 4 that day. A position whose stock turned
illiquid vanished without an exit row, and the window's start moved which
positions were open. Now every bar on record is read, last night's held
stocks are evaluated whatever their level (positions only, kept out of the
score ranking and the signal cards), and each previous-night position the
run drops anyway is kept in tw.position_revisions.

No database: the loaders, the worker, the pool and the saves are stand-ins.
"""
import sys
import types
from datetime import date

import pytest

import analysis.daily_snapshot as ds
import analysis.intraday_snapshot as isn
import daily_update as du
from daily_update_guards import no_side_effects  # noqa: F401

DAY = date(2026, 10, 19)
PREV = date(2026, 10, 16)
CAUSES = ("kept", "exited", "exited_unseen", "redated", "gone", "not_run")


class _SyncPool:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def imap_unordered(self, fn, items, chunksize=1):
        return map(fn, items)


def _result(sid):
    return {"sid": sid, "lp": 1.0, "sp": 1.0, "tv": 1.0, "fires": ["pick"],
            "open_positions": [], "lp_d1": None, "sp_d1": None, "lp_d2": None,
            "sp_d2": None, "lp_d3": None, "sp_d3": None, "vol_status": 0,
            "recent_exits": {"long": {}, "short": {}}, "last_date": DAY}


def _stand_ins(monkeypatch, mod, *, active, held):
    """Records what each save receives: the stock ids of its results."""
    got = {"items": [], "runs": []}
    monkeypatch.setattr(mod, "Pool", _SyncPool, raising=False)
    import multiprocessing
    monkeypatch.setattr(multiprocessing, "Pool", _SyncPool)

    def worker(item):
        got["items"].append(item)
        return _result(item[0] if isinstance(item, tuple) else item), []
    monkeypatch.setattr(mod, "_eval_stock", worker)
    monkeypatch.setattr(mod, "_load_active_stock_ids", lambda d: list(active))
    monkeypatch.setattr(mod, "load_held_stock_ids", lambda d: list(held))
    monkeypatch.setattr(mod, "_load_prev_score_ranks", lambda *a: {})
    monkeypatch.setattr(mod, "save_run", lambda *a: got["runs"].append(a))

    def save(name):
        def _save(*args):
            results = args[-2] if name == "_save_score_side" else args[-1]
            got.setdefault(name, []).append(sorted(r["sid"] for r in results))
            if name == "_save_open_positions":
                return {"long": 0, "short": 0, "exited_long": 0, "exited_short": 0,
                        "revisions": 0, "exited_unseen": 0,
                        "revision_counts": dict.fromkeys(CAUSES, 0)}
            if name == "_save_signal_fires":
                return {s: 0 for s, _ in mod.SIGNALS}
            return 0
        return _save
    for name in ("_save_score_side", "_save_signal_fires", "_save_open_positions"):
        monkeypatch.setattr(mod, name, save(name))
    monkeypatch.setitem(sys.modules, "analysis.tx_status", types.SimpleNamespace(
        compute_and_save_tx_status=lambda **k: None))
    if mod is ds:
        for check in ("_check_market_breadth_fresh", "_check_market_vol_fresh",
                      "_check_liquidity_fresh"):
            monkeypatch.setattr(ds, check, lambda d: None)
    else:
        from contextlib import contextmanager
        import analysis.market_thermometer as mt
        monkeypatch.setattr(isn, "_compute_volume_scale", lambda now, final_pass=False: 1.0)
        monkeypatch.setattr(isn, "compute_live_breadth", lambda *a, **k: {
            k: 0 for k in ("total", "short_up", "short_down", "medium_up", "medium_down",
                           "long_up", "long_down")})
        monkeypatch.setattr(isn, "_save_breadth_sidecar", lambda *a: None)

        @contextmanager
        def get_cursor(commit=True):
            yield object()
        monkeypatch.setattr(isn, "get_cursor", get_cursor)
        monkeypatch.setattr(mt, "build_intraday_stance", lambda cur, scale, now: None)
    return got


@pytest.mark.parametrize("mod", [ds, isn], ids=["daily", "intraday"])
def test_held_stock_below_the_cutoff_is_evaluated_for_its_positions_only(monkeypatch, mod):
    got = _stand_ins(monkeypatch, mod, active=["1101", "2330"], held=["2330", "3008"])
    mod.run(DAY) if mod is ds else mod.run()
    sids = [i[0] if isinstance(i, tuple) else i for i in got["items"]]
    assert sorted(sids) == ["1101", "2330", "3008"]
    assert got["_save_open_positions"] == [["1101", "2330", "3008"]]
    assert got["_save_score_side"] == [["1101", "2330"], ["1101", "2330"]]
    assert got["_save_signal_fires"] == [["1101", "2330"]]
    # Loaded counts the held-only stock; scored does not.
    assert [r[-2:] for r in got["runs"]] == [(3, 2)]


def test_daily_reads_every_bar_on_record(monkeypatch):
    got = _stand_ins(monkeypatch, ds, active=["1101"], held=[])
    ds.run(DAY)
    assert got["items"] == [("1101", None, DAY)]
    assert got["runs"][0][3] is None
    assert not hasattr(ds, "HISTORY_DAYS")


def test_intraday_and_nightly_share_the_held_set():
    assert isn.load_held_stock_ids is ds.load_held_stock_ids


# ── classifying last night's positions ─────────────────────────────────────

def _prev(sid, side, entry):
    return {"snapshot_date": PREV, "stock_id": sid, "side": side, "entry_date": entry,
            "entry_price": 50.0, "entry_tier": "buy"}


def _pos(side, entry, exited=False):
    return {"side": side, "entry_date": entry, "is_exited": exited}


def _ran(sid, *positions, exits=None):
    return {"sid": sid, "open_positions": list(positions),
            "recent_exits": {"long": {}, "short": {}, **(exits or {})}}


def test_each_held_position_ends_in_exactly_one_class():
    prev = [_prev("1101", "long", date(2026, 9, 1)),    # kept
            _prev("2330", "long", date(2026, 9, 2)),    # exited
            _prev("2603", "long", date(2026, 9, 3)),    # redated
            _prev("3008", "short", date(2026, 9, 4)),   # gone
            _prev("6505", "long", date(2026, 9, 5)),    # not_run
            _prev("2454", "long", date(2026, 9, 7))]    # exited_unseen
    results = [_ran("1101", _pos("long", date(2026, 9, 1))),
               _ran("2330", _pos("long", date(2026, 9, 2), exited=True)),
               _ran("2603", _pos("long", date(2026, 10, 1))),
               # held on the other side only: still gone on the short side
               _ran("3008", _pos("long", date(2026, 10, 2))),
               # exited on 10-17, a night with no snapshot between 10-16 and 10-19
               _ran("2454", exits={"long": {date(2026, 9, 7): date(2026, 10, 17)}})]
    revisions, counts = ds._classify_revisions(prev, results)
    assert counts == dict.fromkeys(CAUSES, 1)
    assert [(v["stock_id"], v["cause"], v["new_entry_date"], v["new_is_exited"], v["exit_date"])
            for v in revisions] == [
        ("2603", "redated", date(2026, 10, 1), False, None),
        ("3008", "gone", None, None, None),
        ("6505", "not_run", None, None, None),
        ("2454", "exited_unseen", None, None, date(2026, 10, 17))]
    assert all(v["snapshot_date"] == PREV and v["entry_price"] == 50.0 for v in revisions)


def test_a_new_path_that_already_exited_is_a_redate_marked_exited():
    # The rerun entered on another date and exited today: the position held
    # last night is not the one that exited.
    revisions, counts = ds._classify_revisions(
        [_prev("2603", "long", date(2026, 9, 3))],
        [_ran("2603", _pos("long", date(2026, 10, 1), exited=True))])
    assert counts["redated"] == 1
    assert (revisions[0]["new_entry_date"], revisions[0]["new_is_exited"]) == (
        date(2026, 10, 1), True)


def test_an_exit_on_or_before_the_previous_snapshot_is_a_rerun_change():
    # Last night showed it held; an exit the rerun now puts on that night or
    # earlier rewrites history, it is not an unseen exit.
    for exit_day in (PREV, date(2026, 10, 1)):
        revisions, counts = ds._classify_revisions(
            [_prev("2454", "long", date(2026, 9, 7))],
            [_ran("2454", exits={"long": {date(2026, 9, 7): exit_day}})])
        assert counts["gone"] == 1 and revisions[0]["exit_date"] is None, exit_day


def test_nothing_held_last_night_revises_nothing():
    assert ds._classify_revisions([], [_ran("1101")]) == ([], dict.fromkeys(CAUSES, 0))


def test_worker_reports_recent_exits_before_the_last_bar(monkeypatch):
    import numpy as np
    from datetime import timedelta
    n = 80                                   # the worker skips stocks under 60 bars
    days = [date(2026, 7, 1) + timedelta(days=i) for i in range(n)]
    data = types.SimpleNamespace(
        n=n, dates=days, turnover=np.ones(n),
        volume_result=types.SimpleNamespace(volume_status=np.zeros(n, dtype=int)))
    def trade(entry, exit_):
        return types.SimpleNamespace(entry_date=entry, exit_date=exit_, defense_events=[])
    trades = [trade(days[50], days[55]),     # older than RECENT_EXIT_BARS
              trade(days[62], days[70]),     # recent
              trade(days[75], days[79])]     # on the last bar: today's own row
    spec = types.SimpleNamespace(
        long_tiers=[1], short_tiers=[1], long_floor_period=8, short_floor_period=8,
        signals=types.SimpleNamespace(long_exit=None, short_exit=None))
    pct = types.SimpleNamespace(pct=1.0)
    board = types.SimpleNamespace(evaluate=lambda d, i: types.SimpleNamespace(
        total=types.SimpleNamespace(long=pct, short=pct)))
    monkeypatch.setattr(ds, "load_stock_data", lambda *a, **k: data)
    monkeypatch.setattr(ds, "_BOARD", board)
    monkeypatch.setattr(ds, "post_break_blackout", lambda d: np.ones(d.n, dtype=bool))
    monkeypatch.setattr(ds, "unified_long_factory", lambda d: spec)
    monkeypatch.setattr(ds, "unified_short_factory", lambda d: spec)
    monkeypatch.setattr(ds, "run_side_backtest_tiered",
                        lambda d, side, *a, **k: types.SimpleNamespace(trades=trades))
    monkeypatch.setattr(ds, "RECENT_EXIT_BARS", 15)
    r, errors = ds._eval_stock(("2330", None, days[-1]))
    assert errors == [] and r["last_date"] == days[-1]
    assert r["recent_exits"] == {"long": {days[62]: days[70]}, "short": {days[62]: days[70]}}


# ── the write: positions and revisions in one transaction ──────────────────

class _Conn:
    def __init__(self, prev_rows):
        self.log = []
        self.params = []
        self.prev_rows = prev_rows

    def cursor(self):
        conn = self

        class _Cur:
            description = [(c,) for c in ("snapshot_date", "stock_id", "side", "entry_date",
                                          "entry_price", "entry_tier")]

            def execute(self, sql, params=None):
                conn.log.append(" ".join(sql.split()[:3]))
                conn.params.append(params)

            def fetchall(self):
                return [tuple(r.values()) for r in conn.prev_rows]
        return _Cur()

    def commit(self):
        self.log.append("COMMIT")

    def close(self):
        self.log.append("CLOSE")


def test_revisions_are_written_with_the_positions(monkeypatch):
    conn = _Conn([_prev("3008", "short", date(2026, 9, 4)),
                  _prev("2454", "long", date(2026, 9, 7))])
    batches = []
    monkeypatch.setattr(ds, "get_connection", lambda: conn)
    monkeypatch.setattr(ds, "execute_batch", lambda cur, sql, rows: batches.append(
        (" ".join(sql.split())[:32], rows)) or conn.log.append("BATCH"))
    monkeypatch.setattr(ds, "build_commit", lambda: conn.log.append("BUILD_COMMIT") or "abc")
    counts = ds._save_open_positions(DAY, [
        {**_ran("3008"), "tv": 1.0},
        {**_ran("2454", exits={"long": {date(2026, 9, 7): date(2026, 10, 17)}}), "tv": 1.0}])
    # The commit is read before the transaction opens.
    assert conn.log == ["BUILD_COMMIT", "SELECT p.snapshot_date, p.stock_id,",
                        "DELETE FROM tw.open_positions", "BATCH",
                        "DELETE FROM tw.position_revisions", "BATCH", "COMMIT", "CLOSE"]
    # Every statement is about the run's own date; the previous night is read
    # as the latest one before it.
    assert conn.params == [(DAY,), (DAY,), (DAY,)]
    assert batches[1][1] == [
        (DAY, PREV, "3008", "short", date(2026, 9, 4), 50.0, "buy", "gone",
         None, None, None, "abc"),
        (DAY, PREV, "2454", "long", date(2026, 9, 7), 50.0, "buy", "exited_unseen",
         None, None, date(2026, 10, 17), "abc")]
    assert counts["revisions"] == 1 and counts["exited_unseen"] == 1
    assert counts["revision_counts"]["gone"] == 1


def test_a_rerun_with_nothing_dropped_clears_the_date(monkeypatch):
    conn = _Conn([])
    monkeypatch.setattr(ds, "get_connection", lambda: conn)
    monkeypatch.setattr(ds, "execute_batch", lambda *a: conn.log.append("BATCH"))
    monkeypatch.setattr(ds, "build_commit", lambda: "abc")
    ds._save_open_positions(DAY, [])
    assert "DELETE FROM tw.position_revisions" in conn.log
    assert conn.log.count("BATCH") == 1


def test_the_previous_night_is_read_as_the_latest_before_the_date():
    sql = " ".join(ds._PREV_HELD.split())
    assert "MAX(snapshot_date) FROM tw.open_positions WHERE snapshot_date < %s" in sql
    assert "AND NOT p.is_exited" in sql


# ── the nightly summary ────────────────────────────────────────────────────

def test_summary_carries_the_revision_line():
    msg = du._build_daily_update_message(DAY, [("A", "ok")], {}, ["持倉重算改寫 3 筆（見 tw.position_revisions）"])
    assert msg.splitlines() == [f"[每日更新] {DAY} 全部成功（成功 1 / 失敗 0 / 跳過 0）",
                                "持倉重算改寫 3 筆（見 tw.position_revisions）"]
    msg = du._build_daily_update_message(DAY, [("A", "failed")], {}, ["持倉重算改寫 3 筆（見 tw.position_revisions）"])
    assert "持倉重算改寫 3 筆" in msg and "失敗：A" in msg


def test_summary_without_notes_is_unchanged():
    assert du._build_daily_update_message(DAY, [("A", "ok")]) == (
        f"[每日更新] {DAY} 全部成功（成功 1 / 失敗 0 / 跳過 0）")
