"""analysis.live_reconcile: the monthly consistency check of the nightly
snapshot against the deployed build's backtest, and the holdout figures.
No database, git or backtest is touched: all three are replaced by fakes."""
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import pytest

import analysis.live_reconcile as lr
from signal_backtest.engine import HOLDOUT_START
from signal_backtest.trade import REASON_EXIT_DEFENSE, REASON_EXIT_END

S, D = date(2026, 9, 30), date(2026, 10, 30)     # previous month's last night, this one's


def _trade(sid, entry, exit_, pnl, reason=REASON_EXIT_DEFENSE, side="做多"):
    return {"股票代號": sid, "股票名稱": sid, "方向": side, "進場日期": entry, "進場價": 10.0,
            "出場日期": exit_, "出場價": 11.0, "出場原因": reason, "持倉天數": 5,
            "報酬率": pnl, "防守價變化": []}


def _write(bt_dir, trades_by_key):
    for key in [k for k, _, _ in lr.SIGNALS] + ["unified_long", "unified_short"]:
        (bt_dir / key).mkdir(parents=True, exist_ok=True)
        rows = trades_by_key.get(key, [])
        df = pd.DataFrame(rows, columns=list(_trade("x", D, D, 0.0)))
        df.to_parquet(bt_dir / key / "trades.parquet", index=False)


# ── month ────────────────────────────────────────────────────────────────────


def test_months():
    assert lr.previous_month(date(2026, 11, 1)) == "2026-10"
    assert lr.previous_month(date(2027, 1, 3)) == "2026-12"
    assert lr.month_bounds("2026-02") == (date(2026, 2, 1), date(2026, 2, 28))
    assert lr.month_bounds("2026-12") == (date(2026, 12, 1), date(2026, 12, 31))


# ── backtest records ─────────────────────────────────────────────────────────


def test_backtest_records_cover_the_window_only(tmp_path):
    _write(tmp_path, {"unified_long": [
        _trade("1101", date(2026, 9, 1), date(2026, 9, 30), 0.05),          # exited before (S)
        _trade("1102", date(2026, 9, 1), date(2026, 10, 1), 0.05),          # exited in window
        _trade("1103", date(2026, 10, 1), D, -0.02),                        # exited on D
        _trade("1104", date(2026, 10, 1), date(2026, 11, 2), 0.01),         # exited after D: held
        _trade("1105", date(2026, 10, 5), D, 0.03, REASON_EXIT_END),        # open at the end
        _trade("1106", date(2026, 10, 31), date(2026, 11, 5), 0.01),        # entered after D
    ]}, )
    bt = lr.load_backtest(tmp_path, S, D).set_index("stock_id")
    assert sorted(bt.index) == ["1102", "1103", "1104", "1105"]
    assert bt.loc["1102", "state"] == "exited" and bt.loc["1102", "exit_date"] == date(2026, 10, 1)
    assert bt.loc["1103", "pnl"] == pytest.approx(-0.02)
    assert bt.loc["1104", "state"] == "held" and bt.loc["1104", "exit_date"] is None
    assert bt.loc["1105", "state"] == "held"
    assert (bt["side"] == "long").all()


def test_backtest_short_side_is_read(tmp_path):
    _write(tmp_path, {"unified_short": [_trade("2330", date(2026, 10, 2), date(2026, 10, 8),
                                               0.04, side="做空")]})
    bt = lr.load_backtest(tmp_path, S, D)
    assert list(bt["side"]) == ["short"]


# ── classification ───────────────────────────────────────────────────────────


def _rec(sid, entry, exit_=None, pnl=None, side="long"):
    return dict(stock_id=sid, side=side, entry_date=entry, exit_date=exit_, pnl=pnl,
                state="held" if exit_ is None else "exited")


E = date(2026, 10, 1)
X = date(2026, 10, 9)


def _classify(live, bt, revised=(), low=(), version="same"):
    calls = []

    def version_of(sid, side, entry, stop):
        calls.append((sid, stop))
        return version(sid) if callable(version) else version

    res = lr.classify(lr._records(live), lr._records(bt), revised=set(revised),
                      low_level=set(low), version_of=version_of)
    return res.set_index("stock_id"), calls


def test_classes_and_causes():
    live = [_rec("A", E, X, 0.05),             # consistent exit
            _rec("B", E),                      # consistent held
            _rec("C", E, X, None),             # unseen exit, no price
            _rec("D", E, X, 0.05),             # return differs
            _rec("F", E),                      # live only, recorded as rewritten
            _rec("G", E),                      # live only, below the level (not a universe cause)
            _rec("H", E, X, 0.01)]             # held in backtest instead
    bt = [_rec("A", E, X, 0.05 + 5e-5),
          _rec("B", E),
          _rec("C", E, X, 0.07),
          _rec("D", E, X, 0.03),
          _rec("I", E),                        # backtest only, below the level
          _rec("J", E, X, 0.02),               # backtest only, version differs
          _rec("H", E)]
    res, _ = _classify(live, bt, revised=[("F", "long", E)],
                       low=[("I", "long", E), ("G", "long", E)],
                       version=lambda sid: {"J": "diff", "G": "unknown"}.get(sid, "same"))
    assert res.loc["A", "cls"] == "一致" and res.loc["A", "cause"] is None
    assert res.loc["B", "cls"] == "一致"
    assert res.loc["C", "cls"] == "一致"
    assert (res.loc["D", "cls"], res.loc["D", "cause"]) == ("同筆不同結果", "未解釋")
    assert (res.loc["F", "cls"], res.loc["F", "cause"]) == ("只在線上", "已記改寫")
    assert (res.loc["G", "cls"], res.loc["G", "cause"]) == ("只在線上", "版本未知")
    assert (res.loc["H", "cls"], res.loc["H", "cause"]) == ("同筆不同結果", "未解釋")
    assert (res.loc["I", "cls"], res.loc["I", "cause"]) == ("只在回測", "母體外")
    assert (res.loc["J", "cls"], res.loc["J", "cause"]) == ("只在回測", "版本不同")


def test_exit_date_differs():
    res, _ = _classify([_rec("A", E, X, 0.05)], [_rec("A", E, date(2026, 10, 12), 0.05)])
    assert res.loc["A", "cls"] == "同筆不同結果"


def test_version_span_ends_at_exit_or_runs_to_the_last_night():
    later = date(2026, 10, 12)
    _, calls = _classify([_rec("A", E, X, 0.05), _rec("B", E, X, 0.01)],
                         [_rec("A", E, later, 0.02), _rec("B", E)])
    assert dict(calls) == {"A": later, "B": None}


def test_closure_counts_each_side_once():
    live = [_rec(s, E) for s in "ABC"]
    bt = [_rec(s, E) for s in "BCD"]
    res, _ = _classify(live, bt)
    assert len(res) == 4
    assert res["cls"].value_counts().to_dict() == {"一致": 2, "只在線上": 1, "只在回測": 1}


def test_duplicate_keys_refused():
    with pytest.raises(lr.ReconcileError):
        lr._records([_rec("A", E), _rec("A", E, X, 0.1)])


# ── version of a night ───────────────────────────────────────────────────────


class _Cur:
    def __init__(self, answers):
        self.answers, self.sql, self._rows = answers, [], []

    def execute(self, sql, params=None):
        self.sql.append((sql, params))
        for needle, rows in self.answers.items():
            if needle in sql:
                self._rows = rows(params) if callable(rows) else rows
                return
        raise AssertionError(f"unexpected SQL: {sql}")

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0]


N1, N2, N3 = date(2026, 10, 16), date(2026, 10, 19), date(2026, 10, 20)


BUILT = "2026-10-18T23:35:25"     # BUILD_INFO's local time, no zone


def _ran(day, hour=21):
    return datetime(day.year, day.month, day.day, hour, 45).astimezone()


def _version_of(monkeypatch, stamps, diffs, uncommitted=("db/industry_data.py",), ran=None):
    ran = ran or {}
    cur = _Cur({"DISTINCT snapshot_date": [{"snapshot_date": n} for n in (N1, N2, N3)],
                "tw.snapshot_runs": [{"snapshot_date": n, "build_commit": s,
                                      "created_at": ran.get(n, _ran(n))}
                                     for n, s in stamps.items()]})
    calls = []

    def git(*a, check=True):
        calls.append(a)
        return type("R", (), {"returncode": diffs.get(a[3], 128)})()
    monkeypatch.setattr(lr, "_git", git)
    v = lr.make_version_of(cur, {"commit": "aaa", "uncommitted_sources": list(uncommitted),
                                 "built_at": BUILT}, N3)
    return v, calls


def test_version_of(monkeypatch):
    # The deployed build is aaa+dirty, its only uncommitted file outside the code.
    v, calls = _version_of(monkeypatch, {N2: "aaa+dirty", N3: "bbb"}, {"bbb": 1})
    assert v("x", "long", N2, N2) == "same"          # the deployed build's own stamp
    assert v("x", "long", N1, N2) == "unknown"       # 10-16 has no run record
    assert v("x", "long", N2, None) == "diff"        # 10-20 ran bbb, whose code differs
    assert calls[0][:4] == ("diff", "--quiet", "aaa", "bbb")
    assert set(calls[0][5:]) == {"signal_backtest", "analysis", "backtest"}
    v, _ = _version_of(monkeypatch, {N2: "aaa+dirty", N3: "bbb"}, {"bbb": 0})
    assert v("x", "long", N2, None) == "same"        # bbb's code is the same


def test_uncommitted_code_cannot_be_compared(monkeypatch):
    # The deployed build carries uncommitted analysis code: even its own
    # stamp is not known to be the code the night ran.
    v, _ = _version_of(monkeypatch, {N2: "aaa+dirty"}, {}, uncommitted=("analysis/score.py",))
    assert v("x", "long", N2, N2) == "unknown"
    # Another commit, same committed code, but run dirty.
    v, _ = _version_of(monkeypatch, {N2: "bbb+dirty"}, {"bbb": 0})
    assert v("x", "long", N2, N2) == "unknown"
    # The deployed commit run clean while the deployed build was dirty elsewhere.
    v, _ = _version_of(monkeypatch, {N2: "aaa"}, {})
    assert v("x", "long", N2, N2) == "same"


def test_same_dirty_stamp_before_the_build_is_unknown(monkeypatch):
    # An earlier dirty build of the same commit stamped its nights alike; only
    # nights run after the deployed build are its own.
    v, _ = _version_of(monkeypatch, {N1: "aaa+dirty", N2: "aaa+dirty"}, {},
                       ran={N1: _ran(N1), N2: _ran(N2)})
    assert v("x", "long", N1, N1) == "unknown"       # 10-16 21:45, before 10-18 23:35
    assert v("x", "long", N2, N2) == "same"          # 10-19 21:45, after


# ── the snapshot's records ───────────────────────────────────────────────────


def test_live_records_take_each_exit_once(monkeypatch):
    A, B, C, H = "1101", "2330", "2454", "2884"
    cur = _Cur({
        "WHERE is_exited": [
            dict(stock_id=A, side="long", entry_date=E, exit_date=date(2026, 9, 25), pnl=Decimal("0.01")),
            dict(stock_id=A, side="long", entry_date=E, exit_date=date(2026, 10, 2), pnl=Decimal("0.01")),
            dict(stock_id=B, side="long", entry_date=E, exit_date=date(2026, 10, 5), pnl=Decimal("0.02")),
            dict(stock_id=B, side="long", entry_date=E, exit_date=date(2026, 10, 7), pnl=Decimal("0.02"))],
        "cause IN ('exited_unseen', 'exited_hidden')": [
            dict(stock_id=C, side="short", entry_date=E, exit_date=date(2026, 10, 8)),
            dict(stock_id=H, side="long", entry_date=E, exit_date=date(2026, 9, 29))],
        "NOT is_exited": [dict(stock_id="6505", side="long", entry_date=E)]})
    live, repeats = lr.load_live(cur, S, D)
    got = {(r.stock_id, r.state, r.exit_date) for r in live.itertuples()}
    # A exited before the window (its repeat inside it is not a new exit); H's
    # hidden exit is before the window too.
    assert got == {(B, "exited", date(2026, 10, 5)), (C, "exited", date(2026, 10, 8)),
                   ("6505", "held", None)}
    assert repeats == 2
    sql = {s.split("FROM")[1].split()[0] + ("held" if "NOT is_exited" in s else ""): (s, p)
           for s, p in cur.sql}
    exits_sql, exits_p = sql["tw.open_positions"]
    assert "is_exited AND snapshot_date <= %s" in exits_sql and exits_p == (D,)
    assert "ORDER BY snapshot_date" in exits_sql
    held_sql, held_p = sql["tw.open_positionsheld"]
    assert "snapshot_date = %s AND NOT is_exited" in held_sql and held_p == (D,)
    rev_sql, rev_p = sql["tw.position_revisions"]
    # Only what the month's nights recorded: a later night may date a gap exit
    # back onto D, which D showed held.
    assert "exit_date <= %s AND snapshot_date <= %s" in rev_sql and rev_p == (D, D)


def test_low_level_needs_every_night_below(monkeypatch):
    cur = _Cur({"unnest": [{"sid": "2603", "side": "short", "d0": E}]})
    assert lr.load_low_level(cur, [("2603", "short", E, D), ("2330", "long", E, X)]) == {
        ("2603", "short", E)}
    s, p = cur.sql[0]
    assert "NOT EXISTS" in s and "l.stock_id = k.sid" in s
    assert "BETWEEN k.d0 AND k.d1" in s and "money_level >= %s" in s
    assert p == (["2603", "2330"], ["short", "long"], [E, E], [D, X], 4)
    assert lr.load_low_level(_Cur({}), []) == set()


def test_revised_keys_and_their_window():
    cur = _Cur({"cause IN ('redated'": [
        dict(stock_id="1101", side="long", entry_date=E, new_entry_date=X),
        dict(stock_id="2330", side="short", entry_date=E, new_entry_date=None)]})
    assert lr.load_revised(cur, S, D) == {("1101", "long", E), ("1101", "long", X),
                                          ("2330", "short", E)}
    s, p = cur.sql[0]
    assert "'gone', 'not_run'" in s and "exited" not in s
    assert "snapshot_date > %s AND snapshot_date <= %s" in s and p == (S, D)


def test_window_reads_the_month_and_the_night_before():
    cur = _Cur({"BETWEEN": [{"d": D}], "snapshot_date < %s": [{"d": S}]})
    assert lr.load_window(cur, date(2026, 10, 1), date(2026, 10, 31)) == (S, D)
    assert [p for _, p in cur.sql] == [(date(2026, 10, 1), date(2026, 10, 31)), (date(2026, 10, 1),)]
    with pytest.raises(lr.ReconcileError):
        lr.load_window(_Cur({"BETWEEN": [{"d": None}]}), date(2026, 10, 1), date(2026, 10, 31))


# ── holdout ──────────────────────────────────────────────────────────────────


def test_holdout_section(tmp_path):
    before = date(2025, 3, 3)
    after = HOLDOUT_START
    end = date(2026, 11, 30)
    _write(tmp_path, {
        "pick": [_trade("1", before, date(2025, 3, 10), 0.10),
                 _trade("2", before, date(2025, 3, 11), -0.05),
                 _trade("3", after, date(2026, 11, 10), 0.08),
                 _trade("4", after, date(2026, 11, 12), -0.02)],
        "unified_long": [_trade("5", after, date(2026, 11, 20), 0.03),
                         _trade("6", after, end, 0.01, REASON_EXIT_END),        # still held
                         _trade("7", after, date(2026, 12, 3), 0.04)],          # exits after end
    })
    h = lr.holdout_section(tmp_path, end)
    assert h["合池"]["holdout"]["n"] == 2
    assert set(h["合池"]["years"]) == {2025}
    assert h["合池"]["years"][2025]["n"] == 2
    assert h["unified 多"]["holdout"]["n"] == 1
    assert h["unified 多"]["holdout_open"] == 2
    assert h["unified 空"]["holdout"] is None


# ── the backtest run ─────────────────────────────────────────────────────────


class _Popen:
    calls = []

    def __init__(self, cmd, **kw):
        _Popen.calls.append(cmd)
        self.pid = 1

    def wait(self, timeout=None):
        return 0


def _src(tmp_path, flag):
    (tmp_path / "src" / "signal_backtest").mkdir(parents=True)
    (tmp_path / "src" / "signal_backtest" / "__main__.py").write_text(
        'p.add_argument("--include-holdout")' if flag else "", encoding="utf-8")
    return tmp_path / "src"


def test_backtest_takes_every_trade(tmp_path, monkeypatch):
    monkeypatch.setattr(lr.subprocess, "Popen", _Popen)
    _Popen.calls.clear()
    lr.run_backtest(_src(tmp_path, True), tmp_path / "out", date(2026, 12, 1), raw=True)
    assert "--include-holdout" in _Popen.calls[0]
    assert "unified_long" in _Popen.calls[0][_Popen.calls[0].index("--signals") + 1]
    assert "--cache" not in _Popen.calls[0]       # it would be written into the worktree


def test_backtest_runs_raw_on_a_build_with_the_baseline_defaults(tmp_path, monkeypatch):
    # the snapshot has neither total returns nor the short-sale constraints
    monkeypatch.setattr(lr.subprocess, "Popen", _Popen)
    _Popen.calls.clear()
    src = _src(tmp_path, True)
    lr.run_backtest(src, tmp_path / "out", date(2026, 12, 1), raw=True)
    assert "--raw-return" not in _Popen.calls[0]      # an older build is raw already
    (src / "signal_backtest" / "__main__.py").write_text(
        'p.add_argument("--include-holdout")\np.add_argument("--raw-return")', encoding="utf-8")
    lr.run_backtest(src, tmp_path / "out", date(2026, 12, 1), raw=True)
    cmd = _Popen.calls[1]
    assert "--raw-return" in cmd and cmd[cmd.index("--short-constraints") + 1] == "off"


def test_baseline_backtest_runs_on_the_build_defaults(tmp_path, monkeypatch):
    # the holdout is measured as the adopted versions are: total returns and
    # the short-sale constraints, which a v370+ build runs by default
    monkeypatch.setattr(lr.subprocess, "Popen", _Popen)
    _Popen.calls.clear()
    src = _src(tmp_path, True)
    (src / "signal_backtest" / "__main__.py").write_text(
        'p.add_argument("--include-holdout")\np.add_argument("--raw-return")', encoding="utf-8")
    lr.run_backtest(src, tmp_path / "out", date(2026, 12, 1), raw=False)
    cmd = _Popen.calls[0]
    assert "--include-holdout" in cmd
    assert "--raw-return" not in cmd and "--short-constraints" not in cmd


def test_worktree_commands(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(lr, "WORKTREE", tmp_path / "wt")
    monkeypatch.setattr(lr, "_git", lambda *a, check=True: calls.append(a))
    lr.prepare_worktree("abc")
    assert calls == [("worktree", "remove", "--force", str(tmp_path / "wt")),
                     ("worktree", "add", "-f", "--detach", str(tmp_path / "wt"), "abc")]
    assert not any("prune" in a for a in calls)


def test_build_without_the_flag_before_the_cut(tmp_path, monkeypatch):
    monkeypatch.setattr(lr.subprocess, "Popen", _Popen)
    _Popen.calls.clear()
    lr.run_backtest(_src(tmp_path, False), tmp_path / "out", date(2026, 11, 1), raw=True)
    assert "--include-holdout" not in _Popen.calls[0]


def test_build_without_the_flag_after_the_cut(tmp_path, monkeypatch):
    monkeypatch.setattr(lr.subprocess, "Popen", _Popen)
    _Popen.calls.clear()
    with pytest.raises(lr.ReconcileError, match="需重建"):
        lr.run_backtest(_src(tmp_path, False), tmp_path / "out", HOLDOUT_START, raw=True)
    assert _Popen.calls == []


def test_backtest_failure(tmp_path, monkeypatch):
    class Bad(_Popen):
        def wait(self, timeout=None):
            return 3
    monkeypatch.setattr(lr.subprocess, "Popen", Bad)
    with pytest.raises(lr.ReconcileError, match="exit 3"):
        lr.run_backtest(_src(tmp_path, True), tmp_path / "out", date(2026, 12, 1), raw=True)


# ── the whole run ────────────────────────────────────────────────────────────


def _run(tmp_path, monkeypatch, *, backtest=None, sent=True, info=None, events=None,
         baseline=True, holdout_dirs=None):
    events = [] if events is None else events
    monkeypatch.setattr(lr, "OUT_ROOT", tmp_path / "out")
    monkeypatch.setattr(lr, "REPORT_DIR", tmp_path / "reports")
    monkeypatch.setattr(lr, "REPO", tmp_path)
    monkeypatch.setattr(lr, "deployed_build", lambda: info or {"commit": "abc123", "uncommitted_sources": []})
    monkeypatch.setattr(lr, "prepare_worktree", lambda c: events.append(("add", c)) or tmp_path)
    monkeypatch.setattr(lr, "remove_worktree", lambda: events.append(("remove",)))
    monkeypatch.setattr(lr, "has_baseline_defaults", lambda src: baseline)

    def fake_backtest(src, out, today, *, raw):
        events.append(("backtest", raw))
        if backtest:
            backtest(raw)
        # The baseline run books 1101 differently: read for (a), it would
        # turn the consistent position into a different result.
        pnl = 0.05 if raw else 0.07
        _write(out, {"unified_long": [_trade("1101", E, X, pnl)],
                     "unified_short": [_trade("2603", E, D, 0.0, REASON_EXIT_END, side="做空")],
                     "pick": [_trade("1101", E, X, pnl)]})
    monkeypatch.setattr(lr, "run_backtest", fake_backtest)
    if holdout_dirs is not None:
        orig = lr.holdout_section
        monkeypatch.setattr(lr, "holdout_section",
                            lambda d, end: holdout_dirs.append(d) or orig(d, end))
    cur = _Cur({"MAX(snapshot_date) AS d FROM tw.open_positions WHERE snapshot_date BETWEEN":
                [{"d": D}],
                "MAX(snapshot_date) AS d FROM tw.open_positions WHERE snapshot_date <": [{"d": S}],
                # NUMERIC arrives as Decimal, as psycopg2 returns it
                "WHERE is_exited": [dict(stock_id="1101", side="long", entry_date=E,
                                         exit_date=X, pnl=Decimal("0.05000"))],
                "cause IN ('exited_unseen', 'exited_hidden')": [],
                "NOT is_exited": [dict(stock_id="2330", side="short", entry_date=E)],
                "cause IN ('redated'": [],
                "unnest": [],
                "DISTINCT snapshot_date": [{"snapshot_date": D}],
                "tw.snapshot_runs": [{"snapshot_date": D, "build_commit": "abc123",
                                      "created_at": _ran(D)}]})

    @contextmanager
    def fake_cursor(commit=True):
        assert commit is False
        yield cur
    monkeypatch.setattr(lr, "get_cursor", fake_cursor)
    monkeypatch.setattr(lr, "send", lambda text: events.append(("send", text)) or sent)
    rc = lr.reconcile("2026-10", today=date(2026, 11, 1), do_send=True)
    return rc, events, cur


def test_reconcile_end_to_end(tmp_path, monkeypatch):
    holdout_dirs = []
    rc, events, cur = _run(tmp_path, monkeypatch, holdout_dirs=holdout_dirs)
    assert rc == 0
    # (a) on the raw run, (b) on the baseline run, both in the one worktree
    assert events[:-1] == [("add", "abc123"), ("backtest", True), ("backtest", False), ("remove",)]
    assert holdout_dirs == [tmp_path / "out" / "2026-10" / "bt_baseline"]
    text = events[-1][1]
    assert "【月對帳】2026-10" in text and "部署 abc123" in text
    assert "線上 2 筆、回測 2 筆" in text
    assert "一致 1｜同筆不同結果 0｜只在線上 1｜只在回測 1" in text
    assert "版本不同" not in text and "未解釋 2" in text
    assert "尚未開始" in text and "重複出場列" not in text
    assert f"樣本外（{HOLDOUT_START} 起進場；總報酬＋做空限制）：尚未開始" in text
    assert "早於 v370" not in text
    report = (tmp_path / "reports" / "2026-10.md").read_text(encoding="utf-8")
    assert "| 2330 | 做空 |" in report and "| 2603 | 做空 |" in report
    assert "口徑：總報酬＋做空限制。" in report
    # The backtest-only position, held to the month's last night, is checked
    # for every night from its entry to that night.
    assert [p for s, p in cur.sql if "unnest" in s] == [(["2603"], ["short"], [E], [D], 4)]
    assert not any(s.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for s, _ in cur.sql)
    # The backtest outputs hold the holdout's trades: gone once read.
    assert not (tmp_path / "out" / "2026-10" / "bt").exists()
    assert not (tmp_path / "out" / "2026-10" / "bt_baseline").exists()


def test_build_before_v370_reads_the_holdout_raw(tmp_path, monkeypatch):
    holdout_dirs = []
    rc, events, _ = _run(tmp_path, monkeypatch, baseline=False, holdout_dirs=holdout_dirs)
    assert rc == 0
    assert [e for e in events if e[0] == "backtest"] == [("backtest", True)]
    assert holdout_dirs == [tmp_path / "out" / "2026-10" / "bt"]
    text = events[-1][1]
    assert "⚠ 部署版早於 v370，樣本外為原始口徑：未計股利、無做空限制" in text
    assert "；原始口徑：未計股利、無做空限制）：尚未開始" in text
    report = (tmp_path / "reports" / "2026-10.md").read_text(encoding="utf-8")
    assert "只能沿用 (a) 的回測" in report


def test_backtest_output_removed_when_reading_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(lr, "classify", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
    with pytest.raises(RuntimeError):
        _run(tmp_path, monkeypatch)
    assert not (tmp_path / "out" / "2026-10" / "bt").exists()
    assert not (tmp_path / "out" / "2026-10" / "bt_baseline").exists()


def test_worktree_removed_when_the_backtest_fails(tmp_path, monkeypatch):
    events = []

    def boom(raw):
        raise lr.ReconcileError("backtest exit 1")
    with pytest.raises(lr.ReconcileError):
        _run(tmp_path, monkeypatch, backtest=boom, events=events)
    assert [e[0] for e in events] == ["add", "backtest", "remove"]


def test_baseline_backtest_failure_fails_the_run(tmp_path, monkeypatch):
    # no report with (a) alone: the run fails and the bat alerts
    events = []

    def boom(raw):
        if raw:
            return
        raise lr.ReconcileError("backtest exit 1")
    with pytest.raises(lr.ReconcileError):
        _run(tmp_path, monkeypatch, backtest=boom, events=events)
    assert events == [("add", "abc123"), ("backtest", True), ("backtest", False), ("remove",)]
    assert not (tmp_path / "out" / "2026-10" / "bt").exists()
    assert not (tmp_path / "out" / "2026-10" / "bt_baseline").exists()
    assert not (tmp_path / "reports" / "2026-10.md").exists()


def test_undelivered_summary_fails(tmp_path, monkeypatch):
    rc, events, _ = _run(tmp_path, monkeypatch, sent=False)
    assert rc == 1


def test_uncommitted_backtest_code_is_flagged(tmp_path, monkeypatch):
    rc, events, _ = _run(tmp_path, monkeypatch, info={
        "commit": "abc123", "uncommitted_sources": ["signal_backtest/engine.py", "db/industry_data.py"]})
    text = events[-1][1]
    assert "部署含未提交改動：signal_backtest/engine.py" in text
    assert "industry_data" not in text
    rc, events, _ = _run(tmp_path, monkeypatch, info={
        "commit": "abc123", "uncommitted_sources": ["analysis\\score.py"]})
    assert "部署含未提交改動：analysis\\score.py" in events[-1][1]


def test_repeated_exit_rows_are_reported(tmp_path, monkeypatch):
    orig = lr.load_live
    monkeypatch.setattr(lr, "load_live", lambda cur, s, e: (orig(cur, s, e)[0], 3))
    rc, events, _ = _run(tmp_path, monkeypatch)
    assert "重複出場列 3 筆" in events[-1][1]


def test_main_exit_code(monkeypatch):
    monkeypatch.setattr(lr, "reconcile", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
    assert lr.main(["2026-10", "--no-send"]) == 1
    monkeypatch.setattr(lr, "reconcile", lambda month, today, do_send: 0 if (month, do_send) == ("2026-10", False) else 5)
    assert lr.main(["2026-10", "--no-send"]) == 0


def test_summary_after_the_cut():
    res = pd.DataFrame(columns=["cls", "cause"])
    hold = {"合池": {"holdout": {"n": 3, "pf": 1.5, "win": 40.0, "payoff": 2.0}, "holdout_open": 1,
                    "years": {2024: {"pf": 1.2}, 2025: {"pf": 2.0}, 2023: {"pf": 0.9}}}}
    text = lr.summary_text("2026-11", S, date(2026, 11, 30), "abc12345", [], res, 0, 0, hold,
                           lr.REPO / "logs" / "reconcile" / "2026-11.md", basis=lr.BASIS_BASELINE)
    assert f"樣本外（{HOLDOUT_START} 起進場，累計至 2026-11-30；總報酬＋做空限制）：" in text
    assert "合池：3 筆 PF 1.50 勝率 40.0% 賺賠比 2.00，持有中 1（樣本內逐年 PF 0.90～2.00，中位 1.20）" in text
    assert "尚未開始" not in text
