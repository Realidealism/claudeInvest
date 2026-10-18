"""analysis.snapshot_runs: the build commit a snapshot run records, and the
row it writes. No database: get_cursor is a stand-in (the table and its
constraints were checked on an isolated PostgreSQL 16, see tmp/sysreview-2026-10/step6)."""
from __future__ import annotations

import subprocess
import sys
import types
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone

import pytest

import analysis.snapshot_runs as sr

HEAD = "c0ffee" * 6 + "abcd"


@pytest.fixture
def no_stamp(monkeypatch):
    monkeypatch.setitem(sys.modules, "_build_stamp", None)   # import raises ImportError


def _git(head=HEAD, dirty="", rc=0):
    def run(argv, **kwargs):
        if argv[:2] == ["git", "rev-parse"]:
            return subprocess.CompletedProcess(argv, rc, stdout=head + "\n" if head else "")
        return subprocess.CompletedProcess(argv, rc, stdout=dirty)
    return run


def test_an_exe_reads_its_stamp(monkeypatch):
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setitem(sys.modules, "_build_stamp", types.SimpleNamespace(COMMIT="abc+dirty"))
    monkeypatch.setattr(sr.subprocess, "run", lambda *a, **k: pytest.fail("git was asked"))
    assert sr.build_commit() == "abc+dirty"


def test_from_source_a_stamp_left_behind_is_ignored(monkeypatch):
    # An interrupted build can leave _build_stamp.py in the tree.
    monkeypatch.setitem(sys.modules, "_build_stamp", types.SimpleNamespace(COMMIT="old"))
    monkeypatch.setattr(sr.subprocess, "run", _git())
    assert sr.build_commit() == HEAD


def test_from_source_it_is_git_head(no_stamp, monkeypatch):
    monkeypatch.setattr(sr.subprocess, "run", _git())
    assert sr.build_commit() == HEAD


def test_from_source_with_uncommitted_sources_it_is_dirty(no_stamp, monkeypatch):
    monkeypatch.setattr(sr.subprocess, "run", _git(dirty=" M analysis/score.py\n"))
    assert sr.build_commit() == HEAD + "+dirty"


@pytest.mark.parametrize("run", [_git(rc=128), _git(head="")])
def test_git_that_cannot_say_is_unknown(no_stamp, monkeypatch, run):
    monkeypatch.setattr(sr.subprocess, "run", run)
    assert sr.build_commit() == "unknown"


def test_git_missing_is_unknown(no_stamp, monkeypatch):
    def missing(*a, **k):
        raise FileNotFoundError("git")
    monkeypatch.setattr(sr.subprocess, "run", missing)
    assert sr.build_commit() == "unknown"


def test_an_exe_without_a_stamp_does_not_ask_git(no_stamp, monkeypatch):
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sr.subprocess, "run", lambda *a, **k: pytest.fail("git was asked"))
    assert sr.build_commit() == "unknown"


def _record(monkeypatch):
    calls, commits = [], []

    @contextmanager
    def cursor(commit=True):
        commits.append(commit)
        yield types.SimpleNamespace(execute=lambda sql, params: calls.append((sql, params)))

    monkeypatch.setattr(sr, "get_cursor", cursor)
    monkeypatch.setattr(sr, "build_commit", lambda: "abc")
    return calls, commits


D = date(2026, 10, 19)
T = datetime(2026, 10, 19, 9, 5, tzinfo=timezone(timedelta(hours=8)))


@pytest.mark.parametrize("kind, when, days", [("daily", None, 900), ("intraday", T, None)])
def test_save_run_replaces_its_own_date_or_pass(monkeypatch, kind, when, days):
    calls, commits = _record(monkeypatch)
    sr.save_run(kind, D, when, days, 1500, 1490)
    (delete, dparams), (insert, iparams) = calls
    assert delete.startswith("DELETE FROM tw.snapshot_runs")
    assert "snapshot_time IS NOT DISTINCT FROM %s" in delete
    assert dparams == (kind, D, when)
    assert insert.startswith("INSERT INTO tw.snapshot_runs")
    assert iparams == (kind, D, when, "abc", days, 1500, 1490)
    assert commits == [True]      # one transaction, committed
