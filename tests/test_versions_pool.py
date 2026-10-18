"""Pooled six-signal stats in the version history (signal_backtest._versions).

The version DB is a temporary file and the trades are written to tmp_path;
data/signal_versions.db is never opened.
"""
from __future__ import annotations

import sqlite3
from argparse import Namespace

import pandas as pd
import pytest

import signal_backtest._compare as cmp
import signal_backtest._versions as ver

# Raw returns per signal. Net of the 0.4% cost: +.096 -.054 +.016 -.014 +.026
# -.024 and 0.0, so pooled PF_net = .138 / .092 = 1.5. Raw winners .10 .02
# .03 .004 (4 of 7), losers -.05 -.01 -.02: payoff = (.154/4) / (.08/3) = 1.44375.
RETURNS = {"pick": [0.10, -0.05], "touch": [0.02], "buy": [-0.01], "sell": [0.03],
           "buy_flee": [-0.02], "sell_flee": [0.004]}
LAST_EXIT = "2026-10-15"


def _write(base, returns=RETURNS, last_exit=LAST_EXIT):
    for i, (sig, rs) in enumerate(returns.items()):
        d = base / sig
        d.mkdir(parents=True, exist_ok=True)
        exits = [last_exit if (sig == "sell" and j == 0) else f"2026-09-{10 + i:02d}"
                 for j in range(len(rs))]
        pd.DataFrame({"報酬率": rs, "出場日期": pd.to_datetime(exits),
                      "持倉天數": [5] * len(rs)}).to_parquet(d / "trades.parquet")
    return base


def test_pool_is_the_six_signals_concatenated(tmp_path):
    p = cmp.compute_pool(_write(tmp_path))
    assert p["n"] == 7
    assert p["pf_net"] == pytest.approx(1.5)
    assert p["win_pct"] == pytest.approx(400 / 7)
    assert p["payoff"] == pytest.approx(1.44375)
    assert p["data_cutoff"] == LAST_EXIT


def test_pool_without_one_signal_is_none(tmp_path):
    base = _write(tmp_path)
    (base / "sell_flee" / "trades.parquet").unlink()
    assert cmp.compute_pool(base) is None


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "versions.db"
    monkeypatch.setattr(ver, "DB_PATH", path)
    monkeypatch.setattr(ver, "_commit_id", lambda: "7edaf55d-dirty")
    return path


def _add(version_id, scan_dir):
    ver.cmd_add(Namespace(version_id=version_id, summary=f"s {version_id}",
                          scan_dir=str(scan_dir), snapshot=None))


def test_add_records_the_pool_and_the_commit(db, tmp_path):
    _add("v1", _write(tmp_path / "v1"))
    row = sqlite3.connect(db).execute(
        "SELECT pool_n, pool_pf_net, pool_payoff, data_cutoff, commit_id "
        "FROM versions WHERE version_id = 'v1'").fetchone()
    assert row[0] == 7 and row[1] == pytest.approx(1.5) and row[2] == pytest.approx(1.44375)
    assert row[3:] == (LAST_EXIT, "7edaf55d-dirty")


def test_add_with_a_signal_missing_leaves_the_pool_empty(db, tmp_path):
    base = _write(tmp_path / "v1")
    (base / "buy" / "trades.parquet").unlink()
    _add("v1", base)
    row = sqlite3.connect(db).execute(
        "SELECT pool_n, pool_pf_net, commit_id FROM versions").fetchone()
    assert row == (None, None, "7edaf55d-dirty")


def test_list_shows_the_pool_and_its_delta(db, tmp_path, capsys):
    _add("v1", _write(tmp_path / "v1"))
    better = {**RETURNS, "sell_flee": [0.014]}    # net +.010 more: PF (.148)/(.092)
    _add("v2", _write(tmp_path / "v2", better))
    ver.cmd_list(Namespace())
    out = capsys.readouterr().out
    v1 = next(line for line in out.splitlines() if line.startswith("v1"))
    v2 = next(line for line in out.splitlines() if line.startswith("v2"))
    assert "1.5000" in v1 and "1.44" in v1 and "57.1" in v1 and LAST_EXIT in v1
    assert f"{0.148 / 0.092:.4f}" in v2
    assert f"{0.148 / 0.092 - 1.5:+.4f} " in v2      # same data cutoff: no mark


def test_list_marks_a_delta_across_data_windows(db, tmp_path, capsys):
    _add("v1", _write(tmp_path / "v1"))
    _add("v2", _write(tmp_path / "v2", last_exit="2026-10-16"))
    ver.cmd_list(Namespace())
    v2 = next(line for line in capsys.readouterr().out.splitlines() if line.startswith("v2"))
    assert "2026-10-16" in v2 and "+0.0000*" in v2


def test_old_schema_gains_the_columns_once_and_keeps_its_rows(db):
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE versions (version_id TEXT PRIMARY KEY, date TEXT NOT NULL, "
                 "summary TEXT, snapshot_path TEXT, cost_pct REAL DEFAULT 0.004)")
    conn.execute("INSERT INTO versions VALUES ('v0', '2026-01-01', 'old', 'x', 0.004)")
    conn.commit()
    conn.close()
    ver._connect().close()
    ver._connect().close()      # a second open must not try to add them again
    conn = sqlite3.connect(db)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(versions)")]
    assert cols[-6:] == [name for name, _ in ver.VERSION_COLUMNS_ADDED]
    assert conn.execute("SELECT version_id, summary, pool_pf_net FROM versions").fetchall() \
        == [("v0", "old", None)]
