"""The daily factory's holdout: trades entered on or after HOLDOUT_START stay
out of every backtest and sweep output unless --include-holdout asks for
them, and a dir that kept them cannot be registered as a version."""
import argparse
import sys
from datetime import date, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest

import signal_backtest.__main__ as cli
import signal_backtest._versions as versions
import signal_backtest.batch as batch
import signal_backtest.sweep as sweep
from signal_backtest.engine import HOLDOUT_START
from signal_backtest.trade import SideResult, Trade

BEFORE = HOLDOUT_START - timedelta(days=1)
AFTER = HOLDOUT_START + timedelta(days=1)


def _t(entry, sid="1101"):
    return Trade(sid, sid, "做多", entry, 10.0, [], entry + timedelta(days=3), 11.0,
                 "防守價觸發", 3, 0.1)


TRADES = [_t(BEFORE), _t(HOLDOUT_START), _t(AFTER)]


def test_cut_is_the_ruled_date():
    assert HOLDOUT_START == date(2026, 11, 2)


def test_trades_written_without_the_holdout(tmp_path):
    batch._write_trades(TRADES, tmp_path / "a.parquet")
    assert list(pd.read_parquet(tmp_path / "a.parquet")["進場日期"]) == [BEFORE]
    batch._write_trades(TRADES, tmp_path / "b.parquet", include_holdout=True)
    assert len(pd.read_parquet(tmp_path / "b.parquet")) == 3


def test_sides_count_what_was_written(tmp_path):
    side = SideResult("1101", "1101", "做多", BEFORE, AFTER, list(TRADES))
    batch._write_sides([side], tmp_path / "a.parquet")
    assert list(pd.read_parquet(tmp_path / "a.parquet")["交易數"]) == [1]
    batch._write_sides([side], tmp_path / "b.parquet", include_holdout=True)
    assert list(pd.read_parquet(tmp_path / "b.parquet")["交易數"]) == [3]


@pytest.mark.parametrize("include, n", [(False, 1), (True, 3)])
def test_multi_run_honours_the_flag(tmp_path, monkeypatch, include, n):
    monkeypatch.setattr(batch, "fetch_listed_stocks", lambda: [("1101", "x")])
    side = SideResult("1101", "1101", "做多", BEFORE, AFTER, list(TRADES))
    monkeypatch.setattr(batch, "_process_one_stock", lambda sid, names, si, cache, short_events, futures_days, adjusted, total_return: (
        sid, {"pick": list(TRADES)}, {"pick": [side]}, {"pick": []}))
    batch.run_batch_multi({"pick": object()}, tmp_path, workers=1, include_holdout=include)
    assert len(pd.read_parquet(tmp_path / "pick" / "trades.parquet")) == n
    assert list(pd.read_parquet(tmp_path / "pick" / "sides.parquet")["交易數"]) == [n]


def _cli(monkeypatch, tmp_path, *extra):
    seen = {}

    def fake_multi(**kw):
        seen.update(kw)
        d = kw["output_dir"] / "pick"
        d.mkdir(parents=True, exist_ok=True)
        batch._write_trades(TRADES, d / "trades.parquet", kw["include_holdout"])
        return {"pick": (d / "trades.parquet", d / "sides.parquet")}
    monkeypatch.setattr(cli, "run_batch_multi", fake_multi)
    monkeypatch.setattr(cli, "format_report", lambda *a, **k: "")
    monkeypatch.setattr(cli, "aggregate", lambda df: SimpleNamespace(
        per_stock=pd.DataFrame({"a": [1]})))
    monkeypatch.setattr(sys, "argv", ["x", "--signals", "pick", "--output", str(tmp_path), *extra])
    cli.main()
    return seen


def test_cli_flag_marks_the_dir(tmp_path, monkeypatch, capsys):
    seen = _cli(monkeypatch, tmp_path, "--include-holdout")
    assert seen["include_holdout"] is True
    assert (tmp_path / batch.HOLDOUT_MARKER).exists()
    assert "不得用於調參" in capsys.readouterr().err


def test_cli_default_leaves_no_mark(tmp_path, monkeypatch):
    seen = _cli(monkeypatch, tmp_path)
    assert seen["include_holdout"] is False
    assert not (tmp_path / batch.HOLDOUT_MARKER).exists()


def test_cli_default_clears_an_old_mark(tmp_path, monkeypatch):
    # A clean run into a dir an earlier --include-holdout run used.
    (tmp_path / batch.HOLDOUT_MARKER).write_text("x", encoding="utf-8")
    # and one a --signal pick --include-holdout run left in the signal's dir
    (tmp_path / "pick").mkdir()
    (tmp_path / "pick" / batch.HOLDOUT_MARKER).write_text("x", encoding="utf-8")
    _cli(monkeypatch, tmp_path)
    assert not (tmp_path / batch.HOLDOUT_MARKER).exists()
    assert not (tmp_path / "pick" / batch.HOLDOUT_MARKER).exists()


@pytest.mark.parametrize("where", ["", "pick"])
def test_versions_add_refuses_a_holdout_dir(tmp_path, monkeypatch, where):
    (tmp_path / where).mkdir(exist_ok=True)
    (tmp_path / where / batch.HOLDOUT_MARKER).write_text("x", encoding="utf-8")
    monkeypatch.setattr(versions, "_connect", lambda: pytest.fail("opened the version DB"))
    with pytest.raises(SystemExit) as e:
        versions.cmd_add(argparse.Namespace(scan_dir=str(tmp_path), version_id="v999",
                                            summary="x", snapshot=None))
    assert e.value.code == 1


def test_versions_add_takes_an_unmarked_dir(tmp_path, monkeypatch):
    opened = []

    def connect():
        opened.append(1)
        raise RuntimeError("stop here")
    monkeypatch.setattr(versions, "_connect", connect)
    with pytest.raises(RuntimeError):
        versions.cmd_add(argparse.Namespace(scan_dir=str(tmp_path), version_id="v999",
                                            summary="x", snapshot=None))
    assert opened == [1]


def test_sweep_never_sees_the_holdout(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    mod = SimpleNamespace(NAME="t", SIGNAL="pick", GRID=[(1,)])
    monkeypatch.setattr(sweep, "_load_spec", lambda p: mod)
    monkeypatch.setattr(sweep, "_init_worker", lambda p: None)
    monkeypatch.setattr(sweep, "fetch_listed_stocks", lambda: [("1101", "x"), ("1102", "y")])
    monkeypatch.setattr(sweep, "load_short_inputs", lambda mode: (None, None))
    old = date(2020, 1, 6)

    def rows(args):
        sid = args[0]
        return {0: [{"股票代號": sid, "股票名稱": sid, "方向": "做多", "進場日期": d,
                     "進場價": 10.0, "出場日期": d + timedelta(days=3), "出場價": 11.0,
                     "出場原因": "防守價觸發", "持倉天數": 3, "報酬率": r}
                    for d, r in ((old, 0.1), (BEFORE, -0.05), (HOLDOUT_START, 0.5), (AFTER, -0.2))]}
    monkeypatch.setattr(sweep, "_process_one_stock", rows)
    sweep.run_sweep("spec.py", workers=1, use_cache=False, no_archive=True)
    df = pd.read_parquet(tmp_path / "tmp" / "sweeps" / "t" / "trades_0.parquet")
    assert sorted(set(df["進場日期"])) == [old, BEFORE]
    out = capsys.readouterr().out
    assert f"[{old} ~ {BEFORE}]" in out


def test_cron_alert_has_the_reconcile_event(monkeypatch):
    import telegram_bot.cron_alert as ca
    sent = []
    monkeypatch.setattr(ca, "send_sync", lambda text: sent.append(text) or True)
    assert ca.main(["live_reconcile_failed", "1"]) == 0
    assert sent == ["[排程] 月對帳失敗（exit 1），見 logs\\live_reconcile.log"]
