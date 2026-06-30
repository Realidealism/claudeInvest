from datetime import datetime

from broker.types import Side
from core.engine import RoundTrip
from data.trade_log import TradeLog


def _rt(points, reason=""):
    return RoundTrip(
        symbol="TM0000", side=Side.BUY, lot=1,
        entry_ts=datetime(2026, 6, 20, 9, 5), entry_price=44000.0,
        exit_ts=datetime(2026, 6, 20, 9, 30), exit_price=44000.0 + points,
        points=points, reason=reason,
    )


def test_trade_log_writes_header_and_rows(tmp_path):
    p = tmp_path / "paper.csv"
    log = TradeLog(str(p), point_value=10.0)
    log.append(_rt(12, "pick"))
    log.append(_rt(-5, "sell_flee"))
    lines = p.read_text(encoding="utf-8").strip().splitlines()
    assert lines[0].startswith("entry_ts,exit_ts,symbol,side,lot")
    assert lines[0].endswith("gross_ntd,signal")
    assert len(lines) == 3                      # header + 2 trades
    assert ",buy,1,44000.0,44012.0,12,120.0,pick" in lines[1]   # gross = 12*10*1
    assert lines[2].endswith("-5,-50.0,sell_flee")             # gross = -5*10*1


def test_trade_log_appends_across_instances(tmp_path):
    p = tmp_path / "paper.csv"
    TradeLog(str(p)).append(_rt(3))
    TradeLog(str(p)).append(_rt(4))             # reopening must not rewrite header
    lines = p.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 3                       # header + 2 trades (one header only)
