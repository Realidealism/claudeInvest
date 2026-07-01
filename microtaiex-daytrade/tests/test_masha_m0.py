from datetime import date, datetime

from broker.types import Bar, OpenClose, OrderRequest, Side, Trade
from core import clock
from position.state_machine import PositionStateMachine
from risk.risk_manager import RiskConfig, RiskManager
from strategy import timing


# ── §8 timing ───────────────────────────────────────────────────────────

def test_quadruple_witching_and_third_friday():
    assert clock.third_friday(2024, 12) == date(2024, 12, 20)
    assert clock.is_quadruple_witching(date(2024, 12, 20))       # 3rd Fri of Dec
    assert not clock.is_quadruple_witching(date(2024, 12, 13))   # 2nd Fri
    assert not clock.is_quadruple_witching(date(2024, 11, 15))   # non-quarter month


def test_in_tradeable_window():
    d = datetime(2024, 12, 16, 9, 0)               # Monday day session
    assert timing.in_tradeable_window(d)           # 09:00 in 08:45-11:00
    assert not timing.in_tradeable_window(d.replace(hour=12))     # 12:00 out
    assert timing.in_tradeable_window(d.replace(hour=16))         # 16:00 night window
    assert not timing.in_tradeable_window(d.replace(hour=18))     # 18:00 out
    assert not timing.in_tradeable_window(d.replace(hour=6))      # off-session


def test_is_excluded_bar():
    # monthly settlement (3rd Wed 2024-12-18): near-month close 13:30, last 6 bars → cutoff 13:00
    assert timing.is_excluded_bar(datetime(2024, 12, 18, 13, 5))
    assert not timing.is_excluded_bar(datetime(2024, 12, 18, 12, 0))
    # weekly settlement (2nd Wed 2024-12-11): cutoff 13:30
    assert timing.is_excluded_bar(datetime(2024, 12, 11, 13, 35))
    assert not timing.is_excluded_bar(datetime(2024, 12, 11, 13, 20))
    # 四巫日 (3rd Fri 2024-12-20): cutoff 13:30
    assert timing.is_excluded_bar(datetime(2024, 12, 20, 13, 35))
    # normal Monday: never excluded
    assert not timing.is_excluded_bar(datetime(2024, 12, 16, 13, 40))


# ── §9.1 出村 caps ──────────────────────────────────────────────────────

def _holding(side, entry):
    pos = PositionStateMachine()
    pos.on_order_submitted(OrderRequest("TM", side, 1, entry, open_close=OpenClose.OPEN))
    pos.on_trade(Trade("o", "TM", side, entry, 1, datetime(2024, 12, 16, 9, 0)))
    return pos


def _bar(close, high=None, low=None):
    return Bar("TM", datetime(2024, 12, 16, 9, 0), close, high or close, low or close, close, 1, "5m")


def test_per_trade_point_cap():
    rm = RiskManager(RiskConfig(max_loss_points_per_trade=20))
    pos = _holding(Side.BUY, 100.0)
    assert rm.check_stop(_bar(85.0), pos, atr=None) is None      # -15 pt, within cap
    assert rm.check_stop(_bar(79.0), pos, atr=None) is not None  # -21 pt → exit (ATR-independent)


def test_daily_loss_and_profit_halt():
    rm = RiskManager(RiskConfig(max_daily_loss_points=60, daily_profit_target_points=50))
    rm.register_trade_pnl_points(-40); assert not rm.halted
    rm.register_trade_pnl_points(-25); assert rm.halted           # 65 ≥ 60 → 停手
    rm.reset_session(); assert not rm.halted
    rm.register_trade_pnl_points(30); assert not rm.halted
    rm.register_trade_pnl_points(25); assert rm.halted            # +55 ≥ 50 → 鎖利停手


def test_masha_entry_bar_stop():
    rm = RiskManager(RiskConfig(stop_mode="masha"))
    pos = _holding(Side.BUY, 100.0)
    rm.set_entry_bar(high=102.0, low=98.0)
    assert rm.check_stop(_bar(99.0), pos, atr=None) is None       # close ≥ entry low
    assert rm.check_stop(_bar(97.0), pos, atr=None) is not None   # close < entry low → exit
