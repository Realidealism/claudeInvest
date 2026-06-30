from datetime import datetime

from broker.types import Bar, OpenClose, OrderRequest, Side, Trade
from position.state_machine import PositionStateMachine
from risk.risk_manager import RiskConfig, RiskManager
from strategy.base import Signal, SignalType


def _signal(stype, price=100.0):
    return Signal("TMFR1", stype, datetime(2024, 12, 18, 9, 0), price)


def _holding(side, entry, lot=1):
    pos = PositionStateMachine()
    pos.on_order_submitted(OrderRequest("TMFR1", side, lot, entry, open_close=OpenClose.OPEN))
    pos.on_trade(Trade("o", "TMFR1", side, entry, lot, datetime(2024, 12, 18, 9, 0)))
    return pos


def _bar(close):
    return Bar("TMFR1", datetime(2024, 12, 18, 9, 0), close, close, close, close, 1, "5m")


def test_flat_long_signal_opens():
    rm = RiskManager(RiskConfig(max_lots=1))
    order = rm.evaluate_signal(_signal(SignalType.LONG), PositionStateMachine())
    assert order is not None
    assert order.side is Side.BUY and order.open_close is OpenClose.OPEN and order.lot == 1


def test_same_direction_no_duplicate():
    rm = RiskManager()
    assert rm.evaluate_signal(_signal(SignalType.LONG), _holding(Side.BUY, 100.0)) is None


def test_opposite_signal_exits_to_flip():
    rm = RiskManager()
    order = rm.evaluate_signal(_signal(SignalType.SHORT), _holding(Side.BUY, 100.0))
    assert order is not None and order.side is Side.SELL and order.open_close is OpenClose.COVER


def test_force_close_blocks_entry():
    rm = RiskManager()
    assert rm.evaluate_signal(_signal(SignalType.LONG), PositionStateMachine(), force_close=True) is None


def test_halt_blocks_entry():
    rm = RiskManager(RiskConfig(max_daily_loss_points=10))
    rm.register_trade_pnl_points(-10)
    assert rm.halted
    assert rm.evaluate_signal(_signal(SignalType.LONG), PositionStateMachine()) is None


def _bar2(high, low, close):
    return Bar("TMFR1", datetime(2024, 12, 18, 9, 0), close, high, low, close, 1, "5m")


def test_stop_loss_triggers():
    rm = RiskManager(RiskConfig(stop_loss_atr_mult=2.0))
    pos = _holding(Side.BUY, 100.0)
    assert rm.check_stop(_bar(97.0), pos, atr=2.0) is None         # extreme 100, trail 96
    order = rm.check_stop(_bar(95.0), pos, atr=2.0)                # 95 <= 96
    assert order is not None and order.side is Side.SELL and order.open_close is OpenClose.COVER


def test_trailing_stop_ratchets_up():
    rm = RiskManager(RiskConfig(stop_loss_atr_mult=2.0))
    pos = _holding(Side.BUY, 100.0)
    assert rm.check_stop(_bar2(high=110, low=108, close=110), pos, atr=2.0) is None  # extreme->110
    # pull back: trail = 110 - 4 = 106; close 105 <= 106 -> exit (locked in profit)
    order = rm.check_stop(_bar2(high=106, low=105, close=105), pos, atr=2.0)
    assert order is not None and order.side is Side.SELL


def test_trailing_stop_does_not_loosen_on_atr_expansion():
    rm = RiskManager(RiskConfig(stop_loss_atr_mult=2.0))
    pos = _holding(Side.BUY, 100.0)
    # extreme stays 100; stop armed at 100 - 2*2 = 96
    assert rm.check_stop(_bar2(high=100, low=98, close=99), pos, atr=2.0) is None
    # ATR expands to 5 -> raw line would loosen to 100 - 10 = 90, but the ratchet
    # holds it at 96; close 92 sits above 90 yet below 96 -> still exits.
    order = rm.check_stop(_bar2(high=100, low=91, close=92), pos, atr=5.0)
    assert order is not None and order.side is Side.SELL


def test_stop_buffer_tolerates_small_breach():
    rm = RiskManager(RiskConfig(stop_loss_atr_mult=2.0, stop_buffer_atr=0.5))
    pos = _holding(Side.BUY, 100.0)
    # extreme 100, atr 2 -> line 96; buffer 0.5*2=1 -> fires only at close <= 95.
    assert rm.check_stop(_bar2(high=100, low=95, close=95.5), pos, atr=2.0) is None  # poke tolerated
    order = rm.check_stop(_bar2(high=100, low=94, close=94.0), pos, atr=2.0)         # breach beyond buffer
    assert order is not None and order.side is Side.SELL


def test_force_close_exits_holding():
    rm = RiskManager()
    order = rm.force_close(_holding(Side.SELL, 100.0), price=101.0)
    assert order is not None and order.side is Side.BUY


def test_flat_signal_on_empty_is_noop():
    rm = RiskManager()
    assert rm.evaluate_signal(_signal(SignalType.FLAT), PositionStateMachine()) is None


def test_daily_loss_accumulates_to_halt():
    rm = RiskManager(RiskConfig(max_daily_loss_points=10))
    rm.register_trade_pnl_points(-6)
    assert not rm.halted
    rm.register_trade_pnl_points(-4)
    assert rm.halted
    rm.reset_session()
    assert not rm.halted and rm.daily_loss_points == 0.0
