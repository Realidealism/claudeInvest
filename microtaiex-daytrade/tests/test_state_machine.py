from datetime import datetime

from broker.types import OpenClose, OrderRequest, Side, Trade
from position.state_machine import PositionStateMachine, PosState


def _open(side):
    return OrderRequest("TMFR1", side, 1, 100.0, open_close=OpenClose.OPEN)


def _cover(side):
    return OrderRequest("TMFR1", side, 1, 105.0, open_close=OpenClose.COVER)


def _fill(side, price):
    return Trade("o1", "TMFR1", side, price, 1, datetime(2024, 12, 18, 9, 0))


def test_full_lifecycle():
    pos = PositionStateMachine()
    assert pos.state is PosState.FLAT

    assert pos.on_order_submitted(_open(Side.BUY)) is True
    assert pos.state is PosState.PENDING_ENTRY

    pos.on_trade(_fill(Side.BUY, 100.0))
    assert pos.state is PosState.HOLDING
    assert pos.side is Side.BUY and pos.entry_price == 100.0 and pos.lot == 1

    assert pos.on_order_submitted(_cover(Side.SELL)) is True
    assert pos.state is PosState.PENDING_EXIT

    pos.on_trade(_fill(Side.SELL, 105.0))
    assert pos.is_flat() and pos.side is None and pos.entry_price is None


def test_duplicate_entry_rejected():
    pos = PositionStateMachine()
    assert pos.on_order_submitted(_open(Side.BUY)) is True
    assert pos.on_order_submitted(_open(Side.BUY)) is False   # in flight, reject
    assert pos.state is PosState.PENDING_ENTRY


def test_exit_without_position_rejected():
    pos = PositionStateMachine()
    assert pos.on_order_submitted(_cover(Side.SELL)) is False
    assert pos.state is PosState.FLAT
