from datetime import datetime

from broker.sim import SimBroker
from broker.types import ConnectionStatus, OpenClose, OrderRequest, Side, Tick


def _tick(sec, price, vol=1):
    return Tick(symbol="TMFR1", ts=datetime(2024, 12, 18, 9, 0, sec), price=price, volume=vol)


def test_feed_ticks_emits_each():
    b = SimBroker()
    got = []
    b.set_on_tick(got.append)
    b.feed_ticks([_tick(1, 100), _tick(2, 101), _tick(3, 102)])
    assert len(got) == 3
    assert [t.price for t in got] == [100, 101, 102]


def test_connect_emits_connected():
    b = SimBroker()
    states = []
    b.set_on_connection(states.append)
    b.connect()
    assert states == [ConnectionStatus.CONNECTED]


def test_place_order_fills_and_updates_position():
    b = SimBroker()
    trades = []
    b.set_on_trade(trades.append)
    b.feed_tick(_tick(1, 100))
    res = b.place_order(OrderRequest(symbol="TMFR1", side=Side.BUY, lot=1, price=100.0,
                                     open_close=OpenClose.OPEN))
    assert res.accepted
    assert len(trades) == 1 and trades[0].side is Side.BUY
    pos = b.list_positions()
    assert len(pos) == 1
    assert pos[0].side is Side.BUY and pos[0].lot == 1 and pos[0].avg_price == 100.0
    # closing flattens
    b.place_order(OrderRequest(symbol="TMFR1", side=Side.SELL, lot=1, price=105.0,
                               open_close=OpenClose.COVER))
    assert b.list_positions() == []
