from datetime import datetime

from broker.types import Bar, OpenClose, OrderRequest, Side, Trade
from position.state_machine import PositionStateMachine
from risk.risk_manager import RiskConfig, RiskManager
from strategy import measured_move as mm
from strategy import signals_masha as sm


def _b(o, h, l, c):
    return Bar("TM", datetime(2024, 12, 16, 9, 0), o, h, l, c, 1, "5m")


def _holding(side, entry):
    pos = PositionStateMachine()
    pos.on_order_submitted(OrderRequest("TM", side, 1, entry, open_close=OpenClose.OPEN))
    pos.on_trade(Trade("o", "TM", side, entry, 1, datetime(2024, 12, 16, 9, 0)))
    return pos


# ── §7.1 幅位 ───────────────────────────────────────────────────────────

def test_impulse_box_and_target():
    up = [_b(8, 9, 8, 8.5), _b(8.5, 11, 8.4, 10), _b(10, 15, 10, 14)]   # low early, high late
    box = mm.impulse_box(up)
    assert box == (8.0, 15.0, "up")
    assert mm.box_height(box) == 7.0
    assert mm.midline(box) == 11.5
    assert mm.target_1to1("long", 100.0, box) == 107.0
    assert mm.target_1to1("short", 100.0, box) == 93.0


# ── §2.4c 力竭 ──────────────────────────────────────────────────────────

def test_exhaustion():
    rev = [_b(8, 10, 8, 10), _b(10, 11, 6, 7)]            # reverse (short) engulf vs long
    assert sm.exhaustion(rev, "long") is True
    doji = [_b(9, 10, 8, 9), _b(10, 11, 9, 10.02)]        # doji → 力竭 either side
    assert sm.exhaustion(doji, "long") and sm.exhaustion(doji, "short")
    wick = [_b(10, 10, 9, 10), _b(10, 13, 10, 10.5)]      # long upper wick vs long
    assert sm.exhaustion(wick, "long") is True


# ── §2.4b 移動停利 + §7.1 滿足點 (risk) ──────────────────────────────────

def test_masha_trailing_stop():
    rm = RiskManager(RiskConfig(stop_mode="masha", masha_trail_enabled=True, masha_trail_mode="low"))
    pos = _holding(Side.BUY, 100.0)
    rm.set_entry_bar(high=102.0, low=98.0)
    # up-bar ratchets the trail to its low (103)
    assert rm.check_stop(_b(101, 106, 103, 105), pos, atr=None) is None
    # next bar closes below the trailed 103 → exit (locks profit)
    assert rm.check_stop(_b(105, 105, 101, 102), pos, atr=None) is not None


def test_masha_target_exit():
    rm = RiskManager(RiskConfig(stop_mode="masha", masha_use_target=True))
    pos = _holding(Side.BUY, 100.0)
    rm.set_entry_bar(high=102.0, low=98.0)
    rm.set_target(110.0)
    order = rm.check_stop(_b(107.5, 111, 107, 108), pos, atr=None)   # high 111 ≥ target 110
    assert order is not None and order.price == 110.0                # fills at target
