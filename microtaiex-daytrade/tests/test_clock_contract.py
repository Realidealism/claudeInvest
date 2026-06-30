from datetime import date, datetime

from core import clock
from core.clock import Session


def test_contract_code():
    assert clock.contract_code(2024, 12) == "TMFL4"
    assert clock.contract_code(2025, 1) == "TMFA5"


def test_third_wednesday_and_settlement():
    assert clock.third_wednesday(2024, 12) == date(2024, 12, 18)
    assert clock.is_settlement_day(date(2024, 12, 18))
    assert not clock.is_settlement_day(date(2024, 12, 17))


def test_front_contract_rolls_after_settlement():
    assert clock.front_contract(date(2024, 12, 18)) == "TMFL4"   # on settlement day
    assert clock.front_contract(date(2024, 12, 19)) == "TMFA5"   # rolled to Jan 2025


def test_sessions():
    d = date(2024, 12, 18)
    assert clock.session_of(datetime.combine(d, datetime.min.time()).replace(hour=9)) is Session.DAY
    assert clock.session_of(datetime(2024, 12, 18, 13, 45)) is Session.CLOSED   # right edge excluded
    assert clock.session_of(datetime(2024, 12, 18, 14, 0)) is Session.CLOSED
    assert clock.session_of(datetime(2024, 12, 18, 16, 0)) is Session.NIGHT
    assert clock.session_of(datetime(2024, 12, 19, 3, 0)) is Session.NIGHT     # after midnight
    assert clock.session_of(datetime(2024, 12, 19, 6, 0)) is Session.CLOSED


def test_force_close():
    assert clock.should_force_close(datetime(2024, 12, 18, 13, 44))
    assert not clock.should_force_close(datetime(2024, 12, 18, 13, 43))
    # night session before midnight closes at 05:00 next day
    fc = clock.force_close_at(datetime(2024, 12, 18, 16, 0))
    assert fc == datetime(2024, 12, 19, 4, 59)
