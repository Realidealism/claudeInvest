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


def test_sessions_weekend_closed():
    # TAIFEX trades Mon-Fri only. 2026-07-17 Fri / 18 Sat / 19 Sun / 20 Mon.
    # Saturday day/evening hours are CLOSED even though the clock time is in-window
    # (this is what restart-looped the paper watchdog on 2026-07-18).
    assert clock.session_of(datetime(2026, 7, 18, 10, 27)) is Session.CLOSED   # Sat day
    assert clock.session_of(datetime(2026, 7, 18, 16, 0)) is Session.CLOSED    # Sat evening: no Sat night
    # but the 00:00-05:00 tail on Saturday belongs to Friday's night session
    assert clock.session_of(datetime(2026, 7, 18, 3, 0)) is Session.NIGHT      # Fri night tail
    assert clock.session_of(datetime(2026, 7, 17, 16, 0)) is Session.NIGHT     # Fri night open
    # Sunday fully closed
    assert clock.session_of(datetime(2026, 7, 19, 3, 0)) is Session.CLOSED
    assert clock.session_of(datetime(2026, 7, 19, 10, 0)) is Session.CLOSED
    # Monday: no Sunday-night tail, but day/evening resume
    assert clock.session_of(datetime(2026, 7, 20, 3, 0)) is Session.CLOSED     # no Sun night tail
    assert clock.session_of(datetime(2026, 7, 20, 10, 0)) is Session.DAY
    assert clock.session_of(datetime(2026, 7, 20, 16, 0)) is Session.NIGHT
    assert not clock.should_force_close(datetime(2026, 7, 18, 13, 44))         # Sat: nothing to close


def test_force_close():
    assert clock.should_force_close(datetime(2024, 12, 18, 13, 44))
    assert not clock.should_force_close(datetime(2024, 12, 18, 13, 43))
    # night session before midnight closes at 05:00 next day
    fc = clock.force_close_at(datetime(2024, 12, 18, 16, 0))
    assert fc == datetime(2024, 12, 19, 4, 59)
