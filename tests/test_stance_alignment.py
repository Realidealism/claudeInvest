"""Which trading day a day's 攻防 stance governs.

buy's entry gate, buy's defense and sell's entry gate all read the previous
trading day's stance. They used to take "the next row of the stance frame",
and the frame is an inner merge of five tables: a day one of them missed had
no row, so T-1's state skipped a missing T and landed on T+1, and a missing
last row meant T-1's state blocked nothing that night. The governed day is now
the next TAIEX trading day. On a frame without gaps -- every trading day since
2018-06 -- nothing changes, which the first test pins.

No database: the stance frame and the TAIEX calendar are stand-ins.
"""
from contextlib import contextmanager
from datetime import date

import pandas as pd
import pytest

import db.connection
import analysis.market_thermometer as mt
import signal_backtest.signal  # noqa: F401  (registers the factories before buy_sell)
from signal_backtest.factories import _conditions as c
from signal_backtest.factories.buy_sell import STANCE_HELD_DEFENSE
import export.generate as gen
from daily_update_guards import no_side_effects  # noqa: F401

TAIEX = [date(2026, 10, d) for d in (5, 6, 7, 8, 12, 13, 14, 15, 16)]   # 10-9 a holiday


def frame(rows):
    """rows: (day, defensive, mode, held)"""
    return pd.DataFrame({
        "d": pd.to_datetime([r[0] for r in rows]),
        "defensive": [r[1] for r in rows],
        "mode": pd.Series([r[2] for r in rows], dtype=object),
        "held": [r[3] for r in rows],
    })


class _Cursor:
    def __init__(self, taiex):
        self.taiex = taiex

    def execute(self, sql, params=None):
        assert sql.lstrip().upper().startswith("SELECT"), sql
        assert "index_prices" in sql and "index_id = 'TAIEX'" in sql
        # bisect needs the calendar in order; the fake only serves it sorted
        # because the query asks for it.
        assert "ORDER BY trade_date" in sql

    def fetchall(self):
        return [{"trade_date": d} for d in self.taiex]


@pytest.fixture
def stance(monkeypatch):
    """Serve a stance frame and a TAIEX calendar; returns the setter."""
    def serve(rows, taiex=TAIEX):
        monkeypatch.setattr(mt, "daily_stance_frame", lambda cur: frame(rows))

        @contextmanager
        def get_cursor(commit=True):
            yield _Cursor(taiex)
        monkeypatch.setattr(db.connection, "get_cursor", get_cursor)
        for fn in (c._stance_rows, c._stance_block_dates, c._stance_long_held_dates):
            fn.cache_clear()
    yield serve
    for fn in (c._stance_rows, c._stance_block_dates, c._stance_long_held_dates):
        fn.cache_clear()


def old_shift(rows, on):
    """The previous rule: a day governs the next row of the frame."""
    return frozenset(rows[i + 1][0] for i in range(len(rows) - 1) if on(rows[i]))


def top(day, held=1):
    return (day, True, "top", held)


def calm(day):
    return (day, False, None, 0)


def test_without_gaps_nothing_changes(stance):
    rows = [calm(TAIEX[0]), top(TAIEX[1], 1), top(TAIEX[2], 2), (TAIEX[3], True, "swing", 3),
            calm(TAIEX[4]), (TAIEX[5], True, "trend", 7), (TAIEX[6], True, "trend", 8),
            calm(TAIEX[7]), top(TAIEX[8])]
    stance(rows)
    assert c._stance_block_dates() == old_shift(rows, lambda r: r[1] and r[2] in ("top", "trend"))
    for k in (STANCE_HELD_DEFENSE, c.SELL_STANCE_GATE[0]):
        assert c._stance_long_held_dates(k) == old_shift(rows, lambda r, k=k: r[1] and r[3] >= k)


def test_holiday_is_skipped_like_any_non_trading_day(stance):
    stance([calm(TAIEX[2]), top(TAIEX[3]), calm(TAIEX[4])])
    assert c._stance_block_dates() == {date(2026, 10, 12)}   # 10-8 governs 10-12, not 10-9


def test_missing_last_row_still_blocks_tonight(stance):
    # 10-16 has no stance row (an input missed it); 10-15 was defensive.
    stance([calm(TAIEX[6]), top(TAIEX[7], 8)])
    assert date(2026, 10, 16) in c._stance_block_dates()
    assert date(2026, 10, 16) in c._stance_long_held_dates(STANCE_HELD_DEFENSE)


def test_missing_middle_row_does_not_carry_the_older_state_forward(stance):
    # 10-13 is missing: 10-12's state governs 10-13; nothing says what 10-14 is.
    stance([top(TAIEX[4]), calm(TAIEX[6])])
    assert c._stance_block_dates() == {date(2026, 10, 13)}


def test_last_row_governs_nothing_until_its_next_trading_day_exists(stance):
    # Intraday on 10-16: no TAIEX row for today yet, so the gate stays off.
    stance([top(TAIEX[7], 9)], taiex=TAIEX[:8])
    assert c._stance_block_dates() == frozenset()
    assert c._stance_long_held_dates(c.SELL_STANCE_GATE[0]) == frozenset()


def test_a_day_never_governs_itself(stance):
    stance([top(TAIEX[8])])
    assert TAIEX[8] not in c._stance_block_dates()


# ── the frontend's "pending" flag for today's buy entries ──────────────────

class _FlagCursor:
    """Answers the flag's one query from the TAIEX calendar: the last
    trading day strictly before the date asked about."""

    def __init__(self, taiex):
        self.taiex, self.answer = taiex, None

    def execute(self, sql, params=None):
        assert "index_prices" in sql and "trade_date < %s" in sql, sql
        (before,) = params
        self.answer = max((d for d in self.taiex if d < before), default=None)

    def fetchone(self):
        return {"d": self.answer}


@pytest.mark.parametrize("rows, expected", [
    ([calm(TAIEX[6]), top(TAIEX[7])], True),     # yesterday defensive
    ([top(TAIEX[6]), calm(TAIEX[7])], False),    # yesterday calm
    # Yesterday's row is missing: the gate blocks nothing at the close, and
    # the flag must not mark entries off the day before's state.
    ([calm(TAIEX[5]), top(TAIEX[6])], False),
])
def test_pending_flag_reads_the_previous_trading_day_only(monkeypatch, rows, expected):
    monkeypatch.setattr(mt, "daily_stance_frame", lambda cur: frame(rows))
    assert gen._buy_stance_blocked(_FlagCursor(TAIEX), TAIEX[8]) is expected
