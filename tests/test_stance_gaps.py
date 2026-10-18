"""The nightly check that the 攻防 stance had every input it reads.

A day one of the five inner-merged tables missed has no stance row; a row can
also hold a NULL input, and the options vote reads NaN when its table missed
the day. None of that raised, and buy/sell gated on whatever was left. These
cover what _stance_gaps reports for the last ten trading days. No database:
the stance frame and every query are stand-ins.
"""
from contextlib import contextmanager
from datetime import date

import numpy as np
import pandas as pd
import pytest

import analysis.market_thermometer as mt
import daily_update as du
from daily_update_guards import no_side_effects  # noqa: F401

DAYS = [date(2026, 10, d) for d in (1, 2, 5, 6, 7, 8, 12, 13, 14, 15, 16)]
T = DAYS[-1]
COLUMNS = ("mgn", "noi", "tx", "st", "c", "v", "opt_bull")
LABELS = [label for label, _ in du._STANCE_SOURCES]


class _Cursor:
    def __init__(self, taiex, absent):
        self.taiex, self.absent, self.result = taiex, absent, []

    def execute(self, sql, params):
        assert sql.lstrip().upper().startswith("SELECT"), sql
        if "ORDER BY trade_date DESC" in sql:
            day, limit = params
            self.result = [{"trade_date": d} for d in sorted(self.taiex, reverse=True)
                           if d <= day][:limit]
            return
        if "MIN(trade_date)" in sql:
            (day,) = params
            self.result = [{"d": min((d for d in self.taiex if d > day), default=None)}]
            return
        (day,) = params
        if sql == du._OPTIONS_SOURCE[1]:
            gone = (du._OPTIONS_SOURCE[0], day) in self.absent
            self.result = [] if gone else [{"call_put": "C"}, {"call_put": "P"}]
            return
        label = next(lbl for lbl, q in du._STANCE_SOURCES if q == sql)
        self.result = [] if (label, day) in self.absent else [{"?column?": 1}]

    def fetchone(self):
        return self.result[0] if self.result else None

    def fetchall(self):
        return self.result


def check(monkeypatch, *, rows=DAYS, nan=(), absent=(), taiex=DAYS, trade_date=T):
    """rows: days the stance frame has; nan: (day, column) holes; absent:
    (source label, day) the per-source queries do not find."""
    values = {col: [np.nan if (d, col) in nan else 1.0 for d in rows] for col in COLUMNS}
    m = pd.DataFrame({"d": pd.to_datetime(rows), **values})
    monkeypatch.setattr(mt, "daily_stance_frame", lambda cur: m)

    @contextmanager
    def get_cursor(commit=True):
        yield _Cursor(taiex, set(absent))
    monkeypatch.setattr(du, "get_cursor", get_cursor)
    return du._stance_gaps(trade_date)


@pytest.mark.parametrize("fragment", [
    "tw.margin_summary", "tw.taifex_inst_futures", "product='臺股期貨'",
    "investor='外資及陸資'", "index_id='TAIEX'", "tw.market_breadth",
    "tw.taifex_futures_daily", "contract='TX'", "session='一般'",
    "contract_month NOT LIKE '%%/%%'", "tw.taifex_inst_options", "product LIKE '臺指%%'",
    "investor LIKE '外資%%'",
])
def test_source_queries_copy_the_stance_frames_filters(fragment):
    # _STANCE_SOURCES copies _load_merged_frame's filters to name a gap's
    # source. A filter changed there and not here would name the wrong one.
    import inspect
    loader = inspect.getsource(mt._load_merged_frame).replace(" ", "")
    ours = " ".join(sql for _, sql in (*du._STANCE_SOURCES, du._OPTIONS_SOURCE)).replace(" ", "")
    assert fragment.replace(" ", "") in loader
    assert fragment.replace(" ", "") in ours


def test_every_input_there(monkeypatch):
    assert check(monkeypatch) == []


def test_tonight_short_of_an_input(monkeypatch):
    assert check(monkeypatch, rows=DAYS[:-1], absent={("期交所外資期貨", T)}) == [
        "10-16 缺：期交所外資期貨；明晚 buy／sell 的攻防閘門會沒有依據。"
        "補跑：daily_update.exe 2026-10-16"]


def test_yesterday_still_short_names_tonight_as_the_day_without_a_gate(monkeypatch):
    rows = [d for d in DAYS if d != DAYS[-2]]
    assert check(monkeypatch, rows=rows, absent={("期交所台指期", DAYS[-2])}) == [
        "10-15 缺：期交所台指期；10-16 的 buy／sell 攻防閘門沒有依據。"
        "補跑：daily_update.exe 2026-10-15"]


def test_row_with_a_null_input_is_a_gap(monkeypatch):
    assert check(monkeypatch, nan={(DAYS[-3], "mgn")}, absent={("融資餘額", DAYS[-3])}) == [
        "10-14 缺：融資餘額；10-15 的 buy／sell 攻防閘門沒有依據。"
        "補跑：daily_update.exe 2026-10-14"]


@pytest.mark.parametrize("column", ["noi", "tx", "st", "c", "v"])
def test_every_input_column_is_checked(monkeypatch, column):
    assert len(check(monkeypatch, nan={(T, column)})) == 1


def test_options_hole_is_a_gap(monkeypatch):
    lines = check(monkeypatch, nan={(T, "opt_bull")}, absent={("期交所外資選擇權", T)})
    assert lines == ["10-16 缺：期交所外資選擇權；明晚 buy／sell 的攻防閘門會沒有依據。"
                     "補跑：daily_update.exe 2026-10-16"]


def test_options_hole_before_the_table_starts_is_not_a_gap(monkeypatch):
    old = [date(2023, 6, d) for d in (19, 20, 21)]
    assert check(monkeypatch, rows=old, taiex=old, trade_date=old[-1],
                 nan={(d, "opt_bull") for d in old}) == []


def test_days_before_the_stance_spine_are_not_gaps(monkeypatch):
    # The spine starts 2018-06-05; a ten-day window reaching back past it
    # finds TAIEX days with no stance row by design.
    taiex = [date(2018, 5, 31), date(2018, 6, 1), date(2018, 6, 4), date(2018, 6, 5),
             date(2018, 6, 6)]
    assert check(monkeypatch, rows=taiex[3:], taiex=taiex, trade_date=taiex[-1]) == []


def test_source_that_cannot_be_named(monkeypatch):
    lines = check(monkeypatch, rows=DAYS[:-1])
    assert lines and "無法判定" in lines[0]


def test_only_the_last_ten_trading_days(monkeypatch):
    assert du._STANCE_LOOKBACK == 10
    spine = [date(2026, 9, 30)] + DAYS   # the spine starts before the window
    # DAYS[0] is the eleventh trading day back, DAYS[1] the tenth.
    assert check(monkeypatch, rows=[d for d in spine if d != DAYS[0]], taiex=spine) == []
    lines = check(monkeypatch, rows=[d for d in spine if d != DAYS[1]], taiex=spine)
    assert [line[:5] for line in lines] == ["10-02"]


def test_several_gaps_each_name_their_own_day_newest_first(monkeypatch):
    # Newest first: a long reason is cut from the end in the summary.
    rows = [d for d in DAYS if d not in (DAYS[-4], DAYS[-1])]
    lines = check(monkeypatch, rows=rows, absent={(LABELS[0], DAYS[-4]), (LABELS[0], T)})
    assert [line[:5] for line in lines] == ["10-16", "10-13"]
    assert "明晚" in lines[0] and "10-14 的 buy／sell" in lines[1]


def test_backfilled_date_names_the_day_after_it(monkeypatch):
    # Re-running 10-15 once 10-16 has traded: its gap left 10-16 without a
    # gate, which is no longer "tomorrow night".
    rows = [d for d in DAYS if d != DAYS[-2]]
    lines = check(monkeypatch, rows=rows, trade_date=DAYS[-2])
    assert len(lines) == 1 and "10-16 的 buy／sell 攻防閘門沒有依據" in lines[0]
