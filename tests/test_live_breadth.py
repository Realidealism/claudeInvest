"""Live (intraday) breadth trends and the ScoreBoard's 大盤 cells.

tw.market_breadth gets today's row only after the close, so the intraday pass
computes the trends from its forming bars and hands them to the ScoreBoard.
No database: the board's DB breadth is a plain dict and the stock is a stand-in.
"""
from datetime import date
from types import SimpleNamespace

import numpy as np
import pytest

import analysis.score as score
from analysis.market_breadth import (
    LIVE_COUNT_KEYS, LIVE_NORMAL_KEYS, TREND_CODE, BreadthDay, live_trend_codes,
)

DAY = date(2026, 11, 3)
YESTERDAY = date(2026, 11, 2)


@pytest.fixture(autouse=True)
def _no_live_breadth():
    score._LIVE_BREADTH.clear()
    yield
    score._LIVE_BREADTH.clear()


def _counts(total, **normal):
    counts = dict.fromkeys(LIVE_COUNT_KEYS + LIVE_NORMAL_KEYS, 0)
    counts["total"] = total
    counts.update(normal)
    return counts


# ── live_trend_codes ───────────────────────────────────────────────────────

@pytest.mark.parametrize("up, down, expected", [
    (60, 10, 2),    # up > neutral(30) > down
    (40, 35, 1),    # up > down, neutral(25) not between
    (10, 60, -2),   # down > neutral(30) > up
    (35, 40, -1),
    (20, 20, 0),    # neutral 60 > 50
    (30, 30, 0),    # up == down
])
def test_live_trend_codes_classify_like_the_close(up, down, expected):
    codes = live_trend_codes(_counts(100, medium_up_normal=up, medium_down_normal=down))
    assert codes["medium_trend"] == expected
    # The nightly row's medium_trend is BreadthDay.medium_trend over the same counts.
    day = BreadthDay(DAY, 120, 100, *([0] * 18))
    day.medium_up, day.medium_down = up, down
    assert codes["medium_trend"] == TREND_CODE[day.medium_trend]


def test_live_trend_codes_read_normal_counts_not_the_total_ones():
    # *_up / *_down are normal ∪ forming (the sidecar's semantics); the trends
    # the 大盤 cells read are built on sort_normal only.
    counts = _counts(100, medium_up_normal=10, medium_down_normal=60)
    counts.update(medium_up=70, medium_down=0)
    assert live_trend_codes(counts)["medium_trend"] == -2


def test_live_trend_codes_with_no_stocks_are_neutral():
    assert live_trend_codes(_counts(0)) == {
        "short_trend": 0, "medium_trend": 0, "long_trend": 0}


# ── the 大盤 cells ─────────────────────────────────────────────────────────

def _card(db_breadth):
    """The six medium 大盤 cells over `db_breadth`, as build_scoreboard adds them."""
    card = score.ScoreCard("medium")
    for codes, state, pts in score.BREADTH_LONG_RULES:
        name = f"大盤medium_{state}_{pts:+.0f}"
        score._add_breadth_item(card, name, pts, db_breadth, "medium_trend",
                                "medium", state, codes)
    return card


def _stock(up, down):
    """One-bar stand-in on DAY whose medium sort_normal is up / down / neither."""
    sn = SimpleNamespace(up=np.array([up]), down=np.array([down]))
    return SimpleNamespace(
        dates=[DAY],
        close_result=SimpleNamespace(ma=SimpleNamespace(sort_normal={"medium": sn})))


def _fired(card, data):
    return sorted(it.name for it in card.long_items if it.evaluate(data, 0))


def test_without_a_db_row_or_live_trends_no_breadth_cell_fires():
    card = _card({YESTERDAY: {"medium_trend": 2}})
    assert _fired(card, _stock(False, True)) == []


def test_live_trends_feed_the_cells_when_the_db_has_no_row():
    card = _card({YESTERDAY: {"medium_trend": -2}})
    score.set_live_breadth(DAY, {"short_trend": 0, "medium_trend": 2, "long_trend": 0})
    assert _fired(card, _stock(False, True)) == ["大盤medium_down_-10"]
    assert _fired(card, _stock(False, False)) == ["大盤medium_none_-5"]


def test_the_db_row_wins_over_live_trends():
    card = _card({DAY: {"medium_trend": -2}})
    score.set_live_breadth(DAY, {"short_trend": 0, "medium_trend": 2, "long_trend": 0})
    assert _fired(card, _stock(True, False)) == ["大盤medium_up_+10"]


def test_set_live_breadth_keeps_only_the_latest_day():
    score.set_live_breadth(YESTERDAY, {"medium_trend": 2})
    score.set_live_breadth(DAY, {"medium_trend": -1})
    assert score._LIVE_BREADTH == {DAY: {"medium_trend": -1}}


# ── the intraday worker ────────────────────────────────────────────────────

def test_the_intraday_worker_hands_its_live_trends_to_the_scoreboard(monkeypatch):
    import analysis.intraday_snapshot as isn
    from db.price_availability import PriceUnavailable

    def unavailable(*a, **k):
        raise PriceUnavailable("emerging board")
    monkeypatch.setattr(isn, "load_stock_data_intraday", unavailable)
    trends = {"short_trend": 0, "medium_trend": -2, "long_trend": 1}
    isn._eval_stock("2330", live_breadth=(DAY, trends))
    assert score._LIVE_BREADTH == {DAY: trends}
