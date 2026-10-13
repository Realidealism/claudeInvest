"""Tests for the corporate-action paragraph of the daily health check.

The paragraph used to list every resumption at a new price scale for five
days, recorded or not. Over 60 trading days that was 95 lines on 39 days for
20 events, 18 of which the signal factory already blanks out. A paragraph
that is there most days is not read, so it now lists only the breaks the
factory does not cover, in the factory's own universe.

No database and no Telegram: the cursor is a fake that answers the three
queries from canned rows. The universe filter lives in SQL, so it is checked
against the query text and against the factory's own queries.
"""
import re
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

import pytest

import telegram_bot.health_check as hc
from signal_backtest.factories import _conditions as factory

REPO = Path(factory.__file__).resolve().parents[2]

# Ten consecutive weekdays, oldest first; CAL[-1] is the latest trading day.
CAL = [date(2026, 9, 21) + timedelta(days=i) for i in (0, 1, 2, 3, 4, 7, 8, 9, 10, 11)]
RESUME = CAL[-2]                       # inside the five-day reporting window
HALTED = CAL[4:8]                      # four missing days before RESUME


class _Cursor:
    def __init__(self, prices, reductions):
        self.prices, self.reductions, self.sql, self._rows = prices, reductions, [], []

    def execute(self, sql, params=()):
        self.sql.append(sql)
        if "tw.index_prices" in sql:
            self._rows = [{"d": d} for d in reversed(CAL)]
        elif "tw.daily_prices" in sql:
            self._rows = [{"stock_id": sid, "d": d, "close_price": px, "name": "測試"}
                          for sid, series in self.prices.items() for d, px in series.items()]
        elif "tw.capital_changes" in sql:
            self._rows = [{"stock_id": sid, "effective_date": d, "step": step}
                          for sid, d, step in self.reductions]
        else:
            raise AssertionError(f"unexpected query: {sql}")

    def fetchall(self):
        return self._rows


@pytest.fixture
def db(monkeypatch):
    """Install a fake database; returns a function that runs the check on it."""
    def run(prices, reductions=()):
        cur = _Cursor(prices, list(reductions))

        @contextmanager
        def fake_cursor(commit=False):
            yield cur
        monkeypatch.setattr(hc, "get_cursor", fake_cursor)
        run.cursor = cur
        return hc._corporate_actions(CAL[-1])
    return run


def _series(before, after, halted=HALTED, resume=RESUME):
    """Trades at `before`, is absent on `halted`, comes back at `after`."""
    return {d: (before if d < resume else after) for d in CAL if d not in halted}


# ── what still needs someone ──────────────────────────

def test_unrecorded_break_in_the_blind_band_is_reported(db):
    assert db({"1111": _series(100, 125)}) == [
        f"1111 測試 {RESUME:%m-%d} 復牌 100 -> 125 (+25%)，capital_changes 無紀錄"]


def test_unrecorded_fall_in_the_blind_band_is_reported(db):
    assert db({"1111": _series(100, 70)}) == [
        f"1111 測試 {RESUME:%m-%d} 復牌 100 -> 70 (-30%)，capital_changes 無紀錄"]


def test_reduction_too_small_to_be_blanked_is_reported_with_its_step(db):
    lines = db({"1111": _series(100, 125)}, [("1111", RESUME, 0.05)])
    assert lines == [f"1111 測試 {RESUME:%m-%d} 復牌 100 -> 125 (+25%)，減資紀錄幅度僅 5.0%，未達靜默門檻"]


def test_reduction_dated_on_another_day_does_not_cover_the_break(db):
    """The factory matches the effective date to a bar exactly; a row dated
    inside the halt lands on no bar and blanks nothing."""
    lines = db({"1111": _series(100, 125)}, [("1111", HALTED[1], 0.29)])
    assert lines == [f"1111 測試 {RESUME:%m-%d} 復牌 100 -> 125 (+25%)，"
                     f"減資紀錄生效日 {HALTED[1]:%m-%d} 對不上復牌日"]


def test_a_reduction_on_another_stock_covers_nothing(db):
    assert len(db({"1111": _series(100, 125)}, [("2222", RESUME, 0.29)])) == 1


# ── what the factory already handles ────────────────────────

def test_recorded_reduction_on_the_resumption_day_is_left_out(db):
    assert db({"1111": _series(100, 125)}, [("1111", RESUME, 0.29)]) == []


@pytest.mark.parametrize("after", [155, 45, 10])
def test_step_beyond_the_jump_threshold_is_left_out_without_any_record(db, after):
    assert db({"1111": _series(100, after)}) == []


def test_only_the_uncovered_break_is_listed_among_several(db):
    lines = db({"1111": _series(100, 125), "2222": _series(100, 125), "3333": _series(100, 20)},
               [("2222", RESUME, 0.29)])
    assert [l.split()[0] for l in lines] == ["1111"]


# ── the thresholds are strict, as in the factory ──────────────────

def test_step_exactly_at_the_seam_threshold_is_not_covered(db):
    assert len(db({"1111": _series(100, 125)}, [("1111", RESUME, 0.10)])) == 1
    assert db({"1111": _series(100, 125)}, [("1111", RESUME, 0.1001)]) == []


def test_jump_just_under_and_just_over_the_jump_threshold(db):
    assert len(db({"1111": _series(1000, 1399)})) == 1
    assert db({"1111": _series(1000, 1401)}) == []


def test_reduction_without_a_usable_ratio_counts_as_no_step(db):
    lines = db({"1111": _series(100, 125)}, [("1111", RESUME, None)])
    assert lines == [f"1111 測試 {RESUME:%m-%d} 復牌 100 -> 125 (+25%)，減資紀錄幅度僅 0.0%，未達靜默門檻"]


# ── the detection itself is unchanged ───────────────────────

def test_small_move_after_a_halt_is_not_a_break(db):
    assert db({"1111": _series(100, 110)}) == []


def test_two_missing_days_are_not_a_halt(db):
    assert db({"1111": _series(100, 125, halted=HALTED[2:])}) == []


def test_resumption_older_than_the_reporting_window_is_dropped(db):
    old = CAL[4]
    assert db({"1111": _series(100, 125, halted=CAL[1:4], resume=old)}) == []


# ── same thresholds and same universe as the factory ────────────────

def test_thresholds_equal_the_factory_constants():
    assert hc._QUIET_JUMP == factory.BREAK_JUMP
    assert hc._QUIET_SEAM_STEP == factory.SEAM_MIN_STEP
    assert factory.SEAM_FROM_CAPITAL_CHANGES is True and factory.BREAK_BLACKOUT_BARS


UNIVERSE = [r"is_active\s*=\s*TRUE", r"security_type\s*=\s*'STOCK'",
            r"market\s+IN\s*\(\s*'TWSE'\s*,\s*'TPEx'\s*\)"]


def test_the_price_query_is_limited_to_the_factory_universe(db):
    db({"1111": _series(100, 125)})
    price_sql = next(s for s in db.cursor.sql if "tw.daily_prices" in s)
    for pred in UNIVERSE:
        assert re.search(r"s\." + pred, price_sql), pred
    assert "event_type = 'REDUCTION'" in next(s for s in db.cursor.sql if "tw.capital_changes" in s)


@pytest.mark.parametrize("path", ["signal_backtest/batch.py", "analysis/daily_snapshot.py",
                                  "analysis/intraday_snapshot.py"])
def test_the_factory_still_uses_that_universe(path):
    """If the factory widens its universe, this probe has to follow."""
    source = (REPO / path).read_text(encoding="utf-8")
    for pred in UNIVERSE:
        assert re.search(pred, source), f"{path}: {pred}"


# ── the daily message ────────────────────────────────

@pytest.fixture
def daily(monkeypatch):
    sent = []
    monkeypatch.setattr(hc, "_latest_trading_day", lambda: date(2026, 10, 5))
    monkeypatch.setattr(hc, "_scalar", lambda sql, params=(): date(2026, 10, 5) if "MAX(" in sql and "updated_at" not in sql
                        else hc.datetime.now(hc.timezone.utc) if "updated_at" in sql else 0)
    monkeypatch.setattr(hc, "_dead_column_groups", lambda latest: [])
    monkeypatch.setattr(hc, "_stale_exes", lambda: [])
    monkeypatch.setattr(hc, "_calendar_gap", lambda today, latest: ([], latest))
    monkeypatch.setattr(hc, "_backup_section", lambda now: None)
    monkeypatch.setattr(hc, "send_sync", lambda text, **kw: sent.append((text, kw)) or True)
    return sent


def test_a_day_with_only_handled_breaks_is_a_silent_ok(daily, db, monkeypatch):
    db({"2222": _series(100, 125), "3333": _series(100, 20)}, [("2222", RESUME, 0.29)])
    assert hc.run_daily() == 0
    assert daily == [("[健康檢查] ✓ 系統正常（資料更新至 10-05）", {"silent": True})]


def test_an_uncovered_break_makes_the_day_unhealthy(daily, db):
    db({"1111": _series(100, 125)})
    assert hc.run_daily() == 1
    assert daily == [("[健康檢查] 公司行為，斷點未被訊號工廠靜默涵蓋：\n"
                      f"- 1111 測試 {RESUME:%m-%d} 復牌 100 -> 125 (+25%)，capital_changes 無紀錄", {})]
