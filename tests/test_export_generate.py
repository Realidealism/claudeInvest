"""Tests for the pure parts of export/generate.py.

This module produces every JSON the frontend reads, and had no coverage at
all. The bucketing helpers and the atomic writer are decidable without a
database, so they are covered here; the export_* functions that take a live
cursor are not.
"""
import json
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from export.generate import (
    YIELD_THRESHOLDS,
    _breadth_row_from_counts,
    _serial,
    _vix_rating,
    _vix_thresholds,
    _write,
    _yield_rating,
)


# ── _serial ────────────────────────────────────────────────────────────────

def test_serial_renders_dates_as_iso():
    assert _serial(date(2026, 8, 7)) == "2026-08-07"
    assert _serial(datetime(2026, 8, 7, 13, 30)) == "2026-08-07T13:30:00"


def test_serial_renders_decimal_as_float():
    """Postgres NUMERIC arrives as Decimal, which json cannot encode."""
    out = _serial(Decimal("123.45"))
    assert out == 123.45
    assert isinstance(out, float)


def test_serial_refuses_unknown_types():
    """Silently stringifying would ship a wrong-typed field to the frontend."""
    with pytest.raises(TypeError):
        _serial(object())


# ── _write ─────────────────────────────────────────────────────────────────

def test_write_produces_readable_json(tmp_path):
    target = tmp_path / "out.json"
    _write({"a": 1, "d": date(2026, 8, 7)}, target)
    assert json.loads(target.read_text(encoding="utf-8")) == {
        "a": 1, "d": "2026-08-07",
    }


def test_write_keeps_chinese_unescaped(tmp_path):
    target = tmp_path / "out.json"
    _write({"name": "台積電"}, target)
    assert "台積電" in target.read_text(encoding="utf-8")


def test_write_creates_missing_directories(tmp_path):
    target = tmp_path / "deep" / "deeper" / "out.json"
    _write({"a": 1}, target)
    assert target.exists()


def test_write_leaves_no_temp_files_behind(tmp_path):
    target = tmp_path / "out.json"
    _write({"a": 1}, target)
    assert [p.name for p in tmp_path.iterdir()] == ["out.json"]


def test_failed_write_preserves_the_previous_file(tmp_path):
    """A reader must never catch a half-written or truncated JSON.

    The publish step and the telegram push both read these files while the
    exporter may be rewriting them.
    """
    target = tmp_path / "out.json"
    _write({"good": 1}, target)

    with pytest.raises(TypeError):
        _write({"bad": object()}, target)

    assert json.loads(target.read_text(encoding="utf-8")) == {"good": 1}
    assert [p.name for p in tmp_path.iterdir()] == ["out.json"]


# ── _vix_rating / _vix_thresholds ──────────────────────────────────────────

_VIX = {"p20": 14.0, "p50": 18.0, "p80": 24.0}


@pytest.mark.parametrize("close,expected", [
    (10.0, "calm"),
    (13.99, "calm"),
    (14.0, "low"),        # boundary belongs to the upper bucket
    (17.99, "low"),
    (18.0, "elevated"),
    (23.99, "elevated"),
    (24.0, "panic"),
    (80.0, "panic"),
])
def test_vix_rating_buckets(close, expected):
    assert _vix_rating(close, _VIX) == expected


def test_vix_rating_is_none_without_a_close():
    assert _vix_rating(None, _VIX) is None


@pytest.mark.parametrize("thresholds", [
    {"p20": None, "p50": 18.0, "p80": 24.0},
    {"p20": 14.0, "p50": None, "p80": 24.0},
    {"p20": 14.0, "p50": 18.0, "p80": None},
    {},
])
def test_vix_rating_is_none_when_a_threshold_is_missing(thresholds):
    """Better a blank badge than a bucket derived from a partial window."""
    assert _vix_rating(20.0, thresholds) is None


def test_vix_thresholds_are_ordered_percentiles():
    t = _vix_thresholds([float(v) for v in range(1, 101)])
    assert t["p20"] < t["p50"] < t["p80"]


def test_vix_thresholds_ignore_nan():
    clean = _vix_thresholds([10.0, 20.0, 30.0])
    with_nan = _vix_thresholds([10.0, float("nan"), 20.0, 30.0])
    assert clean == with_nan


def test_vix_thresholds_are_none_for_no_usable_values():
    empty = {"p20": None, "p50": None, "p80": None}
    assert _vix_thresholds([]) == empty
    assert _vix_thresholds([float("nan"), float("nan")]) == empty


# ── _yield_rating ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("spread,expected", [
    (-1.0, "inverted"),
    (-0.01, "inverted"),
    (0.0, "flat"),         # inversion is strictly below zero
    (0.49, "flat"),
    (0.5, "normal"),
    (1.49, "normal"),
    (1.5, "steep"),
    (3.0, "steep"),
])
def test_yield_rating_buckets(spread, expected):
    assert _yield_rating(spread) == expected


def test_yield_rating_is_none_without_a_spread():
    assert _yield_rating(None) is None


def test_yield_thresholds_are_ordered():
    assert (YIELD_THRESHOLDS["flat"]
            < YIELD_THRESHOLDS["normal"]
            < YIELD_THRESHOLDS["steep"])


# ── _breadth_row_from_counts ───────────────────────────────────────────────

def test_breadth_fractions_are_shares_of_the_total():
    row = _breadth_row_from_counts(
        date(2026, 8, 7), 1000,
        short_up=300, short_down=200,
        medium_up=400, medium_down=100,
        long_up=500, long_down=250,
    )
    assert (row["s_up"], row["s_dn"], row["s_neu"]) == (0.3, 0.2, 0.5)
    assert (row["m_up"], row["m_dn"], row["m_neu"]) == (0.4, 0.1, 0.5)
    assert (row["l_up"], row["l_dn"], row["l_neu"]) == (0.5, 0.25, 0.25)


def test_breadth_survives_an_empty_universe():
    """A zero total must not divide by zero mid-export."""
    row = _breadth_row_from_counts(date(2026, 8, 7), 0, 0, 0, 0, 0, 0, 0)
    assert row["s_up"] == row["s_dn"] == 0
    assert row["s_neu"] == 1.0


def test_breadth_neutral_never_goes_negative():
    """Counts can exceed the total when a stock qualifies on both sides."""
    row = _breadth_row_from_counts(
        date(2026, 8, 7), 100,
        short_up=80, short_down=80,
        medium_up=0, medium_down=0,
        long_up=0, long_down=0,
    )
    assert row["s_neu"] == 0.0


def test_breadth_omits_intraday_fields_for_a_settled_day():
    row = _breadth_row_from_counts(date(2026, 8, 7), 10, 1, 1, 1, 1, 1, 1)
    assert "is_intraday" not in row
    assert "intraday_time" not in row


def test_breadth_marks_intraday_rows():
    """The frontend swaps the date label for an "intraday HH:MM" tag on these."""
    row = _breadth_row_from_counts(
        date(2026, 8, 7), 10, 1, 1, 1, 1, 1, 1,
        is_intraday=True, intraday_time="13:05",
    )
    assert row["is_intraday"] is True
    assert row["intraday_time"] == "13:05"


def test_breadth_row_is_json_serialisable():
    """It goes straight into _write, whose only escape hatch is _serial."""
    row = _breadth_row_from_counts(date(2026, 8, 7), 10, 1, 1, 1, 1, 1, 1)
    assert json.loads(json.dumps(row, default=_serial))["date"] == "2026-08-07"
