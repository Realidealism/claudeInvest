"""Fill-sensitivity section of signal_backtest._compare: entries that closed on
the published limit price, one-price days among them, and the PF once they
are dropped."""
from datetime import date

import numpy as np
import pandas as pd
import pytest

from signal_backtest import _compare as c
from signal_backtest.trade import REASON_EXIT_DEFENSE, REASON_EXIT_END

D1, D2, D3 = date(2024, 3, 1), date(2024, 3, 4), date(2024, 3, 5)
nan = np.nan


def _bars(rows):
    """rows: (stock_id, day, high, low, close, limit_up, limit_down, ref, prev_close)."""
    df = pd.DataFrame(rows, columns=["stock_id", "trade_date", "high", "low", "close",
                                     "limit_up", "limit_down", "ref", "prev_close"])
    return df.set_index(["stock_id", "trade_date"])


def _trades(rows):
    """rows: (stock_id, entry_date, pnl[, exit_date, exit_reason]); without an
    exit the trade is still open at the end of data."""
    return pd.DataFrame({
        "股票代號": [r[0] for r in rows],
        "進場日期": [r[1] for r in rows],
        "報酬率": [r[2] for r in rows],
        "出場日期": [r[3] if len(r) > 3 else D3 for r in rows],
        "持倉天數": [1] * len(rows),
        "出場原因": [r[4] if len(r) > 3 else REASON_EXIT_END for r in rows],
    })


BARS = _bars([
    ("1101", D1, 11.0, 10.2, 11.0, 11.0, 9.0, 10.0, 10.0),      # long limit-up, traded through
    ("1102", D1, 11.0, 11.0, 11.0, 11.0, 9.0, 10.0, 10.0),      # long limit-up, one price
    ("1103", D1, 10.5, 10.0, 10.4, 11.0, 9.0, 10.0, 10.0),      # +4%
    ("1268", D1, 161.5, 150.0, 161.0, 161.5, 132.5, 147.0, 147.0),  # +9.5%, a tick short
    ("3027", D1, 10.5, 9.6, 10.5, 10.5, 8.64, 9.59, 9.59),      # +9.49%, on the limit
    ("2201", D1, 9.5, 9.0, 9.0, 11.0, 9.0, 10.0, 10.0),         # short limit-down
    ("2202", D1, 9.0, 9.0, 9.0, 11.0, 9.0, 10.0, 10.0),         # short limit-down, one price
    ("3301", D2, 9.4, 9.0, 9.0, 10.45, 8.55, 9.5, 10.0),        # ex-rights: -10% on prev, not a limit
    ("4401", D2, 11.0, 10.0, 11.0, nan, nan, nan, 10.0),        # no published limit
    ("1597", D2, 43.1, 38.7, 43.1, 47.4, 38.8, 43.1, 39.2),     # ref a day off: a missed limit-up
    ("5501", D2, 10.2, 10.0, 10.1, 11.1, 9.1, 10.1, 10.0),      # ref == close on a +1% day
])


def flags(rows, side):
    return c.limit_entry_flags(_trades(rows), side, BARS)


def row_flags(df, side):
    return c.limit_entry_flags(df, side, BARS).join(c.limit_exit_flags(df, side, BARS))


def test_long_limit_up_and_one_price():
    f = flags([("1101", D1, .1), ("1102", D1, .1), ("1103", D1, .1)], "多")
    assert f["at_limit"].tolist() == [True, True, False]
    assert f["one_price"].tolist() == [False, True, False]
    assert f["matched"].all()


def test_published_limit_not_a_ratio():
    # 161 is +9.5% but a tick under 161.5; 10.50 is +9.49% and is the limit
    f = flags([("1268", D1, .1), ("3027", D1, .1)], "多")
    assert f["at_limit"].tolist() == [False, True]


def test_side_picks_the_limit_direction():
    rows = [("1101", D1, .1), ("2201", D1, .1), ("2202", D1, .1)]
    assert flags(rows, "多")["at_limit"].tolist() == [True, False, False]
    f = flags(rows, "空")
    assert f["at_limit"].tolist() == [False, True, True]
    assert f["one_price"].tolist() == [False, False, True]


def test_ex_rights_day_is_not_a_limit():
    assert not flags([("3301", D2, .1)], "空")["at_limit"].item()


def test_missing_limit_or_bar_is_unmatched_and_counted():
    df = _trades([("1101", D1, .1), ("4401", D2, .1), ("9999", D1, -.05)])
    f = c.limit_entry_flags(df, "多", BARS)
    assert f["matched"].tolist() == [True, False, False]
    assert f["at_limit"].tolist() == [True, False, False]
    row = c._sensitivity_row(df, f.join(c.limit_exit_flags(df, "多", BARS)))
    assert (row["n"], row["unmatched"], row["at_limit"]) == (3, 2, 1)


def test_suspect_ref():
    f = flags([("1597", D2, .1), ("5501", D2, .1), ("3301", D2, .1), ("1101", D1, .1)], "多")
    assert f["suspect"].tolist() == [True, False, False, False]
    assert not f["at_limit"].iloc[0]          # the miss the column warns about
    df = _trades([("1597", D2, .1), ("5501", D2, .1)])
    assert c._sensitivity_row(df, row_flags(df, "多"))["suspect"] == 1


EXITS = _bars([
    ("1101", D3, 9.5, 9.0, 9.0, 11.0, 9.0, 10.0, 10.0),         # closed limit-down
    ("1102", D3, 9.0, 9.0, 9.0, 11.0, 9.0, 10.0, 10.0),         # limit-down, one price
    ("1103", D3, 11.0, 10.5, 11.0, 11.0, 9.0, 10.0, 10.0),      # limit-up
    ("1104", D3, 10.2, 9.8, 10.0, 11.0, 9.0, 10.0, 10.0),       # flat
])
X = REASON_EXIT_DEFENSE


def test_exit_against_the_position():
    rows = [("1101", D1, .1, D3, X), ("1102", D1, .1, D3, X), ("1103", D1, .1, D3, X),
            ("1104", D1, .1, D3, X)]
    long_ = c.limit_exit_flags(_trades(rows), "多", EXITS)
    assert long_["exit_at_limit"].tolist() == [True, True, False, False]
    assert long_["exit_one_price"].tolist() == [False, True, False, False]
    short = c.limit_exit_flags(_trades(rows), "空", EXITS)
    assert short["exit_at_limit"].tolist() == [False, False, True, False]
    assert long_["exit_matched"].all()


def test_open_at_end_is_not_an_exit():
    f = c.limit_exit_flags(_trades([("1102", D1, .1, D3, REASON_EXIT_END)]), "多", EXITS)
    assert f["exit_matched"].item() and not f["exit_at_limit"].item()


def test_missing_exit_bar_is_unmatched():
    # 1103's entry bar is there, its exit bar (D2) is not
    df = _trades([("1101", D1, .1, D3, X), ("1103", D1, .1, D2, X)])
    f = c.limit_exit_flags(df, "多", EXITS)
    assert f["exit_matched"].tolist() == [True, False]
    bars = pd.concat([BARS, EXITS])
    flags = c.limit_entry_flags(df, "多", bars).join(c.limit_exit_flags(df, "多", bars))
    row = c._sensitivity_row(df, flags)
    assert (row["unmatched"], row["exit_at_limit"], row["exit_one_price"]) == (1, 1, 0)


def test_section_fetches_exit_bars_and_counts_them(tmp_path, monkeypatch):
    calls = []

    def fake(keys):
        calls.append(set(keys))
        return pd.concat([BARS, EXITS])

    monkeypatch.setattr(c, "_fetch_bars", fake)
    for key, _, side in c.SIGNALS:
        _write(tmp_path, key, [("1101", D1, .1, D3, X), ("1103", D1, -.02, D3, X)])
    rows = c.limit_sensitivity(tmp_path)
    assert ("1101", D3) in calls[0] and ("1101", D1) in calls[0]
    by = {r[0]: r[2] for r in rows}
    assert by["抄底"]["exit_at_limit"] == 1 and by["摸頭"]["exit_at_limit"] == 1
    assert by["合池"]["exit_at_limit"] == 6


def test_close_beyond_the_limit_counts_as_at_limit():
    # bad data (close over the published limit) is still on the limit side
    bars = _bars([("1105", D1, 11.2, 10.5, 11.2, 11.0, 9.0, 10.0, 10.0)])
    assert c.limit_entry_flags(_trades([("1105", D1, .1)]), "多", bars)["at_limit"].item()


def test_sensitivity_row_pf_drops_the_right_trades():
    df = _trades([("1101", D1, .10), ("1102", D1, .20), ("1103", D1, -.05)])
    row = c._sensitivity_row(df, row_flags(df, "多"))
    net = np.array([.10, .20, -.05]) - c.COST_PCT
    assert row["pf_all"] == pytest.approx((net[0] + net[1]) / -net[2])
    assert row["pf_no_one"] == pytest.approx(net[0] / -net[2])         # 1102 dropped
    assert row["pf_no_limit"] == pytest.approx(0.0)                    # only the loser left
    assert (row["at_limit"], row["one_price"], row["suspect"]) == (2, 1, 0)


def _write(base, key, rows):
    (base / key).mkdir(parents=True)
    df = _trades(rows) if rows else _trades([]).astype({"報酬率": float})
    df.to_parquet(base / key / "trades.parquet")


@pytest.fixture
def fetched(monkeypatch):
    calls = []

    def fake(keys):
        calls.append(keys)
        return BARS

    monkeypatch.setattr(c, "_fetch_bars", fake)
    return calls


def test_pool_is_the_six_signals_and_unified_listed_after(tmp_path, fetched):
    long_rows, short_rows = [("1101", D1, .1), ("1103", D1, -.02)], [("2202", D1, .05)]
    for key, _, side in c.SIGNALS:
        _write(tmp_path, key, long_rows if side == "多" else short_rows)
    _write(tmp_path, "unified_long", long_rows)
    rows = c.limit_sensitivity(tmp_path)
    labels = [r[0] for r in rows]
    assert labels == [lab for _, lab, _ in c.SIGNALS] + ["合池", "合多"]
    pool = rows[6][2]
    assert pool["n"] == 3 * 2 + 3 * 1
    assert pool["at_limit"] == 3 + 3          # 1101 on each long, 2202 on each short
    assert pool["one_price"] == 3             # 2202 is one price
    assert len(fetched) == 1                  # one query for every signal


def test_no_pool_when_a_signal_is_missing(tmp_path, fetched):
    for key, _, side in c.SIGNALS[:5]:
        _write(tmp_path, key, [("1101", D1, .1)])
    labels = [r[0] for r in c.limit_sensitivity(tmp_path)]
    assert "合池" not in labels and len(labels) == 5


def test_a_signal_without_trades(tmp_path, fetched, capsys):
    for key, _, side in c.SIGNALS:
        _write(tmp_path, key, [] if key == "touch" else [("1101", D1, .1), ("1103", D1, -.02)])
    rows = c.limit_sensitivity(tmp_path)
    assert dict((r[0], r[2]) for r in rows)["摸頭"] is None
    assert rows[6][0] == "合池" and rows[6][2]["n"] == 5 * 2
    assert c.compare(tmp_path) is True
    assert "no trades" in capsys.readouterr().out


def test_no_trades_at_all_queries_nothing(tmp_path, fetched, capsys):
    for key, _, _ in c.SIGNALS:
        _write(tmp_path, key, [])
    rows = c.limit_sensitivity(tmp_path)
    assert [r[2] for r in rows] == [None] * 6 and not fetched
    assert c.compare(tmp_path) is True
    assert "查價失敗" not in capsys.readouterr().out


def test_db_failure_prints_the_rest_and_exits_1(tmp_path, monkeypatch, capsys):
    for key, _, _ in c.SIGNALS:
        _write(tmp_path, key, [("1101", D1, .1), ("1103", D1, -.02)])

    def boom(keys):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(c, "_fetch_bars", boom)
    monkeypatch.setattr("sys.argv", ["_compare", str(tmp_path)])
    with pytest.raises(SystemExit) as e:
        c.main()
    assert e.value.code == 1
    out = capsys.readouterr().out
    assert "【目標 2】" in out and "查價失敗" in out and "connection refused" in out


def test_report_prints_the_section(tmp_path, fetched, capsys):
    for key, _, side in c.SIGNALS:
        _write(tmp_path, key, [("1101", D1, .1), ("1103", D1, -.02)])
    assert c.compare(tmp_path) is True
    out = capsys.readouterr().out
    assert "【成交可行性】" in out and "合池" in out and "一字確定買不到" in out
    assert "參考價疑錯" in out
