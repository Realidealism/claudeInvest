"""攻防解除原因 (compute_stance exits) 與盤中即時加權指數 (_intraday_taiex / build_intraday_stance)."""
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

import analysis.market_thermometer as mt

TZ = timezone(timedelta(hours=8))


def _exits(st, hot_wide, swing=None, swing_exit=None, trend_exit=None):
    n = len(st)
    z = np.zeros(n, dtype=bool)
    _, _, _, exits = mt.compute_stance(
        np.asarray(st), np.asarray(hot_wide, dtype=bool),
        swing=z if swing is None else np.asarray(swing, dtype=bool),
        swing_exit=z if swing_exit is None else np.asarray(swing_exit, dtype=bool),
        trend_exit=z if trend_exit is None else np.asarray(trend_exit, dtype=bool),
        swing_max_hold=5, return_modes=True)
    return list(exits)


def test_swing_max_hold_release_is_labelled():
    sw = [False, True] + [False] * 8
    ex = _exits([0] * 10, [False] * 10, swing=sw)
    assert ex == [""] * 6 + ["swing_maxhold"] + [""] * 3


def test_swing_regained_5d_high_is_labelled():
    sw = [False, True] + [False] * 8
    sx = [False] * 3 + [True] + [False] * 6
    assert _exits([0] * 10, [False] * 10, swing=sw, swing_exit=sx)[3] == "swing_hi"


def test_trend_leg_exits_are_labelled():
    st = [0, -1, -1, -1, 1, 0]
    assert _exits(st, [True] * 6)[4] == "rec"
    tx = [False] * 3 + [True] + [False] * 2
    assert _exits([0, -1, -1, -1, -1, -1], [True] * 6, trend_exit=tx)[3] == "trend_hi"


def test_attack_text_names_the_exit_in_the_first_clause():
    txt = mt._stance3_text("攻擊", True, -2, exit_reason="swing_maxhold", trend_ready=True)
    head = txt.split("；")[0]
    assert "守滿" in head and "未站回" in head
    assert "明日可能轉趨勢防守" in txt
    plain = mt._stance3_text("攻擊", True, -2)
    assert plain == "攻擊中（排列：強空）"
    assert "明日" not in mt._stance3_text("攻擊", True, -2, exit_reason="panic", trend_ready=True)


class _Cur:
    def __init__(self, row):
        self.row = row

    def execute(self, sql, params):
        pass

    def fetchone(self):
        return self.row


def test_intraday_taiex_fresh_stale_and_after_close():
    now = datetime(2026, 10, 22, 10, 30, tzinfo=TZ)
    assert mt._intraday_taiex(_Cur({"time_bucket": "10:30", "taiex_close": 46000}), now) == 46000.0
    assert mt._intraday_taiex(_Cur({"time_bucket": "10:00", "taiex_close": 46000}), now) is None
    assert mt._intraday_taiex(_Cur(None), now) is None
    late = datetime(2026, 10, 22, 15, 0, tzinfo=TZ)
    assert mt._intraday_taiex(_Cur({"time_bucket": "13:50", "taiex_close": 46041.43}), late) == 46041.43


def _frame(n=300):
    rng = np.random.default_rng(0)
    d = pd.bdate_range("2025-01-01", periods=n)
    tx = 20000 + np.cumsum(rng.normal(0, 100, n))
    c = tx * 1.001
    return pd.DataFrame({
        "d": d, "mgn": 3e11 + rng.normal(0, 1e9, n), "noi": rng.normal(-3e4, 5e3, n),
        "pct_from_high": (tx / pd.Series(tx).rolling(55, min_periods=1).max() - 1) * 100,
        "tx": tx, "to": 4e11 + rng.normal(0, 1e10, n), "lim": rng.uniform(0, 3, n),
        "st": rng.integers(-2, 3, n), "sd_pct": rng.uniform(10, 50, n),
        "arr_spread": rng.normal(0, 10, n), "long_up_pct": rng.uniform(20, 60, n),
        "o": c, "h": c + 50, "l": c - 50, "c": c, "v": rng.uniform(5e4, 1e5, n),
        "ref": np.r_[c[0], c[:-1]], "opt_bull": rng.normal(0, 1e4, n)})


def test_build_intraday_stance_uses_live_tx_or_falls_back(monkeypatch):
    f = _frame()
    now = datetime(2026, 10, 22, 10, 30, tzinfo=TZ)
    monkeypatch.setattr(mt, "_load_merged_frame", lambda cur: f.copy())
    monkeypatch.setattr(mt, "_tx_forming_bar", lambda vs, now: (20000.0, 20100.0, 19900.0, 20050.0, 6e4))
    monkeypatch.setattr(mt, "_intraday_short_trend_total", lambda: None)
    monkeypatch.setattr(mt, "_intraday_taiex", lambda cur, now: 12345.0)
    live = mt.build_intraday_stance(None, 1.0, now)
    assert live["tx"] == 12345.0 and live["tx_source"] == "live"
    monkeypatch.setattr(mt, "_intraday_taiex", lambda cur, now: None)
    stale = mt.build_intraday_stance(None, 1.0, now)
    assert stale["tx"] == round(float(f["tx"].iloc[-1]), 2) and stale["tx_source"] == "prev_close"


def test_exit_bookkeeping_does_not_touch_the_latch():
    rng = np.random.default_rng(1)
    n = 400
    kw = dict(top_vote=rng.random(n) < 0.03, price=20000 + np.cumsum(rng.normal(0, 100, n)),
              panic_exit=rng.random(n) < 0.01, top_reentry_win=60,
              swing=rng.random(n) < 0.05, swing_exit=rng.random(n) < 0.2,
              trend_exit=rng.random(n) < 0.1, swing_max_hold=5)
    st, hw = rng.integers(-2, 3, n), rng.random(n) < 0.5
    ee, safe = rng.random(n) < 0.02, rng.random(n) < 0.5
    plain = mt.compute_stance(st, hw, ee, safe, **kw)
    out, modes, held, exits = mt.compute_stance(st, hw, ee, safe, return_modes=True, **kw)
    assert (plain == out).all()
    released = np.r_[False, out[:-1] & ~out[1:]]
    assert ((exits != "") == released).all()
    assert set(exits) - {""} <= set(mt.EXIT_TEXT)


def test_rec_without_bullish_arrangement_is_worded_as_three_days():
    assert "連 3 天不空" in mt._stance3_text("攻擊", True, 0, exit_reason="rec")
    assert "排列回多方" in mt._stance3_text("攻擊", True, 1, exit_reason="rec")


def test_intraday_release_is_marked():
    assert "盤中波段防守守滿" in mt._stance3_text("攻擊", True, -2, exit_reason="swing_maxhold", intraday=True)


def test_intraday_taiex_1330_bucket_is_not_the_close_and_errors_fall_back():
    late = datetime(2026, 10, 22, 15, 0, tzinfo=TZ)
    assert mt._intraday_taiex(_Cur({"time_bucket": "13:30", "taiex_close": 46000}), late) is None
    assert mt._intraday_taiex(_Cur({"time_bucket": "13:35", "taiex_close": 46000}), late) == 46000.0

    class Broken(_Cur):
        def execute(self, sql, params):
            raise RuntimeError("db down")
    assert mt._intraday_taiex(Broken(None), late) is None
    utc = datetime(2026, 10, 22, 2, 30, tzinfo=timezone.utc)      # 10:30 TPE
    assert mt._intraday_taiex(_Cur({"time_bucket": "10:30", "taiex_close": 1.0}), utc) == 1.0
