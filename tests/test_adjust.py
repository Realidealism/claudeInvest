"""Back-adjustment factors (backtest.adjust) and the adjusted series."""
from datetime import date, timedelta

import numpy as np
import pytest

from backtest import adjust
from backtest.adjust import compute_events, cumulative

D0 = date(2024, 7, 1)


def _days(n, start=D0):
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _rows(closes, refs=None, dates=None):
    dates = dates or _days(len(closes))
    refs = refs or [None] + list(closes[:-1])
    return [dict(trade_date=d, open_price=c, high_price=c, low_price=c, close_price=c,
                 volume=1000.0, turnover=c * 1000, ref_price=r)
            for d, c, r in zip(dates, closes, refs)]


def _events(rows, divs=(), cal=(), cap=(), stock_id="9999", market="TWSE", trading_days=None):
    td = trading_days or [r["trade_date"] for r in rows]
    return compute_events(stock_id, rows, list(divs), list(cal), list(cap), market, td)


def _div(i, rows, cash=0.0, sd=0.0):
    return dict(ex_date=rows[i]["trade_date"], cash=cash, sd=sd)


def test_cash_dividend_scales_the_bars_before_the_ex_date():
    rows = _rows([100, 100, 95, 95], refs=[None, 100, 95, 95])
    ev, _ = _events(rows, divs=[_div(2, rows, cash=5)])
    assert [(e.bar, e.source) for e in ev] == [(2, "dividends")]
    pf, vf = cumulative(4, ev)
    assert pf == pytest.approx([0.95, 0.95, 1, 1])
    assert vf == pytest.approx([1, 1, 1, 1])
    # the adjusted close before the ex-date equals the ex-date's reference price
    assert 100 * pf[1] == pytest.approx(95 * pf[2])


def test_stock_dividend_scales_price_and_volume():
    rows = _rows([120, 120, 100, 100], refs=[None, 120, 100, 100])
    ev, _ = _events(rows, divs=[_div(2, rows, sd=2)])    # 2 NTD of par = 0.2 shares
    pf, vf = cumulative(4, ev)
    assert pf[:2] == pytest.approx([1 / 1.2] * 2)
    assert vf == pytest.approx([1.2, 1.2, 1, 1])


def test_calendar_ratio_is_shares_per_share_and_dividends_win_on_the_same_date():
    rows = _rows([63.5, 63.5, 55.9, 55.9], refs=[None, 63.5, 55.9, 55.9])
    cal = [dict(ex_date=rows[2]["trade_date"], cash=2.0, ratio=0.1)]
    ev, _ = _events(rows, cal=cal)
    assert ev[0].source == "calendar"
    assert ev[0].price_factor == pytest.approx((63.5 - 2) / 1.1 / 63.5)
    assert ev[0].volume_factor == pytest.approx(1.1)
    ev, _ = _events(rows, divs=[_div(2, rows, cash=2, sd=1)], cal=cal)
    assert [e.source for e in ev] == ["dividends"]


def test_ex_date_without_a_bar_takes_the_next_bar():
    days = _days(5)
    rows = _rows([100, 100, 95, 95], dates=[days[0], days[1], days[3], days[4]])
    ev, _ = _events(rows, divs=[dict(ex_date=days[2], cash=5, sd=0)], trading_days=days)
    assert [e.bar for e in ev] == [2]


def test_loss_reduction_uses_the_ratio_for_volume():
    rows = _rows([10, 10, 20, 20], refs=[None, 10, 20, 20])
    cap = [dict(effective_date=rows[2]["trade_date"], ratio=0.5, note="彌補虧損")]
    ev, _ = _events(rows, cap=cap)
    assert (ev[0].price_factor, ev[0].volume_factor) == pytest.approx((2.0, 0.5))


def test_cash_refund_reduction_solves_the_reduction_rate():
    # P = 20, 40% of shares cancelled with 4 NTD back each old share:
    # ref = (20 - 4) / 0.6 = 26.67, ratio = P / ref = 0.75, shares x 0.6
    rows = _rows([20, 20, 26.67, 26.67], refs=[None, 20, 26.67, 26.67])
    cap = [dict(effective_date=rows[2]["trade_date"], ratio=0.75, note="Cash refund")]
    ev, _ = _events(rows, cap=cap)
    assert ev[0].price_factor == pytest.approx(1 / 0.75)
    assert ev[0].volume_factor == pytest.approx(0.6)


def test_reduction_row_the_prices_do_not_show_is_not_applied():
    rows = _rows([7.22, 7.22, 7.31, 7.31], refs=[None, 7.22, 7.22, 7.31])
    cap = [dict(effective_date=rows[2]["trade_date"], ratio=0.5, note="Making up losses")]
    ev, cand = _events(rows, cap=cap)
    assert ev == []
    assert [(c.bar, c.outcome) for c in cand] == [(2, "capital_mismatch")]


def test_reduction_without_a_ref_is_checked_against_the_open():
    rows = _rows([10, 10, 22, 22], refs=[None, 10, None, 22])
    cap = [dict(effective_date=rows[2]["trade_date"], ratio=0.5, note="彌補虧損")]
    ev, _ = _events(rows, cap=cap)
    assert [e.source for e in ev] == ["capital"]
    rows[2]["open_price"] = 10.2
    ev, cand = _events(rows, cap=cap)
    assert ev == [] and [c.outcome for c in cand] == ["capital_mismatch"]


def test_reduction_ratio_holds_a_dividend_that_went_ex_while_halted():
    # ratio = close before the halt / ref after it, dividend inside: the
    # dividend keeps its share count only
    rows = _rows([40, 40, 80, 80], refs=[None, 40, 80, 80])
    divs = [_div(2, rows, cash=2, sd=1)]
    cap = [dict(effective_date=rows[2]["trade_date"], ratio=0.5, note="彌補虧損")]
    ev, cand = _events(rows, divs=divs, cap=cap)
    assert sorted(e.source for e in ev) == ["capital", "dividends"]
    pf, vf = cumulative(4, ev)
    assert 40 * pf[1] == pytest.approx(80)
    assert vf[1] == pytest.approx(0.5 * 1.1)


def test_two_reduction_rows_on_one_bar_apply_once():
    rows = _rows([10, 10, 20, 20], refs=[None, 10, 20, 20])
    cap = [dict(effective_date=rows[1]["trade_date"] + timedelta(days=1), ratio=0.5, note="彌補虧損"),
           dict(effective_date=rows[2]["trade_date"], ratio=0.5, note="彌補虧損")]
    ev, cand = _events(rows, cap=cap)
    assert [e.source for e in ev] == ["capital"]
    assert [c.outcome for c in cand] == ["capital_duplicate"]


def test_cash_refund_below_par_still_shrinks_volume():
    # P = 7, 30% of shares cancelled with 3 NTD back each old share:
    # ref = (7 - 3) / 0.7 = 5.714 (below P), ratio = P / ref = 1.225, shares x 0.7
    ref = 4 / 0.7
    rows = _rows([7, 7, ref, ref], refs=[None, 7, ref, ref])
    cap = [dict(effective_date=rows[2]["trade_date"], ratio=7 / ref, note="現金減資")]
    ev, _ = _events(rows, cap=cap)
    assert ev[0].volume_factor == pytest.approx(0.7)


def test_unexplained_ref_drop_is_filled_from_the_reference_price():
    rows = _rows([100, 100, 97, 97], refs=[None, 100, 97.5, 97])
    ev, cand = _events(rows)
    assert [(e.bar, e.source) for e in ev] == [(2, "ref")]
    assert ev[0].price_factor == pytest.approx(0.975)
    assert [(c.bar, c.outcome) for c in cand] == [(2, "used")]


def test_small_ref_drops_are_not_events():
    rows = _rows([100, 100, 99.8, 99.8], refs=[None, 100, 99.7, 99.8])
    ev, cand = _events(rows)
    assert ev == [] and cand == []


def test_ref_equal_to_close_next_to_the_drop_is_contamination():
    # bar 3 has ref == its own close while the close moved: the defect shape
    rows = _rows([100, 100, 97, 98, 98], refs=[None, 100, 97.5, 98, 98])
    ev, cand = _events(rows)
    assert ev == []
    assert [(c.bar, c.outcome) for c in cand] == [(2, "contaminated")]


def test_drop_after_a_missing_trading_day_is_not_filled():
    days = _days(5)
    rows = _rows([100, 100, 97, 97], refs=[None, 100, 97.5, 97],
                 dates=[days[0], days[1], days[3], days[4]])
    ev, cand = _events(rows, trading_days=days)
    assert ev == []
    assert [c.outcome for c in cand] == ["not_contiguous"]


def test_known_reference_price_defects_are_not_filled():
    dates = _days(4, start=date(2020, 12, 14))
    assert dates[2] == date(2020, 12, 16)
    rows = _rows([100, 100, 97, 97], refs=[None, 100, 97.5, 97], dates=dates)
    ev, cand = _events(rows, market="TWSE")
    assert ev == [] and [c.outcome for c in cand] == ["known_defect"]
    ev, cand = _events(rows, market="TPEx")
    assert [c.outcome for c in cand] == ["used"]
    dates = _days(4, start=date(2025, 3, 3))
    rows = _rows([100, 100, 97, 97], refs=[None, 100, 97.5, 97], dates=dates)
    _, cand = _events(rows, stock_id="6423")
    assert [c.outcome for c in cand] == ["known_defect"]


def test_ref_implied_factor_below_the_floor_is_not_used():
    rows = _rows([100, 100, 40, 40], refs=[None, 100, 40.5, 40])
    ev, cand = _events(rows)
    assert ev == [] and [c.outcome for c in cand] == ["out_of_range"]


def test_events_compound_and_the_last_bar_stays_raw():
    rows = _rows([100, 100, 95, 95, 90, 90], refs=[None, 100, 95, 95, 90, 90])
    ev, _ = _events(rows, divs=[_div(2, rows, cash=5), _div(4, rows, cash=5)])
    pf, _ = cumulative(6, ev)
    assert pf == pytest.approx([0.95 * 90 / 95] * 2 + [90 / 95] * 2 + [1, 1])


# ── the adjusted series ──────────────────────────────────────────────────────

def _walk(n=300, seed=1):
    rng = np.random.default_rng(seed)
    c = 50 * np.exp(np.cumsum(rng.normal(0, 0.02, n)))
    rows = _rows([float(round(x, 2)) for x in c], dates=_days(n))
    for r in rows:
        r["high_price"] = r["close_price"] * 1.01
        r["low_price"] = r["close_price"] * 0.99
        r["volume"] = float(rng.integers(500, 5000))
        r["turnover"] = r["volume"] * r["close_price"]
    return rows


@pytest.fixture
def no_market_state(monkeypatch):
    from backtest import data
    monkeypatch.setattr(data, "calculate_market_state", lambda dates: None)
    return data


def test_no_factors_is_bit_identical_to_ones(no_market_state):
    data = no_market_state
    rows = _walk()
    a = data.build_stock_data("9999", "x", rows, [])
    b = data.build_stock_data("9999", "x", rows, [], np.ones(len(rows)), np.ones(len(rows)))
    for f in ("open", "high", "low", "close", "volume", "turnover", "ref_price"):
        assert np.array_equal(getattr(a, f), getattr(b, f)), f
    assert a.price_factor is None


def test_factors_scale_prices_ref_and_volume_but_not_turnover(no_market_state):
    data = no_market_state
    rows = _walk()
    n = len(rows)
    pf = np.where(np.arange(n) < 150, 0.9, 1.0)
    vf = np.where(np.arange(n) < 150, 1.2, 1.0)
    raw = data.build_stock_data("9999", "x", rows, [])
    adj = data.build_stock_data("9999", "x", rows, [], pf, vf)
    assert np.allclose(adj.close, raw.close * pf, rtol=1e-6)
    assert np.allclose(adj.low, raw.low * pf, rtol=1e-6)
    assert np.allclose(adj.ref_price[1:], raw.ref_price[1:] * pf[1:], rtol=1e-6)
    assert np.allclose(adj.volume, raw.volume * vf, rtol=1e-6)
    assert np.array_equal(adj.turnover, raw.turnover)
    assert np.allclose(adj.price_factor, pf)


def test_adjusted_and_raw_caches_have_separate_keys_that_survive_cleanup(monkeypatch):
    from backtest import data
    monkeypatch.setattr(data, "_get_score_fingerprint", lambda: "fp")
    raw = data._cache_path("2330", D0)
    adj = data._cache_path("2330", D0, adjusted=True)
    assert raw != adj
    assert raw.name.endswith("_fp.pkl") and adj.name.endswith("_fp.pkl")


def test_load_stock_data_adjusts_only_when_asked(monkeypatch, no_market_state):
    data = no_market_state
    rows = _walk()
    monkeypatch.setattr(data, "require_available", lambda cur, sid: None)
    monkeypatch.setattr(data, "get_cursor", _NullCursor)
    monkeypatch.setattr(data, "fetch_stock_name", lambda sid: "x")
    monkeypatch.setattr(data, "_fetch_prices", lambda sid, s, e: rows)
    monkeypatch.setattr(data, "fetch_dividends", lambda sid, dates: [])
    pf = np.where(np.arange(len(rows)) < 100, 0.8, 1.0)
    monkeypatch.setattr(adjust, "adjustment", lambda sid, r: (pf, np.ones(len(r)), []))
    assert data.load_stock_data("9999").price_factor is None
    assert np.allclose(data.load_stock_data("9999", adjusted=True).price_factor, pf)


class _NullCursor:
    def __init__(self, *a, **k):
        pass

    def __enter__(self):
        return None

    def __exit__(self, *a):
        return False


def test_cli_marks_and_clears_the_dir(tmp_path, monkeypatch):
    from tests.test_holdout import _cli
    from signal_backtest import batch
    seen = _cli(monkeypatch, tmp_path, "--adjusted-prices")
    assert seen["adjusted"] is True
    assert (tmp_path / batch.ADJUSTED_PRICES_MARKER).exists()
    seen = _cli(monkeypatch, tmp_path)
    assert seen["adjusted"] is False
    assert not (tmp_path / batch.ADJUSTED_PRICES_MARKER).exists()


def test_versions_add_refuses_an_adjusted_dir(tmp_path, monkeypatch):
    import argparse
    from signal_backtest import _versions as versions, batch
    (tmp_path / batch.ADJUSTED_PRICES_MARKER).write_text("adjusted\n", encoding="utf-8")
    monkeypatch.setattr(versions, "_connect", lambda: pytest.fail("opened the version DB"))
    with pytest.raises(SystemExit) as e:
        versions.cmd_add(argparse.Namespace(scan_dir=str(tmp_path), version_id="v999",
                                            summary="x", snapshot=None))
    assert e.value.code == 1


def test_batch_worker_passes_the_flag(monkeypatch):
    from signal_backtest import batch
    seen = {}

    def fake_load(sid, use_cache=False, adjusted=False):
        seen["adjusted"] = adjusted
        raise RuntimeError("stop")
    monkeypatch.setattr(batch, "load_stock_data", fake_load)
    batch._process_one_stock("9999", ["pick"], 1, False, None, None, True)
    assert seen["adjusted"] is True
