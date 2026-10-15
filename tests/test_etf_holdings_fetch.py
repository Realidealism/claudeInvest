"""Fetch-level behaviour of the fhtrust (lagged) and KGI holdings sources.

No network and no database: fetch() and the trading-day lookup are replaced,
and the fhtrust file is a small workbook built in memory.  Results are read
through exception type names, so the version before the fixes fails these
tests on their assertions rather than on a missing attribute.
"""
from __future__ import annotations

from datetime import date
from io import BytesIO
from types import SimpleNamespace

import pytest

import scrapers.etf_holdings as eh

TRADE_DATE = date(2026, 10, 12)      # a Monday after a long weekend
PREV_TRADING_DAY = date(2026, 10, 8)


def _xlsx(rows) -> bytes:
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    for r in rows:
        ws.append(r)
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _fhtrust_fake(files: dict):
    """fetch() stand-in: files maps YYYYMMDD to xlsx bytes; other dates answer
    200 with the JSON string fhtrust returns when there is no file."""
    calls = []

    def fetch(url, **kwargs):
        day = url.rsplit("/", 1)[-1]
        calls.append(day)
        return SimpleNamespace(status_code=200, content=files.get(day, b'"no file"'))

    return fetch, calls


def _outcome(fn, *args):
    try:
        return fn(*args), None
    except Exception as e:  # noqa: BLE001 - the type name is the assertion
        return None, type(e).__name__


HOLDINGS = _xlsx([   # fhtrust columns: code, name, shares, amount, weight
    ["code", "name", "shares", "amount", "weight"],
    ["2330", "台積電", "1,000", "1,000,000", "5.0"],
    ["LITE US", "Lumentum", "200", "400,000", "2.0"],
])


def test_lagged_takes_the_previous_trading_days_file(monkeypatch):
    fetch, calls = _fhtrust_fake({PREV_TRADING_DAY.strftime("%Y%m%d"): HOLDINGS})
    monkeypatch.setattr(eh, "fetch", fetch)
    monkeypatch.setattr(eh, "_prev_trading_day", lambda d: PREV_TRADING_DAY, raising=False)
    holdings, err = _outcome(eh._fetch_fhtrust, "ETF26", TRADE_DATE, True)
    assert err is None
    assert calls == ["20261008"]
    assert [h["stock_id"] for h in holdings] == ["2330", "LITE US"]


def test_lagged_does_not_walk_back_past_a_missing_file(monkeypatch):
    """The previous trading day's file is not out yet; an older file exists
    and was stored the day before. Taking it would repeat that snapshot."""
    fetch, calls = _fhtrust_fake({"20261007": HOLDINGS, "20261011": HOLDINGS})
    monkeypatch.setattr(eh, "fetch", fetch)
    monkeypatch.setattr(eh, "_prev_trading_day", lambda d: PREV_TRADING_DAY, raising=False)
    holdings, err = _outcome(eh._fetch_fhtrust, "ETF26", TRADE_DATE, True)
    assert err == "ScrapeError"
    assert calls == ["20261008"]


def test_unlagged_takes_its_own_day(monkeypatch):
    fetch, calls = _fhtrust_fake({TRADE_DATE.strftime("%Y%m%d"): HOLDINGS})
    monkeypatch.setattr(eh, "fetch", fetch)
    holdings, err = _outcome(eh._fetch_fhtrust, "ETF23", TRADE_DATE, False)
    assert err is None and calls == ["20261012"] and len(holdings) == 2


def _kgi_page(rows) -> str:
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    table = ("<table><tr><th>股票代號</th><th>股票名稱</th><th>股數</th><th>權重</th></tr>"
             f"{body}</table>")
    return f"<html><body>{table}{table}</body></html>"   # desktop and mobile copies


def _kgi(monkeypatch, rows):
    monkeypatch.setattr(eh, "fetch", lambda url, **kw: SimpleNamespace(status_code=200, text=_kgi_page(rows)))
    return _outcome(eh._fetch_kgi, "J024")


def test_kgi_total_row_is_not_a_holding(monkeypatch, capsys):
    holdings, err = _kgi(monkeypatch, [
        ["2330", "台積電", "830,000", "9.25%"],
        ["2383", "台光電", "225,000", "5.56%"],
        ["合計", "", "1,055,000", "14.81%"],
    ])
    assert err is None
    assert [h["stock_id"] for h in holdings] == ["2330", "2383"]
    assert holdings[0]["shares"] == 830000 and holdings[0]["weight"] == 9.25
    assert "合計" in capsys.readouterr().out


def test_kgi_spacer_row_does_not_fail_the_fund(monkeypatch):
    holdings, err = _kgi(monkeypatch, [
        ["2330", "台積電", "830,000", "9.25%"],
        ["", "", "", ""],
        ["查無資料"],
    ])
    assert err is None
    assert [h["stock_id"] for h in holdings] == ["2330"]


def test_kgi_bad_number_on_a_stock_row_still_fails(monkeypatch):
    holdings, err = _kgi(monkeypatch, [["2330", "台積電", "n/a", "9.25%"]])
    assert err == "ScrapeError"


def test_kgi_page_with_no_stock_row_fails(monkeypatch):
    """Every row skipped (layout change, or only a "no data" row) must not pass
    as an empty fund: scrape_date would report that as a skip, not a failure."""
    holdings, err = _kgi(monkeypatch, [["查無資料"]])
    assert err == "ScrapeError"
    holdings, err = _kgi(monkeypatch, [["2330.TW", "台積電", "830,000", "9.25%"]])
    assert err == "ScrapeError"


def test_kgi_accepts_listed_code_shapes(monkeypatch):
    codes = ["2330", "00631L", "006208", "2881A", "910322"]
    holdings, err = _kgi(monkeypatch, [[c, "x", "1,000", "1.0%"] for c in codes])
    assert err is None and [h["stock_id"] for h in holdings] == codes


def test_kgi_rejects_fullwidth_digits(monkeypatch):
    holdings, err = _kgi(monkeypatch, [["２３３０", "x", "1,000", "1.0%"], ["2330", "x", "1,000", "1.0%"]])
    assert err is None and [h["stock_id"] for h in holdings] == ["2330"]


def test_registry_lag_flag_reaches_the_fetcher(monkeypatch):
    seen = []
    monkeypatch.setattr(eh, "_fetch_fhtrust", lambda code, d, lagged=False: seen.append((code, lagged)) or [])
    for etf in eh.ETF_REGISTRY:
        if etf["source"] == "fhtrust":
            eh._fetch_holdings(etf, TRADE_DATE)
    assert ("ETF26", True) in seen and ("ETF23", False) in seen


def test_prev_trading_day_missing_raises(monkeypatch):
    from contextlib import contextmanager

    @contextmanager
    def empty_cursor(commit=True):
        yield SimpleNamespace(execute=lambda *a, **k: None, fetchone=lambda: {"d": None})

    monkeypatch.setattr(eh, "get_cursor", empty_cursor)
    _, err = _outcome(eh._prev_trading_day, date(1990, 1, 2))
    assert err == "ScrapeError"


@pytest.mark.needs_db
def test_prev_trading_day_from_taiex_rows():
    """Read-only: the session is set read-only before the lookup runs."""
    import db.connection as conn_mod
    from config.settings import DB_CONFIG

    old = DB_CONFIG.get("options")
    if conn_mod._pool is not None:
        conn_mod._pool.closeall()
    conn_mod._pool = None
    DB_CONFIG["options"] = "-c default_transaction_read_only=on"
    try:
        assert eh._prev_trading_day(date(2026, 10, 12)) == date(2026, 10, 8)
        assert eh._prev_trading_day(date(2026, 10, 14)) == date(2026, 10, 13)
    finally:
        if conn_mod._pool is not None:
            conn_mod._pool.closeall()
        conn_mod._pool = None
        if old is None:
            DB_CONFIG.pop("options", None)
        else:
            DB_CONFIG["options"] = old
