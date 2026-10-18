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


def _kgi_page(rows, nav_date="2026/10/12") -> str:
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    table = ("<table><tr><th>股票代號</th><th>股票名稱</th><th>股數</th><th>權重</th></tr>"
             f"{body}</table>")
    nav = ("" if nav_date is None else
           f'<input id="LatestNAVDate" name="LatestNAVDate" type="hidden" value="{nav_date}" />')
    return f"<html><body>{nav}{table}{table}</body></html>"   # desktop and mobile copies


def _kgi(monkeypatch, rows, nav_date="2026/10/12"):
    page = _kgi_page(rows, nav_date)
    monkeypatch.setattr(eh, "fetch", lambda url, **kw: SimpleNamespace(status_code=200, text=page))
    return _outcome(eh._fetch_kgi, "J024", TRADE_DATE)


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
    as an empty fund."""
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


# ---------------------------------------------------------------------------
# Data date: sources without a date in the request must serve trade_date's file
# ---------------------------------------------------------------------------

def _ezmoney_page(tran_date):
    import html
    import json
    detail = {"DetailCode": "2330", "DetailName": "台積電", "Share": 1000.0, "NavRate": 5.0}
    if tran_date is not None:
        detail["TranDate"] = tran_date
    assets = [{"AssetCode": "ST", "Details": [detail]}]
    return f'<div id="DataAsset" data-content="{html.escape(json.dumps(assets), quote=True)}"></div>'


class _Session:
    """get_session() stand-in: post() answers the JSON for the call."""

    def __init__(self, body):
        self.body = body

    def _resp(self, body):
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: body)

    def get(self, url, **kw):          # Allianz XSRF token
        return self._resp({"token": "t"})

    def post(self, url, **kw):
        return self._resp(self.body)


def _session_source(monkeypatch, body):
    monkeypatch.setattr(eh, "get_session", lambda: _Session(body))
    monkeypatch.setattr(eh, "_wait_for_rate_limit", lambda domain: None)


def _with_fund_asset(nav_date):
    fund_asset = None if nav_date is None else {"NavDate": nav_date}
    return {"Entries": {"Data": {"FundAsset": fund_asset, "Table": [
        {"Columns": [{}] * 5, "Rows": [["1", "2330", "台積電", "1,000", "5.0%"]]}]}}}


def _nomura_body(nav_date):
    body = _with_fund_asset(nav_date)
    body["Entries"]["Data"]["Table"][0] = {"Columns": [{}] * 4,
                                          "Rows": [["2330", "台積電", "1,000", "5.0"]]}
    return body


def _prepare(monkeypatch, source, found):
    """Fake one source's response dated found (None: the date field is
    missing) and return its fetcher with the arguments for TRADE_DATE."""
    if source == "ezmoney":
        monkeypatch.setattr(eh, "fetch", lambda url, **kw: SimpleNamespace(text=_ezmoney_page(found)))
        return eh._fetch_ezmoney, ("49YTW", TRADE_DATE)
    if source == "capitalfund":
        pcf = {} if found is None else {"date1": "2026-10-13", "date2": found}
        _session_source(monkeypatch, {"data": {"pcf": pcf, "stocks": [
            {"stocNo": "2330", "stocName": "台積電", "share": 1000, "weight": 5.0}]}})
        return eh._fetch_capitalfund, ("399", TRADE_DATE)
    if source == "allianz":
        _session_source(monkeypatch, _with_fund_asset(found))
        return eh._fetch_allianz, ("E0001", TRADE_DATE)
    if source == "nomura":
        _session_source(monkeypatch, _nomura_body(found))
        return eh._fetch_nomura, ("00980A", TRADE_DATE)
    if source == "kgi":
        page = _kgi_page([["2330", "台積電", "1,000", "5.0%"]], nav_date=found)
        monkeypatch.setattr(eh, "fetch", lambda url, **kw: SimpleNamespace(status_code=200, text=page))
        return eh._fetch_kgi, ("J024", TRADE_DATE)
    raise AssertionError(source)


SAME_DAY = {"ezmoney": "2026-10-12T00:00:00", "capitalfund": "2026-10-12",
            "allianz": "2026/10/12", "nomura": "2026/10/12", "kgi": "2026/10/12"}
DAY_BEFORE = {"ezmoney": "2026-10-08T00:00:00", "capitalfund": "2026-10-08",
              "allianz": "2026/10/08", "nomura": "2026/10/08", "kgi": "2026/10/08"}


@pytest.mark.parametrize("source", sorted(SAME_DAY))
def test_source_dated_trade_date_passes(monkeypatch, source):
    fn, args = _prepare(monkeypatch, source, SAME_DAY[source])
    holdings, err = _outcome(fn, *args)
    assert err is None and [h["stock_id"] for h in holdings] == ["2330"]


@pytest.mark.parametrize("source", sorted(SAME_DAY))
def test_source_dated_another_day_fails(monkeypatch, source):
    fn, args = _prepare(monkeypatch, source, DAY_BEFORE[source])
    with pytest.raises(eh.ScrapeError, match="data is for 2026-10-08, expected 2026-10-12"):
        fn(*args)


@pytest.mark.parametrize("source", sorted(SAME_DAY))
def test_source_without_a_date_fails(monkeypatch, source):
    fn, args = _prepare(monkeypatch, source, None)
    with pytest.raises(eh.ScrapeError, match="no data date"):
        fn(*args)


@pytest.mark.parametrize("tran_date, err", [("2026-10-08T00:00:00", None),
                                            ("2026-10-12T00:00:00", "ScrapeError")])
def test_lagged_ezmoney_fund_must_serve_the_previous_trading_day(monkeypatch, tran_date, err):
    """00988A still serves the previous trading day's file at night; today's
    would be a file the other funds are not on yet."""
    etf = next(e for e in eh.ETF_REGISTRY if e["etf_id"] == "00988A")
    assert etf.get("lagged") is True
    monkeypatch.setattr(eh, "_prev_trading_day", lambda d: PREV_TRADING_DAY)
    monkeypatch.setattr(eh, "fetch", lambda url, **kw: SimpleNamespace(text=_ezmoney_page(tran_date)))
    assert _outcome(eh._fetch_holdings, etf, TRADE_DATE)[1] == err


def _ezmoney_rows_page(rows):
    import html
    import json
    data = json.dumps([{"AssetCode": "ST", "Details": rows}])
    return f'<div id="DataAsset" data-content="{html.escape(data, quote=True)}"></div>'


def test_ezmoney_rows_of_two_dates_fail(monkeypatch):
    # Neither date is TRADE_DATE, so only the two-dates check names the cause.
    rows = [{"DetailCode": c, "DetailName": "x", "Share": 1.0, "NavRate": 1.0, "TranDate": d}
            for c, d in (("2330", "2026-10-07T00:00:00"), ("2317", "2026-10-08T00:00:00"))]
    page = _ezmoney_rows_page(rows)
    monkeypatch.setattr(eh, "fetch", lambda url, **kw: SimpleNamespace(text=page))
    with pytest.raises(eh.ScrapeError, match="carry dates"):
        eh._fetch_ezmoney("49YTW", TRADE_DATE)


def test_ezmoney_row_that_is_not_a_holding_does_not_count_its_date(monkeypatch):
    rows = [{"DetailCode": "2330", "DetailName": "x", "Share": 1.0, "NavRate": 1.0,
             "TranDate": "2026-10-12T00:00:00"},
            {"DetailCode": "", "DetailName": "合計", "Share": None}]
    page = _ezmoney_rows_page(rows)
    monkeypatch.setattr(eh, "fetch", lambda url, **kw: SimpleNamespace(text=page))
    assert [h["stock_id"] for h in eh._fetch_ezmoney("49YTW", TRADE_DATE)] == ["2330"]


@pytest.mark.parametrize("found, err", [
    ("2026/10/12", None), ("2026-10-12", None), ("2026-10-12T16:31:14", None),
    (" 2026/10/12 ", None), ("2026/1/5", "data is for 2026-01-05"),
    ("", "no data date"), ("10/12/2026", "no data date"), ("2026/13/40", None)])
def test_data_date_formats(found, err):
    if found == "2026/13/40":
        # Not a date at all: still an exception, which scrape_date counts as a failure.
        with pytest.raises(ValueError):
            eh._check_data_date("x", "f", found, TRADE_DATE)
        return
    if err is None:
        eh._check_data_date("x", "f", found, TRADE_DATE)
    else:
        with pytest.raises(eh.ScrapeError, match=err):
            eh._check_data_date("x", "f", found, TRADE_DATE)


@pytest.mark.parametrize("source", sorted(SAME_DAY))
def test_dispatcher_hands_the_trade_date_to_each_source(monkeypatch, source):
    etf = next(e for e in eh.ETF_REGISTRY if e["source"] == source and not e.get("lagged"))
    _prepare(monkeypatch, source, SAME_DAY[source])
    assert [h["stock_id"] for h in eh._fetch_holdings(etf, TRADE_DATE)] == ["2330"]
    _prepare(monkeypatch, source, DAY_BEFORE[source])
    with pytest.raises(eh.ScrapeError, match="expected 2026-10-12"):
        eh._fetch_holdings(etf, TRADE_DATE)


# ---------------------------------------------------------------------------
# scrape_date: a fund with no holdings, or another day's file, is a failure
# ---------------------------------------------------------------------------

def _scrape(monkeypatch, fetched, registry=None, stored=()):
    """Run scrape_date over funds whose fetch outcome is fetched[etf_id] (a
    holdings list, or an exception to raise); the DB is replaced, with the
    funds in stored already on record for the day. With a registry, the real
    fetchers run instead, on whatever fetch() was faked."""
    from contextlib import contextmanager

    saved = []
    monkeypatch.setattr(eh, "_already_stored",
                        lambda etf_id, d: d == TRADE_DATE and etf_id in stored)

    @contextmanager
    def cursor(commit=True):
        yield SimpleNamespace()

    def fetch_holdings(etf, trade_date):
        out = fetched[etf["etf_id"]]
        if isinstance(out, Exception):
            raise out
        return out

    if registry is None:
        monkeypatch.setattr(eh, "ETF_REGISTRY", [{"etf_id": k, "source": "x"} for k in fetched])
        monkeypatch.setattr(eh, "_fetch_holdings", fetch_holdings)
    else:
        monkeypatch.setattr(eh, "ETF_REGISTRY", registry)
    monkeypatch.setattr(eh, "get_cursor", cursor)
    monkeypatch.setattr(eh, "_get_prev_holdings", lambda cur, etf_id, d: {})
    monkeypatch.setattr(eh, "_save_holdings", lambda cur, etf_id, d, h: saved.append(etf_id))
    try:
        eh.scrape_date(TRADE_DATE)
    except RuntimeError as e:
        return saved, str(e)
    return saved, None


ONE = [{"stock_id": "2330", "stock_name": "台積電", "shares": 1000, "weight": 5.0}]


def test_scrape_date_counts_a_fund_with_no_holdings_as_failed(monkeypatch):
    saved, err = _scrape(monkeypatch, {"A": ONE, "B": [], "C": ONE})
    assert saved == ["A", "C"]
    assert err is not None and err.endswith("failed for: B")


def test_scrape_date_stores_nothing_for_another_days_file(monkeypatch):
    # Real fetchers: fund A's page is for the trade date, fund B's for the day before.
    pages = {"A": _ezmoney_page("2026-10-12T00:00:00"), "B": _ezmoney_page("2026-10-08T00:00:00")}
    monkeypatch.setattr(eh, "fetch", lambda url, params=None, **kw:
                        SimpleNamespace(text=pages[params["fundCode"]]))
    registry = [{"etf_id": k, "source": "ezmoney", "fund_code": k} for k in pages]
    saved, err = _scrape(monkeypatch, {}, registry)
    assert saved == ["A"]
    assert err is not None and err.endswith("failed for: B")


def test_scrape_date_skips_a_fund_already_stored_for_the_day(monkeypatch):
    # A rerun the next evening: B's undated source now serves the next day's
    # file, but B is on record for the day, so it is neither fetched nor failed.
    later = eh.ScrapeError("ezmoney B data is for 2026-10-13, expected 2026-10-12")
    saved, err = _scrape(monkeypatch, {"A": ONE, "B": later}, stored={"B"})
    assert saved == ["A"] and err is None


@pytest.mark.parametrize("row, expected", [({"?column?": 1}, True), (None, False)])
def test_already_stored_asks_for_that_fund_on_that_day(monkeypatch, row, expected):
    from contextlib import contextmanager
    seen = []

    @contextmanager
    def cursor(commit=True):
        assert commit is False
        yield SimpleNamespace(execute=lambda sql, params: seen.append((sql, params)),
                              fetchone=lambda: row)

    monkeypatch.setattr(eh, "get_cursor", cursor)
    assert eh._already_stored("00981A", TRADE_DATE) is expected
    (sql, params), = seen
    assert "tw.etf_holdings " in sql and "etf_id = %s AND trade_date = %s" in sql
    assert params == ("00981A", TRADE_DATE)


def test_scrape_date_all_good_raises_nothing(monkeypatch):
    saved, err = _scrape(monkeypatch, {"A": ONE, "B": ONE})
    assert saved == ["A", "B"] and err is None
