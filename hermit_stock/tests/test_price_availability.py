"""Emerging-board price guard in hermit's db_adapter and screener.

Needs the parent project's database; every connection is read-only
(PGOPTIONS default_transaction_read_only=on, proven by the first test).
1260 is an emerging-board code, 2330 is listed.  Assertions compare the
exception type name so a run against the old code fails on the assertion.
"""
from __future__ import annotations

import os
from datetime import date

import pytest

from hermit_stock.data.adapters import db_adapter
from hermit_stock.data.models import StockMeta
from hermit_stock.reports import screener

ESB = "1260"
LISTED = "2330"


@pytest.fixture(scope="module", autouse=True)
def read_only_db():
    old = os.environ.get("PGOPTIONS")
    os.environ["PGOPTIONS"] = "-c default_transaction_read_only=on"
    try:
        yield
    finally:
        if old is None:
            os.environ.pop("PGOPTIONS", None)
        else:
            os.environ["PGOPTIONS"] = old


def test_session_is_read_only():
    import psycopg2
    with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction):
        with db_adapter._connect() as conn, conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE _ro_canary (x int)")


def test_load_daily_prices_unavailable():
    try:
        db_adapter.load_daily_prices(ESB)
        name = None
    except Exception as e:  # noqa: BLE001 - the type name is the assertion
        name = type(e).__name__
    assert name == "PriceUnavailable"
    assert len(db_adapter.load_daily_prices(LISTED)) > 1000


def test_load_all_daily_prices_drops_unavailable(caplog):
    with caplog.at_level("WARNING"):
        out = db_adapter.load_all_daily_prices([ESB, LISTED])
    assert set(out) == {LISTED}
    assert ESB in caplog.text


def test_screener_skips_unavailable_ticker():
    """Give 1260 the same financials as 2330 so both reach the price stage;
    only 2330 may come out."""
    reports = db_adapter.load_quarterly_reports(LISTED)
    assert reports
    metas = [StockMeta(ticker=t, name=t, market="TWSE", industry="x", listed_date=None)
             for t in (ESB, LISTED)]
    rows = screener.run_screener(
        as_of_label=date.today().isoformat(),
        metas=metas,
        quarterly_by_ticker={ESB: reports, LISTED: reports},
        monthly_by_ticker={},
        with_valuation=True,
        min_valuation_score=-100,
    )
    tickers = {r.ticker for r in rows}
    assert LISTED in tickers and ESB not in tickers
