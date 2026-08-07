"""Tests for the batched sweeper write path in intraday/store.py.

The sweeper upserts ~2200 rows every 20 seconds all session. It used to send
two statements per row; those round trips accounted for most of the work in
each cycle, which is why the configured 20s interval was landing at 23-24s.
Batching them through execute_values introduces one hazard the per-row loop
did not have -- a duplicate key inside a single multi-row ON CONFLICT DO
UPDATE is a hard error -- so the de-duplication is covered explicitly.
"""
from datetime import date

import psycopg2
import pytest

import db.connection as dbc
import intraday.store as store
from intraday.store import _prepare_quote_rows

TODAY = date(2026, 8, 7)


def _rec(stock_id="2330", **over):
    rec = {
        "stock_id": stock_id, "name": "TSMC",
        "open_price": 1000.0, "high_price": 1010.0,
        "low_price": 990.0, "last_price": 1005.0,
        "last_size": 10, "last_trade_at": None,
        "total_volume": 12345, "total_value": 678901, "tx_count": 42,
        "change_price": 5.0, "change_pct": 0.5, "amplitude": 2.0,
        "limit_up": 1100.0, "limit_down": 900.0,
    }
    rec.update(over)
    return rec


# ── _prepare_quote_rows ────────────────────────────────────────────────────

def test_quote_tuple_arity_matches_the_insert_template():
    """17 placeholders plus a server-side NOW(); a drift here misaligns every
    column silently."""
    _, quotes = _prepare_quote_rows([_rec()], "TWSE", TODAY)
    assert len(quotes[0]) == 17


def test_duplicate_stock_ids_collapse_to_one_row():
    """Postgres refuses a multi-row ON CONFLICT DO UPDATE that touches the same
    row twice, so a repeated id in one snapshot must not reach the statement."""
    records = [_rec(last_price=1.0), _rec(last_price=2.0), _rec(last_price=3.0)]
    stocks, quotes = _prepare_quote_rows(records, "TWSE", TODAY)
    assert len(quotes) == 1
    assert len(stocks) == 1


def test_the_last_duplicate_wins():
    """Matches the old per-row loop, where the final write survived."""
    records = [_rec(last_price=1.0), _rec(last_price=3.0)]
    _, quotes = _prepare_quote_rows(records, "TWSE", TODAY)
    assert quotes[0][5] == 3.0          # last_price is the sixth column


def test_records_without_a_stock_id_are_dropped():
    _, quotes = _prepare_quote_rows([{"last_price": 1.0}, _rec()], "TWSE", TODAY)
    assert len(quotes) == 1


def test_unclassifiable_ids_are_dropped():
    """They would fail the foreign key into tw.stocks anyway."""
    _, quotes = _prepare_quote_rows([_rec(stock_id="NOTASTOCK")], "TWSE", TODAY)
    assert quotes == []


def test_missing_name_falls_back_to_the_id():
    stocks, _ = _prepare_quote_rows([_rec(name=None)], "TWSE", TODAY)
    assert stocks[0][1] == "2330"


def test_market_and_source_are_stamped():
    stocks, quotes = _prepare_quote_rows([_rec()], "TPEx", TODAY)
    assert stocks[0][2] == "TPEx"
    assert quotes[0][16] == "rest_sweep"


def test_empty_input_produces_nothing():
    assert _prepare_quote_rows([], "TWSE", TODAY) == ([], [])


# ── real round trip, rolled back ───────────────────────────────────────────

@pytest.mark.needs_db
def test_batched_upsert_writes_the_expected_values(monkeypatch):
    """Exercises the actual SQL against the real table, then rolls back.

    The column list, the template and the tuple order only agree if this
    reads back what it wrote.
    """
    try:
        conn = psycopg2.connect(**dbc.DB_CONFIG)
    except Exception as exc:
        pytest.skip(f"database unavailable: {type(exc).__name__}: {exc}")

    from contextlib import contextmanager
    from psycopg2.extras import RealDictCursor

    @contextmanager
    def _tx_cursor(commit=True):
        cur = conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cur
        finally:
            cur.close()

    monkeypatch.setattr(store, "get_cursor", _tx_cursor)

    probe_id = "9999"
    try:
        written = store.upsert_quotes(
            [_rec(stock_id=probe_id, name="PROBE", last_price=123.5,
                  total_volume=777, tx_count=7)],
            market="TSE", trade_date=TODAY,
        )
        assert written == 1

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT stock_id, trade_date, last_price, total_volume, "
                "       tx_count, source, updated_at "
                "FROM tw.intraday_quotes WHERE stock_id = %s",
                (probe_id,),
            )
            row = cur.fetchone()

        assert row is not None, "batched upsert wrote nothing"
        assert row["stock_id"] == probe_id
        assert row["trade_date"] == TODAY
        assert float(row["last_price"]) == 123.5
        assert row["total_volume"] == 777
        assert row["tx_count"] == 7
        assert row["source"] == "rest_sweep"
        assert row["updated_at"] is not None      # NOW() in the template fired
    finally:
        conn.rollback()
        conn.close()
