"""Tests for the db/connection.py connection pool.

Opening a connection to this database costs ~16 ms against ~0.6 ms to run a
query on an open one, and get_cursor is called thousands of times per intraday
pass, so connections are pooled. Pooling brings failure modes the old
connect-every-time code did not have -- connections that die while idle,
transaction state left behind by the previous borrower, exhaustion under
concurrency -- and nothing in this repo catches connection-level errors, so
the pool has to handle all of them itself.

Marked needs_db: these exercise a real PostgreSQL, and skip without one.
"""
import multiprocessing as mp
import threading

import psycopg2
import pytest

import db.connection as dbc
from db.connection import get_cursor

pytestmark = pytest.mark.needs_db


@pytest.fixture(autouse=True)
def _require_db():
    try:
        conn = psycopg2.connect(**dbc.DB_CONFIG)
    except Exception as exc:
        pytest.skip(f"database unavailable: {type(exc).__name__}: {exc}")
    conn.close()


@pytest.fixture
def fresh_pool():
    """Tear down any existing pool so a test can observe construction."""
    if dbc._pool is not None:
        dbc._pool.closeall()
    dbc._pool = None
    dbc._pool_pid = None
    yield
    if dbc._pool is not None:
        dbc._pool.closeall()
    dbc._pool = None
    dbc._pool_pid = None


def _open_connection_count():
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT count(*) AS n FROM pg_stat_activity "
                    "WHERE datname IS NOT NULL")
        return cur.fetchone()["n"]


def _backend_pid():
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT pg_backend_pid() AS p")
        return cur.fetchone()["p"]


def test_pool_is_built_lazily(fresh_pool):
    """Never at import time.

    Under spawn every worker process re-imports this module, and an eager
    pool would open connections in processes that may never touch the DB.
    """
    assert dbc._pool is None
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT 1")
    assert dbc._pool is not None


def test_connection_is_reused_across_calls(fresh_pool):
    first = _backend_pid()
    assert all(_backend_pid() == first for _ in range(10))


def test_exceptions_do_not_leak_connections(fresh_pool):
    before = _open_connection_count()
    for _ in range(10):
        with pytest.raises(ValueError):
            with get_cursor() as cur:
                cur.execute("SELECT 1")
                raise ValueError("boom")
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT 1 AS v")
        assert cur.fetchone()["v"] == 1
    assert _open_connection_count() <= before + 1


def test_failed_statement_does_not_poison_the_next_borrower(fresh_pool):
    """A pooled connection carries its aborted transaction to whoever is next
    unless it is rolled back on the way in or out."""
    with pytest.raises(psycopg2.Error):
        with get_cursor() as cur:
            cur.execute("SELECT * FROM table_that_does_not_exist")

    with get_cursor(commit=False) as cur:
        cur.execute("SELECT 1 AS v")
        assert cur.fetchone()["v"] == 1


def test_read_only_block_does_not_hold_a_transaction_open(fresh_pool):
    """commit=False used to be released by closing the connection.

    A pooled connection is reused, so it has to be ended explicitly or it goes
    back idle-in-transaction still holding its snapshot.
    """
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT 1")

    with get_cursor(commit=False) as cur:
        cur.execute("SELECT state FROM pg_stat_activity "
                    "WHERE pid = pg_backend_pid()")
        assert cur.fetchone()["state"] != "idle in transaction"


def test_connection_that_died_while_idle_is_replaced(fresh_pool):
    """The failure mode pooling introduces: a DB restart or dropped socket
    leaves the pool holding handles that no caller checks."""
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT 1")

    killed = 0
    for conn in list(dbc._pool._pool):
        conn.close()
        killed += 1
    assert killed, "expected at least one idle pooled connection"

    with get_cursor(commit=False) as cur:
        cur.execute("SELECT 42 AS v")
        assert cur.fetchone()["v"] == 42


def test_concurrent_callers_do_not_raise(fresh_pool):
    """The telegram bot runs six watchers plus command handlers through
    asyncio.to_thread, so borrows genuinely overlap."""
    errors = []

    def hammer():
        try:
            for _ in range(20):
                with get_cursor(commit=False) as cur:
                    cur.execute("SELECT 1 AS v")
                    assert cur.fetchone()["v"] == 1
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=hammer) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors


def test_exhaustion_falls_back_to_an_unpooled_connection(fresh_pool):
    """getconn raises rather than waiting once every slot is checked out.

    Before pooling a burst like this always succeeded, and no caller in this
    repo catches connection errors, so it has to keep succeeding. Each of the
    simultaneous holders must get a real backend of its own.
    """
    n = dbc.POOL_MAX * 2
    seen, lock = set(), threading.Lock()
    barrier = threading.Barrier(n, timeout=30)
    errors = []

    def hold():
        try:
            with get_cursor(commit=False) as cur:
                cur.execute("SELECT pg_backend_pid() AS p")
                with lock:
                    seen.add(cur.fetchone()["p"])
                barrier.wait()
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=hold) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len(seen) == n, f"only {len(seen)} of {n} holders got a connection"


def _worker_queries(_):
    """Runs in a spawned child, which must build its own pool."""
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT 1 AS v")
        assert cur.fetchone()["v"] == 1
    return True


def test_spawned_workers_each_get_their_own_pool(fresh_pool):
    """Backtests and the snapshot daemon fan out with multiprocessing."""
    before = _open_connection_count()
    with mp.Pool(3) as pool:
        assert all(pool.map(_worker_queries, range(6)))
    assert _open_connection_count() <= before + 1
