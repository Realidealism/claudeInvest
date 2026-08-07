import os
import sys
import threading
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import PoolError, ThreadedConnectionPool
from contextlib import contextmanager

from config.settings import DB_CONFIG

# Opening a connection to this DB measures ~16 ms; running a query on an
# already-open one measures ~0.6 ms. get_cursor() is called ~3k times per
# intraday pass, so the handshake is worth avoiding.
#
# Kept deliberately small. Backtests and the snapshot daemon fan out with
# multiprocessing, and Windows spawns rather than forks, so every worker
# re-imports this module and builds its own pool. With max_connections=100
# (97 usable) the cap has to leave room for ~24 concurrent worker processes
# plus three long-running services.
POOL_MIN = 1
POOL_MAX = int(os.getenv("DB_POOL_MAX", "4"))

_pool = None
_pool_pid = None
_pool_lock = threading.Lock()


def get_connection():
    """Create a new, unpooled database connection.

    Callers of this own the connection and close it themselves, so it stays
    outside the pool -- handing them a pooled connection would mean their
    close() destroyed it instead of returning it.
    """
    return psycopg2.connect(**DB_CONFIG)


def _get_pool():
    """Return this process's pool, building it on first use.

    Built lazily, never at import: under spawn every worker re-imports this
    module, and an eager pool would open connections in processes that may
    never touch the DB. The pid guard rebuilds rather than reusing a pool
    inherited through a fork, whose sockets would be shared with the parent.
    """
    global _pool, _pool_pid
    if _pool is not None and _pool_pid == os.getpid():
        return _pool
    with _pool_lock:
        if _pool is None or _pool_pid != os.getpid():
            _pool = ThreadedConnectionPool(POOL_MIN, POOL_MAX, **DB_CONFIG)
            _pool_pid = os.getpid()
    return _pool


def _is_live(conn) -> bool:
    """Is this pooled connection still usable?

    Pooling introduces a failure mode the old connect-every-time code did not
    have: a connection that died while idle (DB restart, network drop, server
    idle timeout). No caller in this repo catches OperationalError or
    InterfaceError, so a dead connection handed out here would surface as an
    unexplained failure of whatever that caller was doing. Check instead.

    The rollback also clears any transaction state the previous borrower left
    behind, so the caller always starts clean.
    """
    if conn.closed:
        return False
    try:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return True
    except psycopg2.Error:
        return False


def _checkout(pool):
    """Borrow a live connection. Returns (conn, came_from_pool)."""
    for _ in range(POOL_MAX + 1):
        try:
            candidate = pool.getconn()
        except PoolError:
            # Every pooled connection is checked out. Fall back to a dedicated
            # connection instead of failing: getconn() raises rather than
            # waiting, and the telegram bot runs six watchers plus command
            # handlers concurrently through asyncio.to_thread. Before pooling
            # a burst like that always succeeded, and no caller in this repo
            # catches connection errors, so it must keep succeeding. Pooling
            # is an optimisation; it must never be a new way to fail.
            return psycopg2.connect(**DB_CONFIG), False
        if _is_live(candidate):
            return candidate, True
        # Closing it frees the slot, so the next getconn() dials a fresh one.
        pool.putconn(candidate, close=True)

    raise psycopg2.OperationalError(
        "No live database connection available after discarding "
        f"{POOL_MAX + 1} pooled connections"
    )


@contextmanager
def get_cursor(commit=True):
    """Context manager that provides a cursor and handles commit/rollback."""
    pool = _get_pool()
    conn, pooled = _checkout(pool)

    cursor = None
    broken = False
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        yield cursor
        if commit:
            conn.commit()
        else:
            # The old code just closed the connection here, which discarded the
            # transaction. A pooled connection is reused, so it has to be ended
            # explicitly or it goes back idle-in-transaction holding its
            # snapshot and any locks.
            conn.rollback()
    except Exception:
        try:
            conn.rollback()
        except psycopg2.Error:
            broken = True
        raise
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except psycopg2.Error:
                broken = True
        if pooled:
            pool.putconn(conn, close=broken)
        else:
            conn.close()


def init_db():
    """Apply pending migration files to the database.

    Applied filenames are recorded in public.schema_migrations so each file
    runs exactly once. The first run against a pre-existing DB re-executes
    every file (they are all idempotent) and records them as the baseline;
    after that, init_db() is a fast no-op unless new files appear.

    Migration files are append-only from here on: editing an already-applied
    file has no effect — write a new numbered file instead.
    """
    import os

    # Support PyInstaller bundled paths
    base = getattr(sys, '_MEIPASS', os.path.dirname(__file__))
    migration_dir = os.path.join(base, "db", "migrations") if hasattr(sys, '_MEIPASS') else os.path.join(os.path.dirname(__file__), "migrations")
    migration_files = sorted(
        f for f in os.listdir(migration_dir) if f.endswith(".sql")
    )

    conn = get_connection()
    try:
        cursor = conn.cursor()
        # Serialize concurrent init_db() calls (cron jobs + intraday daemons
        # start in parallel); lock is released on commit/rollback.
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext('init_db'))")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS public.schema_migrations ("
            "  filename   TEXT PRIMARY KEY,"
            "  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()"
            ")"
        )
        cursor.execute("SELECT filename FROM public.schema_migrations")
        applied = {row[0] for row in cursor.fetchall()}

        ran = 0
        for filename in migration_files:
            if filename in applied:
                continue
            filepath = os.path.join(migration_dir, filename)
            with open(filepath, "r", encoding="utf-8") as f:
                sql = f.read()
            cursor.execute(sql)
            cursor.execute(
                "INSERT INTO public.schema_migrations (filename) VALUES (%s)",
                (filename,),
            )
            print(f"Executed: {filename}")
            ran += 1
        conn.commit()
        if ran:
            print(f"Database initialized successfully ({ran} migration(s) applied).")
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    init_db()