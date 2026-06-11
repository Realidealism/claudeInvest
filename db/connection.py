import sys
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

from config.settings import DB_CONFIG


def get_connection():
    """Create a new database connection."""
    return psycopg2.connect(**DB_CONFIG)


@contextmanager
def get_cursor(commit=True):
    """Context manager that provides a cursor and handles commit/rollback."""
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        yield cursor
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
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