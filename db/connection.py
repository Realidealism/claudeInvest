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
    """Run all migration files to initialize the database."""
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
        for filename in migration_files:
            filepath = os.path.join(migration_dir, filename)
            with open(filepath, "r", encoding="utf-8") as f:
                sql = f.read()
            cursor.execute(sql)
            print(f"Executed: {filename}")
        conn.commit()
        print("Database initialized successfully.")
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    init_db()