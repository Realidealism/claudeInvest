"""Every .py file that mentions daily_prices must be classified in daily_prices_readers_table.

A new reader of tw.daily_prices fails this test until someone decides how it
stands on emerging-board prices.  Plain text scan, no imports, no database.
"""
from __future__ import annotations

from pathlib import Path

from tests.daily_prices_readers_table import READERS

ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {"tmp", "build", "dist", "node_modules", ".venv", ".git"}
CATEGORIES = {"1", "2", "3", "4", "5", "W", "T", "X"}
HANDLING = {"guarded", "excludes-U", "unchanged", "known-gap", "cli-only", "n/a"}


def _files_mentioning_daily_prices() -> set[str]:
    found = set()
    for path in ROOT.rglob("*.py"):
        rel = path.relative_to(ROOT)
        if SKIP_DIRS & set(rel.parts[:-1]):
            continue
        if "daily_prices" in path.read_text(encoding="utf-8", errors="replace"):
            found.add(rel.as_posix())
    return found


def test_inventory_matches_table():
    found = _files_mentioning_daily_prices()
    missing = sorted(found - set(READERS))
    stale = sorted(set(READERS) - found)
    assert not missing, f"classify these in tests/daily_prices_readers_table.py: {missing}"
    assert not stale, f"no longer mention daily_prices, remove from the table: {stale}"


def test_table_values_are_known():
    for rel, (cat, handling, note) in READERS.items():
        assert cat in CATEGORIES, rel
        assert handling in HANDLING, rel
        assert note, rel
        if cat in ("2", "3") and handling == "n/a":
            raise AssertionError(f"{rel}: a category {cat} reader needs a handling decision")
