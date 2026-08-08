"""Tests for the built-artifact freshness probe in the daily health check.

A commit only reaches production when the exe carrying it is rebuilt, and
nothing enforced that: the sweeper served code from 2026-06-15 for five weeks
because no script covered its spec. This probe compares each exe against
every module it bundles.

Built against a synthetic tree rather than the real repo so the assertions do
not depend on whatever was last rebuilt here.
"""
import os
import time
from pathlib import Path

import pytest

import telegram_bot.health_check as hc


def _tree(root: Path, *, hidden=("pkg.dynamic",), entry_imports="import pkg.direct"):
    """A miniature repo: one spec, one rebuild script, a small import graph."""
    (root / "dist").mkdir(parents=True, exist_ok=True)
    (root / "pkg").mkdir(exist_ok=True)
    (root / "pkg" / "__init__.py").write_text("", encoding="utf-8")
    (root / "pkg" / "direct.py").write_text("import pkg.deep\n", encoding="utf-8")
    (root / "pkg" / "deep.py").write_text("x = 1\n", encoding="utf-8")
    (root / "pkg" / "dynamic.py").write_text("y = 2\n", encoding="utf-8")
    (root / "app.py").write_text(entry_imports + "\n", encoding="utf-8")
    (root / "app.spec").write_text(
        "a = Analysis(\n"
        "    ['app.py'],\n"
        f"    hiddenimports={list(hidden)!r},\n"
        ")\n"
        "exe = EXE(pyz, a.scripts, name='app', console=True)\n",
        encoding="utf-8",
    )
    (root / "rebuild.bat").write_text(
        "@echo off\r\npyinstaller --clean app.spec\r\n", encoding="ascii"
    )
    exe = root / "dist" / "app.exe"
    exe.write_bytes(b"stub")
    return exe


def _set_mtime(path: Path, when: float):
    os.utime(path, (when, when))


@pytest.fixture
def repo(tmp_path, monkeypatch):
    monkeypatch.setattr(hc, "_REPO_ROOT", tmp_path)
    return tmp_path


def test_spec_contents_reads_entry_hiddenimports_and_name(repo):
    _tree(repo)
    roots, name = hc._spec_contents(repo / "app.spec")
    assert name == "app"
    assert "app" in roots
    assert "pkg.dynamic" in roots


def test_bundle_includes_hidden_imports(repo):
    """The reason a walk from the entry alone is not enough.

    daily_update loads its scrapers through importlib, so they appear only in
    hiddenimports -- exactly the modules changed most often.
    """
    _tree(repo)
    roots, _ = hc._spec_contents(repo / "app.spec")
    names = {p.name for p in hc._bundled_sources(roots)}
    assert "dynamic.py" in names


def test_bundle_follows_imports_transitively(repo):
    _tree(repo)
    roots, _ = hc._spec_contents(repo / "app.spec")
    names = {p.name for p in hc._bundled_sources(roots)}
    assert {"app.py", "direct.py", "deep.py"} <= names


def test_current_exe_is_not_flagged(repo):
    exe = _tree(repo)
    _set_mtime(exe, time.time() + 60)      # built after every source
    assert hc._stale_exes() == []


def test_exe_older_than_a_bundled_module_is_flagged(repo):
    exe = _tree(repo)
    _set_mtime(exe, time.time() - 3600)
    problems = hc._stale_exes()
    assert len(problems) == 1
    assert "app.exe" in problems[0]


def test_a_stale_hidden_import_is_caught(repo):
    """The five-week case: only a dynamically loaded module changed."""
    exe = _tree(repo)
    now = time.time()
    for p in repo.rglob("*.py"):
        _set_mtime(p, now - 7200)
    _set_mtime(exe, now - 3600)
    assert hc._stale_exes() == []          # nothing newer than the build yet

    _set_mtime(repo / "pkg" / "dynamic.py", now)
    problems = hc._stale_exes()
    assert len(problems) == 1
    assert "dynamic.py" in problems[0]


def test_specs_no_rebuild_script_mentions_are_ignored(repo):
    """Manual tools nobody keeps current must not put a permanent entry in the
    daily message -- that would turn the healthy silent ping into a standing
    alert and destroy the dead-man's switch."""
    exe = _tree(repo)
    _set_mtime(exe, time.time() - 3600)
    assert hc._stale_exes() != []
    (repo / "rebuild.bat").unlink()
    assert hc._maintained_specs() == []
    assert hc._stale_exes() == []


def test_missing_exe_is_not_flagged(repo):
    """Never built means never deployed; there is nothing to be stale."""
    exe = _tree(repo)
    exe.unlink()
    assert hc._stale_exes() == []
