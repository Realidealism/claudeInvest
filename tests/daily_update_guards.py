"""Shared safety net for tests that drive daily_update.

daily_update writes to the production database, pushes to git and sends
Telegram messages. A test that forgets one stand-in must fail, not do the real
thing. Import the fixture into a test module to switch it on for every test:

    from daily_update_guards import no_side_effects  # noqa: F401

Raising is not enough on its own: the pipeline wraps most steps in
`except Exception`, which would turn the refusal into a "failed" step and let
the test pass. Every refusal is also recorded and checked at teardown.
"""
import subprocess

import psycopg2
import pytest
import requests

import daily_update as du
import db.connection
import telegram_bot.notify

# For the tests that exercise the run log itself (into tmp_path).
real_open_run_log = du._open_run_log


@pytest.fixture(autouse=True)
def no_side_effects(monkeypatch):
    reached = []

    def refuse(what):
        def _refuse(*args, **kwargs):
            reached.append(what)
            raise AssertionError(f"test reached {what}")
        return _refuse

    monkeypatch.setattr(psycopg2, "connect", refuse("psycopg2.connect"))
    monkeypatch.setattr(db.connection, "_get_pool", refuse("the connection pool"))
    monkeypatch.setattr(db.connection, "get_cursor", refuse("db.connection.get_cursor"))
    monkeypatch.setattr(du, "get_cursor", refuse("daily_update.get_cursor"))
    monkeypatch.setattr(requests.sessions.Session, "request", refuse("the network"))
    monkeypatch.setattr(subprocess, "run", refuse("subprocess.run"))
    monkeypatch.setattr(subprocess, "Popen", refuse("subprocess.Popen"))
    monkeypatch.setattr(du, "send_sync", refuse("daily_update.send_sync"))
    monkeypatch.setattr(telegram_bot.notify, "send_sync", refuse("telegram_bot.notify.send_sync"))
    monkeypatch.setattr(du, "_git_push_frontend", refuse("git push"))
    # Unfrozen, the run log lands in the repo's own logs/ folder.
    monkeypatch.setattr(du, "_open_run_log", refuse("the run log"))
    yield reached
    assert reached == [], f"test reached real side effects: {reached}"
