"""Tests for the HTTP-error backoff in utils/http_client.py.

A requests.Response is falsy for every 4xx/5xx status, so the old
`e.response.status_code if e.response else None` always produced None: the
429/503 backoff never ran and the log printed "HTTP None".

No network: the session is replaced and the sleeps are recorded.
"""
import pytest
import requests

import utils.http_client as hc

URL = "https://example.invalid/x"


class _Session:
    """Answers GET and POST with the given status codes, in order."""

    def __init__(self, statuses):
        self.statuses = list(statuses)
        self.calls = 0

    def _respond(self, *args, **kwargs):
        self.calls += 1
        resp = requests.Response()
        resp.status_code = self.statuses.pop(0)
        resp._content = b'{"ok": true}'
        return resp

    get = post = _respond


@pytest.fixture
def client(monkeypatch):
    sleeps = []
    monkeypatch.setattr(hc, "_wait_for_rate_limit", lambda domain: None)
    monkeypatch.setattr(hc.time, "sleep", sleeps.append)
    monkeypatch.setattr(hc.random, "uniform", lambda a, b: a)   # jitter at its lower bound

    def serve(statuses):
        session = _Session(statuses)
        monkeypatch.setattr(hc, "get_session", lambda: session)
        return session

    return serve, sleeps


CALLS = pytest.mark.parametrize("call", [hc.fetch, hc.post, hc.post_json],
                                ids=["fetch", "post", "post_json"])


@CALLS
@pytest.mark.parametrize("status", [429, 503])
def test_rate_limit_backs_off_longer_each_time(client, call, status):
    serve, sleeps = client
    session = serve([status] * 3)
    assert call(URL) is None
    assert session.calls == hc.MAX_RETRIES
    # BACKOFF_BASE * 2**attempt + jitter(1): 6s, then 11s.
    assert sleeps == [6.0, 11.0]


@CALLS
@pytest.mark.parametrize("status", [404, 500])
def test_other_http_errors_keep_the_flat_wait(client, call, status):
    serve, sleeps = client
    session = serve([status] * 3)
    assert call(URL) is None
    assert session.calls == hc.MAX_RETRIES
    assert sleeps == [hc.BACKOFF_BASE, hc.BACKOFF_BASE]


@CALLS
def test_nothing_is_waited_after_the_last_attempt(client, call):
    serve, sleeps = client
    serve([429] * 3)
    call(URL)
    assert len(sleeps) == hc.MAX_RETRIES - 1


@CALLS
def test_recovers_after_one_rate_limit(client, call):
    serve, sleeps = client
    session = serve([429, 200])
    assert call(URL) is not None
    assert session.calls == 2
    assert sleeps == [6.0]


@CALLS
def test_the_log_names_the_status(client, call, capsys):
    serve, _ = client
    serve([429] * 3)
    call(URL)
    out = capsys.readouterr().out
    assert "HTTP 429 on attempt 1" in out
    assert "HTTP None" not in out
