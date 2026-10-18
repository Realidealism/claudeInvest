"""Behaviour of the emerging-board price guard (db.price_availability) at each reader.

Runs against the configured database, read-only: every connection opened
while these tests run gets default_transaction_read_only=on, so any write
raises ReadOnlySqlTransaction (test_session_is_read_only proves it).  Write
paths and outward sends are replaced with recorders on top of that.

Assertions on the exception compare the type name, so a run against the old
code fails on the assertion (it raised TypeError or nothing) rather than on
an import error.  1260 and 6604 are emerging-board codes, 2330 is listed.
"""
from __future__ import annotations

import asyncio
import json
import time
from contextlib import contextmanager
from datetime import date, timedelta
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.needs_db

ESB = ("1260", "6604")
LISTED = "2330"


@pytest.fixture(scope="module", autouse=True)
def read_only_db():
    """Rebuild the pool so every connection is read-only, and restore it after."""
    import db.connection as conn_mod
    from config.settings import DB_CONFIG

    def reset_pool():
        if conn_mod._pool is not None:
            try:
                conn_mod._pool.closeall()
            except Exception:
                pass
        conn_mod._pool = None
        conn_mod._pool_pid = None

    old = DB_CONFIG.get("options")
    reset_pool()
    DB_CONFIG["options"] = "-c default_transaction_read_only=on"
    try:
        yield
    finally:
        reset_pool()
        if old is None:
            DB_CONFIG.pop("options", None)
        else:
            DB_CONFIG["options"] = old


def _raised_name(fn, *args, **kwargs) -> str | None:
    try:
        fn(*args, **kwargs)
    except Exception as e:  # noqa: BLE001 - the type name is the assertion
        return type(e).__name__
    return None


def _u_codes() -> set[str]:
    """U computed independently of the code under test (the expected value)."""
    from db.connection import get_cursor
    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT stock_id FROM tw.stocks WHERE market = 'ESB'
            UNION
            SELECT stock_id FROM tw.daily_prices
            WHERE open_price IS NULL AND turnover IS NULL
              AND COALESCE(close_price, high_price, low_price, volume) IS NOT NULL
        """)
        return {r["stock_id"] for r in cur.fetchall()}


# ── the read-only guarantee itself ──────────────────────────────────────────


def test_session_is_read_only():
    import psycopg2
    from config.settings import DB_CONFIG
    from db.connection import get_cursor

    with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction):
        with get_cursor() as cur:
            cur.execute("CREATE TEMP TABLE _ro_canary (x int)")
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cur = conn.cursor()
        cur.execute("SHOW transaction_read_only")
        assert cur.fetchone()[0] == "on"
    finally:
        conn.close()


# ── the definition of U (ruling on wp v0.2, B) ─────────────────────────────


def test_definition_covers_all_three_classes():
    """market='ESB' alone, an X row alone, and neither — over VALUES lists, so
    the class with no live example today (a non-ESB code with an X row) stays
    covered."""
    from db.connection import get_cursor
    from db.price_availability import UNAVAILABLE_CODES_TEMPLATE, unavailable_codes

    stocks = """(SELECT * FROM (VALUES
        ('A1', 'ESB'), ('B2', 'TWSE'), ('C3', 'TPEx'), ('D4', 'TWSE'), ('E5', 'TWSE')
    ) v(stock_id, market))"""
    # columns: stock_id, open, high, low, close, volume, turnover
    prices = """(SELECT stock_id, open_price::numeric, high_price::numeric, low_price::numeric,
                        close_price::numeric, volume::bigint, turnover::bigint FROM (VALUES
        ('A1', 10, 11, 9, 10, 1000, 10000),      -- ESB, listed-shaped row
        ('B2', 20, 21, 19, 20, 2000, 40000),
        ('B2', NULL, 21, 19, 20, 2000, NULL),    -- X row on a non-ESB code
        ('C3', 30, 31, 29, 30, 3000, 90000),     -- ordinary listed code
        ('D4', NULL, NULL, NULL, NULL, NULL, NULL),  -- empty row is not X
        ('E5', NULL, 51, 49, 50, 5000, 0)        -- turnover 0 is not NULL: not X
    ) v(stock_id, open_price, high_price, low_price, close_price, volume, turnover))"""
    sql = UNAVAILABLE_CODES_TEMPLATE.format(stocks=stocks, prices=prices)
    with get_cursor(commit=False) as cur:
        assert unavailable_codes(cur, sql=sql) == {"A1", "B2"}
        assert unavailable_codes(cur, ["A1", "B2", "C3", "D4", "E5"], sql=sql) == {"A1", "B2"}
        assert unavailable_codes(cur, ["C3"], sql=sql) == set()
        assert unavailable_codes(cur, ["B2"], sql=sql) == {"B2"}


def test_live_codes_and_batch_matches_full():
    from db.connection import get_cursor
    from db.price_availability import unavailable_codes

    with get_cursor(commit=False) as cur:
        full = unavailable_codes(cur)
        assert full == _u_codes()
        assert set(ESB) <= full and LISTED not in full
        sample = sorted(full)[:50] + [LISTED, "2317"]
        assert unavailable_codes(cur, sample) == set(sorted(full)[:50])


# ── 4.1 by-code readers ─────────────────────────────────────────────────────


@pytest.mark.parametrize("code", ESB)
def test_load_stock_data_unavailable(code):
    from backtest.data import load_stock_data
    assert _raised_name(load_stock_data, code) == "PriceUnavailable"
    assert _raised_name(load_stock_data, code, use_cache=True) == "PriceUnavailable"


def test_load_stock_data_listed_still_loads():
    from backtest.data import load_stock_data
    data = load_stock_data(LISTED, start_date=date.today() - timedelta(days=200))
    assert data.n > 60 and data.stock_id == LISTED


@pytest.mark.parametrize("code", ESB)
def test_fetch_all_daily_prices_unavailable(code):
    from analysis.realtime_data import _fetch_all_daily_prices
    assert _raised_name(_fetch_all_daily_prices, code) == "PriceUnavailable"


def test_fetch_all_daily_prices_listed():
    from analysis.realtime_data import _fetch_all_daily_prices
    assert len(_fetch_all_daily_prices(LISTED)) > 1000


def test_daily_snapshot_worker_marks_and_warns(capsys):
    from analysis.daily_snapshot import _eval_stock
    today = date.today()
    # Skipped, not a per-stock error (analysis.snapshot_errors).
    assert _eval_stock((ESB[0], today - timedelta(days=400), today)) == ({"_unavailable": ESB[0]}, [])
    assert f"{ESB[0]} skipped: price unavailable" in capsys.readouterr().err


def test_intraday_snapshot_worker_marks_and_warns(capsys):
    from analysis.intraday_snapshot import _eval_stock
    assert _eval_stock(ESB[0]) == ({"_unavailable": ESB[0]}, [])
    assert f"{ESB[0]} skipped: price unavailable" in capsys.readouterr().err


def test_signal_streaks_drop_unavailable(capsys):
    from export.generate import _compute_signal_streaks
    snap = date.today()
    out = _compute_signal_streaks(snap, {ESB[0]: ["pick"], LISTED: ["pick"]}, today_via_intraday=False)
    assert ("pick", LISTED) in out
    assert ("pick", ESB[0]) not in out
    assert f"{ESB[0]} dropped from operations" in capsys.readouterr().err


class _Recorder:
    def __init__(self, result=None):
        self.calls = []
        self.result = result

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.result


def _fake_update(text=None):
    replies = []

    async def reply_text(msg, **kwargs):
        replies.append(msg)

    update = SimpleNamespace(message=SimpleNamespace(text=text, reply_text=reply_text))
    return update, replies


def test_watch_add_refuses_before_writing(monkeypatch):
    import telegram_bot.handlers.watch as watch

    real_get_cursor = watch.get_cursor
    writes = []

    @contextmanager
    def guarded_get_cursor(commit=True):
        if commit:
            writes.append("write cursor opened")
            yield SimpleNamespace(execute=lambda *a, **k: writes.append(a),
                                  fetchone=lambda: None, fetchall=lambda: [])
        else:
            with real_get_cursor(commit=False) as cur:
                yield cur

    monkeypatch.setattr(watch, "get_cursor", guarded_get_cursor)

    reply = watch._add(ESB[0])
    assert "不可用" in reply
    assert writes == []

    update, replies = _fake_update(text=f"{ESB[0]} {ESB[1]}")
    context = SimpleNamespace(user_data={watch._MODE_KEY: "add", watch._MODE_TS_KEY: time.time()}, args=[])
    handler = getattr(watch.cmd_watch_input, "__wrapped__", watch.cmd_watch_input)
    asyncio.run(handler(update, context))
    assert len(replies) == 2 and all("不可用" in r for r in replies)
    assert writes == []


def test_score_reply_refuses(monkeypatch):
    """/score and the social-monitor inline replies share _build_reply; for an
    emerging-board code it returns the notice before reading anything else."""
    import telegram_bot.handlers.score as score

    state = _Recorder()
    monkeypatch.setattr(score, "detect_state", state)
    monkeypatch.setattr(score, "_maybe_refresh_alerts", _Recorder())
    monkeypatch.setattr(score, "_get_turnover_rank_and_price", _Recorder((None, None, None)))

    update, replies = _fake_update()
    context = SimpleNamespace(args=[ESB[0]])
    handler = getattr(score.cmd_score, "__wrapped__", score.cmd_score)
    asyncio.run(handler(update, context))
    assert len(replies) == 1 and "不可用" in replies[0]

    assert "不可用" in score._build_reply(ESB[1])
    assert state.calls == []
    assert score._get_turnover_rank_and_price.calls == []


def test_social_monitor_inline_score_refuses(monkeypatch):
    import telegram_bot.handlers.score as score
    from telegram_bot.watchers.social_monitor import _inline_scores

    state = _Recorder()
    monkeypatch.setattr(score, "detect_state", state)
    monkeypatch.setattr(score, "_maybe_refresh_alerts", _Recorder())
    blocks = _inline_scores([SimpleNamespace(ticker=ESB[0])])
    assert len(blocks) == 1 and "不可用" in blocks[0]
    assert state.calls == []


def test_save_signals_drops_unavailable():
    from db.connection import get_cursor
    from strategies.registry import save_signals

    class InsertRecorder:
        def __init__(self, cur):
            self._cur = cur
            self.inserted = []

        def execute(self, sql, args=None):
            if sql.lstrip().upper().startswith("INSERT"):
                self.inserted.append(args[1])
            else:
                self._cur.execute(sql, args)

        def fetchall(self):
            return self._cur.fetchall()

    def sig(t):
        return {"signal_type": "etf_multi_consensus", "ticker": t, "ticker_name": t, "funds": [],
                "trigger_date": date.today(), "trigger_period": "202610", "weight_change": 0,
                "evidence": {}}

    with get_cursor(commit=False) as cur:
        rec = InsertRecorder(cur)
        n = save_signals([sig(ESB[0]), sig(LISTED)], rec)
    assert rec.inserted == [LISTED] and n == 1


def test_chart_fetch_refuses():
    from charts.app import fetch_price_data
    today = date.today()
    assert _raised_name(fetch_price_data, ESB[0], str(today - timedelta(days=90)), str(today))         == "PriceUnavailable"
    assert fetch_price_data(LISTED, str(today - timedelta(days=90)), str(today)) is not None


def test_signal_factory_refuses_unavailable_stocks(monkeypatch, capsys):
    import sys
    import signal_backtest.__main__ as sb

    def must_not_run(*a, **k):
        raise AssertionError("backtest should not start")

    monkeypatch.setattr(sb, "run_batch", must_not_run)
    monkeypatch.setattr(sb, "run_batch_multi", must_not_run)
    monkeypatch.setattr(sys, "argv", ["signal_backtest", "--signals", "pick", "--stocks", f"{ESB[0]},{LISTED}"])
    with pytest.raises(SystemExit) as exc:
        sb.main()
    assert exc.value.code == 1
    assert ESB[0] in capsys.readouterr().err


def test_watch_summary_marks_unavailable(monkeypatch):
    import telegram_bot.handlers.score as score

    seen = []
    real = score._load_volume_status

    def spy(tickers):
        seen.extend(tickers)
        return real(tickers)

    monkeypatch.setattr(score, "_load_volume_status", spy)
    text = score.build_watchlist_summary([ESB[0], LISTED])
    line = next(l for l in text.splitlines() if f" {ESB[0]} " in l)
    assert "不可用" in line
    assert ESB[0] not in seen and LISTED in seen
    ctx = score._batch_fetch_sides_and_quotes([ESB[0], LISTED])
    assert ctx[ESB[0]].get("unavailable") is True
    assert "ref_close" not in ctx[ESB[0]] and ctx[ESB[0]]["last_price"] is None
    assert "ref_close" in ctx[LISTED]


def test_tw_watchlist_drops_unavailable(monkeypatch, caplog):
    import intraday.watchlist as wl

    real_get_cursor = wl.get_cursor

    class FakeWatchlistCursor:
        """Serves a test-double watchlist; every other query goes to the DB."""

        def __init__(self, cur):
            self._cur = cur
            self._fake = None

        def execute(self, sql, args=None):
            if "portfolio.watchlist" in sql:
                self._fake = [{"symbol": s} for s in (LISTED, ESB[0], "2317")]
            else:
                self._fake = None
                self._cur.execute(sql, args)

        def fetchall(self):
            return self._fake if self._fake is not None else self._cur.fetchall()

    @contextmanager
    def fake_get_cursor(commit=True):
        with real_get_cursor(commit=False) as cur:
            yield FakeWatchlistCursor(cur)

    monkeypatch.setattr(wl, "get_cursor", fake_get_cursor)
    with caplog.at_level("WARNING"):
        out = wl.load_tw_watchlist()
    assert ESB[0] not in out and LISTED in out and "2317" in out
    assert ESB[0] in caplog.text


def test_month_end_prices_drops_unavailable():
    from db.connection import get_cursor
    from strategies.utils import month_end_prices
    last = date.today().replace(day=1) - timedelta(days=1)
    period = f"{last.year}{last.month:02d}"
    with get_cursor(commit=False) as cur:
        prices = month_end_prices(cur, period, [ESB[0], LISTED])
    assert LISTED in prices and ESB[0] not in prices


# ── 4.2 chip universe and export ────────────────────────────────────────────


def test_chip_universe_excludes_u():
    from chip_model.db_access import load_common_universe
    universe = load_common_universe()
    assert len(universe) > 1500
    assert universe & _u_codes() == set()


def test_chip_only_path_exports_without_u(monkeypatch, tmp_path):
    """The weekend update_chip_only path goes through the guarded universe.
    Scraper, git push, report and Telegram are recorders; the export goes to
    tmp_path, never frontend/."""
    import chip_model.db_access as dba
    import daily_update
    import export.generate as gen
    import scrapers.shareholder_distribution as sd
    import telegram_bot.chip_report as chip_report

    universe_calls = _Recorder()
    real_universe = dba.load_common_universe

    def spy_universe():
        universe_calls()
        return real_universe()

    real_export = gen.export_chip_picks
    export_dirs = []

    def export_to_tmp(cur, out):
        export_dirs.append(out)
        return real_export(cur, tmp_path)

    send = _Recorder()
    push = _Recorder()
    monkeypatch.setattr(dba, "load_common_universe", spy_universe)
    monkeypatch.setattr(gen, "export_chip_picks", export_to_tmp)
    monkeypatch.setattr(sd, "scrape_date", _Recorder())
    monkeypatch.setattr(daily_update, "_git_push_frontend", push)
    monkeypatch.setattr(daily_update, "send_sync", send)
    monkeypatch.setattr(chip_report, "build_chip_report", _Recorder("report"))

    daily_update.update_chip_only(date.today())

    assert len(export_dirs) == 1 and universe_calls.calls
    assert len(push.calls) == 1 and len(send.calls) == 1   # recorders, nothing sent
    picks = json.loads((tmp_path / "chip_picks.json").read_text(encoding="utf-8"))
    codes = set()

    def collect(node):
        if isinstance(node, dict):
            for k, v in node.items():
                if k in ("ticker", "stock_id", "code") and isinstance(v, str):
                    codes.add(v)
                collect(v)
        elif isinstance(node, list):
            for v in node:
                collect(v)

    collect(picks)
    assert len(codes) > 20
    assert codes & _u_codes() == set()


# ── 4.3 whole-table readers ─────────────────────────────────────────────────


class _CaptureCursor:
    """Delegates to a real cursor; keeps the rows of queries whose SQL matches."""

    def __init__(self, cur, needle):
        self._cur = cur
        self._needle = needle
        self._capturing = False
        self.captured = []

    def execute(self, sql, args=None):
        self._capturing = all(n in sql for n in self._needle)
        return self._cur.execute(sql, args)

    def fetchall(self):
        rows = self._cur.fetchall()
        if self._capturing:
            self.captured.append(rows)
        return rows

    def fetchone(self):
        return self._cur.fetchone()

    def __getattr__(self, name):
        return getattr(self._cur, name)


_LATEST_CLOSE = ("SELECT DISTINCT ON (stock_id) stock_id, close_price", "FROM tw.daily_prices")


def _latest_close_plain(cur) -> dict:
    cur.execute("""
        SELECT DISTINCT ON (stock_id) stock_id, close_price
        FROM tw.daily_prices
        WHERE close_price IS NOT NULL
        ORDER BY stock_id, trade_date DESC
    """)
    return {r["stock_id"]: r["close_price"] for r in cur.fetchall()}


def _etf_scanners(cur):
    import signals.etf_abnormal_exit as s1
    import signals.etf_consecutive_reduction as s2
    import signals.etf_multi_exit as s3
    from strategies.etf_abnormal_position import EtfAbnormalPosition
    from strategies.etf_consecutive_accumulation import EtfConsecutiveAccumulation
    from strategies.etf_multi_consensus import EtfMultiConsensus
    from strategies.quarterly_dormant_etf_active import QuarterlyDormantEtfActive

    cur.execute("SELECT MAX(trade_date) AS d FROM tw.etf_holdings_diff")
    td = cur.fetchone()["d"]
    cur.execute("SELECT MAX(period) AS p FROM tw.fund_holdings_quarterly")
    qp = cur.fetchone()["p"]
    return [
        ("etf_abnormal_exit", lambda c: s1.scan(td, c)),
        ("etf_consecutive_reduction", lambda c: s2.scan(td, c)),
        ("etf_multi_exit", lambda c: s3.scan(td, c)),
        ("etf_abnormal_position", lambda c: EtfAbnormalPosition().scan_daily(td, c)),
        ("etf_consecutive_accumulation", lambda c: EtfConsecutiveAccumulation().scan_daily(td, c)),
        ("etf_multi_consensus", lambda c: EtfMultiConsensus().scan_daily(td, c)),
        ("quarterly_dormant_etf_active", lambda c: QuarterlyDormantEtfActive().scan(qp, c, td)),
    ]


def test_etf_latest_close_queries_exclude_u_and_keep_the_rest():
    from db.connection import get_cursor

    u = _u_codes()
    with get_cursor(commit=False) as cur:
        plain = _latest_close_plain(cur)
        assert set(plain) & u, "plain query should see emerging-board codes"
        expected = {k: v for k, v in plain.items() if k not in u}
        for name, run in _etf_scanners(cur):
            cap = _CaptureCursor(cur, _LATEST_CLOSE)
            run(cap)
            assert len(cap.captured) == 1, f"{name}: latest-close query not reached"
            got = {r["stock_id"]: r["close_price"] for r in cap.captured[0]}
            assert set(got) & u == set(), name
            assert got == expected, f"{name}: non-U prices changed"


def test_flow_month_prices_exclude_u(tmp_path):
    from db.connection import get_cursor
    from export.generate import export_flow

    u = _u_codes()
    with get_cursor(commit=False) as cur:
        cap = _CaptureCursor(cur, ("DISTINCT ON (stock_id) stock_id, close_price",
                                   "trade_date >= %s AND trade_date < %s"))
        export_flow(cap, tmp_path)
    assert cap.captured, "_month_prices was not reached"
    for rows in cap.captured:
        codes = {r["stock_id"] for r in rows}
        assert len(codes) > 1000 and codes & u == set()


def test_exception_survives_process_boundary():
    import pickle
    from db.price_availability import PriceUnavailable
    e = pickle.loads(pickle.dumps(PriceUnavailable(ESB[0])))
    assert type(e).__name__ == "PriceUnavailable"
    assert e.code == ESB[0] and str(e) == str(PriceUnavailable(ESB[0]))


def test_signals_export_drops_stored_unavailable_signals(tmp_path):
    """A signal already stored on an emerging-board code (injected into the
    query as an extra row; nothing is written) must not reach signals.json,
    and every stored listed signal still must."""
    from collections import Counter

    import psycopg2
    from psycopg2.extras import RealDictCursor

    from config.settings import DB_CONFIG
    from export.generate import export_signals

    cols = ("id, signal_type, ticker, ticker_name, funds, trigger_date, trigger_period, "
            "weight_change, evidence, created_at")
    fake = (f"(SELECT {cols} FROM tw.signals UNION ALL SELECT -1, 'etf_multi_consensus', "
            f"'{ESB[0]}', 'x', ARRAY['x']::text[], DATE '2026-10-14', '202610', 0::numeric, "
            f"'{{}}'::jsonb, now())")

    class InjectCursor:
        def __init__(self, cur):
            self._cur = cur

        def execute(self, sql, args=None):
            assert sql.count("FROM tw.signals s") == 1
            return self._cur.execute(sql.replace("FROM tw.signals s", f"FROM {fake} s"), args)

        def __getattr__(self, name):
            return getattr(self._cur, name)

    # One snapshot for the expected rows and the export.
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT ticker FROM tw.signals
            WHERE ticker NOT IN (
                SELECT stock_id FROM tw.stocks WHERE market = 'ESB'
                UNION
                SELECT stock_id FROM tw.daily_prices
                WHERE open_price IS NULL AND turnover IS NULL
                  AND COALESCE(close_price, high_price, low_price, volume) IS NOT NULL)
        """)
        expected = Counter(r["ticker"] for r in cur.fetchall())
        export_signals(InjectCursor(cur), tmp_path)
    finally:
        conn.rollback()
        conn.close()
    data = json.loads((tmp_path / "signals.json").read_text(encoding="utf-8"))
    published = Counter(e["ticker"] for entries in data["by_type"].values() for e in entries)
    assert ESB[0] not in published
    assert sum(expected.values()) > 0
    assert published == expected
