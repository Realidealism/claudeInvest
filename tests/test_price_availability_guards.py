"""Pin the emerging-board price guard at every reader that step 1 changed.

Reads source with ast, imports nothing, so it runs without a database.  The
list is files plus functions: several of them (watch.py, intraday/watchlist.py,
daily_snapshot.py) never mention daily_prices, so only this test notices a
removed check.  tests/test_daily_prices_readers.py pins the file inventory.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HERMIT_ADAPTER = "hermit_stock/src/hermit_stock/data/adapters/db_adapter.py"


def _tree(rel: str) -> ast.Module:
    return ast.parse((ROOT / rel).read_text(encoding="utf-8"))


def _func(rel: str, name: str) -> ast.FunctionDef | ast.AsyncFunctionDef:
    found = [n for n in ast.walk(_tree(rel))
             if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name]
    assert len(found) == 1, f"{rel}: expected one function {name}, found {len(found)}"
    return found[0]


def _body(fn) -> list[ast.stmt]:
    body = fn.body
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
            and isinstance(body[0].value.value, str):
        body = body[1:]
    return body


def _calls(node, name: str) -> bool:
    for n in ast.walk(node):
        if isinstance(n, ast.Call):
            f = n.func
            if (isinstance(f, ast.Name) and f.id == name) or \
                    (isinstance(f, ast.Attribute) and f.attr == name):
                return True
    return False


def _names(node, name: str) -> bool:
    return any(isinstance(n, ast.Name) and n.id == name for n in ast.walk(node))


def _catches(node, exc: str) -> bool:
    for n in ast.walk(node):
        if isinstance(n, ast.ExceptHandler) and n.type is not None:
            t = n.type
            if (isinstance(t, ast.Name) and t.id == exc) or \
                    (isinstance(t, ast.Attribute) and t.attr == exc):
                return True
    return False


def _broad_handler(h: ast.ExceptHandler) -> bool:
    t = h.type
    if t is None:
        return True
    names = t.elts if isinstance(t, ast.Tuple) else [t]
    return any(getattr(n, "id", getattr(n, "attr", None)) in ("Exception", "BaseException")
               for n in names)


def _first_stmt_requires(fn) -> bool:
    """The first statement after the docstring is the check, either bare or as
    the first statement inside a `with` / `try` that opens its cursor.  A `try`
    around it may only catch specific exceptions and every handler must return
    or raise, so a swallowed check fails."""
    stmt = _body(fn)[0]
    while isinstance(stmt, (ast.With, ast.Try)):
        if isinstance(stmt, ast.Try) and any(
                _broad_handler(h) or not any(isinstance(n, (ast.Return, ast.Raise)) for n in ast.walk(h))
                for h in stmt.handlers):
            return False  # swallowed, or falls through to the price read
        stmt = stmt.body[0]
    return isinstance(stmt, ast.Expr) and _calls(stmt, "require_available")


# Readers whose very first action must be the check (before any cache or write).
FIRST_STATEMENT = [
    ("backtest/data.py", "load_stock_data"),
    ("analysis/realtime_data.py", "_fetch_all_daily_prices"),
    ("telegram_bot/handlers/watch.py", "_add"),
    ("telegram_bot/handlers/score.py", "_build_reply"),
]


@pytest.mark.parametrize("rel,name", FIRST_STATEMENT)
def test_check_is_first_statement(rel, name):
    assert _first_stmt_requires(_func(rel, name)), f"{rel}:{name} must start with require_available"


# Readers that must call the check somewhere (by-code reads).
CALLS_CHECK = [
    ("telegram_bot/handlers/score.py", "_batch_fetch_sides_and_quotes", "unavailable_codes"),
    ("intraday/watchlist.py", "load_tw_watchlist", "unavailable_codes"),
    ("charts/app.py", "_load_watchlist", "unavailable_codes"),
    ("charts/app.py", "fetch_price_data", "require_available"),
    ("signal_backtest/__main__.py", "main", "unavailable_codes"),
    ("strategies/utils.py", "month_end_prices", "unavailable_codes"),
    ("strategies/registry.py", "save_signals", "unavailable_codes"),
    (HERMIT_ADAPTER, "load_daily_prices", "_unavailable_codes"),
    (HERMIT_ADAPTER, "load_all_daily_prices", "_unavailable_codes"),
]


@pytest.mark.parametrize("rel,name,check", CALLS_CHECK)
def test_reader_calls_check(rel, name, check):
    assert _calls(_func(rel, name), check), f"{rel}:{name} must call {check}"


def test_batch_fetch_checks_before_reading_prices():
    fn = _func("telegram_bot/handlers/score.py", "_batch_fetch_sides_and_quotes")
    src = ast.unparse(fn)
    assert src.index("unavailable_codes(") < src.index("tw.daily_prices")


# Whole-table queries that must exclude U through the shared fragment.
EXCLUDES_U = [
    ("chip_model/db_access.py", "load_common_universe"),
    ("export/generate.py", "_month_prices"),
    ("signals/etf_abnormal_exit.py", "scan"),
    ("signals/etf_consecutive_reduction.py", "scan"),
    ("signals/etf_multi_exit.py", "scan"),
    ("strategies/etf_abnormal_position.py", "scan_daily"),
    ("strategies/etf_consecutive_accumulation.py", "scan_daily"),
    ("strategies/etf_multi_consensus.py", "scan_daily"),
    ("strategies/quarterly_dormant_etf_active.py", "scan"),
]


@pytest.mark.parametrize("rel,name", EXCLUDES_U)
def test_query_excludes_u(rel, name):
    fn = _func(rel, name)
    hits = [n.args[0] for n in ast.walk(fn)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "execute"
            and n.args and isinstance(n.args[0], ast.JoinedStr) and _names(n.args[0], "UNAVAILABLE_CODES_SQL")]
    assert hits, f"{rel}:{name} must execute a query that uses UNAVAILABLE_CODES_SQL"
    for js in hits:
        text = "".join(v.value for v in js.values if isinstance(v, ast.Constant))
        assert "NOT IN (" in text, f"{rel}:{name} must exclude U with NOT IN"


# Callers that must handle PriceUnavailable explicitly instead of swallowing it.
CATCHES = [
    ("analysis/daily_snapshot.py", "_eval_stock", "PriceUnavailable"),
    ("analysis/intraday_snapshot.py", "_eval_stock", "PriceUnavailable"),
    ("export/generate.py", "_compute_signal_streaks", "PriceUnavailable"),
    ("telegram_bot/handlers/score.py", "_load_volume_status", "PriceUnavailable"),
    ("telegram_bot/handlers/score.py", "_build_reply", "PriceUnavailable"),
    ("telegram_bot/handlers/watch.py", "_add", "PriceUnavailable"),
    ("charts/app.py", "update_chart", "PriceUnavailable"),
    ("hermit_stock/src/hermit_stock/reports/screener.py", "run_screener", "PriceUnavailable"),
]


@pytest.mark.parametrize("rel,name,exc", CATCHES)
def test_caller_catches_explicitly(rel, name, exc):
    fn = _func(rel, name)
    assert _catches(fn, exc), f"{rel}:{name} must catch {exc}"
    handlers = [n for n in ast.walk(fn) if isinstance(n, ast.Try)]
    for t in handlers:
        types = [h.type for h in t.handlers]
        names = [getattr(x, "id", getattr(x, "attr", None)) for x in types]
        if exc in names and "Exception" in names:
            assert names.index(exc) < names.index("Exception"), \
                f"{rel}:{name}: {exc} must be caught before Exception"


def _template(rel: str) -> str:
    for n in ast.walk(_tree(rel)):
        if isinstance(n, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == "UNAVAILABLE_CODES_TEMPLATE" for t in n.targets):
            assert isinstance(n.value, ast.Constant)
            return n.value.value
    raise AssertionError(f"{rel}: UNAVAILABLE_CODES_TEMPLATE not found")


def test_hermit_copy_is_verbatim():
    assert _template(HERMIT_ADAPTER) == _template("db/price_availability.py")


def test_hermit_message_matches():
    def message(rel):
        for n in ast.walk(_tree(rel)):
            if isinstance(n, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == "UNAVAILABLE_MESSAGE" for t in n.targets):
                return n.value.value
    assert message(HERMIT_ADAPTER) == message("db/price_availability.py")
