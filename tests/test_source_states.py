"""What each event-driven scraper reports about the legs it asked.

These sources can legitimately have nothing to report (no disposal today, no
transfer filed this month), and every one of them used to fold "nothing to
report", "no answer" and "a page we cannot read" into the same empty list, so
a dead source was recorded as a quiet day. Each leg now ends as one of
utils.format_shift's states, and only a leg that did not answer or could not
be read lands in failed_sources.

The "nothing to report" pages are the raw bytes the sources served on
2026-10-17 (tests/fixtures/source_states). No network and no database: every
request and every save is replaced.
"""
import json
import pathlib
import types
from contextlib import contextmanager
from datetime import date, timedelta

import pytest
import requests

import scrapers.capital_reductions as cr
import scrapers.dividend_calendar as dc
import scrapers.insider_pledge_events as ipe
import scrapers.insider_share_transfers as ist
import scrapers.investor_conferences as ic
import scrapers.shareholder_distribution as sd
import scrapers.short_cover_calendar as scc
import scrapers.stock_alerts as sa
import scrapers.taifex_institutional as tinst
from daily_update_guards import no_side_effects  # noqa: F401
from utils.format_shift import (EMPTY, NO_RESPONSE, OK, UNREADABLE, ScrapeResult,
                                failed_legs, openapi_rows, twse_rwd_rows)

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "source_states"


def fixture_bytes(name):
    return (FIXTURES / name).read_bytes()


def fixture_json(name):
    return json.loads(fixture_bytes(name).decode("utf-8"))


class _Resp:
    """The parts of a requests.Response the scrapers read."""

    def __init__(self, body: bytes, status=200):
        self.content = body
        self.text = body.decode("utf-8", "replace")
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}", response=self)


# ── shared helpers ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("data, state", [
    (fixture_json("twse_punish_empty.json"), EMPTY),
    ({"stat": "OK", "data": [["1", "x"]]}, OK),
    (None, NO_RESPONSE),
    ({"stat": "很抱歉，查詢日期大於今日"}, UNREADABLE),
    ({"stat": "OK"}, UNREADABLE),
    ("<html>busy</html>", UNREADABLE),
])
def test_twse_rwd_states(data, state):
    assert twse_rwd_rows(data)[1] == state


@pytest.mark.parametrize("data, state", [
    ([], EMPTY), ([{"a": 1}], OK), (None, NO_RESPONSE), ({"error": "x"}, UNREADABLE),
])
def test_openapi_states(data, state):
    assert openapi_rows(data)[1] == state


def test_failed_legs_names_only_the_failures():
    legs = {"甲": OK, "乙": EMPTY, "丙": NO_RESPONSE, "丁": UNREADABLE}
    assert failed_legs(legs) == ("丙（沒有回應）", "丁（回應讀不懂）")


# ── stock alerts ───────────────────────────────────────────────────────────

NOTICE_ROW = ["1", "2330", "台積電", "1", "連續三日", "115/10/16", "1,000", "20"]
PUNISH_ROW = [1, "115/10/15", "2330", "台積電", 1, "連續三次", "115/10/16～115/10/29",
              "第一次處置", "內容", ""]
TPEX_PUNISH_ROW = [1, "115/10/15", "6488", "環球晶", 1, "115/10/16~115/10/29", "原因",
                   "第一次處置", "內容", "500", "20", ""]
TPEX_EMPTY = fixture_json("tpex_attention_empty.json")


def tpex(rows):
    answer = json.loads(json.dumps(TPEX_EMPTY))
    answer["tables"][0]["data"] = rows
    return answer


NORMAL = {
    sa.TWSE_NOTICE_URL: {"stat": "OK", "data": [NOTICE_ROW]},
    sa.TWSE_PUNISH_URL: {"stat": "OK", "data": [PUNISH_ROW]},
    sa.TPEX_ATTENTION_URL: tpex([NOTICE_ROW]),
    sa.TPEX_DISPOSAL_URL: tpex([TPEX_PUNISH_ROW]),
}


def serve_alerts(monkeypatch, **override):
    answers = {**NORMAL, **{getattr(sa, k): v for k, v in override.items()}}
    monkeypatch.setattr(sa, "fetch_json_retry", lambda url, **kw: answers[url])
    monkeypatch.setattr(sa, "post_json_retry", lambda url, **kw: answers[url])
    monkeypatch.setattr(sa, "_save", lambda records: len(records))


PAST = date(2026, 10, 16)


def test_alerts_every_leg_answered(monkeypatch):
    serve_alerts(monkeypatch)
    result = sa.scrape_date(PAST)
    assert result.failed_sources == () and result.records == 4


def test_alerts_quiet_disposal_lists_are_fine(monkeypatch):
    serve_alerts(monkeypatch, TWSE_PUNISH_URL=fixture_json("twse_punish_empty.json"),
                 TPEX_DISPOSAL_URL=TPEX_EMPTY)
    assert sa.scrape_date(PAST).failed_sources == ()


@pytest.mark.parametrize("leg, answer, entry", [
    ("TWSE_PUNISH_URL", None, "上市處置股（沒有回應）"),
    ("TWSE_NOTICE_URL", {"stat": "查詢失敗"}, "上市注意股（回應讀不懂）"),
    ("TPEX_DISPOSAL_URL", {"tables": []}, "上櫃處置股（回應讀不懂）"),
    ("TPEX_ATTENTION_URL", None, "上櫃注意股（沒有回應）"),
    # Rows came back but none parsed: the layout changed.
    ("TWSE_NOTICE_URL", {"stat": "OK", "data": [["x"]]}, "上市注意股（回應讀不懂）"),
])
def test_alerts_failed_leg(monkeypatch, leg, answer, entry):
    serve_alerts(monkeypatch, **{leg: answer})
    assert sa.scrape_date(PAST).failed_sources == (entry,)


def test_alerts_empty_attention_list_on_a_past_day_is_a_miss(monkeypatch):
    serve_alerts(monkeypatch, TWSE_NOTICE_URL=fixture_json("twse_punish_empty.json"),
                 TPEX_ATTENTION_URL=TPEX_EMPTY)
    assert sa.scrape_date(PAST).failed_sources == ("上市注意股（0 筆）", "上櫃注意股（0 筆）")


def test_alerts_empty_attention_list_for_a_future_day_is_fine(monkeypatch):
    # The next-day disposal pull asks about a day that has not traded yet.
    serve_alerts(monkeypatch, TWSE_NOTICE_URL=fixture_json("twse_punish_empty.json"),
                 TPEX_ATTENTION_URL=TPEX_EMPTY)
    assert sa.scrape_date(date.today() + timedelta(days=3)).failed_sources == ()


def test_alerts_range_backfill_still_returns_a_count(monkeypatch):
    serve_alerts(monkeypatch)
    assert sa.scrape_range(PAST, PAST) == 4


# ── dividend and short-cover boards ────────────────────────────────────────

DIV_TWSE_ROW = ["115年10月20日", "2330", "台積電", "息", "0", "0", "0", "5.0"]
DIV_TPEX_ROW = {"SecuritiesCompanyCode": "6488", "ExRrightsExDividendDate": "1151020",
                "CompanyName": "環球晶", "ExRrightsExDividend": "息", "CashDividend": "3"}
SC_TWSE_ROW = ["2330", "台積電", "115.10.20", "115.10.27", "股東常會"]
SC_TPEX_ROW = {"SecuritiesCompanyCode": "6488", "ShortSaleSuspensionStartDate": "1151020",
               "ShortSaleSuspensionEndDate": "1151027", "Reason": "股東常會"}


@pytest.mark.parametrize("mod, twse_row, tpex_row, prefix", [
    (dc, DIV_TWSE_ROW, DIV_TPEX_ROW, "除權息預告"),
    (scc, SC_TWSE_ROW, SC_TPEX_ROW, "停券預告"),
])
@pytest.mark.parametrize("twse, tpex_answer, failed", [
    ("rows", "rows", ()),
    ("empty", [], ()),                       # an empty board is a quiet window
    (None, "rows", ("上市{}（沒有回應）",)),
    ({"stat": "系統忙碌"}, "rows", ("上市{}（回應讀不懂）",)),
    ("rows", None, ("上櫃{}（沒有回應）",)),
    ("rows", {"message": "error"}, ("上櫃{}（回應讀不懂）",)),
])
def test_board_legs(monkeypatch, mod, twse_row, tpex_row, prefix, twse, tpex_answer, failed):
    answers = {
        mod.TWSE_URL: {"stat": "OK", "data": [twse_row]} if twse == "rows"
        else {"stat": "OK", "data": []} if twse == "empty" else twse,
        mod.TPEX_URL: [tpex_row] if tpex_answer == "rows" else tpex_answer,
    }
    monkeypatch.setattr(mod, "fetch_json_retry", lambda url, **kw: answers[url])
    monkeypatch.setattr(mod, "_save", lambda records: len(records))
    assert mod.scrape_date(PAST).failed_sources == tuple(f.format(prefix) for f in failed)


# ── TAIFEX institutional ───────────────────────────────────────────────────

@pytest.mark.parametrize("fut, opt, failed", [
    (69, 30, ()),
    (69, 0, ("期交所法人選擇權",)),
    (0, 30, ("期交所法人期貨",)),
    (0, 0, ("期交所法人期貨", "期交所法人選擇權")),
])
def test_taifex_institutional_legs(monkeypatch, fut, opt, failed):
    rows = {tinst.FUT_URL: fut, tinst.OPT_URL: opt}
    monkeypatch.setattr(tinst, "_run", lambda url, sql, parse, start, end:
                        ScrapeResult(rows[url], rows[url], 0))
    result = tinst.scrape_date(PAST)
    assert result.failed_sources == failed and result.records == fut + opt


# ── capital reductions ─────────────────────────────────────────────────────

def test_every_unanswered_reduction_candidate_is_named(monkeypatch):
    monkeypatch.setattr(cr, "_candidates", lambda d: ["3085", "6166", "2330"])
    monkeypatch.setattr(cr, "_fetch", lambda ds, sid, s, e: None if sid in ("3085", "2330") else [])
    monkeypatch.setattr(cr, "_save", lambda parsed: 0)
    result = cr.scrape_date(PAST)
    assert result.failed_sources == ("FinMind 3085", "FinMind 2330")
    # parse_errors counts rows elsewhere; stocks there would trip the shift check.
    assert result.parse_errors == 0


def test_reduction_candidates_all_answered(monkeypatch):
    monkeypatch.setattr(cr, "_candidates", lambda d: ["3085", "6166"])
    monkeypatch.setattr(cr, "_fetch", lambda ds, sid, s, e: [])
    monkeypatch.setattr(cr, "_save", lambda parsed: 0)
    assert cr.scrape_date(PAST).failed_sources == ()


# ── shareholder distribution ───────────────────────────────────────────────

def test_csv_without_a_dated_row_is_a_failure(monkeypatch):
    monkeypatch.setattr(sd, "_run_completed", False)
    monkeypatch.setattr(sd, "fetch_opendata", lambda: (None, {}))
    assert sd.scrape_date(PAST).failed_sources == ("集保 OpenData CSV（讀不到資料日期）",)


def test_guarded_distribution_run_is_not_a_failure(monkeypatch):
    monkeypatch.setattr(sd, "_run_completed", True)
    assert sd.scrape_date(PAST).failed_sources == ()


# ── MOPS: share transfers ──────────────────────────────────────────────────

def _mops_session(monkeypatch, mod, answers):
    """answers: a list of _Resp or Exception, served in order."""
    answers = list(answers)

    def post(url, **kwargs):
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(mod, "_get_mops_session", lambda: types.SimpleNamespace(post=post))
    monkeypatch.setattr(mod, "_mops_rate_limit", lambda: None)
    return answers


TRANSFER_ROW = ("<tr>" + "".join(f"<td>{c}</td>" for c in [
    "1", "115/10/16", "2330", "台積電", "董事", "某甲", "洽特定人", "1,000", "", "",
    "1,000", "", "", "", "", "", "115/10/20~115/11/19"]) + "</tr>")


@pytest.mark.parametrize("answer, state, n", [
    (_Resp(fixture_bytes("mops_transfers_empty.html")), EMPTY, 0),
    (_Resp(f"<html><table>{TRANSFER_ROW}</table></html>".encode()), OK, 1),
    (_Resp("<html><body>查詢過於頻繁，請稍後再試</body></html>".encode()), UNREADABLE, 0),
    (requests.ConnectionError("reset"), NO_RESPONSE, 0),
    (_Resp(b"", status=502), NO_RESPONSE, 0),
])
def test_transfer_month_state(monkeypatch, answer, state, n):
    _mops_session(monkeypatch, ist, [answer])
    rows, got = ist.fetch_state("sii", 115, 10)
    assert got == state and len(rows) == n


def test_transfer_fetch_keeps_returning_a_list(monkeypatch):
    # The backfill script calls fetch() and expects rows.
    _mops_session(monkeypatch, ist, [_Resp(f"<table>{TRANSFER_ROW}</table>".encode())])
    assert len(ist.fetch("sii", 115, 10)) == 1


def test_transfer_scrape_names_the_failed_query(monkeypatch):
    _mops_session(monkeypatch, ist, [
        _Resp(fixture_bytes("mops_transfers_empty.html")),   # sii this month
        requests.Timeout("slow"),                           # otc this month
        _Resp(f"<table>{TRANSFER_ROW}</table>".encode()),    # sii last month
        _Resp(fixture_bytes("mops_transfers_empty.html")),   # otc last month
    ])
    monkeypatch.setattr(ist, "save_transfers", lambda rows: len(rows))
    result = ist.scrape_date(date(2026, 10, 16))
    assert result.failed_sources == ("內部人轉讓 上櫃 2026-10（沒有回應）",)
    assert result.records == 1


# ── MOPS: pledge events ────────────────────────────────────────────────────

PLEDGE_ROW = ("<tr class='odd'>" + "".join(f"<td>{c}</td>" for c in [
    "2330", "台積電", "董事", "某乙", "115/10/15", "1,000", "0", "5,000", "銀行", "",
    "115/10/16"]) + "</tr>")


@pytest.fixture
def no_retry_wait(monkeypatch):
    monkeypatch.setattr(ipe, "_RETRY_WAIT", 0)


@pytest.mark.parametrize("answers, state, n, left", [
    ([_Resp(fixture_bytes("mops_pledge_empty.html"))], EMPTY, 0, 0),
    ([_Resp(f"<table>{PLEDGE_ROW}</table>".encode())], OK, 1, 0),
    ([_Resp("<html>系統忙碌</html>".encode())], UNREADABLE, 0, 0),
    # One retry for a server error or a dropped connection ...
    ([_Resp(b"", 502), _Resp(f"<table>{PLEDGE_ROW}</table>".encode())], OK, 1, 0),
    ([requests.ConnectionError("reset"), _Resp(b"", 502)], NO_RESPONSE, 0, 0),
    # ... none for a client error, which will not change.
    ([_Resp(b"", 404), _Resp(f"<table>{PLEDGE_ROW}</table>".encode())], NO_RESPONSE, 0, 1),
])
def test_pledge_stock_state(monkeypatch, no_retry_wait, answers, state, n, left):
    remaining = _mops_session(monkeypatch, ipe, answers)
    rows, got = ipe.fetch_events_state("2330", "1150901", "1151016")
    assert got == state and len(rows) == n
    assert len(remaining) == left


def test_pledge_without_zeu_codes_fails_and_can_try_again(monkeypatch):
    monkeypatch.setattr(ipe, "_run_completed", False)
    monkeypatch.setattr(ipe, "_run_failed", ())
    monkeypatch.setattr(ipe, "fetch_zeu_codes", lambda: [])
    result = ipe.scrape_date(PAST)
    assert result.failed_sources == ("MoneyDJ 質設異動清單（沒有回應或沒有代號）",)
    assert ipe._run_completed is False


def test_pledge_names_every_stock_that_failed(monkeypatch, no_retry_wait):
    monkeypatch.setattr(ipe, "_run_completed", False)
    monkeypatch.setattr(ipe, "_run_failed", ())
    monkeypatch.setattr(ipe, "fetch_zeu_codes", lambda: ["2330", "5288", "8097"])
    monkeypatch.setattr(ipe, "save_events", lambda events: len(events))
    _mops_session(monkeypatch, ipe, [
        _Resp(f"<table>{PLEDGE_ROW}</table>".encode()),
        _Resp(b"", 502), _Resp(b"", 502),                 # 5288: retried, still down
        _Resp(fixture_bytes("mops_pledge_empty.html")),   # 8097: nothing in range
    ])
    result = ipe.scrape_date(PAST)
    assert result.failed_sources == ("MOPS 5288（沒有回應）",)
    assert result.records == 1 and ipe._run_completed is True
    # The run asks about today whatever the date, so the guarded calls that
    # follow in a range backfill carry its failures to the dates that are judged.
    again = ipe.scrape_date(date(2026, 10, 15))
    assert again.records == 0 and again.failed_sources == ("MOPS 5288（沒有回應）",)


# ── MOPS: investor conferences ─────────────────────────────────────────────

CONF_ROW = ("<tr>" + "".join(f"<td>{c}</td>" for c in [
    "2330", "台積電", "115/10/16", "14:00", "自辦", "法說會", "233020251016M001.pdf"]) + "</tr>")


@pytest.mark.parametrize("html, state, n", [
    (fixture_bytes("mops_conferences_empty.html").decode("utf-8"), EMPTY, 0),
    (f"<table>{CONF_ROW}</table>", OK, 1),
    ("<html><body>系統忙碌</body></html>", UNREADABLE, 0),
    (None, NO_RESPONSE, 0),
])
def test_conference_month_state(monkeypatch, html, state, n):
    monkeypatch.setattr(ic, "_post", lambda url, data, binary=False: html)
    rows, got = ic.fetch_month_state("sii", 115, 10)
    assert got == state and len(rows) == n


class _Cursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, sql, params=None):
        assert sql.lstrip().upper().startswith("SELECT"), sql
        if "local_path IS NULL" in sql:
            # The deck window and the last-try rule share one length.
            assert params[2] == timedelta(days=ic._DECK_WINDOW_DAYS) == timedelta(days=30)

    def fetchall(self):
        return self.rows


def _cursor_with(rows):
    @contextmanager
    def get_cursor(commit=True):
        yield _Cursor(rows)
    return get_cursor


T = date(2026, 10, 16)        # a Friday
T_NEXT = date(2026, 10, 19)   # the next trading day


def _conference_night(monkeypatch, due, archived=()):
    monkeypatch.setattr(ic, "fetch_month_state", lambda m, y, mo: ([], EMPTY))
    monkeypatch.setattr(ic, "save", lambda rows: 0)
    monkeypatch.setattr(ic, "_next_trading_day", lambda d: T_NEXT)
    monkeypatch.setattr(ic, "get_cursor", _cursor_with(
        [{"stock_id": s, "conf_date": d, "file_zh": f"{s}.pdf"} for s, d in due]))
    monkeypatch.setattr(ic, "download", lambda name, out: f"x/{name}" if name[:4] in archived else None)
    monkeypatch.setattr(ic, "_mark_downloaded", lambda *a: None)
    return ic.scrape_date(T)


def test_deck_is_reported_only_on_its_last_try(monkeypatch):
    result = _conference_night(monkeypatch, due=[
        ("1101", T - timedelta(days=30)),        # oldest in the window: last try
        ("1102", T_NEXT - timedelta(days=31)),   # gone by the next night: last try
        ("1103", T_NEXT - timedelta(days=30)),   # still in the window next night
        ("1104", T),                              # just held
    ])
    assert result.failed_sources == (
        f"法說會簡報 1101 {T - timedelta(days=30)}（最後一次下載仍失敗）",
        f"法說會簡報 1102 {T_NEXT - timedelta(days=31)}（最後一次下載仍失敗）",
    )


def test_deck_archived_on_its_last_try_is_not_reported(monkeypatch):
    result = _conference_night(monkeypatch, due=[("1101", T - timedelta(days=30))],
                               archived=("1101",))
    assert result.failed_sources == ()


def test_conference_index_query_without_an_answer_is_reported(monkeypatch):
    states = iter([OK, NO_RESPONSE, EMPTY, OK])
    monkeypatch.setattr(ic, "fetch_month_state", lambda m, y, mo: ([], next(states)))
    monkeypatch.setattr(ic, "save", lambda rows: 0)
    monkeypatch.setattr(ic, "_next_trading_day", lambda d: T_NEXT)
    monkeypatch.setattr(ic, "get_cursor", _cursor_with([]))
    assert ic.scrape_date(T).failed_sources == ("法說會索引 上市 115/11（沒有回應）",)


@pytest.mark.parametrize("day, closed, expected", [
    (date(2026, 10, 16), [], date(2026, 10, 19)),                     # Friday -> Monday
    (date(2026, 10, 16), [date(2026, 10, 19)], date(2026, 10, 20)),   # Monday closed
    (date(2026, 10, 20), [], date(2026, 10, 21)),
])
def test_next_trading_day(monkeypatch, day, closed, expected):
    monkeypatch.setattr(ic, "get_cursor", _cursor_with([{"holiday_date": d} for d in closed]))
    assert ic._next_trading_day(day) == expected
