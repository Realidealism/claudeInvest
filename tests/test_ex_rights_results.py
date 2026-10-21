"""The exchanges' ex-rights results into tw.dividends (scrapers.ex_rights_results,
ruling 2026-10-21). Fixtures follow the answers of 2026-06-01."""
from datetime import date
from pathlib import Path

import pytest

from scrapers import ex_rights_results as x
from utils.format_shift import EMPTY, NO_RESPONSE, OK, UNREADABLE

TWSE_FIELDS = ["資料日期", "股票代號", "股票名稱", "除權息前收盤價", "除權息參考價", "權值+息值", "權/息",
               "漲停價格", "跌停價格", "開盤競價基準", "減除股利參考價", "詳細資料"]
DETAIL_FIELDS = ["股票代號", "股票名稱", "(每股配發現金股利)除息", "(增資配股) 除權",
                 "A. 按普通股股東持股比例每千股無償配股", "B. 員工紅利轉增資", "C. (有償) 現金增資"]
TPEX_FIELDS = ["除權息日期", "代號", "名稱", "除權息前收盤價", "除權息參考價", "權值", "息值", "權值+息值",
               "權/息", "漲停價", "跌停價", "開始交易基準價", "減除股利參考價", "現金股利", "每仟股無償配股",
               "現金增資股數", "現金增資認購價"]
D = date(2026, 6, 1)


def _twse(*rows):
    return {"stat": "OK", "fields": TWSE_FIELDS,
            "data": [["115年06月01日", sid, "x", prev, ref, val, kind, "", "", "", "", ""]
                     for sid, prev, ref, val, kind in rows]}


def _detail(cash, free):
    return {"stat": "ok", "fields": DETAIL_FIELDS,
            "data": [["1720  ", "生達 ", f"{cash} 元／股", "", f"{free} 股", "0 股", "0 股"]]}


def _tpex(*rows):
    return {"date": "20260601~20260601", "tables": [{"title": "", "fields": TPEX_FIELDS, "data": [
        ["115/06/01", sid, "x", prev, ref, "0", "0", "0", kind, "", "", "", "", cash, free, "0", "0"]
        for sid, prev, ref, kind, cash, free in rows]}]}


def test_roc_dates():
    assert x.roc_date("115年06月01日") == D == x.roc_date("115/06/01")
    assert x.roc_date("") is None


def test_twse_cash_event_takes_its_value_and_a_rights_event_waits_for_the_detail():
    rows, state = x.parse_twse(_twse(("2612", "57.70", "55.50", "2.200000", "息"),
                                     ("1720", "63.50", "55.90", "7.600000", "權息")))
    assert state == OK
    assert (rows[0]["cash"], rows[0]["per_thousand"]) == (2.2, 0.0)
    assert (rows[1]["cash"], rows[1]["per_thousand"]) == (None, None)


def test_twse_empty_and_unreadable():
    assert x.parse_twse({"stat": "很抱歉，沒有符合條件的資料!"}) == ([], EMPTY)
    assert x.parse_twse({"stat": "OK", "fields": TWSE_FIELDS, "data": []}) == ([], EMPTY)
    assert x.parse_twse({"stat": "OK", "fields": ["x"], "data": [["1"]]})[1] == UNREADABLE
    assert x.parse_twse("<html>")[1] == UNREADABLE


def test_detail_reads_cash_and_free_shares():
    assert x.parse_twse_detail(_detail("2", "100")) == (2.0, 100.0)
    assert x.parse_twse_detail({"stat": "ok", "data": []}) is None


def test_tpex_rows_and_kind():
    rows, state = x.parse_tpex(_tpex(("4527", "45.95", "42.95", "除息", "3.00000000", "0.00000000"),
                                     ("3709", "88.80", "86.35", "除權", "0.00000000", "0.00000000")))
    assert state == OK
    assert [(r["stock_id"], r["kind"], r["cash"], r["per_thousand"]) for r in rows] == [
        ("4527", "息", 3.0, 0.0), ("3709", "權", 0.0, 0.0)]
    quiet = {"tables": [{"title": "", "totalCount": 0, "fields": TPEX_FIELDS, "data": []}], "stat": "ok"}
    assert x.parse_tpex(quiet) == ([], EMPTY)          # 2026-04-30
    assert x.parse_tpex({"tables": []})[1] == UNREADABLE
    assert x.parse_tpex({"tables": [{"data": []}]})[1] == UNREADABLE
    assert x.parse_tpex({"x": 1})[1] == UNREADABLE


def test_a_date_that_does_not_parse_makes_the_board_unreadable():
    bad = _twse(("2612", "57.70", "55.50", "2.200000", "息"))
    bad["data"][0][0] = "2026-06-01"
    assert x.parse_twse(bad) == ([], UNREADABLE)


def test_rows_keep_the_table_unit_and_skip_a_bare_capital_increase():
    events = [{"stock_id": "1720", "ex_date": D, "cash": 2.0, "per_thousand": 100.0, "source": "twse"},
              {"stock_id": "3709", "ex_date": D, "cash": 0.0, "per_thousand": 0.0, "source": "tpex"}]
    assert x.to_rows(events) == [{"stock_id": "1720", "ex_date": D, "cash": 2.0, "stock": 1.0, "source": "twse"}]


def test_self_check_flags_amounts_that_miss_the_reference_price():
    good = {"stock_id": "1720", "ex_date": D, "prev": 63.5, "ref": 55.9, "cash": 2.0, "per_thousand": 100.0}
    assert x.self_check([good]) == []
    assert x.self_check([dict(good, per_thousand=10.0)]) == [f"1720 {D}"]


def _collect(monkeypatch, answers):
    calls = []

    def fetch(url, params=None):
        calls.append(url)
        data = answers.get(url.rsplit("/", 1)[-1])
        if data is None:
            return None
        return type("R", (), {"json": lambda self: data})()
    monkeypatch.setattr(x, "fetch", fetch)
    events, legs = x.collect(D, D)
    return events, legs, calls


def test_collect_splits_a_rights_event_with_the_detail(monkeypatch):
    events, legs, calls = _collect(monkeypatch, {
        "TWT49U": _twse(("1720", "63.50", "55.90", "7.600000", "權息")),
        "TWT49UDetail": _detail("2", "100"),
        "exDailyQ": _tpex(("4527", "45.95", "42.95", "除息", "3.0", "0"))})
    assert legs == {"上市除權息結果": OK, "上櫃除權息結果": OK}
    assert [(e["stock_id"], e["cash"], e["per_thousand"]) for e in events] == [("1720", 2.0, 100.0),
                                                                              ("4527", 3.0, 0.0)]
    assert sum(c.endswith("TWT49UDetail") for c in calls) == 1


def test_collect_reports_a_failed_detail_and_a_silent_board(monkeypatch):
    events, legs, _ = _collect(monkeypatch, {
        "TWT49U": _twse(("1720", "63.50", "55.90", "7.600000", "權息"), ("2612", "57.70", "55.50", "2.2", "息")),
        "exDailyQ": None})
    assert [e["stock_id"] for e in events] == ["2612"]
    assert legs == {"上市除權息結果": NO_RESPONSE, "上櫃除權息結果": NO_RESPONSE}


def test_write_upserts_by_source_and_drops_the_finmind_row():
    assert "ON CONFLICT (stock_id, ex_date, source) DO UPDATE" in x._UPSERT
    assert "source = 'finmind' AND stock_id = %(stock_id)s AND ex_date = %(ex_date)s" in x._DROP_FINMIND


def test_scrape_date_reports_legs_and_self_check(monkeypatch):
    seen = {}

    def collect(a, b, done=None):
        seen.update(start=a, end=b, done=done)
        return ([{"stock_id": "1720", "ex_date": D, "prev": 63.5, "ref": 55.9,
                  "cash": 2.0, "per_thousand": 10.0, "source": "twse"}],
                {"上市除權息結果": OK, "上櫃除權息結果": NO_RESPONSE})
    monkeypatch.setattr(x, "collect", collect)
    monkeypatch.setattr(x, "_written_since", lambda start: {("1101", D)})
    written = []
    monkeypatch.setattr(x, "write", lambda rows: written.extend(rows) or [])
    res = x.scrape_date(D)
    assert res.records == 1 and written[0]["stock"] == 0.1
    assert res.failed_sources == ("上櫃除權息結果（沒有回應）", f"除權息金額與參考價不符 1720 {D}")
    # the run looks back, so a skipped day (a typhoon closure) is read later
    assert (seen["start"], seen["end"]) == (date(2026, 5, 18), D) and seen["done"] == {("1101", D)}


def test_hermit_backfill_only_replaces_finmind_rows():
    src = (Path(__file__).resolve().parents[1] / "hermit_stock/src/hermit_stock/scrapers/dividends.py").read_text(
        encoding="utf-8")
    assert "DELETE FROM tw.dividends WHERE stock_id = %s AND source = 'finmind'" in src
    assert "AND source <> 'finmind')" in src
    assert "rows = [r for r in rows if r.ex_date < OFFICIAL_FROM]" in src
    assert "OFFICIAL_FROM = date(2026, 4, 30)" in src


@pytest.mark.parametrize("label", ["Ex-rights results (除權息結果)"])
def test_registered_and_packaged(label):
    import daily_update as du
    entry = next(e for e in du.SCRAPERS if e[0] == label)
    assert entry[1] == "scrapers.ex_rights_results" and entry[3] is False
    spec = (Path(__file__).resolve().parents[1] / "daily_update.spec").read_text(encoding="utf-8")
    assert "'scrapers.ex_rights_results'" in spec


def test_write_leaves_out_codes_tw_stocks_does_not_list(monkeypatch):
    class Cur:
        def __init__(self):
            self.sql = []

        def execute(self, sql, params=None):
            self.sql.append(sql)

        def fetchall(self):
            return [{"stock_id": "2330"}]

    cur, batches = Cur(), []

    class Ctx:
        def __enter__(self):
            return cur

        def __exit__(self, *a):
            return False

    import psycopg2.extras
    monkeypatch.setattr(x, "get_cursor", lambda: Ctx())
    monkeypatch.setattr(psycopg2.extras, "execute_batch", lambda c, sql, rows, page_size: batches.append(rows))
    rows = [{"stock_id": "2330", "ex_date": D, "cash": 6.0, "stock": 0.0, "source": "twse"},
            {"stock_id": "00984D", "ex_date": D, "cash": 0.1, "stock": 0.0, "source": "twse"}]
    assert x.write(rows) == ["00984D"]
    assert [[r["stock_id"] for r in b] for b in batches] == [["2330"], ["2330"]]


def test_collect_skips_the_detail_of_a_rights_event_already_written(monkeypatch):
    events, legs, calls = _collect_done(monkeypatch, {("1720", D)})
    assert [e["stock_id"] for e in events] == ["2612"]
    assert not any(c.endswith("TWT49UDetail") for c in calls)
    assert legs["上市除權息結果"] == OK


def _collect_done(monkeypatch, done):
    calls = []
    answers = {"TWT49U": _twse(("1720", "63.50", "55.90", "7.600000", "權息"),
                               ("2612", "57.70", "55.50", "2.2", "息")),
               "exDailyQ": {"tables": [{"fields": TPEX_FIELDS, "data": []}]}}

    def fetch(url, params=None):
        calls.append(url)
        data = answers.get(url.rsplit("/", 1)[-1])
        return None if data is None else type("R", (), {"json": lambda self: data})()
    monkeypatch.setattr(x, "fetch", fetch)
    events, legs = x.collect(D, D, done=done)
    return events, legs, calls


def test_self_check_reads_the_reference_price_without_a_capital_increase():
    # 7610 2026-07-13: 減除股利參考價 2207.73 fits the amounts; the 除權息參考價
    # 2181.01 also holds a cash capital increase and would not
    rows, _ = x.parse_twse({"stat": "OK", "fields": TWSE_FIELDS, "data": [
        ["115年07月13日", "7610", "x", "2650.00", "2181.01", "468.99", "權息", "", "", "", "2207.73", ""]]})
    assert rows[0]["ref"] == 2207.73
    rows[0]["cash"], rows[0]["per_thousand"] = 0.5, 200.1
    assert x.self_check(rows) == []
