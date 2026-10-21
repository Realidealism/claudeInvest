"""Stock futures in tw.taifex_futures_daily and the contract list
(scrapers.taifex_futures, scrapers.stock_futures_contracts; ruling 2026-10-21)."""
from datetime import date
from types import SimpleNamespace

import pytest

from scrapers import stock_futures_contracts as sfc
from scrapers import taifex_futures as tf

D = date(2026, 10, 21)

_HEADERS = ["股票期貨、 選擇權 商品代碼", "標的證券", "證券代號", "標的證券 簡稱", "是否為 股票期貨 標的",
            "是否為 股票選擇權 標的", "是否為 股票選擇權週契約 標的", "上市普通股 標的證券", "上櫃普通股 標的證券",
            "上市ETF 標的證券", "上櫃ETF 標的證券", "標準型證 券股數/ 受益權單位", "一般交易時段", "盤後交易時段"]


def _row(code, sid, fut=True, lot="2000"):
    cells = [code, "x公司", sid, "x", "●  是股票期貨標的" if fut else "", "", "", "", "", "", "", lot,
             "8:45~13:45", "-"]
    return "<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>"


def _page(*rows):
    head = "<tr>" + "".join(f"<th>{h}</th>" for h in _HEADERS) + "</tr>"
    return (f"<html><body><table><tr><td>nav</td></tr></table>"
            f"<table>{head}{''.join(rows)}</table></body></html>")


# ── futures feed: which contracts are kept ─────────────────────────────────

@pytest.mark.parametrize("code,kept", [
    ("TX", True), ("MTX", True), ("TMF", True), ("CDF", True), ("NAF", True), ("GTF", True),
    ("TE", False), ("TXO", False), ("CD", False), ("CDFF", False), ("cdf", False),
])
def test_kept_contracts(code, kept):
    assert tf._keep(code) is kept


def _csv(*rows):
    head = ",".join(f"h{i}" for i in range(19))
    return "\n".join([head, *rows])


def _fut(contract, month, session="一般", volume="10"):
    cells = ["2026/10/21", contract, month, "1", "2", "0.5", "1.5", "0", "0", volume, "1.5", "3",
             "1", "2", "9", "0.1", "", session, "0"]
    return ",".join(cells)


def test_parse_keeps_stock_futures_and_their_spread_rows_and_drops_others():
    rows, api, err = tf._parse(_csv(_fut("TX", "202611"), _fut("CDF", "202611"),
                                    _fut("CDF", "202611/202612"), _fut("CDF", "202611", "盤後"),
                                    _fut("TE", "202611")))
    got = sorted((r["contract"], r["contract_month"], r["session"]) for r in rows)
    assert got == [("CDF", "202611", "一般"), ("CDF", "202611", "盤後"),
                   ("CDF", "202611/202612", "一般"), ("TX", "202611", "一般")]
    assert (api, err) == (5, 0)


# ── contract list ─────────────────────────────────────────────────────────

def test_parse_list_reads_codes_lots_and_the_code_na():
    rows, api, err = sfc.parse(_page(_row("CD", "2330"), _row("NA", "3105"), _row("QF", "2330", lot="100"),
                                     _row("ZZ", "1101", fut=False), _row("ZY", "1102", lot="")))
    assert rows == [
        {"contract": "CDF", "stock_id": "2330", "lot": 2000, "standard": True},
        {"contract": "NAF", "stock_id": "3105", "lot": 2000, "standard": True},
        {"contract": "QFF", "stock_id": "2330", "lot": 100, "standard": False},
    ]
    assert (api, err) == (5, 1)


def test_parse_list_reads_the_saved_page_shape():
    # the live page writes lots as "2,000"
    rows, _, err = sfc.parse(_page(_row("CA", "1303", lot="2,000"), _row("EH", "0050", lot="10,000")))
    assert [(r["contract"], r["lot"], r["standard"]) for r in rows] == [("CAF", 2000, True), ("EHF", 10000, True)]
    assert err == 0


def test_parse_list_without_the_table_is_none():
    assert sfc.parse("<html><body><table><tr><td>維護中</td></tr></table></body></html>") is None
    assert sfc.parse("<html>no table</html>") is None


class _Cur:
    """Answers the scraper's queries by their text: the stocks the table
    holds, the unknown traded codes, and how many standard codes the feed has."""

    def __init__(self, unknown, held, in_feed):
        self.unknown, self.held, self.in_feed = unknown, held, in_feed
        self.sql, self.inserted = "", []

    def execute(self, sql, params=None):
        self.sql = sql
        if "'unknown'" in sql:
            self.inserted.append(params[0])

    def fetchall(self):
        if "stock_id IS NOT NULL" in self.sql:
            return [{"contract": c, "stock_id": s} for c, s in self.held.items()]
        return [{"contract": c} for c in self.unknown]

    def fetchone(self):
        return {"n": self.in_feed}


def _scrape(monkeypatch, page, unknown=(), held=None, in_feed=None):
    standard = sfc.parse(page)[0] if page and sfc.parse(page) else []
    n = sum(r["standard"] for r in standard)
    cur = _Cur(list(unknown), held or {}, n if in_feed is None else in_feed)
    written = []

    class Ctx:
        def __enter__(self):
            return cur

        def __exit__(self, *a):
            return False

    import psycopg2.extras
    monkeypatch.setattr(sfc, "get_cursor", lambda: Ctx())
    monkeypatch.setattr(psycopg2.extras, "execute_batch", lambda c, sql, rows, page_size: written.extend(rows))
    monkeypatch.setattr(sfc, "fetch", lambda url: None if page is None else SimpleNamespace(
        content=page.encode("utf-8")))
    return sfc.scrape_date(D), written, cur


def test_scrape_upserts_the_list_with_the_run_date(monkeypatch):
    res, written, _ = _scrape(monkeypatch, _page(_row("CD", "2330"), _row("QF", "2330", lot="100")))
    assert res.records == 2 and res.failed_sources == ()
    assert [(w["contract"], w["d"]) for w in written] == [("CDF", D), ("QFF", D)]


def test_upsert_never_moves_last_listed_back_nor_a_code_to_another_stock():
    assert "GREATEST(t.last_listed, EXCLUDED.last_listed)" in sfc._UPSERT
    assert "WHERE t.stock_id IS NULL OR t.stock_id = EXCLUDED.stock_id" in sfc._UPSERT


def test_scrape_reports_a_code_that_moved_to_another_stock(monkeypatch):
    res, _, _ = _scrape(monkeypatch, _page(_row("CD", "2330")), held={"CDF": "2303"})
    assert res.failed_sources == ("期貨代碼 CDF 改對應 2303→2330（未更新）",)


def test_scrape_records_an_unknown_traded_code_once(monkeypatch):
    res, _, cur = _scrape(monkeypatch, _page(_row("CD", "2330")), unknown=["XYF"])
    assert res.failed_sources == ("未知期貨代碼 XYF（已登記 unknown）",)
    assert cur.inserted == ["XYF"]
    assert "ON CONFLICT (contract) DO NOTHING" in sfc._UNKNOWN and "'unknown'" in sfc._UNKNOWN


def test_scrape_reports_a_feed_missing_the_standard_contracts(monkeypatch):
    page = _page(*[_row(f"C{chr(65 + i)}", f"{1100 + i}") for i in range(10)])
    res, _, _ = _scrape(monkeypatch, page, in_feed=9)
    assert res.failed_sources == ()
    res, _, _ = _scrape(monkeypatch, page, in_feed=8)
    assert res.failed_sources == ("個股期行情只有 8/10 檔標準型契約",)


@pytest.mark.parametrize("page,why", [(None, "沒有回應"), ("<html>maintenance</html>", "回應讀不懂")])
def test_scrape_without_a_list_writes_nothing_and_fails(monkeypatch, page, why):
    res, written, _ = _scrape(monkeypatch, page)
    assert res.records == 0 and written == [] and why in res.failed_sources[0]


def test_feed_filter_and_the_unknown_code_query_agree():
    # the scraper's SQL regex and taifex_futures' pattern must pick the same codes
    import inspect
    import re
    assert "'^[A-Z]{2}F$'" in inspect.getsource(sfc.scrape_date)
    sql = re.compile(r"^[A-Z]{2}F$")
    for code in ("CDF", "NAF", "TMF", "TX", "MTX", "CDFF", "C1F", "cdf"):
        assert (sql.match(code) is not None) == (tf._THREE_LETTER_F.fullmatch(code) is not None), code


def test_availability_sums_every_standard_code_of_a_stock():
    import inspect
    from signal_backtest import short_constraints as sc
    src = inspect.getsource(sc.load_futures_availability)
    assert "SUM(COALESCE(f.volume, 0))" in src and "GROUP BY c.stock_id, f.trade_date" in src
