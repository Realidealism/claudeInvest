"""Tests for what the monthly revenue scraper reports about each market.

An unanswered request, a month MOPS has not published, a page we cannot read
and a real table all used to come back as an empty list, merged across the two
markets into one count that the caller then dropped.

No network and no database: fetch() and the save are replaced.
"""
import types

import pytest
import requests

import scrapers.revenue as rv
from daily_update_guards import no_side_effects  # noqa: F401

# The placeholder MOPS serves for a month nobody has filed (115/10, fetched
# 2026-10-15): no table, just this text.
UNPUBLISHED = ("<html><head><link rel='stylesheet' href='/ppp.css'></head><body>"
               "<center>上市公司115年10月份(累計與當月)營業收入統計表</center>"
               "<center>本資料由各公司提供 (檔案內容包含國內及國外公司)</center>"
               "<center>查無資料</center></body></html>")

ROW = ("<tr><td>{code}</td><td>名稱</td><td>1,000</td><td>900</td><td>800</td>"
       "<td>11.11</td><td>25.00</td><td>9,000</td><td>8,000</td><td>12.50</td><td>-</td></tr>")
TABLE = "<html><table><tr><th>公司代號</th></tr>" + "".join(
    ROW.format(code=c) for c in ("1101", "2330", "2454")) + "</table></html>"
HEADER_ONLY = "<html><table><tr><th>公司代號</th><th>公司名稱</th></tr></table></html>"
MAINTENANCE = "<html><body>系統維護中，請稍後再試</body></html>"


def page(text):
    return types.SimpleNamespace(content=text.encode("big5"))


def serve(monkeypatch, by_market):
    """by_market: {'sii': page or None or Exception, 'otc': ...}"""
    def fetch(url, timeout=30):
        market = "sii" if "/sii/" in url else "otc"
        answer = by_market[market]
        if isinstance(answer, Exception):
            raise answer
        return answer
    monkeypatch.setattr(rv, "fetch", fetch)


@pytest.mark.parametrize("answer, state, n", [
    (page(TABLE), rv.OK, 3),
    (page(UNPUBLISHED), rv.UNPUBLISHED, 0),
    (None, rv.NO_RESPONSE, 0),
    (requests.exceptions.ChunkedEncodingError("cut"), rv.NO_RESPONSE, 0),
    (page(MAINTENANCE), rv.UNREADABLE, 0),
    (page(HEADER_ONLY), rv.UNREADABLE, 0),
])
def test_one_market_state(monkeypatch, answer, state, n):
    serve(monkeypatch, {"sii": answer, "otc": answer})
    records, got = rv._fetch_revenue(115, 9, "sii")
    assert got == state and len(records) == n


def test_each_market_is_reported_on_its_own(monkeypatch):
    serve(monkeypatch, {"sii": page(TABLE), "otc": None})
    records, by_market = rv.fetch_monthly_revenue(2026, 9)
    assert by_market == {"sii": (rv.OK, 3), "otc": (rv.NO_RESPONSE, 0)}
    assert [r["stock_id"] for r in records] == ["1101", "2330", "2454"]


def test_a_request_error_on_one_market_keeps_the_other(monkeypatch):
    serve(monkeypatch, {"sii": page(TABLE), "otc": requests.exceptions.ChunkedEncodingError("cut")})
    saved = []
    monkeypatch.setattr(rv, "save_monthly_revenue", lambda records, y, m: saved.append(len(records)))
    assert rv.scrape_month(2026, 9) == {"sii": (rv.OK, 3), "otc": (rv.NO_RESPONSE, 0)}
    assert saved == [3]


def test_scrape_range_adds_up_both_markets(monkeypatch):
    serve(monkeypatch, {"sii": page(TABLE), "otc": page(TABLE)})
    monkeypatch.setattr(rv, "save_monthly_revenue", lambda records, y, m: None)
    assert rv.scrape_range(2026, 8, 2026, 9) == 12
