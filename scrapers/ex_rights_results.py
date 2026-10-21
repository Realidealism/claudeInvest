"""
Ex-rights / ex-dividend results (除權除息計算結果表) -> tw.dividends.

Sources (the exchanges' own results, not the announcements in
tw.dividend_calendar):
  TWSE  /rwd/zh/exRight/TWT49U?startDate&endDate   one row per event: close
        before, reference price, 權值+息值, 權/息. A 息 event's 權值+息值 is
        its cash dividend; a 權 / 權息 event is split by
        /rwd/zh/exRight/TWT49UDetail?STK_NO&T1 (cash per share, free shares
        per 1000).
  TPEx  /www/zh-tw/bulletin/exDailyQ?startDate&endDate   cash dividend and free
        shares per 1000 on the row.

Rows with a cash dividend or free shares are upserted as source 'twse' /
'tpex' (ETFs included); stock_dividend keeps the table's unit, NTD of par per
share = free shares per 1000 / 100. An event that is only a cash capital
increase (現增) has neither and is not written. The 'finmind' row of the same
stock and ex-date (hermit backfill of 2026-04-29) is then deleted, so the
yield sums do not count an event twice. (User rulings 2026-10-21.)

A row whose (close before - cash) / (1 + stock / 10) is more than
SELF_CHECK_TOL off the exchange's 減除股利參考價 (the reference price
without a cash capital increase of the same event) is reported: a misread
amount. Each night reads the last LOOKBACK_DAYS days, so a skipped day or a
failed request is picked up later.
"""

from __future__ import annotations

import re
from datetime import date, timedelta

from db.connection import get_cursor
from utils.format_shift import EMPTY, NO_RESPONSE, OK, UNREADABLE, ScrapeResult, failed_legs
from utils.http_client import fetch

TWSE_URL = "https://www.twse.com.tw/rwd/zh/exRight/TWT49U"
TWSE_DETAIL_URL = "https://www.twse.com.tw/rwd/zh/exRight/TWT49UDetail"
TPEX_URL = "https://www.tpex.org.tw/www/zh-tw/bulletin/exDailyQ"
SELF_CHECK_TOL = 0.005
LOOKBACK_DAYS = 14

_UPSERT = """
    INSERT INTO tw.dividends (stock_id, ex_date, cash_dividend, stock_dividend, source)
    VALUES (%(stock_id)s, %(ex_date)s, %(cash)s, %(stock)s, %(source)s)
    ON CONFLICT (stock_id, ex_date, source) DO UPDATE SET
        cash_dividend = EXCLUDED.cash_dividend, stock_dividend = EXCLUDED.stock_dividend
"""
_DROP_FINMIND = """
    DELETE FROM tw.dividends WHERE source = 'finmind' AND stock_id = %(stock_id)s AND ex_date = %(ex_date)s
"""


def roc_date(text: str) -> date | None:
    """'115年06月01日' or '115/06/01' -> date."""
    m = re.fullmatch(r"\s*(\d{2,3})\D(\d{1,2})\D(\d{1,2})\D?\s*", text or "")
    return date(int(m[1]) + 1911, int(m[2]), int(m[3])) if m else None


def _num(text) -> float | None:
    m = re.search(r"-?\d[\d,]*(?:\.\d+)?", str(text or ""))
    return float(m[0].replace(",", "")) if m else None


def _column(fields: list[str], name: str) -> int:
    for i, f in enumerate(fields):
        if name in f.replace(" ", ""):
            return i
    raise KeyError(name)


def parse_twse(data) -> tuple[list[dict], str]:
    """TWT49U rows: (events, state). Amounts of 權 / 權息 events need the detail."""
    if not isinstance(data, dict) or data.get("stat") not in ("OK", "ok"):
        if isinstance(data, dict) and "沒有符合條件" in str(data.get("stat")):
            return [], EMPTY
        return [], UNREADABLE
    rows = data.get("data") or []
    if not rows:
        return [], EMPTY
    f = data.get("fields") or []
    try:
        idx = {k: _column(f, k) for k in ("資料日期", "股票代號", "除權息前收盤價", "減除股利參考價",
                                          "權值+息值", "權/息")}
    except KeyError:
        return [], UNREADABLE
    out = []
    for r in rows:
        kind = str(r[idx["權/息"]]).strip()
        out.append({"ex_date": roc_date(r[idx["資料日期"]]), "stock_id": str(r[idx["股票代號"]]).strip(),
                    "prev": _num(r[idx["除權息前收盤價"]]), "ref": _num(r[idx["減除股利參考價"]]),
                    "kind": kind, "value": _num(r[idx["權值+息值"]]),
                    "cash": _num(r[idx["權值+息值"]]) if kind == "息" else None,
                    "per_thousand": 0.0 if kind == "息" else None, "source": "twse"})
    if not all(e["ex_date"] for e in out):
        return [], UNREADABLE
    return out, OK


def parse_twse_detail(data) -> tuple[float, float] | None:
    """TWT49UDetail -> (cash per share, free shares per 1000)."""
    if not isinstance(data, dict) or not data.get("data"):
        return None
    f, r = data.get("fields") or [], data["data"][0]
    try:
        cash = _num(r[_column(f, "每股配發現金股利")])
        free = _num(r[_column(f, "每千股無償配股")])
    except KeyError:
        return None
    return (cash or 0.0, free or 0.0) if cash is not None or free is not None else None


def parse_tpex(data) -> tuple[list[dict], str]:
    """exDailyQ rows: (events, state)."""
    if not isinstance(data, dict) or not isinstance(data.get("tables"), list):
        return [], UNREADABLE
    # a quiet day is a table with its fields and no rows (2026-04-30)
    tables = data["tables"]
    if not tables or not all(t.get("fields") for t in tables):
        return [], UNREADABLE
    if not any(t.get("data") for t in tables):
        return [], EMPTY
    out = []
    for t in tables:
        f = t["fields"]
        try:
            idx = {k: _column(f, k) for k in ("除權息日期", "代號", "除權息前收盤價", "減除股利參考價",
                                              "權/息", "現金股利", "每仟股無償配股")}
        except KeyError:
            return [], UNREADABLE
        for r in t.get("data") or []:
            out.append({"ex_date": roc_date(r[idx["除權息日期"]]), "stock_id": str(r[idx["代號"]]).strip(),
                        "prev": _num(r[idx["除權息前收盤價"]]), "ref": _num(r[idx["減除股利參考價"]]),
                        "kind": str(r[idx["權/息"]]).strip().replace("除", ""),
                        "cash": _num(r[idx["現金股利"]]) or 0.0,
                        "per_thousand": _num(r[idx["每仟股無償配股"]]) or 0.0, "source": "tpex"})
    if not all(e["ex_date"] for e in out):
        return [], UNREADABLE
    return out, OK


def _get_json(url: str, params: dict):
    resp = fetch(url, params=params)
    if resp is None:
        return None, NO_RESPONSE
    try:
        return resp.json(), OK
    except ValueError:
        return None, UNREADABLE


def collect(start: date, end: date, raw_sink=None,
            done: set | None = None) -> tuple[list[dict], dict[str, str]]:
    """Events with ex-dates in [start, end] from both exchanges, amounts
    filled; and each leg's state (a 權 event whose detail fails makes the
    TWSE leg fail). A 權 event in done ((stock_id, ex_date) already written)
    is left out instead of asking for its detail again. raw_sink(name, data)
    receives every answer."""
    legs: dict[str, str] = {}
    events: list[dict] = []
    data, state = _get_json(TWSE_URL, {"startDate": start.strftime("%Y%m%d"), "endDate": end.strftime("%Y%m%d"),
                                       "response": "json"})
    if raw_sink:
        raw_sink(f"twse_{start:%Y%m%d}_{end:%Y%m%d}", data)
    if state == OK:
        rows, state = parse_twse(data)
        for e in rows:
            if e["cash"] is None and done and (e["stock_id"], e["ex_date"]) in done:
                continue
            if e["cash"] is None:
                d, s = _get_json(TWSE_DETAIL_URL, {"STK_NO": e["stock_id"], "T1": e["ex_date"].strftime("%Y%m%d"),
                                                   "response": "json"})
                if raw_sink:
                    raw_sink(f"twse_detail_{e['stock_id']}_{e['ex_date']:%Y%m%d}", d)
                split = parse_twse_detail(d) if s == OK else None
                if split is None:
                    state = UNREADABLE if s == OK else s
                    continue
                e["cash"], e["per_thousand"] = split
            events.append(e)
    legs["上市除權息結果"] = state
    data, state = _get_json(TPEX_URL, {"startDate": start.strftime("%Y/%m/%d"),
                                       "endDate": end.strftime("%Y/%m/%d"), "response": "json"})
    if raw_sink:
        raw_sink(f"tpex_{start:%Y%m%d}_{end:%Y%m%d}", data)
    if state == OK:
        rows, state = parse_tpex(data)
        events += rows
    legs["上櫃除權息結果"] = state
    return events, legs


def to_rows(events: list[dict]) -> list[dict]:
    """The rows tw.dividends gets: an event with a cash dividend or free shares."""
    out = []
    for e in events:
        cash, free = e["cash"] or 0.0, e["per_thousand"] or 0.0
        if cash > 0 or free > 0:
            out.append({"stock_id": e["stock_id"], "ex_date": e["ex_date"], "cash": cash,
                        "stock": free / 100, "source": e["source"]})
    return out


def self_check(events: list[dict]) -> list[str]:
    """Events whose amounts do not give the exchange's 減除股利參考價 (the
    reference price before any cash capital increase of the same event)."""
    bad = []
    for e in events:
        if not e["prev"] or not e["ref"]:
            continue
        cash, free = e["cash"] or 0.0, e["per_thousand"] or 0.0
        if cash <= 0 and free <= 0:
            continue
        expected = (e["prev"] - cash) / (1 + free / 1000)
        if abs(expected / e["ref"] - 1) > SELF_CHECK_TOL:
            bad.append(f"{e['stock_id']} {e['ex_date']}")
    return bad


def write(rows: list[dict]) -> list[str]:
    """Upsert the rows and drop the FinMind rows they replace. tw.dividends
    keys stock_id to tw.stocks, which does not list every new ETF, preferred
    share or TDR yet (128 rows of 70 codes, 2026-05..10): those rows are left
    out and their codes returned."""
    if not rows:
        return []
    from psycopg2.extras import execute_batch
    with get_cursor() as cur:
        cur.execute("SELECT stock_id FROM tw.stocks WHERE stock_id = ANY(%s)",
                    (sorted({r["stock_id"] for r in rows}),))
        known = {r["stock_id"] for r in cur.fetchall()}
        kept = [r for r in rows if r["stock_id"] in known]
        execute_batch(cur, _UPSERT, kept, page_size=500)
        execute_batch(cur, _DROP_FINMIND, kept, page_size=500)
    return sorted({r["stock_id"] for r in rows} - known)


def _written_since(start: date) -> set:
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT stock_id, ex_date FROM tw.dividends WHERE source IN ('twse', 'tpex') "
                    "AND ex_date >= %s", (start,))
        return {(r["stock_id"], r["ex_date"]) for r in cur.fetchall()}


def scrape_date(trade_date: date) -> ScrapeResult:
    """The results of the LOOKBACK_DAYS calendar days to trade_date: a day
    the nightly run never saw (the typhoon closure of 2026-07-10 had 19
    events) or a request that failed is picked up on a later night; the
    upsert makes the overlap harmless."""
    start = trade_date - timedelta(days=LOOKBACK_DAYS)
    events, legs = collect(start, trade_date, done=_written_since(start))
    rows = to_rows(events)
    unlisted = write(rows)
    written = sum(r["stock_id"] not in unlisted for r in rows)
    bad = self_check(events)
    print(f"  ex-rights results {start}..{trade_date}: events {len(events)}, written {written}, "
          f"not in tw.stocks {unlisted}, legs {legs}, self-check off {bad}")
    failed = failed_legs(legs) + tuple(f"除權息金額與參考價不符 {b}" for b in bad)
    return ScrapeResult(records=written, api_rows=len(events), parse_errors=0, failed_sources=failed)


if __name__ == "__main__":
    import sys
    scrape_date(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today())
