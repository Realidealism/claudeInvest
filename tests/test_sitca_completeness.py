"""Tests for the SITCA scrapers' report of funds they could not get.

A failed request, a page without the fund and a fund missing from tw.funds
were each printed and skipped, so a night where every company failed still
counted as done. The scrapers now return the registered funds still missing
for the period, each with the reason.

No network, no database, no sleeping: every collaborator is replaced.
"""
from contextlib import contextmanager

import pytest

import scrapers.sitca as sc
from daily_update_guards import no_side_effects  # noqa: F401

REGISTERED = [code for funds in sc.FUND_REGISTRY.values() for code, _ in funds]
NOMURA = [code for code, _ in sc.FUND_REGISTRY["A0032"]]
HOLDING = {"ticker": "2330", "ticker_name": "台積電", "is_domestic": True, "weight": 9.1}
FOREIGN = {"ticker": "NVDA", "ticker_name": "NVIDIA", "is_domestic": False, "weight": 8.0}


class Site:
    """Stands in for SITCA and the database for one scrape."""

    def __init__(self, monkeypatch, *, fail=(), parsed=None, in_db=None, on_record=()):
        self.saved = []
        parsed = parsed or {}
        in_db = set(REGISTERED) if in_db is None else set(in_db)

        def query(session, url, base_form, company_code, period):
            if company_code in fail:
                raise TimeoutError("read timed out")
            return company_code                                  # the "html"

        def parse(html, fund_list):
            # Default: every fund of the company parses with one holding.
            return parsed.get(html, {code: [HOLDING] for code, _ in fund_list})

        @contextmanager
        def get_cursor(commit=True):
            yield None

        monkeypatch.setattr(sc, "_setup_period", lambda session, url, period: {})
        monkeypatch.setattr(sc, "_query_company", query)
        monkeypatch.setattr(sc, "_parse_monthly_holdings", parse)
        monkeypatch.setattr(sc, "_parse_quarterly_holdings", parse)
        monkeypatch.setattr(sc, "get_cursor", get_cursor)
        monkeypatch.setattr(sc, "_get_fund_id_map",
                            lambda cur: {c: i for i, c in enumerate(sorted(in_db), 1)})
        monkeypatch.setattr(sc, "_funds_on_record", lambda cur, table, period: set(on_record))
        save = lambda cur, fund_id, period, holdings: self.saved.append(fund_id)  # noqa: E731
        monkeypatch.setattr(sc, "_save_monthly", save)
        monkeypatch.setattr(sc, "_save_quarterly", save)


SCRAPES = pytest.mark.parametrize("scrape", [sc.scrape_monthly, sc.scrape_quarterly],
                                  ids=["monthly", "quarterly"])


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    import time
    monkeypatch.setattr(time, "sleep", lambda s: None)


def test_funds_on_record_query():
    class Cursor:
        def execute(self, sql, params):
            self.sql, self.params = sql, params

        def fetchall(self):
            return [{"code": "a"}, {"code": "b"}]

    cur = Cursor()
    assert sc._funds_on_record(cur, "tw.fund_holdings_monthly", "202608") == {"a", "b"}
    assert "FROM tw.fund_holdings_monthly h JOIN tw.funds f ON f.id = h.fund_id" in cur.sql
    assert cur.params == ("202608",)


def test_registry_has_thirteen_funds():
    assert len(REGISTERED) == 13 == len(set(REGISTERED))


@SCRAPES
def test_complete_period(monkeypatch, scrape):
    site = Site(monkeypatch)
    assert scrape("202608") == []
    assert len(site.saved) == 13


@SCRAPES
def test_failed_company_names_its_funds(monkeypatch, scrape):
    site = Site(monkeypatch, fail={"A0032"})
    assert scrape("202608") == [f"{code}：A0032 查詢失敗：TimeoutError: read timed out"
                                for code in NOMURA]
    assert len(site.saved) == 11                         # the other companies still saved


@SCRAPES
def test_failed_company_is_fine_when_the_period_is_already_on_record(monkeypatch, scrape):
    Site(monkeypatch, fail={"A0032"}, on_record=NOMURA)
    assert scrape("202608") == []


@SCRAPES
def test_every_company_failing_is_every_fund_missing(monkeypatch, scrape):
    Site(monkeypatch, fail=set(sc.FUND_REGISTRY))
    assert len(scrape("202608")) == 13


@SCRAPES
def test_fund_the_page_does_not_list(monkeypatch, scrape):
    allianz = [code for code, _ in sc.FUND_REGISTRY["A0036"]]
    Site(monkeypatch, parsed={"A0036": {allianz[0]: [HOLDING]}})
    assert scrape("202608") == [f"{allianz[1]}：A0036 頁面找不到這檔基金或解析不到持股"]


@SCRAPES
def test_page_without_any_fund(monkeypatch, scrape):
    Site(monkeypatch, parsed={"A0047": {}})
    (code,), = [[c for c, _ in sc.FUND_REGISTRY["A0047"]]]
    assert scrape("202608") == [f"{code}：A0047 頁面找不到這檔基金或解析不到持股"]


@SCRAPES
def test_fund_missing_from_tw_funds(monkeypatch, scrape):
    gone = REGISTERED[0]
    Site(monkeypatch, in_db=set(REGISTERED) - {gone})
    assert scrape("202608") == [f"{gone}：不在 tw.funds"]


@SCRAPES
def test_fund_holding_only_foreign_stocks_still_counts(monkeypatch, scrape):
    # Only domestic holdings are saved; a month without any is still a fund got.
    gc = "unitec-gc-smid"
    Site(monkeypatch, parsed={"A0009": {code: [FOREIGN] if code == gc else [HOLDING]
                                        for code, _ in sc.FUND_REGISTRY["A0009"]}})
    assert scrape("202608") == []


def test_period_not_published_is_raised_before_any_query(monkeypatch):
    def not_yet(session, url, period):
        raise sc.PeriodNotAvailable(f"SITCA period {period} not yet published")

    site = Site(monkeypatch)
    monkeypatch.setattr(sc, "_setup_period", not_yet)
    with pytest.raises(sc.PeriodNotAvailable):
        sc.scrape_monthly("202609")
