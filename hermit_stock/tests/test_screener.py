"""Screener tests with synthetic multi-ticker data (no DB)."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from hermit_stock.data.models import StockMeta
from hermit_stock.reports.screener import run_screener
from hermit_stock.reports.screener_io import _row_dict, to_excel, to_markdown
from tests.fixtures.synthetic import linear_growth_quarters, make_monthly, make_quarter


def _meta(ticker: str, name: str = "", industry: str = "tech") -> StockMeta:
    return StockMeta(ticker=ticker, name=name or ticker, market="TWSE", industry=industry)


def _strong_quarters(ticker: str = "STRONG") -> list:
    return linear_growth_quarters(
        n_quarters=12,
        qoq_growth=Decimal("0.10"),
    )


def _weak_quarters(ticker: str = "WEAK") -> list:
    """No growth, flat fundamentals → most rules fail."""
    out = []
    for i in range(12):
        year = 2020 + i // 4
        q = (i % 4) + 1
        out.append(
            make_quarter(
                year,
                q,
                ticker=ticker,
                revenue=Decimal(100),
                cogs=Decimal(60),
                gross_profit=Decimal(40),
                net_income=Decimal(20),
                eps=Decimal("1.0"),
                inventory=Decimal(15),
                total_assets=Decimal(400),
                total_liabilities=Decimal(160),
                total_equity=Decimal(240),
                equity_attributable=Decimal(240),
                shares_outstanding=Decimal(20),
                operating_cash_flow=Decimal(22),
                capex=Decimal(-5),
                free_cash_flow=Decimal(17),
            )
        )
    return out


def _strong_monthly() -> list:
    """Strong rising monthly revenue, latest YoY > YTD YoY, 12-month high."""
    out = []
    for m in range(1, 13):
        out.append(make_monthly(2023, m, Decimal(100)))
    for m in range(1, 13):
        out.append(
            make_monthly(2024, m, Decimal(100 + m * 10), yoy=Decimal(220) if m == 12 else None)
        )
    return out


def test_run_screener_orders_by_score_descending() -> None:
    metas = [_meta("STRONG"), _meta("WEAK")]
    quarterly = {"STRONG": _strong_quarters(), "WEAK": _weak_quarters()}
    monthly = {"STRONG": _strong_monthly(), "WEAK": []}

    rows = run_screener(
        as_of_label="2024Q3",
        metas=metas,
        quarterly_by_ticker=quarterly,
        monthly_by_ticker=monthly,
        with_valuation=False,
    )

    assert [r.ticker for r in rows] == ["STRONG", "WEAK"]
    assert rows[0].scoreboard.score >= rows[1].scoreboard.score


def test_run_screener_skips_tickers_without_data() -> None:
    metas = [_meta("STRONG"), _meta("EMPTY")]
    quarterly = {"STRONG": _strong_quarters()}
    monthly = {"STRONG": _strong_monthly()}

    rows = run_screener(
        as_of_label="2024Q3",
        metas=metas,
        quarterly_by_ticker=quarterly,
        monthly_by_ticker=monthly,
        with_valuation=False,
    )
    assert {r.ticker for r in rows} == {"STRONG"}


def test_run_screener_respects_lookahead_filter() -> None:
    metas = [_meta("STRONG")]
    quarterly = {"STRONG": _strong_quarters()}
    monthly = {"STRONG": _strong_monthly()}

    early = run_screener(
        as_of_label="2020-06-30",
        metas=metas,
        quarterly_by_ticker=quarterly,
        monthly_by_ticker=monthly,
        with_valuation=False,
    )
    # At 2020-06-30 only the 2020Q1 report is published; not enough for F1/F2
    assert early[0].scoreboard.unknown_count >= 4


def test_to_markdown_returns_table() -> None:
    metas = [_meta("STRONG")]
    quarterly = {"STRONG": _strong_quarters()}
    monthly = {"STRONG": _strong_monthly()}
    rows = run_screener(
        as_of_label="2024Q3",
        metas=metas,
        quarterly_by_ticker=quarterly,
        monthly_by_ticker=monthly,
        with_valuation=False,
    )
    md = to_markdown(rows)
    assert "ticker" in md
    assert "STRONG" in md
    assert md.count("|") > 10


def test_to_excel_writes_file(tmp_path: Path) -> None:
    metas = [_meta("STRONG", "強股", "tech"), _meta("WEAK", "弱股", "old-school")]
    quarterly = {"STRONG": _strong_quarters(), "WEAK": _weak_quarters()}
    monthly = {"STRONG": _strong_monthly(), "WEAK": []}
    rows = run_screener(
        as_of_label="2024Q3",
        metas=metas,
        quarterly_by_ticker=quarterly,
        monthly_by_ticker=monthly,
        with_valuation=False,
    )
    out = tmp_path / "screener.xlsx"
    to_excel(rows, out)
    assert out.exists()
    assert out.stat().st_size > 1000


def test_row_dict_contains_all_columns() -> None:
    metas = [_meta("STRONG")]
    rows = run_screener(
        as_of_label="2024Q3",
        metas=metas,
        quarterly_by_ticker={"STRONG": _strong_quarters()},
        monthly_by_ticker={"STRONG": _strong_monthly()},
        with_valuation=False,
    )
    d = _row_dict(rows[0])
    for code in ("F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"):
        assert code in d
    assert d["total"] >= 0
    assert d["grade"] in ("A", "B", "C", "D")
