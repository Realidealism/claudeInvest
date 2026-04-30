"""PostgreSQL adapter that loads tw.* tables into Pydantic models.

The hermit_stock package shares the parent Invest project's database. DB
credentials are read from the parent .env (loaded via python-dotenv) or from
process environment variables (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD).

Monetary fields in tw.income_statements / tw.balance_sheets / tw.cash_flows
are stored in thousands of NTD. This adapter multiplies by 1000 so all returned
Pydantic models are denominated in NTD.
"""

from __future__ import annotations

import os
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

from ..models import DailyPrice, MonthlyRevenue, QuarterlyReport, StockMeta
from ..publish_date import monthly_publish_date, quarter_period_end, quarter_publish_date

THOUSAND = Decimal(1000)
_DOTENV_LOADED = False


def _load_env_once() -> None:
    """Load .env from the parent Invest project (one-time, idempotent)."""
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    parent_env = Path(__file__).resolve().parents[5] / ".env"
    if parent_env.exists():
        load_dotenv(parent_env)
    else:
        load_dotenv()  # fall back to CWD .env if any
    _DOTENV_LOADED = True


def _db_config() -> dict[str, str]:
    _load_env_once()
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "dbname": os.getenv("DB_NAME", "invest"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", ""),
    }


def _connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(**_db_config())


def _to_decimal_thousand(value: Any) -> Decimal | None:
    """千元 -> 元."""
    if value is None:
        return None
    return Decimal(value) * THOUSAND


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def load_active_stocks(
    markets: tuple[str, ...] = ("TWSE", "TPEx"),
    security_types: tuple[str, ...] = ("STOCK",),
    *,
    include_delisted: bool = False,
) -> list[StockMeta]:
    """List stocks for screener / backtest input.

    `include_delisted=False` (default) keeps only `is_active=True` rows —
    convenient for live screening.
    `include_delisted=True` returns ALL stocks (active + delisted), required
    for survivorship-bias-free backtesting.
    """
    where_active = "" if include_delisted else "AND is_active"
    sql = f"""
        SELECT stock_id, name, market, industry, listed_date
        FROM tw.stocks
        WHERE TRUE {where_active}
          AND market = ANY(%s)
          AND (security_type = ANY(%s) OR security_type IS NULL)
        ORDER BY stock_id
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (list(markets), list(security_types)))
        rows = cur.fetchall()
    return [
        StockMeta(
            ticker=r["stock_id"],
            name=r["name"],
            market=r["market"],
            industry=r["industry"],
            listed_date=r["listed_date"],
        )
        for r in rows
        if r["market"] in ("TWSE", "TPEx")
    ]


def load_stock_meta(ticker: str) -> StockMeta | None:
    sql = """
        SELECT stock_id, name, market, industry, listed_date
        FROM tw.stocks
        WHERE stock_id = %s
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (ticker,))
        row = cur.fetchone()
    if row is None:
        return None
    return StockMeta(
        ticker=row["stock_id"],
        name=row["name"],
        market=row["market"],
        industry=row["industry"],
        listed_date=row["listed_date"],
    )


# tw.cash_flows is stored as period_type='A' (YTD cumulative). Single-quarter
# values are derived by subtracting the prior quarter within the same year via
# a LAG window. This subquery is reused by both per-ticker and batch loaders.
_CASH_FLOWS_Q_FROM_A = """
SELECT
    stock_id, year, quarter,
    operating_cash_flow
        - COALESCE(LAG(operating_cash_flow) OVER w, 0) AS operating_cash_flow,
    capex
        - COALESCE(LAG(capex) OVER w, 0) AS capex,
    investing_cash_flow
        - COALESCE(LAG(investing_cash_flow) OVER w, 0) AS investing_cash_flow,
    financing_cash_flow
        - COALESCE(LAG(financing_cash_flow) OVER w, 0) AS financing_cash_flow,
    free_cash_flow
        - COALESCE(LAG(free_cash_flow) OVER w, 0) AS free_cash_flow
FROM tw.cash_flows
WHERE period_type = 'A'
WINDOW w AS (PARTITION BY stock_id, year ORDER BY quarter)
"""


def load_quarterly_reports(ticker: str) -> list[QuarterlyReport]:
    """Outer-join income/balance/cashflow on (year, quarter), Q-only.

    Returns reports ordered by period_end ascending. Missing fields stay None.
    """
    sql = f"""
        WITH cf_q AS ({_CASH_FLOWS_Q_FROM_A})
        SELECT
            COALESCE(i.year, b.year, c.year)       AS year,
            COALESCE(i.quarter, b.quarter, c.quarter) AS quarter,
            -- Income statement (period_type='Q')
            i.revenue, i.cost_of_revenue, i.gross_profit,
            i.operating_expenses, i.operating_income,
            i.non_operating_income, i.pretax_income, i.tax_expense,
            i.net_income, i.net_income_attributable, i.eps,
            -- Balance sheet
            b.inventory, b.accounts_receivable,
            b.short_term_debt, b.long_term_debt,
            b.total_assets, b.total_liabilities, b.total_equity,
            b.equity_attributable, b.shares_outstanding, b.book_value_per_share,
            -- Cash flow (derived single-quarter from YTD 'A')
            c.operating_cash_flow, c.capex,
            c.investing_cash_flow, c.financing_cash_flow, c.free_cash_flow
        FROM (
            SELECT * FROM tw.income_statements
            WHERE stock_id = %(t)s AND period_type = 'Q'
        ) i
        FULL OUTER JOIN (
            SELECT * FROM tw.balance_sheets WHERE stock_id = %(t)s
        ) b ON b.year = i.year AND b.quarter = i.quarter
        FULL OUTER JOIN (
            SELECT * FROM cf_q WHERE stock_id = %(t)s
        ) c ON c.year = COALESCE(i.year, b.year)
           AND c.quarter = COALESCE(i.quarter, b.quarter)
        ORDER BY 1, 2
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, {"t": ticker})
        rows = cur.fetchall()

    reports: list[QuarterlyReport] = []
    for r in rows:
        year = int(r["year"])
        quarter = int(r["quarter"])
        reports.append(
            QuarterlyReport(
                ticker=ticker,
                period=f"{year}Q{quarter}",
                period_end=quarter_period_end(year, quarter),
                publish_date=quarter_publish_date(year, quarter),
                source="db",
                revenue=_to_decimal_thousand(r["revenue"]),
                cost_of_revenue=_to_decimal_thousand(r["cost_of_revenue"]),
                gross_profit=_to_decimal_thousand(r["gross_profit"]),
                operating_expenses=_to_decimal_thousand(r["operating_expenses"]),
                operating_income=_to_decimal_thousand(r["operating_income"]),
                non_operating_income=_to_decimal_thousand(r["non_operating_income"]),
                pretax_income=_to_decimal_thousand(r["pretax_income"]),
                tax_expense=_to_decimal_thousand(r["tax_expense"]),
                net_income=_to_decimal_thousand(r["net_income"]),
                net_income_attributable=_to_decimal_thousand(r["net_income_attributable"]),
                eps=_to_decimal(r["eps"]),
                inventory=_to_decimal_thousand(r["inventory"]),
                accounts_receivable=_to_decimal_thousand(r["accounts_receivable"]),
                short_term_debt=_to_decimal_thousand(r["short_term_debt"]),
                long_term_debt=_to_decimal_thousand(r["long_term_debt"]),
                total_assets=_to_decimal_thousand(r["total_assets"]),
                total_liabilities=_to_decimal_thousand(r["total_liabilities"]),
                total_equity=_to_decimal_thousand(r["total_equity"]),
                equity_attributable=_to_decimal_thousand(r["equity_attributable"]),
                shares_outstanding=_to_decimal(r["shares_outstanding"]),
                book_value_per_share=_to_decimal(r["book_value_per_share"]),
                operating_cash_flow=_to_decimal_thousand(r["operating_cash_flow"]),
                capex=_to_decimal_thousand(r["capex"]),
                investing_cash_flow=_to_decimal_thousand(r["investing_cash_flow"]),
                financing_cash_flow=_to_decimal_thousand(r["financing_cash_flow"]),
                free_cash_flow=_to_decimal_thousand(r["free_cash_flow"]),
            )
        )
    return reports


def load_all_quarterly_reports(
    tickers: list[str] | None = None,
) -> dict[str, list[QuarterlyReport]]:
    """Batch-load quarterly reports for many tickers in one round-trip.

    If `tickers` is None, returns every ticker that has any data in the
    financial-statement tables.
    """
    where_ticker = ""
    where_cf = ""
    params: dict[str, object] = {}
    if tickers is not None:
        where_ticker = "AND stock_id = ANY(%(tickers)s)"
        where_cf = "WHERE stock_id = ANY(%(tickers)s)"
        params["tickers"] = tickers
    sql = f"""
        WITH cf_q AS ({_CASH_FLOWS_Q_FROM_A})
        SELECT
            COALESCE(i.stock_id, b.stock_id, c.stock_id) AS stock_id,
            COALESCE(i.year, b.year, c.year)             AS year,
            COALESCE(i.quarter, b.quarter, c.quarter)    AS quarter,
            i.revenue, i.cost_of_revenue, i.gross_profit,
            i.operating_expenses, i.operating_income,
            i.non_operating_income, i.pretax_income, i.tax_expense,
            i.net_income, i.net_income_attributable, i.eps,
            b.inventory, b.accounts_receivable,
            b.short_term_debt, b.long_term_debt,
            b.total_assets, b.total_liabilities, b.total_equity,
            b.equity_attributable, b.shares_outstanding, b.book_value_per_share,
            c.operating_cash_flow, c.capex,
            c.investing_cash_flow, c.financing_cash_flow, c.free_cash_flow
        FROM (
            SELECT * FROM tw.income_statements
            WHERE period_type = 'Q' {where_ticker}
        ) i
        FULL OUTER JOIN (
            SELECT * FROM tw.balance_sheets
            WHERE TRUE {where_ticker}
        ) b ON b.stock_id = i.stock_id AND b.year = i.year AND b.quarter = i.quarter
        FULL OUTER JOIN (
            SELECT * FROM cf_q
            {where_cf}
        ) c ON c.stock_id = COALESCE(i.stock_id, b.stock_id)
           AND c.year = COALESCE(i.year, b.year)
           AND c.quarter = COALESCE(i.quarter, b.quarter)
        ORDER BY 1, 2, 3
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    out: dict[str, list[QuarterlyReport]] = {}
    for r in rows:
        if r["stock_id"] is None or r["year"] is None or r["quarter"] is None:
            continue
        ticker = r["stock_id"]
        year = int(r["year"])
        quarter = int(r["quarter"])
        out.setdefault(ticker, []).append(
            QuarterlyReport(
                ticker=ticker,
                period=f"{year}Q{quarter}",
                period_end=quarter_period_end(year, quarter),
                publish_date=quarter_publish_date(year, quarter),
                source="db",
                revenue=_to_decimal_thousand(r["revenue"]),
                cost_of_revenue=_to_decimal_thousand(r["cost_of_revenue"]),
                gross_profit=_to_decimal_thousand(r["gross_profit"]),
                operating_expenses=_to_decimal_thousand(r["operating_expenses"]),
                operating_income=_to_decimal_thousand(r["operating_income"]),
                non_operating_income=_to_decimal_thousand(r["non_operating_income"]),
                pretax_income=_to_decimal_thousand(r["pretax_income"]),
                tax_expense=_to_decimal_thousand(r["tax_expense"]),
                net_income=_to_decimal_thousand(r["net_income"]),
                net_income_attributable=_to_decimal_thousand(r["net_income_attributable"]),
                eps=_to_decimal(r["eps"]),
                inventory=_to_decimal_thousand(r["inventory"]),
                accounts_receivable=_to_decimal_thousand(r["accounts_receivable"]),
                short_term_debt=_to_decimal_thousand(r["short_term_debt"]),
                long_term_debt=_to_decimal_thousand(r["long_term_debt"]),
                total_assets=_to_decimal_thousand(r["total_assets"]),
                total_liabilities=_to_decimal_thousand(r["total_liabilities"]),
                total_equity=_to_decimal_thousand(r["total_equity"]),
                equity_attributable=_to_decimal_thousand(r["equity_attributable"]),
                shares_outstanding=_to_decimal(r["shares_outstanding"]),
                book_value_per_share=_to_decimal(r["book_value_per_share"]),
                operating_cash_flow=_to_decimal_thousand(r["operating_cash_flow"]),
                capex=_to_decimal_thousand(r["capex"]),
                investing_cash_flow=_to_decimal_thousand(r["investing_cash_flow"]),
                financing_cash_flow=_to_decimal_thousand(r["financing_cash_flow"]),
                free_cash_flow=_to_decimal_thousand(r["free_cash_flow"]),
            )
        )
    return out


def load_all_monthly_revenue(
    tickers: list[str] | None = None,
) -> dict[str, list[MonthlyRevenue]]:
    where = ""
    params: dict[str, object] = {}
    if tickers is not None:
        where = "WHERE stock_id = ANY(%(tickers)s)"
        params["tickers"] = tickers
    sql = f"""
        SELECT stock_id, year_month, revenue, mom_pct, yoy_pct
        FROM tw.monthly_revenue
        {where}
        ORDER BY stock_id, year_month
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    out: dict[str, list[MonthlyRevenue]] = {}
    for r in rows:
        rev = _to_decimal_thousand(r["revenue"])
        if rev is None:
            continue
        ym = r["year_month"]
        out.setdefault(r["stock_id"], []).append(
            MonthlyRevenue(
                ticker=r["stock_id"],
                year_month=ym,
                publish_date=monthly_publish_date(ym),
                source="db",
                revenue=rev,
                mom=_to_decimal(r["mom_pct"]),
                yoy=_to_decimal(r["yoy_pct"]),
            )
        )
    return out


def load_monthly_revenue(ticker: str) -> list[MonthlyRevenue]:
    sql = """
        SELECT year_month, revenue, mom_pct, yoy_pct
        FROM tw.monthly_revenue
        WHERE stock_id = %s
        ORDER BY year_month
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (ticker,))
        rows = cur.fetchall()

    out: list[MonthlyRevenue] = []
    for r in rows:
        ym = r["year_month"]
        rev = _to_decimal_thousand(r["revenue"])
        if rev is None:
            continue
        out.append(
            MonthlyRevenue(
                ticker=ticker,
                year_month=ym,
                publish_date=monthly_publish_date(ym),
                source="db",
                revenue=rev,
                mom=_to_decimal(r["mom_pct"]),
                yoy=_to_decimal(r["yoy_pct"]),
            )
        )
    return out


def load_dividends(ticker: str) -> list[tuple[date, Decimal, Decimal]]:
    """Return (ex_date, cash_dividend, stock_dividend) tuples ascending by date."""
    sql = """
        SELECT ex_date, cash_dividend, stock_dividend
        FROM tw.dividends
        WHERE stock_id = %s
        ORDER BY ex_date
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (ticker,))
        rows = cur.fetchall()
    return [
        (
            r["ex_date"],
            _to_decimal(r["cash_dividend"]) or Decimal(0),
            _to_decimal(r["stock_dividend"]) or Decimal(0),
        )
        for r in rows
    ]


def load_capital_reductions(ticker: str) -> list[tuple[date, Decimal]]:
    """Return (effective_date, ratio) tuples ascending by date.

    `ratio` is the price multiplier for forward adjustment (close_pre / close_post).
    """
    sql = """
        SELECT effective_date, ratio
        FROM tw.capital_changes
        WHERE stock_id = %s AND event_type = 'REDUCTION'
        ORDER BY effective_date
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (ticker,))
        rows = cur.fetchall()
    out: list[tuple[date, Decimal]] = []
    for r in rows:
        ratio = _to_decimal(r["ratio"])
        if ratio is None or ratio == 0:
            continue
        out.append((r["effective_date"], ratio))
    return out


def load_all_dividends(
    tickers: list[str] | None = None,
) -> dict[str, list[tuple[date, Decimal, Decimal]]]:
    where = ""
    params: dict[str, object] = {}
    if tickers is not None:
        where = "WHERE stock_id = ANY(%(tickers)s)"
        params["tickers"] = tickers
    sql = f"""
        SELECT stock_id, ex_date, cash_dividend, stock_dividend
        FROM tw.dividends
        {where}
        ORDER BY stock_id, ex_date
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    out: dict[str, list[tuple[date, Decimal, Decimal]]] = {}
    for r in rows:
        out.setdefault(r["stock_id"], []).append(
            (
                r["ex_date"],
                _to_decimal(r["cash_dividend"]) or Decimal(0),
                _to_decimal(r["stock_dividend"]) or Decimal(0),
            )
        )
    return out


def load_all_capital_reductions(
    tickers: list[str] | None = None,
) -> dict[str, list[tuple[date, Decimal]]]:
    where = "WHERE event_type = 'REDUCTION'"
    params: dict[str, object] = {}
    if tickers is not None:
        where += " AND stock_id = ANY(%(tickers)s)"
        params["tickers"] = tickers
    sql = f"""
        SELECT stock_id, effective_date, ratio
        FROM tw.capital_changes
        {where}
        ORDER BY stock_id, effective_date
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    out: dict[str, list[tuple[date, Decimal]]] = {}
    for r in rows:
        ratio = _to_decimal(r["ratio"])
        if ratio is None or ratio == 0:
            continue
        out.setdefault(r["stock_id"], []).append((r["effective_date"], ratio))
    return out


def load_turnover_table(tickers: list[str]) -> pd.DataFrame:
    """Wide DataFrame: index=trade_date, columns=ticker, values=turnover (NTD).

    Daily turnover = close_price * volume (already in tw.daily_prices).
    Used by the liquidity filter at backtest time — at each rebalance date,
    we look up the 60-day rolling mean turnover up to (but not including)
    that day to decide which tickers are tradable.
    """
    sql = """
        SELECT stock_id, trade_date, turnover
        FROM tw.daily_prices
        WHERE stock_id = ANY(%s) AND turnover IS NOT NULL
        ORDER BY stock_id, trade_date
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (tickers,))
        rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["turnover"] = df["turnover"].astype(float)
    return df.pivot(index="trade_date", columns="stock_id", values="turnover").sort_index()


def load_index_close(index_id: str = "TAIEX") -> list[tuple[date, float]]:
    """Load benchmark index close-price series ascending by trade_date."""
    sql = """
        SELECT trade_date, close_price
        FROM tw.index_prices
        WHERE index_id = %s
        ORDER BY trade_date
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (index_id,))
        rows = cur.fetchall()
    return [
        (r["trade_date"], float(r["close_price"])) for r in rows if r["close_price"] is not None
    ]


def load_all_daily_prices(tickers: list[str]) -> dict[str, list[DailyPrice]]:
    """Batch-load daily prices for many tickers in one query."""
    sql = """
        SELECT stock_id, trade_date, open_price, high_price, low_price, close_price, volume
        FROM tw.daily_prices
        WHERE stock_id = ANY(%s)
        ORDER BY stock_id, trade_date
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (tickers,))
        rows = cur.fetchall()
    out: dict[str, list[DailyPrice]] = {}
    for r in rows:
        out.setdefault(r["stock_id"], []).append(
            DailyPrice(
                ticker=r["stock_id"],
                trade_date=r["trade_date"],
                source="db",
                open=_to_decimal(r["open_price"]),
                high=_to_decimal(r["high_price"]),
                low=_to_decimal(r["low_price"]),
                close=_to_decimal(r["close_price"]),
                volume=int(r["volume"]) if r["volume"] is not None else None,
            )
        )
    return out


def load_daily_prices(
    ticker: str, start: date | None = None, end: date | None = None
) -> list[DailyPrice]:
    sql = """
        SELECT trade_date, open_price, high_price, low_price, close_price, volume
        FROM tw.daily_prices
        WHERE stock_id = %s
          AND (%s::date IS NULL OR trade_date >= %s)
          AND (%s::date IS NULL OR trade_date <= %s)
        ORDER BY trade_date
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (ticker, start, start, end, end))
        rows = cur.fetchall()
    return [
        DailyPrice(
            ticker=ticker,
            trade_date=r["trade_date"],
            source="db",
            open=_to_decimal(r["open_price"]),
            high=_to_decimal(r["high_price"]),
            low=_to_decimal(r["low_price"]),
            close=_to_decimal(r["close_price"]),
            volume=int(r["volume"]) if r["volume"] is not None else None,
        )
        for r in rows
    ]
