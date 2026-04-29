"""Backfill tw.dividends from FinMind TaiwanStockDividend.

FinMind row schema (relevant fields):
  CashExDividendTradingDate    -- ex-date for cash dividend (str, '' if none)
  StockExDividendTradingDate   -- ex-date for stock dividend (str, '' if none)
  CashEarningsDistribution     -- cash dividend from retained earnings (NTD/share)
  CashStatutorySurplus         -- cash dividend from capital reserve (NTD/share)
  StockEarningsDistribution    -- stock dividend from retained earnings (shares/share)
  StockStatutorySurplus        -- stock dividend from capital reserve (shares/share)

We sum the two cash sources into one cash_dividend, two stock sources into one
stock_dividend. When cash and stock ex-dates differ, we write two rows (one
per ex-date). If both are empty, the row is skipped.

Idempotency: DELETE then INSERT per ticker. Run is safe to re-run.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from psycopg2.extras import execute_batch

from ..data.adapters.db_adapter import _connect
from .finmind import FinMindClient


@dataclass(frozen=True)
class DividendRow:
    ticker: str
    ex_date: date
    cash_dividend: Decimal  # NTD per share
    stock_dividend: Decimal  # shares per share (e.g. 0.05 = 5%)


def _to_dec(v: Any) -> Decimal:
    if v in (None, "", "0", 0):
        return Decimal(0)
    return Decimal(str(v))


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def parse_finmind_dividend_rows(ticker: str, rows: list[dict[str, Any]]) -> list[DividendRow]:
    """Convert FinMind rows for one ticker to DividendRow records, deduped by ex_date."""
    by_ex_date: dict[date, dict[str, Decimal]] = {}
    for r in rows:
        cash = _to_dec(r.get("CashEarningsDistribution")) + _to_dec(r.get("CashStatutorySurplus"))
        stock = _to_dec(r.get("StockEarningsDistribution")) + _to_dec(
            r.get("StockStatutorySurplus")
        )
        cash_ex = _parse_date(r.get("CashExDividendTradingDate"))
        stock_ex = _parse_date(r.get("StockExDividendTradingDate"))

        if cash > 0 and cash_ex is not None:
            slot = by_ex_date.setdefault(cash_ex, {"cash": Decimal(0), "stock": Decimal(0)})
            slot["cash"] += cash
        if stock > 0 and stock_ex is not None:
            slot = by_ex_date.setdefault(stock_ex, {"cash": Decimal(0), "stock": Decimal(0)})
            slot["stock"] += stock

    out = [
        DividendRow(
            ticker=ticker,
            ex_date=ex,
            cash_dividend=v["cash"],
            stock_dividend=v["stock"],
        )
        for ex, v in sorted(by_ex_date.items())
    ]
    return out


def fetch_one_ticker(
    client: FinMindClient,
    ticker: str,
    *,
    start_date: str = "2010-01-01",
    end_date: str = "2030-12-31",
) -> list[DividendRow]:
    raw = client.fetch(
        "TaiwanStockDividend",
        data_id=ticker,
        start_date=start_date,
        end_date=end_date,
    )
    return parse_finmind_dividend_rows(ticker, raw)


def upsert_dividends(rows: list[DividendRow]) -> int:
    """Replace dividend rows for any ticker present in `rows`.

    DELETE + bulk INSERT per ticker (the existing schema has no unique index).
    """
    if not rows:
        return 0
    by_ticker: dict[str, list[DividendRow]] = {}
    for r in rows:
        by_ticker.setdefault(r.ticker, []).append(r)

    written = 0
    with _connect() as conn, conn.cursor() as cur:
        for ticker, group in by_ticker.items():
            cur.execute("DELETE FROM tw.dividends WHERE stock_id = %s", (ticker,))
            execute_batch(
                cur,
                "INSERT INTO tw.dividends (stock_id, ex_date, cash_dividend, stock_dividend)"
                " VALUES (%s, %s, %s, %s)",
                [(r.ticker, r.ex_date, r.cash_dividend, r.stock_dividend) for r in group],
            )
            written += len(group)
        conn.commit()
    return written


def backfill_dividends(
    tickers: list[str],
    *,
    interval: float = 0.4,
    progress_every: int = 50,
    on_progress: Any = None,
    token: str | None = None,
) -> dict[str, int]:
    """Run the full backfill loop. Returns {ticker: rows_written}."""
    client = FinMindClient(interval=interval, token=token)
    summary: dict[str, int] = {}
    for i, t in enumerate(tickers):
        try:
            rows = fetch_one_ticker(client, t)
        except Exception as e:  # noqa: BLE001 — keep going on per-ticker failures
            summary[t] = -1
            if on_progress:
                on_progress(i + 1, len(tickers), t, error=str(e))
            continue
        n = upsert_dividends(rows)
        summary[t] = n
        if on_progress and (i + 1) % progress_every == 0:
            on_progress(i + 1, len(tickers), t, error=None)
    return summary
