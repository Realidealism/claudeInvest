"""Backfill tw.capital_changes (REDUCTION rows) from FinMind.

FinMind dataset: TaiwanStockCapitalReductionReferencePrice
  date                              -- effective date (post-reduction first trading day)
  ClosingPriceonTheLastTradingDay   -- close just before the reduction
  PostReductionReferencePrice       -- official reference price after reduction
  ReasonforCapitalReduction         -- 'Cash refund' / etc.

We derive `ratio` as the implied price-multiplier for adjustment:
    ratio = ClosingPriceonTheLastTradingDay / PostReductionReferencePrice
For a 減資 that lowers share count, this ratio is < 1 (e.g. 2603 = 0.432),
which is the factor to apply to historical close prices for forward
adjustment.

Idempotency: DELETE existing event_type='REDUCTION' rows for each ticker, then
bulk INSERT.
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
class ReductionRow:
    ticker: str
    effective_date: date
    ratio: Decimal  # close_pre / close_post (< 1 for typical share-cancellation)
    note: str


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def parse_finmind_reduction_rows(ticker: str, rows: list[dict[str, Any]]) -> list[ReductionRow]:
    out: list[ReductionRow] = []
    for r in rows:
        eff = _parse_date(r.get("date"))
        pre = r.get("ClosingPriceonTheLastTradingDay")
        post = r.get("PostReductionReferencePrice")
        if eff is None or pre in (None, 0) or post in (None, 0):
            continue
        ratio = Decimal(str(pre)) / Decimal(str(post))
        out.append(
            ReductionRow(
                ticker=ticker,
                effective_date=eff,
                ratio=ratio,
                note=str(r.get("ReasonforCapitalReduction") or ""),
            )
        )
    out.sort(key=lambda x: x.effective_date)
    return out


def fetch_one_ticker(
    client: FinMindClient,
    ticker: str,
    *,
    start_date: str = "2010-01-01",
    end_date: str = "2030-12-31",
) -> list[ReductionRow]:
    raw = client.fetch(
        "TaiwanStockCapitalReductionReferencePrice",
        data_id=ticker,
        start_date=start_date,
        end_date=end_date,
    )
    return parse_finmind_reduction_rows(ticker, raw)


def upsert_reductions(rows: list[ReductionRow]) -> int:
    if not rows:
        return 0
    by_ticker: dict[str, list[ReductionRow]] = {}
    for r in rows:
        by_ticker.setdefault(r.ticker, []).append(r)
    written = 0
    with _connect() as conn, conn.cursor() as cur:
        for ticker, group in by_ticker.items():
            cur.execute(
                "DELETE FROM tw.capital_changes "
                "WHERE stock_id = %s AND event_type = 'REDUCTION'",
                (ticker,),
            )
            execute_batch(
                cur,
                "INSERT INTO tw.capital_changes "
                "(stock_id, effective_date, event_type, ratio, note) "
                "VALUES (%s, %s, 'REDUCTION', %s, %s)",
                [(r.ticker, r.effective_date, r.ratio, r.note) for r in group],
            )
            written += len(group)
        conn.commit()
    return written


def backfill_reductions(
    tickers: list[str],
    *,
    interval: float = 0.4,
    on_progress: Any = None,
    token: str | None = None,
) -> dict[str, int]:
    client = FinMindClient(interval=interval, token=token)
    summary: dict[str, int] = {}
    for i, t in enumerate(tickers):
        try:
            rows = fetch_one_ticker(client, t)
        except Exception as e:  # noqa: BLE001
            summary[t] = -1
            if on_progress:
                on_progress(i + 1, len(tickers), t, error=str(e))
            continue
        n = upsert_reductions(rows)
        summary[t] = n
        if on_progress and (i + 1) % 200 == 0:
            on_progress(i + 1, len(tickers), t, error=None)
    return summary
