"""Phase 3 screener: batch evaluate F1-F8 + valuation across the universe.

Two-stage design:

  Stage 1 (cheap, in-memory): for every ticker with ≥1 quarterly report or
    monthly revenue, compute the 8 rule results and the total score using
    publish_date-filtered data. This requires no daily prices.

  Stage 2 (per-ticker DB hit): for every ticker that survives `min_valuation_score`,
    pull daily prices, compute PE/PB/PS, build the 5y band, and produce a
    valuation snapshot. Disabled when `with_valuation=False`.

Lookahead-bias defense reuses data/as_of.py — same chokepoint as analyze.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date

import typer

from ..data.adapters import db_adapter
from ..data.as_of import filter_monthly, filter_prices, filter_quarterly
from ..data.models import StockMeta
from ..data.publish_date import parse_period_label, quarter_publish_date
from ..scoring.rules import RuleResult, evaluate_all
from ..scoring.scorer import Scoreboard, score
from ..valuation.bands import rolling_band
from ..valuation.methods import ValuationSnapshot, make_snapshot
from ..valuation.multiples import daily_multiples
from ..valuation.selector import select_valuation_method


@dataclass(frozen=True)
class ScreenRow:
    ticker: str
    name: str
    industry: str | None
    rule_results: list[RuleResult]
    scoreboard: Scoreboard
    valuation: ValuationSnapshot | None  # None if Stage 2 was skipped


def _resolve_as_of(label: str) -> date:
    if "Q" in label:
        y, q = parse_period_label(label)
        return quarter_publish_date(y, q)
    return date.fromisoformat(label)


def run_screener(
    *,
    as_of_label: str,
    metas: list[StockMeta],
    quarterly_by_ticker: dict[str, list],
    monthly_by_ticker: dict[str, list],
    with_valuation: bool = True,
    min_valuation_score: int = 5,
    progress_callback: Callable[[int, int], None] | None = None,
) -> list[ScreenRow]:
    """Pure function: takes pre-loaded data, returns ranked ScreenRows.

    Splits compute from I/O so tests can drive it with synthetic dicts.
    """
    as_of = _resolve_as_of(as_of_label)
    rows: list[ScreenRow] = []

    for meta in metas:
        ticker = meta.ticker
        qs_raw = quarterly_by_ticker.get(ticker, [])
        ms_raw = monthly_by_ticker.get(ticker, [])
        qs = filter_quarterly(qs_raw, as_of)
        ms = filter_monthly(ms_raw, as_of)
        if not qs and not ms:
            continue
        results = evaluate_all(qs, ms)
        sb = score(results)
        rows.append(
            ScreenRow(
                ticker=ticker,
                name=meta.name,
                industry=meta.industry,
                rule_results=results,
                scoreboard=sb,
                valuation=None,
            )
        )

    rows.sort(key=lambda r: (-r.scoreboard.score, r.ticker))

    if not with_valuation:
        return rows

    # Stage 2: pull prices only for survivors above threshold.
    upgraded: list[ScreenRow] = []
    total = sum(1 for r in rows if r.scoreboard.score >= min_valuation_score)
    for r in rows:
        if r.scoreboard.score < min_valuation_score:
            upgraded.append(r)
            continue
        prices = db_adapter.load_daily_prices(r.ticker)
        ps = filter_prices(prices, as_of)
        qs = filter_quarterly(quarterly_by_ticker.get(r.ticker, []), as_of)
        method = select_valuation_method(qs)
        if qs and ps:
            df = daily_multiples(qs, ps)
            band = rolling_band(df, method.lower(), window_years=5) if not df.empty else None
            snap = make_snapshot(df, method, band) if band is not None else None
        else:
            snap = None
        upgraded.append(
            ScreenRow(
                ticker=r.ticker,
                name=r.name,
                industry=r.industry,
                rule_results=r.rule_results,
                scoreboard=r.scoreboard,
                valuation=snap,
            )
        )
        if progress_callback is not None:
            progress_callback(
                len([x for x in upgraded if x.scoreboard.score >= min_valuation_score]), total
            )
    return upgraded


def screen_from_db(
    *,
    as_of_label: str,
    tickers: Iterable[str] | None = None,
    markets: tuple[str, ...] = ("TWSE", "TPEx"),
    with_valuation: bool = True,
    min_valuation_score: int = 5,
) -> list[ScreenRow]:
    """Convenience wrapper that pulls data from the DB and runs the screener."""
    metas = db_adapter.load_active_stocks(markets=markets)
    if tickers is not None:
        wanted = set(tickers)
        metas = [m for m in metas if m.ticker in wanted]

    ticker_list = [m.ticker for m in metas]
    quarterly_by_ticker = db_adapter.load_all_quarterly_reports(ticker_list)
    monthly_by_ticker = db_adapter.load_all_monthly_revenue(ticker_list)

    typer.echo(
        f"[screener] universe={len(metas)}, "
        f"tickers w/ financials={len(quarterly_by_ticker)}, "
        f"tickers w/ monthly={len(monthly_by_ticker)}"
    )

    def _cb(done: int, total: int) -> None:
        if total and done % max(1, total // 20) == 0:
            typer.echo(f"[screener] valuation {done}/{total}")

    return run_screener(
        as_of_label=as_of_label,
        metas=metas,
        quarterly_by_ticker=quarterly_by_ticker,
        monthly_by_ticker=monthly_by_ticker,
        with_valuation=with_valuation,
        min_valuation_score=min_valuation_score,
        progress_callback=_cb,
    )
