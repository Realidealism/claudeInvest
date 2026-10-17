"""Capital reduction (減資) reference prices -> tw.capital_changes.

Why this runs daily: a reduction resets the price scale without the stored
price series being adjusted, so the bars either side of the effective date sit
on different scales. signal_backtest.factories._conditions.post_break_blackout
reads this table to blank those bars out; if the table lags, the seam is
invisible to the factories (6176 and 6241 both resumed in 2026-08 while the
table still stopped at 08-03, because it was only ever filled by a manual
hermit_stock backfill).

Source: FinMind TaiwanStockCapitalReductionReferencePrice, which is per-ticker
only -- the free tier rejects a whole-table query -- so sweeping all ~2000
tickers costs ~13 minutes. Instead only stocks that stopped trading recently
are asked: a reduction always suspends the stock while the new shares are
issued, and 446 of the 447 recorded reductions that have a bar in the price
window were preceded by a gap of >= 2 trading days (the lone exception, 6109
on 2020-09-25, moved 3.5% -- a step too small to distort an indicator anyway).

Only reductions are covered. Splits and par-value changes are not in this
dataset and structurally cannot enter this table; they stay the job of the
price-step test in post_break_blackout.
"""

from datetime import date
from decimal import Decimal

from db.connection import get_cursor
from scrapers.financials import _fetch
from utils.format_shift import ScrapeResult

DATASET = "TaiwanStockCapitalReductionReferencePrice"

# Trading days back to look for resumptions. Wide enough that a run skipped for
# a few days still catches up, cheap enough to stay a handful of tickers.
LOOKBACK_DAYS = 20

# Minimum suspended trading days for a resumption to be worth asking about.
# Every recorded reduction with a bar in the price window except one was
# preceded by a gap of at least two; requiring only one would drag in every
# thinly traded stock that simply had no trade on some day.
MIN_GAP_DAYS = 2


def _candidates(trade_date: date) -> list[str]:
    """Stocks that resumed trading after a gap within the lookback window."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            WITH cal AS (
                SELECT trade_date, row_number() OVER (ORDER BY trade_date) AS i
                FROM tw.index_prices
                WHERE index_id = 'TAIEX' AND trade_date <= %s
            ),
            recent AS (
                SELECT trade_date, i FROM cal
                ORDER BY trade_date DESC LIMIT %s
            ),
            px AS (
                SELECT p.stock_id, c.i, p.trade_date
                FROM tw.daily_prices p
                JOIN cal c ON c.trade_date = p.trade_date
                JOIN tw.stocks s USING (stock_id)
                WHERE p.close_price IS NOT NULL
                  AND s.delisted_date IS NULL
                  AND s.market IN ('TWSE', 'TPEx')
                  AND s.security_type = 'STOCK'
                  AND c.i >= (SELECT MIN(i) FROM recent) - 60
            ),
            seq AS (
                SELECT stock_id, trade_date, i, lag(i) OVER (
                    PARTITION BY stock_id ORDER BY i
                ) AS pi
                FROM px
            )
            SELECT DISTINCT stock_id FROM seq
            WHERE pi IS NOT NULL AND i - pi > %s
              AND trade_date >= (SELECT MIN(trade_date) FROM recent)
            ORDER BY stock_id
            """,
            (trade_date, LOOKBACK_DAYS, MIN_GAP_DAYS),
        )
        return [r["stock_id"] for r in cur.fetchall()]


def _parse(stock_id: str, rows: list[dict]) -> list[tuple]:
    """FinMind rows -> (stock_id, effective_date, ratio, note).

    ratio is the price multiplier that maps post-reduction prices back onto the
    old scale (close before / reference price after), matching the convention
    the hermit_stock backfill already wrote into this table.
    """
    out = []
    for r in rows:
        pre = r.get("ClosingPriceonTheLastTradingDay")
        post = r.get("PostReductionReferencePrice")
        eff = r.get("date")
        if not eff or not pre or not post:
            continue
        try:
            d = date.fromisoformat(eff)
        except ValueError:
            continue
        out.append((stock_id, d, Decimal(str(pre)) / Decimal(str(post)),
                    str(r.get("ReasonforCapitalReduction") or "")))
    return out


def _save(rows: list[tuple]) -> int:
    """Insert rows this table does not already hold. Existing rows are left
    alone: the hermit_stock backfill owns the history and re-deriving a ratio
    from a later FinMind revision would silently move an adopted seam."""
    if not rows:
        return 0
    saved = 0
    with get_cursor() as cur:
        for stock_id, eff, ratio, note in rows:
            cur.execute(
                """
                INSERT INTO tw.capital_changes
                    (stock_id, effective_date, event_type, ratio, note)
                SELECT %s, %s, 'REDUCTION', %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM tw.capital_changes
                    WHERE stock_id = %s AND effective_date = %s
                      AND event_type = 'REDUCTION'
                )
                """,
                (stock_id, eff, ratio, note, stock_id, eff),
            )
            saved += cur.rowcount
    return saved


def scrape_date(trade_date: date) -> ScrapeResult:
    """Ask FinMind about every stock that resumed trading recently.

    Most runs find no new reduction, so an empty result is normal -- the
    daily_update registry must not expect rows from this scraper.
    """
    candidates = _candidates(trade_date)
    print(f"Fetching capital reductions ({trade_date}), "
          f"{len(candidates)} recent resumptions ...")
    parsed, api_rows, failed = [], 0, []
    start = str(date(trade_date.year - 1, 1, 1))
    for stock_id in candidates:
        rows = _fetch(DATASET, stock_id, start, str(trade_date))
        if rows is None:
            failed.append(stock_id)
            continue
        api_rows += len(rows)
        parsed.extend(_parse(stock_id, rows))
    saved = _save(parsed)
    print(f"  Capital reductions: {len(parsed)} events found, {saved} new, "
          f"{len(failed)} fetch errors{': ' + ', '.join(failed) if failed else ''}")
    # Any candidate could be the reduction post_break_blackout needs, so every
    # unanswered one is reported. Failures used to go into parse_errors, whose
    # denominator (api_rows) counts rows, not stocks.
    return ScrapeResult(records=saved, api_rows=api_rows, parse_errors=0,
                        failed_sources=tuple(f"FinMind {s}" for s in failed))


if __name__ == "__main__":
    scrape_date(date.today())
