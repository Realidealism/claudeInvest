-- Global market quotes (commodities, FX, foreign indices) — one long table
-- keyed by (symbol, trade_date) so adding a new instrument never needs a
-- schema change, only a new row in scrapers/market_quote.py SYMBOLS.
--
-- symbol is our own stable key (e.g. 'wti', 'copper', 'sox'), NOT the
-- upstream ticker — the upstream ticker lives in the scraper and may be
-- swapped for a different source without rewriting history.
--
-- Only the raw close is persisted. Everything the dashboard shows on top of
-- it (day change %, TSMC ADR premium, 52-week range) is derived at export
-- time, same convention as tw.yield_curve (see 056_add_yield_curve.sql):
-- a revised upstream point then propagates without a derived-column backfill.
--
-- Non-daily series live here too: BDI is a working-day index we can only
-- scrape as a current value (no free history), and DRAM/NAND spot prices are
-- likewise same-day only. They simply accumulate one row per run.
CREATE TABLE IF NOT EXISTS tw.market_quote (
    symbol      TEXT NOT NULL,
    trade_date  DATE NOT NULL,
    close       NUMERIC(14, 4),
    fetched_at  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_market_quote_date
    ON tw.market_quote (trade_date DESC);
