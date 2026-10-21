-- tw.dividends gets the row's source: 'finmind' (hermit backfill, the rows
-- written 2026-04-29) or 'twse' / 'tpex' (the exchanges' ex-rights results,
-- scrapers.ex_rights_results, daily from 2026-10-21, backfilled from
-- 2026-04-30). One row per stock, ex-date and source.
ALTER TABLE tw.dividends ADD COLUMN IF NOT EXISTS source VARCHAR(16) NOT NULL DEFAULT 'finmind';
CREATE UNIQUE INDEX IF NOT EXISTS uq_dividends_stock_date_source
    ON tw.dividends (stock_id, ex_date, source);
