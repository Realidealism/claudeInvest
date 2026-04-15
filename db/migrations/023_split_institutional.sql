-- Split institutional buy/sell into finer categories.
-- Previously foreign_* combined 外資 + 外資自營商, and dealer_* combined
-- 自行 + 避險. From now on:
--   foreign_*         = 外陸資 (不含外資自營商)
--   foreign_dealer_*  = 外資自營商
--   dealer_*          = 自營商 (自行買賣)
--   dealer_hedge_*    = 自營商 (避險)
-- Existing rows keep their old combined values until the scraper re-runs
-- against the new semantics; backfill via scrapers/institutional.py.

ALTER TABLE tw.daily_prices
    ADD COLUMN IF NOT EXISTS foreign_dealer_buy  BIGINT,
    ADD COLUMN IF NOT EXISTS foreign_dealer_sell BIGINT,
    ADD COLUMN IF NOT EXISTS foreign_dealer_net  BIGINT,
    ADD COLUMN IF NOT EXISTS dealer_hedge_buy    BIGINT,
    ADD COLUMN IF NOT EXISTS dealer_hedge_sell   BIGINT,
    ADD COLUMN IF NOT EXISTS dealer_hedge_net    BIGINT;
