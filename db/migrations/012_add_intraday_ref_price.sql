-- Add reference price (參考價) to the intraday snapshot.
--
-- Source: SinoPac Shioaji Contracts.Stocks.<TSE|OTC>[code].reference, fetched
-- once per trading day before the market opens by pre_market_update.py.
--
-- The same pre-market path also refreshes limit_up / limit_down (which are
-- computed from reference ± 10%) so all three columns are written together
-- and treated as authoritative for the day.

ALTER TABLE tw.intraday_quotes
    ADD COLUMN IF NOT EXISTS ref_price NUMERIC(12, 4);
