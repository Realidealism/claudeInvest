-- Intraday metadata for tw.vix_tw.
--
-- During the TWSE session (09:00–13:45 TPE) we poll the TAIFEX MIS
-- getQuoteListVIX endpoint every ~15 min and upsert the latest snapshot
-- into tw.vix_tw. intraday_time stores the HHMMSS string of that
-- snapshot; NULL means the row holds the official end-of-day value
-- (written by the daily scraper after the session closes).
--
-- The frontend uses NULL vs non-NULL to flip the "盤中 HH:MM" label.
ALTER TABLE tw.vix_tw
    ADD COLUMN IF NOT EXISTS intraday_time TEXT;
