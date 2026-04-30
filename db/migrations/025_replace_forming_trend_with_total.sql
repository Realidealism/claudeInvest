-- Replace forming trend columns with total (normal + forming) trend columns.
-- Encoding unchanged: -3=bear_weakening, -2=strong_bear, -1=bear, 0=neutral,
--                     1=bull, 2=strong_bull, 3=bull_weakening
ALTER TABLE tw.market_breadth
    DROP COLUMN IF EXISTS short_trend_forming,
    DROP COLUMN IF EXISTS medium_trend_forming,
    DROP COLUMN IF EXISTS long_trend_forming,
    ADD COLUMN IF NOT EXISTS short_trend_total  SMALLINT,
    ADD COLUMN IF NOT EXISTS medium_trend_total SMALLINT,
    ADD COLUMN IF NOT EXISTS long_trend_total   SMALLINT;
