-- Total counts are now union of normal + forming (no double-counting),
-- computed at runtime and stored in DB.
-- Ensure all required columns exist.
-- Encoding: -3=bear_exhausting, -2=strong_bear, -1=bear, 0=neutral,
--           1=bull, 2=strong_bull, 3=bull_exhausting
ALTER TABLE tw.market_breadth
    ADD COLUMN IF NOT EXISTS short_up_total    INTEGER,
    ADD COLUMN IF NOT EXISTS short_down_total  INTEGER,
    ADD COLUMN IF NOT EXISTS medium_up_total   INTEGER,
    ADD COLUMN IF NOT EXISTS medium_down_total INTEGER,
    ADD COLUMN IF NOT EXISTS long_up_total     INTEGER,
    ADD COLUMN IF NOT EXISTS long_down_total   INTEGER,
    ADD COLUMN IF NOT EXISTS short_trend_total  SMALLINT,
    ADD COLUMN IF NOT EXISTS medium_trend_total SMALLINT,
    ADD COLUMN IF NOT EXISTS long_trend_total   SMALLINT;

-- Drop trend_forming if it was created by a prior run of this migration
ALTER TABLE tw.market_breadth
    DROP COLUMN IF EXISTS short_trend_forming,
    DROP COLUMN IF EXISTS medium_trend_forming,
    DROP COLUMN IF EXISTS long_trend_forming;
