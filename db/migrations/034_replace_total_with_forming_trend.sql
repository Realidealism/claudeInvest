-- Drop total count columns (now computed as normal + forming at runtime).
-- Rename trend_total to reuse for total-based trend (computed from normal+forming).
-- Encoding: -3=bear_exhausting, -2=strong_bear, -1=bear, 0=neutral,
--           1=bull, 2=strong_bull, 3=bull_exhausting
ALTER TABLE tw.market_breadth
    DROP COLUMN IF EXISTS short_up_total,
    DROP COLUMN IF EXISTS short_down_total,
    DROP COLUMN IF EXISTS medium_up_total,
    DROP COLUMN IF EXISTS medium_down_total,
    DROP COLUMN IF EXISTS long_up_total,
    DROP COLUMN IF EXISTS long_down_total;

-- Ensure trend_total columns exist (may already exist from migration 025)
ALTER TABLE tw.market_breadth
    ADD COLUMN IF NOT EXISTS short_trend_total  SMALLINT,
    ADD COLUMN IF NOT EXISTS medium_trend_total SMALLINT,
    ADD COLUMN IF NOT EXISTS long_trend_total   SMALLINT;

-- Drop trend_forming if it was created by a prior run of this migration
ALTER TABLE tw.market_breadth
    DROP COLUMN IF EXISTS short_trend_forming,
    DROP COLUMN IF EXISTS medium_trend_forming,
    DROP COLUMN IF EXISTS long_trend_forming;
