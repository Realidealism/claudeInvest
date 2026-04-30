-- Add trend classification columns to market breadth snapshot.
-- Encoding: -2=strong_bear, -1=bear, 0=neutral, 1=bull, 2=strong_bull
-- short/medium/long_trend pre-existed as smallint; forming columns added here.
ALTER TABLE tw.market_breadth
    ADD COLUMN IF NOT EXISTS short_trend          SMALLINT,
    ADD COLUMN IF NOT EXISTS medium_trend         SMALLINT,
    ADD COLUMN IF NOT EXISTS long_trend           SMALLINT,
    ADD COLUMN IF NOT EXISTS short_trend_forming   SMALLINT,
    ADD COLUMN IF NOT EXISTS medium_trend_forming  SMALLINT,
    ADD COLUMN IF NOT EXISTS long_trend_forming    SMALLINT;

-- Drop and recreate forming columns if they were already created as TEXT by earlier run.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema='tw' AND table_name='market_breadth'
                 AND column_name='short_trend_forming' AND data_type='text') THEN
        ALTER TABLE tw.market_breadth
            DROP COLUMN short_trend_forming,
            DROP COLUMN medium_trend_forming,
            DROP COLUMN long_trend_forming;
        ALTER TABLE tw.market_breadth
            ADD COLUMN short_trend_forming  SMALLINT,
            ADD COLUMN medium_trend_forming SMALLINT,
            ADD COLUMN long_trend_forming   SMALLINT;
    END IF;
END $$;
