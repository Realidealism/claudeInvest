-- Add trend classification columns to market breadth snapshot.
-- Encoding: -2=strong_bear, -1=bear, 0=neutral, 1=bull, 2=strong_bull
-- short/medium/long_trend pre-existed as smallint.
--
-- Historical note: this migration originally also ADDed
-- short/medium/long_trend_forming, which migration 025 immediately DROPped
-- in favour of trend_total. The ADD/DROP cycle leaked 3 attisdropped slots
-- per init_db() run (PG never reclaims them without a table rewrite).
-- The trend_forming ADDs (and a defensive DO block that re-typed them
-- from TEXT) have been removed; 025's DROP IF EXISTS becomes a no-op on
-- both fresh and post-migration DBs.
ALTER TABLE tw.market_breadth
    ADD COLUMN IF NOT EXISTS short_trend  SMALLINT,
    ADD COLUMN IF NOT EXISTS medium_trend SMALLINT,
    ADD COLUMN IF NOT EXISTS long_trend   SMALLINT;
