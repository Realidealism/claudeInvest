-- Add previous 2 trading-bar scores to score_snapshot rows so the frontend
-- can show short-term trend without having to keep history of every stock's
-- score across days.

ALTER TABLE tw.score_snapshot
    ADD COLUMN IF NOT EXISTS pct_d1 NUMERIC(7, 3),  -- side pct evaluated at data.n-2
    ADD COLUMN IF NOT EXISTS pct_d2 NUMERIC(7, 3);  -- side pct evaluated at data.n-3
