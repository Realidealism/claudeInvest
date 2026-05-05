-- Add pct_d3 (score evaluated at data.n-4) so the frontend can colour the
-- 前2日 column with day-over-day delta against an even earlier bar.

ALTER TABLE tw.score_snapshot
    ADD COLUMN IF NOT EXISTS pct_d3 NUMERIC(7, 3);
