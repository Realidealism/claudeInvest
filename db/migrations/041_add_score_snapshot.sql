-- Daily snapshot of ScoreBoard top-N rankings (long side + short side).
-- Written by analysis/score_snapshot.py at the end of daily_update.
-- One row per (snapshot_date, side, stock_id). Top-100 each side.

CREATE TABLE IF NOT EXISTS tw.score_snapshot (
    snapshot_date  DATE         NOT NULL,
    side           VARCHAR(5)   NOT NULL,                 -- 'long' or 'short'
    rank           INTEGER      NOT NULL,                 -- 1 = best
    stock_id       VARCHAR(10)  NOT NULL REFERENCES tw.stocks(stock_id),
    total_pct      NUMERIC(7, 3) NOT NULL,                -- BoardResult.total.{side}.pct, range -100..+100
    turnover       NUMERIC(20, 2),                        -- TWD on snapshot bar (tie-breaker)
    -- Diff flags vs previous snapshot of the same side
    is_new         BOOLEAN      NOT NULL DEFAULT FALSE,
    prev_rank      INTEGER,
    rank_delta     INTEGER,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, side, stock_id),
    CONSTRAINT score_snapshot_side_chk CHECK (side IN ('long', 'short'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_score_snap_date_side_rank
    ON tw.score_snapshot (snapshot_date, side, rank);
CREATE INDEX IF NOT EXISTS idx_score_snap_date  ON tw.score_snapshot (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_score_snap_stock ON tw.score_snapshot (stock_id);
