-- Track positions that exited on the snapshot day in the daily snapshot
-- table. Mirrors 048_open_positions_intraday_exits for tw.open_positions.
--
-- For exited rows:
--   * current_close holds the exit price (= snapshot-day bar)
--   * pnl_pct is realized pnl
--   * exit_reason is the engine's exit reason (e.g. '破21日均線[buy]')
--   * defense_* columns carry the last pre-exit defense event for
--     uniformity with the unexited path; consumers use is_exited to skip.

ALTER TABLE tw.open_positions
    ADD COLUMN IF NOT EXISTS is_exited   BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS exit_reason VARCHAR(128);

CREATE INDEX IF NOT EXISTS idx_open_pos_exited
    ON tw.open_positions (snapshot_date, is_exited);
