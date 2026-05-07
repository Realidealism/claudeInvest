-- Track positions that exited intraday — i.e. were open going into today
-- (or earlier) but the engine emitted a real exit (non-end-of-history)
-- on today's projected bar.
--
-- For exited rows:
--   * current_close holds the exit price (= projected today bar)
--   * pnl_pct is realized pnl
--   * exit_reason is the engine's exit reason (e.g. '破21日均線[buy]')
--   * defense_* columns are left NULL (no further defense after exit)

ALTER TABLE tw.open_positions_intraday
    ADD COLUMN IF NOT EXISTS is_exited   BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS exit_reason VARCHAR(128);

CREATE INDEX IF NOT EXISTS idx_open_pos_intraday_exited
    ON tw.open_positions_intraday (snapshot_date, is_exited);
