-- Run record for the nightly daily_update pipeline.
--
-- Until now the only trace of a nightly run was its Telegram summary. A step
-- could fail, the process still exited 0, and when the summary could not be
-- delivered nothing was left at all; nothing read the exit code either. The
-- morning health check had no way to tell "ran clean" from "ran and failed"
-- from "never ran".
--
-- Each weekday run now writes one row: 'running' when it starts, the final
-- outcome when it ends. A run that died (scheduler time limit, power loss)
-- leaves 'running' behind, which is itself the signal. The health check reads
-- the newest row per trade_date.
--
-- outcome:
--   success  no step failed and none was blocked
--   partial  only non-core steps failed
--   failed   a core step failed or was blocked, or the run raised
--   aborted  the trading-day gate could not establish the date
--   holiday  the gate found the market closed (or the date is in the future)

CREATE TABLE IF NOT EXISTS tw.daily_update_runs (
    id           BIGSERIAL PRIMARY KEY,
    trade_date   DATE NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at  TIMESTAMPTZ,
    outcome      TEXT NOT NULL DEFAULT 'running'
                 CHECK (outcome IN ('running', 'success', 'partial', 'failed', 'aborted', 'holiday')),
    summary_sent BOOLEAN,
    steps        JSONB
);

CREATE INDEX IF NOT EXISTS daily_update_runs_date_idx
    ON tw.daily_update_runs (trade_date, started_at DESC);

COMMENT ON TABLE tw.daily_update_runs IS
    'One row per daily_update run of a weekday. Written as running at the start and finalised at the end, so a run that died leaves running behind.';
COMMENT ON COLUMN tw.daily_update_runs.steps IS
    'Array of {label, status, core, trace}; status is ok, failed, skip or blocked.';
COMMENT ON COLUMN tw.daily_update_runs.summary_sent IS
    'Whether the Telegram summary was delivered. NULL when no summary was attempted.';
