-- One row per snapshot run: which build wrote the snapshot rows of a date
-- (daily) or of a pass (intraday). The rows themselves carry no version, and
-- the snapshots import the backtest factories, so the build commit is the
-- version that produced them. Written by analysis/snapshot_runs.py.
CREATE TABLE IF NOT EXISTS tw.snapshot_runs (
    kind           TEXT        NOT NULL CHECK (kind IN ('daily', 'intraday')),
    snapshot_date  DATE        NOT NULL,
    snapshot_time  TIMESTAMPTZ,              -- the intraday pass; NULL for daily
    build_commit   TEXT        NOT NULL,     -- commit built from, '+dirty' if not clean
    history_days   INTEGER,                  -- backtest window behind open positions;
                                             -- NULL: every bar on record (intraday)
    stocks_loaded  INTEGER     NOT NULL,     -- universe after the dead-fish filter
    stocks_scored  INTEGER     NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((kind = 'daily') = (snapshot_time IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS snapshot_runs_daily_uq
    ON tw.snapshot_runs (snapshot_date) WHERE kind = 'daily';
CREATE UNIQUE INDEX IF NOT EXISTS snapshot_runs_intraday_uq
    ON tw.snapshot_runs (snapshot_date, snapshot_time) WHERE kind = 'intraday';
