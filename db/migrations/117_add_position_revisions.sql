-- One row per position held (not exited) in the previous nightly snapshot
-- that the next nightly run neither still holds nor exits that day:
--   redated        the rerun holds the stock on that side from another entry
--                  date (new_is_exited: that new position exited today)
--   gone           the rerun has no position on that side
--   not_run        the stock was not evaluated
--   exited_unseen  the position exited between the two snapshots (a night
--                  with no snapshot), on exit_date; a real exit, not a revision
-- Written by analysis.daily_snapshot with that date's tw.open_positions, in
-- the same transaction.
CREATE TABLE IF NOT EXISTS tw.position_revisions (
    snapshot_date       DATE          NOT NULL,  -- the run that dropped it
    prev_snapshot_date  DATE          NOT NULL,  -- the run that last held it
    stock_id            VARCHAR(10)   NOT NULL REFERENCES tw.stocks(stock_id),
    side                VARCHAR(5)    NOT NULL CHECK (side IN ('long', 'short')),
    entry_date          DATE          NOT NULL,  -- as held on prev_snapshot_date
    entry_price         NUMERIC(10,2) NOT NULL,
    entry_tier          VARCHAR(16)   NOT NULL,
    cause               TEXT          NOT NULL
        CHECK (cause IN ('redated', 'gone', 'not_run', 'exited_unseen')),
    new_entry_date      DATE,                    -- redated only
    new_is_exited       BOOLEAN,                 -- redated only
    exit_date           DATE,                    -- exited_unseen only
    build_commit        TEXT          NOT NULL,
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (snapshot_date, stock_id, side, entry_date),
    CHECK ((cause = 'redated') = (new_entry_date IS NOT NULL)),
    CHECK ((cause = 'redated') = (new_is_exited IS NOT NULL)),
    CHECK ((cause = 'exited_unseen') = (exit_date IS NOT NULL))
);
