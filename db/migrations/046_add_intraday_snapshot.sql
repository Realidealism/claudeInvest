-- Intraday (12:50) preview of ScoreBoard rankings + 6 signal-factory fires.
-- Written by intraday_snapshot.py once per trading day after the morning session
-- has completed enough volume that the h(t) full-day estimator is reasonably
-- stable. Mirrors tw.score_snapshot / tw.signal_snapshot but keeps a separate
-- table so backtest queries against the daily snapshot remain clean.
--
-- snapshot_time captures the wall-clock TPE moment of the capture so multiple
-- intraday cuts per day are supported in the future even though we only run
-- one at 12:50 today.

CREATE TABLE IF NOT EXISTS tw.score_snapshot_intraday (
    snapshot_date  DATE         NOT NULL,
    snapshot_time  TIMESTAMPTZ  NOT NULL,
    side           VARCHAR(5)   NOT NULL,
    rank           INTEGER      NOT NULL,
    stock_id       VARCHAR(10)  NOT NULL REFERENCES tw.stocks(stock_id),
    total_pct      NUMERIC(7, 3) NOT NULL,
    turnover       NUMERIC(20, 2),
    -- Diff flags vs the previous trading day's tw.score_snapshot row
    is_new         BOOLEAN      NOT NULL DEFAULT FALSE,
    prev_rank      INTEGER,
    rank_delta     INTEGER,
    pct_d1         NUMERIC(7, 3),
    pct_d2         NUMERIC(7, 3),
    pct_d3         NUMERIC(7, 3),
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, snapshot_time, side, stock_id),
    CONSTRAINT score_snapshot_intraday_side_chk CHECK (side IN ('long', 'short'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_score_snap_intraday_date_time_side_rank
    ON tw.score_snapshot_intraday (snapshot_date, snapshot_time, side, rank);
CREATE INDEX IF NOT EXISTS idx_score_snap_intraday_date
    ON tw.score_snapshot_intraday (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_score_snap_intraday_stock
    ON tw.score_snapshot_intraday (stock_id);


CREATE TABLE IF NOT EXISTS tw.signal_snapshot_intraday (
    snapshot_date  DATE         NOT NULL,
    snapshot_time  TIMESTAMPTZ  NOT NULL,
    signal         VARCHAR(16)  NOT NULL,
    stock_id       VARCHAR(10)  NOT NULL REFERENCES tw.stocks(stock_id),
    turnover       NUMERIC(20, 2),
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, snapshot_time, signal, stock_id),
    CONSTRAINT signal_snapshot_intraday_signal_chk CHECK (
        signal IN ('pick', 'touch', 'buy', 'sell', 'buy_flee', 'sell_flee')
    )
);

CREATE INDEX IF NOT EXISTS idx_signal_snap_intraday_date
    ON tw.signal_snapshot_intraday (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_signal_snap_intraday_date_signal
    ON tw.signal_snapshot_intraday (snapshot_date, signal);
