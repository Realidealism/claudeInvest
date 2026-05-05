-- Daily snapshot of signal-factory fires.
-- Written by analysis/signal_snapshot.py at the end of daily_update.
-- One row per (snapshot_date, signal, stock_id). Lists every active stock
-- whose latest bar triggered one of the 6 factory conditions.

CREATE TABLE IF NOT EXISTS tw.signal_snapshot (
    snapshot_date  DATE         NOT NULL,
    signal         VARCHAR(16)  NOT NULL,
    stock_id       VARCHAR(10)  NOT NULL REFERENCES tw.stocks(stock_id),
    turnover       NUMERIC(20, 2),
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, signal, stock_id),
    CONSTRAINT signal_snapshot_signal_chk CHECK (
        signal IN ('pick', 'touch', 'buy', 'sell', 'buy_flee', 'sell_flee')
    )
);

CREATE INDEX IF NOT EXISTS idx_signal_snap_date
    ON tw.signal_snapshot (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_signal_snap_date_signal
    ON tw.signal_snapshot (snapshot_date, signal);
