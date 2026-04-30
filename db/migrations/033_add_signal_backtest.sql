-- Signal-driven backtest results (one row per completed round-trip trade)

CREATE TABLE IF NOT EXISTS tw.signal_backtest_results (
    id              SERIAL PRIMARY KEY,
    ticker          VARCHAR(10) NOT NULL,
    ticker_name     VARCHAR(100) NOT NULL,
    entry_signal    VARCHAR(40) NOT NULL,
    entry_period    VARCHAR(10) NOT NULL,
    entry_date      DATE NOT NULL,
    entry_price     NUMERIC(10, 2),
    exit_signal     VARCHAR(40),
    exit_period     VARCHAR(10),
    exit_date       DATE,
    exit_price      NUMERIC(10, 2),
    return_pct      NUMERIC(10, 6),
    holding_days    INT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (ticker, entry_period, entry_signal)
);

CREATE INDEX IF NOT EXISTS idx_signal_bt_ticker ON tw.signal_backtest_results (ticker);
CREATE INDEX IF NOT EXISTS idx_signal_bt_entry ON tw.signal_backtest_results (entry_signal);
