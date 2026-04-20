-- Strategy signals detected from fund/ETF holdings cross-reference

CREATE TABLE IF NOT EXISTS tw.signals (
    id              SERIAL PRIMARY KEY,
    signal_type     VARCHAR(40) NOT NULL,
    ticker          VARCHAR(10) NOT NULL,
    ticker_name     VARCHAR(100) NOT NULL,
    funds           TEXT[] NOT NULL,              -- fund names involved
    trigger_date    DATE NOT NULL,
    trigger_period  VARCHAR(10) NOT NULL,         -- '202604M' / '202601Q' / '2026-04-15'
    weight_change   NUMERIC(8, 4),                -- percentage points
    evidence        JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (signal_type, ticker, trigger_period)
);

CREATE INDEX IF NOT EXISTS idx_signals_type ON tw.signals (signal_type);
CREATE INDEX IF NOT EXISTS idx_signals_ticker ON tw.signals (ticker);
CREATE INDEX IF NOT EXISTS idx_signals_period ON tw.signals (trigger_period);
CREATE INDEX IF NOT EXISTS idx_signals_date ON tw.signals (trigger_date);
