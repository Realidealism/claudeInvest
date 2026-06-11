-- Disposal prediction audit log.
--
-- For each (audit_date, stock_id) we record:
--   predicted: bot would have warned "明日進處置" at EOD (audit_date - 1 td)
--   actual:    TWSE actually announced disposal with period_start = audit_date
--   kuan_counts: per-款 counts the bot saw (JSONB, for later threshold tuning)
--   actual_reason: TWSE's announced disposal reason (for triggered rows only)
--
-- A discrepancy is any row where predicted != actual. The audit script
-- pushes those to Telegram and keeps the full history in this table so
-- empirical thresholds for non-第1款 rules can be derived later.
--
-- audit_date is the day disposal would START (= next_trading_day of the
-- EOD on which the bot would have made its call).

CREATE TABLE IF NOT EXISTS tw.disposal_prediction_audit (
    audit_date      DATE NOT NULL,
    stock_id        VARCHAR(10) NOT NULL,
    predicted       BOOLEAN NOT NULL,
    actual          BOOLEAN NOT NULL,
    kuan_counts     JSONB,
    actual_reason   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (audit_date, stock_id)
);

CREATE INDEX IF NOT EXISTS idx_disposal_audit_discrepancy
    ON tw.disposal_prediction_audit (audit_date)
    WHERE predicted != actual;
