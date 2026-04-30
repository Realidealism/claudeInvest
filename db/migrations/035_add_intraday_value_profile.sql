-- Intraday market-wide cumulative value profile.
-- Each row stores the total market turnover (TSE+OTC) at a 5-minute bucket.
-- Used to build the h(t) curve for estimating full-day volume/value.
--
-- The sweeper writes one row per bucket per day. ON CONFLICT takes the
-- greater value since cumulative turnover only increases within a session.

CREATE TABLE IF NOT EXISTS tw.intraday_value_profile (
    trade_date          DATE         NOT NULL,
    time_bucket         VARCHAR(5)   NOT NULL,   -- 'HH:MM', e.g. '09:05'
    market_total_value  BIGINT       NOT NULL,   -- TSE+OTC cumulative turnover (NTD)
    updated_at          TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (trade_date, time_bucket)
);
