-- CNN Fear & Greed Index (https://production.dataviz.cnn.io/index/fearandgreed)
-- Daily snapshot of the headline 0-100 score + 7 sub-indicators.
--
-- headline `score` is the published 0-100 composite.
-- sub-indicator `*_score` is the RAW underlying value (e.g. VIX level,
--   put/call ratio, McClellan Volume Summation Index, S&P500 vs 125-day
--   moving average), which can be negative or thousands.  CNN's own
--   normalisation lives in `*_rating` (extreme fear / fear / neutral /
--   greed / extreme greed), which we surface directly.
CREATE TABLE IF NOT EXISTS tw.cnn_fear_greed (
    trade_date           DATE PRIMARY KEY,
    score                NUMERIC(5,2),
    rating               TEXT,

    momentum_score       DOUBLE PRECISION,  momentum_rating       TEXT,
    strength_score       DOUBLE PRECISION,  strength_rating       TEXT,
    breadth_score        DOUBLE PRECISION,  breadth_rating        TEXT,
    put_call_score       DOUBLE PRECISION,  put_call_rating       TEXT,
    safe_haven_score     DOUBLE PRECISION,  safe_haven_rating     TEXT,
    junk_bond_score      DOUBLE PRECISION,  junk_bond_rating      TEXT,
    volatility_score     DOUBLE PRECISION,  volatility_rating     TEXT,

    fetched_at           TIMESTAMPTZ DEFAULT NOW()
);

-- Migrate columns that may have been created with NUMERIC(5,2) on an
-- earlier run of this migration (raw sub-indicator values overflow that).
ALTER TABLE tw.cnn_fear_greed
    ALTER COLUMN momentum_score   TYPE DOUBLE PRECISION,
    ALTER COLUMN strength_score   TYPE DOUBLE PRECISION,
    ALTER COLUMN breadth_score    TYPE DOUBLE PRECISION,
    ALTER COLUMN put_call_score   TYPE DOUBLE PRECISION,
    ALTER COLUMN safe_haven_score TYPE DOUBLE PRECISION,
    ALTER COLUMN junk_bond_score  TYPE DOUBLE PRECISION,
    ALTER COLUMN volatility_score TYPE DOUBLE PRECISION;
