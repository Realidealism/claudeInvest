-- US stock market schema

CREATE SCHEMA IF NOT EXISTS us;

-- Stock basic info
CREATE TABLE us.stocks (
    ticker      VARCHAR(10) PRIMARY KEY,    -- e.g. 'AAPL'
    name        VARCHAR(200) NOT NULL,       -- e.g. 'Apple Inc.'
    exchange    VARCHAR(20),                 -- 'NYSE', 'NASDAQ', 'AMEX'
    sector      VARCHAR(100),
    industry    VARCHAR(100),
    is_active   BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Daily price data
CREATE TABLE us.daily_prices (
    ticker          VARCHAR(10) NOT NULL REFERENCES us.stocks(ticker),
    trade_date      DATE NOT NULL,
    open_price      NUMERIC(12, 4),
    high_price      NUMERIC(12, 4),
    low_price       NUMERIC(12, 4),
    close_price     NUMERIC(12, 4),
    adj_close       NUMERIC(12, 4),         -- adjusted close (splits/dividends)
    volume          BIGINT,
    PRIMARY KEY (ticker, trade_date)
);

-- Dividend records
CREATE TABLE us.dividends (
    id              SERIAL PRIMARY KEY,
    ticker          VARCHAR(10) NOT NULL REFERENCES us.stocks(ticker),
    ex_date         DATE NOT NULL,
    amount          NUMERIC(10, 4),         -- dividend per share (USD)
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Index for common queries
CREATE INDEX idx_us_daily_prices_date ON us.daily_prices (trade_date);
CREATE INDEX idx_us_dividends_ticker ON us.dividends (ticker, ex_date);