-- Taiwan stock market schema
-- Data sources: TWSE (上市), TPEx (上櫃)

CREATE SCHEMA IF NOT EXISTS tw;

-- Stock basic info
CREATE TABLE IF NOT EXISTS tw.stocks (
    stock_id    VARCHAR(10) PRIMARY KEY,   -- e.g. '2330'
    name        VARCHAR(100) NOT NULL,      -- e.g. '台積電'
    market      VARCHAR(10) NOT NULL,       -- 'TWSE' (上市) or 'TPEx' (上櫃)
    security_type VARCHAR(20),              -- 'STOCK', 'EQUITY_ETF', 'BOND_ETF'
    industry    VARCHAR(50),                -- industry category
    listed_date DATE,                       -- IPO date
    is_active   BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Daily price data (regular session + after-hours + odd-lot)
CREATE TABLE IF NOT EXISTS tw.daily_prices (
    stock_id        VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    trade_date      DATE NOT NULL,
    -- Regular session (一般交易)
    open_price      NUMERIC(12, 2),
    high_price      NUMERIC(12, 2),
    low_price       NUMERIC(12, 2),
    close_price     NUMERIC(12, 2),
    volume          BIGINT,                 -- shares traded
    turnover        BIGINT,                 -- total value (NTD)
    transaction_count INTEGER,              -- number of transactions
    change          NUMERIC(12, 2),         -- price change
    change_pct      NUMERIC(8, 4),          -- price change %
    -- After-hours fixed-price session (盤後定價交易)
    ah_price        NUMERIC(12, 2),
    ah_volume       BIGINT,
    ah_turnover     BIGINT,
    ah_tx_count     INTEGER,
    -- Odd-lot regular session (零股一般交易)
    ol_price        NUMERIC(12, 2),
    ol_volume       BIGINT,
    ol_turnover     BIGINT,
    ol_tx_count     INTEGER,
    -- Odd-lot after-hours session (零股盤後定價)
    ol_ah_price     NUMERIC(12, 2),
    ol_ah_volume    BIGINT,
    ol_ah_turnover  BIGINT,
    ol_ah_tx_count  INTEGER,
    PRIMARY KEY (stock_id, trade_date)
);

-- Dividend records (除權除息)
CREATE TABLE IF NOT EXISTS tw.dividends (
    id              SERIAL PRIMARY KEY,
    stock_id        VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    ex_date         DATE NOT NULL,          -- 除權/除息日
    cash_dividend   NUMERIC(10, 4),         -- 現金股利
    stock_dividend  NUMERIC(10, 4),         -- 股票股利
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Monthly revenue (月營收)
CREATE TABLE IF NOT EXISTS tw.monthly_revenue (
    stock_id        VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    year_month      VARCHAR(7) NOT NULL,    -- e.g. '2026-03'
    revenue         BIGINT NOT NULL,        -- monthly revenue (thousands NTD)
    mom_pct         NUMERIC(12, 2),         -- month-over-month %
    yoy_pct         NUMERIC(12, 2),         -- year-over-year %
    PRIMARY KEY (stock_id, year_month)
);

-- Index for common queries
CREATE INDEX IF NOT EXISTS idx_tw_daily_prices_date ON tw.daily_prices (trade_date);
CREATE INDEX IF NOT EXISTS idx_tw_dividends_stock ON tw.dividends (stock_id, ex_date);
CREATE INDEX IF NOT EXISTS idx_tw_monthly_revenue_date ON tw.monthly_revenue (year_month);