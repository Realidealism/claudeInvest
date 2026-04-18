-- Fund/ETF tracking: managers, fund registry, monthly/quarterly holdings

-- Fund managers
CREATE TABLE IF NOT EXISTS tw.fund_managers (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(50) NOT NULL,
    company     VARCHAR(50) NOT NULL,
    UNIQUE (name, company)
);

-- Fund & ETF registry
CREATE TABLE IF NOT EXISTS tw.funds (
    id              SERIAL PRIMARY KEY,
    code            VARCHAR(20) NOT NULL UNIQUE,  -- ETF: '00981A', fund: SITCA fund code
    name            VARCHAR(100) NOT NULL,
    manager_id      INT REFERENCES tw.fund_managers(id),
    fund_type       VARCHAR(10) NOT NULL CHECK (fund_type IN ('fund', 'etf')),
    company         VARCHAR(50) NOT NULL,
    source          VARCHAR(30),                  -- 'ezmoney', 'capitalfund', 'sitca', etc.
    source_params   JSONB,                        -- source-specific params (fund_code, fund_id, etc.)
    is_active       BOOLEAN NOT NULL DEFAULT TRUE
);

-- Monthly report: Top 10 holdings
CREATE TABLE IF NOT EXISTS tw.fund_holdings_monthly (
    fund_id     INT NOT NULL REFERENCES tw.funds(id),
    period      VARCHAR(6) NOT NULL,              -- 'YYYYMM'
    ticker      VARCHAR(10) NOT NULL,
    ticker_name VARCHAR(100) NOT NULL,
    rank        INT,                              -- 1-10
    weight      NUMERIC(8, 4),                    -- portfolio weight (%)
    PRIMARY KEY (fund_id, period, ticker)
);

CREATE INDEX IF NOT EXISTS idx_fund_holdings_monthly_period
    ON tw.fund_holdings_monthly (period);

CREATE INDEX IF NOT EXISTS idx_fund_holdings_monthly_ticker
    ON tw.fund_holdings_monthly (ticker);

-- Quarterly report: all holdings >= 1% NAV
CREATE TABLE IF NOT EXISTS tw.fund_holdings_quarterly (
    fund_id     INT NOT NULL REFERENCES tw.funds(id),
    period      VARCHAR(6) NOT NULL,              -- 'YYYYQ1' ~ 'YYYYQ4'
    ticker      VARCHAR(10) NOT NULL,
    ticker_name VARCHAR(100) NOT NULL,
    weight      NUMERIC(8, 4),                    -- portfolio weight (%)
    PRIMARY KEY (fund_id, period, ticker)
);

CREATE INDEX IF NOT EXISTS idx_fund_holdings_quarterly_period
    ON tw.fund_holdings_quarterly (period);

CREATE INDEX IF NOT EXISTS idx_fund_holdings_quarterly_ticker
    ON tw.fund_holdings_quarterly (ticker);
