-- Portfolio schema (shared across markets)

CREATE SCHEMA IF NOT EXISTS portfolio;

-- Portfolio groups
CREATE TABLE portfolio.portfolios (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,      -- e.g. 'Main', 'Speculative'
    description TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Transaction records
CREATE TABLE portfolio.transactions (
    id              SERIAL PRIMARY KEY,
    portfolio_id    INTEGER NOT NULL REFERENCES portfolio.portfolios(id),
    market          VARCHAR(10) NOT NULL,    -- 'TW' or 'US'
    symbol          VARCHAR(10) NOT NULL,    -- stock_id or ticker
    action          VARCHAR(4) NOT NULL,     -- 'BUY' or 'SELL'
    trade_date      DATE NOT NULL,
    quantity        NUMERIC(12, 4) NOT NULL, -- shares (TW: units of 1000 for round lots)
    price           NUMERIC(12, 4) NOT NULL,
    fee             NUMERIC(10, 2) DEFAULT 0,   -- broker fee
    tax             NUMERIC(10, 2) DEFAULT 0,   -- transaction tax
    currency        VARCHAR(3) NOT NULL,     -- 'TWD' or 'USD'
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Watchlist
CREATE TABLE portfolio.watchlist (
    id          SERIAL PRIMARY KEY,
    market      VARCHAR(10) NOT NULL,       -- 'TW' or 'US'
    symbol      VARCHAR(10) NOT NULL,
    added_at    TIMESTAMPTZ DEFAULT NOW(),
    notes       TEXT,
    UNIQUE (market, symbol)
);

-- Research notes
CREATE TABLE portfolio.notes (
    id          SERIAL PRIMARY KEY,
    market      VARCHAR(10) NOT NULL,
    symbol      VARCHAR(10) NOT NULL,
    title       VARCHAR(200),
    content     TEXT NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Index
CREATE INDEX idx_portfolio_tx_symbol ON portfolio.transactions (market, symbol);
CREATE INDEX idx_portfolio_tx_date ON portfolio.transactions (trade_date);
CREATE INDEX idx_portfolio_watchlist_market ON portfolio.watchlist (market);