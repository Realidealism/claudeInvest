-- ETF holdings tracking
-- Track daily holdings for selected active ETFs and detect changes

-- Daily ETF holdings snapshot
CREATE TABLE IF NOT EXISTS tw.etf_holdings (
    etf_id      VARCHAR(10) NOT NULL,       -- e.g. '00981A'
    trade_date  DATE NOT NULL,
    stock_id    VARCHAR(10) NOT NULL,        -- held stock code
    stock_name  VARCHAR(100) NOT NULL,
    shares      BIGINT NOT NULL,             -- number of shares held
    weight      NUMERIC(8, 4),               -- portfolio weight (%)
    PRIMARY KEY (etf_id, trade_date, stock_id)
);

CREATE INDEX IF NOT EXISTS idx_etf_holdings_date
    ON tw.etf_holdings (trade_date);

-- Daily ETF holdings changes (diff between consecutive trading days)
CREATE TABLE IF NOT EXISTS tw.etf_holdings_diff (
    etf_id      VARCHAR(10) NOT NULL,
    trade_date  DATE NOT NULL,               -- date the change was detected
    stock_id    VARCHAR(10) NOT NULL,
    stock_name  VARCHAR(100) NOT NULL,
    change_type VARCHAR(10) NOT NULL,        -- 'added', 'removed', 'increased', 'decreased'
    prev_shares BIGINT,                      -- NULL if added
    curr_shares BIGINT,                      -- NULL if removed
    share_diff  BIGINT,                      -- curr - prev (NULL if added/removed)
    prev_weight NUMERIC(8, 4),
    curr_weight NUMERIC(8, 4),
    weight_diff NUMERIC(8, 4),               -- curr - prev
    PRIMARY KEY (etf_id, trade_date, stock_id)
);

CREATE INDEX IF NOT EXISTS idx_etf_holdings_diff_date
    ON tw.etf_holdings_diff (trade_date);

CREATE INDEX IF NOT EXISTS idx_etf_holdings_diff_type
    ON tw.etf_holdings_diff (change_type);
