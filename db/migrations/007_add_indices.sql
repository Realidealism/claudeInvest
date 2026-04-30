-- Market index daily data (指數日行情)
-- Covers TAIEX (加權指數) and TPEx Composite Index (櫃買指數)

CREATE TABLE IF NOT EXISTS tw.index_prices (
    index_id    VARCHAR(20)    NOT NULL,  -- e.g. 'TAIEX', 'TPEx'
    trade_date  DATE           NOT NULL,
    open_price  NUMERIC(12, 2),           -- 開盤指數
    high_price  NUMERIC(12, 2),           -- 最高指數
    low_price   NUMERIC(12, 2),           -- 最低指數
    close_price NUMERIC(12, 2),           -- 收盤指數
    change      NUMERIC(10, 2),           -- 漲跌點數
    change_pct  NUMERIC(8, 4),            -- 漲跌幅(%)
    volume      BIGINT,                   -- 成交股數 (shares)
    turnover    BIGINT,                   -- 成交金額 (NTD)
    tx_count    INTEGER,                  -- 成交筆數
    PRIMARY KEY (index_id, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_tw_index_prices_date ON tw.index_prices (trade_date);

-- Seed index definitions
CREATE TABLE IF NOT EXISTS tw.indices (
    index_id    VARCHAR(20) PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,    -- e.g. '台灣加權指數'
    market      VARCHAR(10)  NOT NULL,    -- 'TWSE' or 'TPEx'
    description TEXT
);

INSERT INTO tw.indices (index_id, name, market) VALUES
    ('TAIEX', '台灣加權股價指數', 'TWSE'),
    ('TPEx',  '櫃買指數',         'TPEx')
ON CONFLICT (index_id) DO NOTHING;
