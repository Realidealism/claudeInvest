-- Intraday real-time quote snapshot table.
-- Fed by two layers:
--   1. REST sweeper: /v1.0/stock/snapshot/quotes/{TSE|OTC}, every ~20s, full market
--   2. WebSocket watcher: trades + books channels for watchlist symbols, millisecond updates
--
-- Storage policy: "latest snapshot only" — PK is stock_id alone, all updates overwrite.
-- The `source` column records which path wrote the most recent row for debugging.

CREATE TABLE IF NOT EXISTS tw.intraday_quotes (
    stock_id        VARCHAR(10) PRIMARY KEY REFERENCES tw.stocks(stock_id),
    trade_date      DATE,
    -- OHLC (filled by rest_sweep; ws_trade only updates last_price)
    open_price      NUMERIC(12, 4),
    high_price      NUMERIC(12, 4),
    low_price       NUMERIC(12, 4),
    last_price      NUMERIC(12, 4),
    -- Latest trade detail
    last_size       BIGINT,             -- shares in most recent trade
    last_trade_at   TIMESTAMPTZ,        -- timestamp of most recent trade
    -- Cumulative session totals
    total_volume    BIGINT,             -- shares traded today
    total_value     BIGINT,             -- turnover (NTD) today
    tx_count        INTEGER,            -- number of transactions today
    -- Change vs previous close
    change_price    NUMERIC(12, 4),
    change_pct      NUMERIC(10, 4),
    amplitude       NUMERIC(10, 4),
    -- Price limits (漲跌停)
    limit_up        NUMERIC(12, 4),
    limit_down      NUMERIC(12, 4),
    -- Top-of-book (filled by ws_book only)
    bid_price       NUMERIC(12, 4),
    bid_size        BIGINT,
    ask_price       NUMERIC(12, 4),
    ask_size        BIGINT,
    -- Metadata
    source          VARCHAR(16),        -- 'rest_sweep' | 'ws_trade' | 'ws_book'
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tw_intraday_updated ON tw.intraday_quotes (updated_at);
CREATE INDEX IF NOT EXISTS idx_tw_intraday_change  ON tw.intraday_quotes (change_pct);
