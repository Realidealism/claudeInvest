-- Cboe volatility indices pulled straight from Cboe's daily-price CSVs.
-- symbol-keyed so one table (and one scraper) covers several series:
--   VIX   = Cboe Volatility Index (SPX options, 30-day)
--   VXSMH = Cboe Semiconductor ETF Volatility Index (SMH options, 30-day)
-- Replaces the /vix page's earlier reliance on cnn_fear_greed.volatility_score
-- for the US series; that column stays in use by the Fear & Greed page.
CREATE TABLE IF NOT EXISTS tw.vix_cboe (
    symbol       TEXT NOT NULL,
    trade_date   DATE NOT NULL,
    close        NUMERIC(6, 2) NOT NULL,
    fetched_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, trade_date)
);
