-- Day trading (當日沖銷) columns for per-stock and market-level data.

-- Per-stock day trading in daily_prices
ALTER TABLE tw.daily_prices
    ADD COLUMN IF NOT EXISTS dt_volume     BIGINT,
    ADD COLUMN IF NOT EXISTS dt_buy_amount BIGINT,
    ADD COLUMN IF NOT EXISTS dt_sell_amount BIGINT;

-- Market-level day trading totals in index_prices
ALTER TABLE tw.index_prices
    ADD COLUMN IF NOT EXISTS dt_volume     BIGINT,
    ADD COLUMN IF NOT EXISTS dt_buy_amount BIGINT,
    ADD COLUMN IF NOT EXISTS dt_sell_amount BIGINT;
