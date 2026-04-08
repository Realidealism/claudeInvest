-- Add price limit columns to tw.daily_prices
-- ref_price  : 開盤競價基準 / 參考價
-- limit_up   : 漲停價
-- limit_down : 跌停價

ALTER TABLE tw.daily_prices
    ADD COLUMN IF NOT EXISTS ref_price   NUMERIC(12, 2),  -- reference price (參考價)
    ADD COLUMN IF NOT EXISTS limit_up    NUMERIC(12, 2),  -- upper price limit (漲停價)
    ADD COLUMN IF NOT EXISTS limit_down  NUMERIC(12, 2);  -- lower price limit (跌停價)