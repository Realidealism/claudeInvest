-- Advance/decline counts (漲跌證券數) per market index per day
ALTER TABLE tw.index_prices ADD COLUMN IF NOT EXISTS advance       INT;  -- 上漲家數
ALTER TABLE tw.index_prices ADD COLUMN IF NOT EXISTS advance_limit INT;  -- 漲停家數
ALTER TABLE tw.index_prices ADD COLUMN IF NOT EXISTS decline       INT;  -- 下跌家數
ALTER TABLE tw.index_prices ADD COLUMN IF NOT EXISTS decline_limit INT;  -- 跌停家數
ALTER TABLE tw.index_prices ADD COLUMN IF NOT EXISTS unchanged     INT;  -- 持平家數
ALTER TABLE tw.index_prices ADD COLUMN IF NOT EXISTS no_trade      INT;  -- 未成交家數
