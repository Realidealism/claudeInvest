-- Add delisted_date column to track when a stock was delisted or merged
ALTER TABLE tw.stocks ADD COLUMN IF NOT EXISTS delisted_date DATE;
