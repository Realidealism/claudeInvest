-- Extra static fields from SinoPac Shioaji contracts, populated once per day
-- by pre_market_update.py alongside ref_price / limit_up / limit_down.

ALTER TABLE tw.intraday_quotes
    ADD COLUMN IF NOT EXISTS category       VARCHAR(4),
    ADD COLUMN IF NOT EXISTS day_trade       BOOLEAN,
    ADD COLUMN IF NOT EXISTS margin_balance  INTEGER,
    ADD COLUMN IF NOT EXISTS short_balance   INTEGER;

COMMENT ON COLUMN tw.intraday_quotes.category      IS 'TSE industry category code from SinoPac contract';
COMMENT ON COLUMN tw.intraday_quotes.day_trade      IS 'Whether day-trading is allowed';
COMMENT ON COLUMN tw.intraday_quotes.margin_balance IS 'Margin trading balance (lots) from SinoPac contract';
COMMENT ON COLUMN tw.intraday_quotes.short_balance  IS 'Short selling balance (lots) from SinoPac contract';
