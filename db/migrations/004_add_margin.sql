-- Add margin trading (融資融券) columns to tw.daily_prices

ALTER TABLE tw.daily_prices
    ADD COLUMN IF NOT EXISTS margin_buy          BIGINT,   -- 融資買進 (shares)
    ADD COLUMN IF NOT EXISTS margin_sell         BIGINT,   -- 融資賣出 (shares)
    ADD COLUMN IF NOT EXISTS margin_cash_repay   BIGINT,   -- 現金償還 (shares)
    ADD COLUMN IF NOT EXISTS margin_prev_balance BIGINT,   -- 前日融資餘額 (shares)
    ADD COLUMN IF NOT EXISTS margin_balance      BIGINT,   -- 今日融資餘額 (shares)
    ADD COLUMN IF NOT EXISTS short_sell          BIGINT,   -- 融券賣出 (shares)
    ADD COLUMN IF NOT EXISTS short_buy           BIGINT,   -- 融券買進 (shares)
    ADD COLUMN IF NOT EXISTS short_repay         BIGINT,   -- 現券償還 (shares)
    ADD COLUMN IF NOT EXISTS short_prev_balance  BIGINT,   -- 前日融券餘額 (shares)
    ADD COLUMN IF NOT EXISTS short_balance       BIGINT,   -- 今日融券餘額 (shares)
    ADD COLUMN IF NOT EXISTS margin_short_offset BIGINT;   -- 資券互抵 (shares)