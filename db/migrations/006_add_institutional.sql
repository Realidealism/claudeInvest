-- Add institutional investor (三大法人) columns to tw.daily_prices
-- All values are in shares (股).
-- foreign_*  : 外陸資合計 (含外資自營商)
-- trust_*    : 投信
-- dealer_*   : 自營商合計 (自行+避險)
-- inst_net   : 三大法人買賣超合計

ALTER TABLE tw.daily_prices
    ADD COLUMN IF NOT EXISTS foreign_buy  BIGINT,  -- 外陸資買進 (shares)
    ADD COLUMN IF NOT EXISTS foreign_sell BIGINT,  -- 外陸資賣出 (shares)
    ADD COLUMN IF NOT EXISTS foreign_net  BIGINT,  -- 外陸資買賣超 (shares)
    ADD COLUMN IF NOT EXISTS trust_buy    BIGINT,  -- 投信買進 (shares)
    ADD COLUMN IF NOT EXISTS trust_sell   BIGINT,  -- 投信賣出 (shares)
    ADD COLUMN IF NOT EXISTS trust_net    BIGINT,  -- 投信買賣超 (shares)
    ADD COLUMN IF NOT EXISTS dealer_buy   BIGINT,  -- 自營商買進 (shares)
    ADD COLUMN IF NOT EXISTS dealer_sell  BIGINT,  -- 自營商賣出 (shares)
    ADD COLUMN IF NOT EXISTS dealer_net   BIGINT,  -- 自營商買賣超 (shares)
    ADD COLUMN IF NOT EXISTS inst_net     BIGINT;  -- 三大法人買賣超合計 (shares)
