-- Stock futures contract -> underlying, for the short-sale constraint
-- (signal_backtest.short_constraints, margin_futures): a short goes through
-- the stock's standard futures where they traded >= 50 lots a day over the
-- 20 trading days before entry (user ruling 2026-10-20/21).
-- kind: 'stock' (stock_id set) or 'other' (index / commodity / FX futures
--   that also end in F, kept so an unknown code can be told apart).
-- source: 'stockLists' (TAIFEX /cht/2/stockLists, synced daily) or
--   'price_match' (delisted codes matched by close vs tw.daily_prices,
--   imported once from the 2026-10-20 research, tmp/short_sale_research).
-- standard: shares_per_lot 2000 (ETF 10000); minis (100 / 1000) are false.
CREATE TABLE IF NOT EXISTS tw.stock_futures_contracts (
    contract       VARCHAR(8) PRIMARY KEY,
    kind           VARCHAR(8) NOT NULL,
    stock_id       VARCHAR(10),
    shares_per_lot INTEGER,
    standard       BOOLEAN NOT NULL DEFAULT FALSE,
    source         VARCHAR(16) NOT NULL,
    last_listed    DATE,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_stock_futures_contracts_stock ON tw.stock_futures_contracts (stock_id);
