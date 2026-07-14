-- Upcoming ex-dividend / ex-rights calendar (除權除息預告表).
--
-- Distinct from tw.dividends, which is a FinMind-sourced HISTORICAL record
-- backfilled manually from hermit_stock. This table is the forward-looking
-- announcement board scraped daily from TWSE/TPEx, and is what drives the
-- Telegram morning-brief ex-dividend alerts.
--
-- No FK to tw.stocks: the preview board lists ETFs and freshly listed issues
-- that may not be in tw.stocks yet, and a missing master row must not drop
-- an upcoming ex-dividend event.

CREATE TABLE IF NOT EXISTS tw.dividend_calendar (
    stock_id        VARCHAR(10) NOT NULL,
    ex_date         DATE NOT NULL,          -- 除權/除息交易日
    market          VARCHAR(8) NOT NULL,    -- TWSE / TPEx
    name            TEXT,
    kind            TEXT,                   -- 息 / 權 / 權息
    cash_dividend   NUMERIC(12, 6),         -- NULL when the amount is not yet announced
    stock_ratio     NUMERIC(12, 6),         -- 無償配股率 (shares per share)
    notified_at     TIMESTAMPTZ,            -- Telegram push dedup: NULL = not yet pushed
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stock_id, ex_date)
);

CREATE INDEX IF NOT EXISTS idx_tw_dividend_calendar_ex_date
    ON tw.dividend_calendar (ex_date);
