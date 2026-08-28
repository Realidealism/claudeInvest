-- The 業務內容 chapter of the annual report, as text.
--
-- tw.product_revenue holds the 營業比重 table from the same filing, but that
-- table is written at whatever granularity the company chose, and for IC
-- designers the choice is usually one line: 聯發科 books 98.22% as 「晶片」,
-- 凌通 books 98.94% as 「IC產品收入」. A quarter of the theme memberships
-- cannot be confirmed against it for that reason.
--
-- The prose two chapters later does not have that problem — it lists the
-- product lines by name, because 業務範圍 is where a company describes what it
-- actually sells. It cannot be turned into percentages, so it lives here as
-- text rather than being forced into the revenue table's shape.
--
-- One row per (stock, year, section); section is the heading the text was
-- found under, so a later pass can tell 業務範圍 from 產品用途.

CREATE TABLE IF NOT EXISTS tw.product_narrative (
    stock_id     VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    fiscal_year  SMALLINT    NOT NULL,
    section      TEXT        NOT NULL,
    body         TEXT        NOT NULL,
    pages        TEXT,                          -- which PDF pages it came from
    source_file  TEXT,
    updated_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stock_id, fiscal_year, section)
);

-- Same shape as tw.product_revenue_status: one row per attempt, so a re-run
-- skips finished work and a failure stays visible instead of looking like
-- "this company describes nothing".
CREATE TABLE IF NOT EXISTS tw.product_narrative_status (
    stock_id     VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    fiscal_year  SMALLINT    NOT NULL,
    status       TEXT        NOT NULL,          -- 'ok' | 'no_report' | 'no_section' | 'download_failed' | 'parse_failed'
    detail       TEXT,
    source_file  TEXT,
    updated_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stock_id, fiscal_year)
);

CREATE INDEX IF NOT EXISTS idx_tw_product_narrative_year
    ON tw.product_narrative (fiscal_year);
