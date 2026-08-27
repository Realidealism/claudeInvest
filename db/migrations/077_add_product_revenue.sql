-- Product-line revenue mix parsed from the annual report chapter
-- 「主要商品(服務)之營業比重」(doc.twse.com.tw t57sb01, F04 中文年報 / FE4 英文版).
-- This is the only source that covers every listed company: the structured MOPS
-- endpoint (ajax_t05st08) became voluntary after IFRS and now has ~226 filers.
-- Granularity is whatever the company chose to disclose, so `item` is free text
-- and a company may report one bucket (台積電: 晶圓) or a dozen.

CREATE TABLE IF NOT EXISTS tw.product_revenue (
    stock_id     VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    fiscal_year  SMALLINT    NOT NULL,          -- AD year of the data, e.g. 2025
    item         TEXT        NOT NULL,
    revenue      NUMERIC(20,0),                 -- NT$ thousands, NULL when only % disclosed
    pct          NUMERIC(7,4),
    source_file  TEXT,
    method       TEXT,                          -- 'table' | 'prose'
    updated_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stock_id, fiscal_year, item)
);

-- One row per attempt, so a re-run can skip finished work and failures stay visible
-- instead of looking like "this company discloses nothing".
CREATE TABLE IF NOT EXISTS tw.product_revenue_status (
    stock_id     VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    fiscal_year  SMALLINT    NOT NULL,
    status       TEXT        NOT NULL,          -- 'ok' | 'no_report' | 'no_section' | 'download_failed' | 'parse_failed'
    detail       TEXT,
    source_file  TEXT,
    updated_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stock_id, fiscal_year)
);

CREATE INDEX IF NOT EXISTS idx_tw_product_revenue_year ON tw.product_revenue (fiscal_year);
