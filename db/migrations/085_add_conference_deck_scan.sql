-- One-off extractability census of investor conference decks (法說會簡報).
--
-- Answers a single question that blocked the product-revenue work: which
-- companies publish a machine-readable product-mix table in their deck? The
-- research in tmp/investor_conf_research.md sampled six companies and found
-- 台積電 draws the table as a bitmap and half the sample omits it entirely, so
-- the whitelist has to be measured rather than assumed.
--
-- This is a snapshot, not a maintained feed: one row per company's latest deck
-- at scan time. Nothing refreshes it daily.

CREATE TABLE IF NOT EXISTS tw.conference_deck_scan (
    stock_id            VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    conf_date           DATE        NOT NULL,
    file_name           TEXT        NOT NULL,
    ext                 VARCHAR(8),           -- pdf | pptx | ppt
    pages               INT,
    total_chars         INT,
    total_images        INT,
    best_page           INT,                  -- page with the most % signs among keyword hits
    best_page_keywords  TEXT[],
    best_page_pct       INT,                  -- count of '%' on that page
    best_page_chars     INT,
    best_page_images    INT,
    best_page_has_table BOOLEAN,              -- extract_tables() found a table there
    bucket              TEXT,                 -- text_table | text_scatter | image_only
                                              -- no_keyword | parse_error | unscanned
    scanned_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stock_id, file_name)
);

CREATE INDEX IF NOT EXISTS idx_tw_deck_scan_bucket ON tw.conference_deck_scan (bucket);
