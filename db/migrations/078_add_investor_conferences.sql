-- Investor conference filings index (法人說明會, MOPS ajax_t100sb02_1).
-- Index only: the presentation files themselves are pulled on demand, because
-- roughly half of them carry no product-revenue breakdown and some draw it as a
-- bitmap (台積電), so a blanket 15-30GB download would mostly be dead weight.

CREATE TABLE IF NOT EXISTS tw.investor_conferences (
    stock_id    VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    conf_date   DATE        NOT NULL,
    conf_time   VARCHAR(10),
    conf_type   TEXT,
    subject     TEXT,
    file_zh     TEXT,                 -- 中文簡報檔名 (…M###.pdf)
    file_en     TEXT,                 -- 英文簡報檔名 (…E###.pdf)
    market      VARCHAR(4),           -- sii | otc
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stock_id, conf_date, file_zh)
);

CREATE INDEX IF NOT EXISTS idx_tw_conf_date ON tw.investor_conferences (conf_date);
