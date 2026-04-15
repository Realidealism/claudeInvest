-- Tracks scan state of statementdog tag pages for daily incremental scraping.
-- status: 'ok' (parsed OK), '404' (no such tag), 'paywall' (quota blocked, retry later)

CREATE TABLE IF NOT EXISTS tw.sd_tag_scan (
    tag_id       INTEGER PRIMARY KEY,
    status       VARCHAR(10) NOT NULL,
    tag_name     VARCHAR(100),
    member_count INTEGER,
    last_attempt TIMESTAMPTZ DEFAULT NOW(),
    attempts     INTEGER DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_sd_tag_scan_status ON tw.sd_tag_scan (status);
