-- Shareholder-meeting (股東常會/臨時會) date calendar.
--
-- Raw 股東會 info, kept separate from tw.short_cover_calendar (068) on purpose:
-- this table holds the OFFICIAL 開會日期 (meeting_date), while the covering-date
-- (融券最後回補日) in short_cover_calendar is DERIVED from it (meeting_date − 60
-- cal days − 6 trading days). Keeping the raw meeting date clean means a wrong
-- derivation can be recomputed for free without re-scraping.
--
-- Sources:
--   Daily (current year, all-market): TWSE OpenAPI t187ap41_L + TPEx t187ap41_O
--     (股東常會(臨時)會 + 開會日期). source = 'openapi_t187'.
--   Historical backfill (2017+, per ROC year × market): MOPS ajax_t108sb31
--     (cell[0]=code, cell[4]=開會日期 ROC). source = 'mops_t108'.
--
-- A company can hold both 常會 and 臨時會 in a year -> multiple rows.

CREATE TABLE IF NOT EXISTS tw.shareholder_meetings (
    id           SERIAL PRIMARY KEY,
    stock_id     TEXT NOT NULL,
    meeting_date DATE NOT NULL,        -- 開會日期
    meeting_type TEXT,                 -- 常會 / 臨時會 (raw label if available)
    market       TEXT,                 -- TWSE / TPEx
    source       TEXT,                 -- openapi_t187 / mops_t108
    updated_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (stock_id, meeting_date)
);

CREATE INDEX IF NOT EXISTS idx_agm_date
    ON tw.shareholder_meetings (meeting_date DESC);
CREATE INDEX IF NOT EXISTS idx_agm_stock
    ON tw.shareholder_meetings (stock_id);
