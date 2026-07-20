-- Short-sale suspension / 融券最後回補日 calendar (停券預告表 + 股東會回補推導).
--
-- Purpose: heavily-shorted stocks must force-cover 融券 before a stock's
-- 停止過戶 window (股東會 or 除權息). This table is the covering-date calendar,
-- fed from two sources:
--   1. FORWARD daily (official): TWSE BFI84U 停券預告表 + TPEx
--      tpex_margin_trading_term. Both give 停券起日(最後回補日)/迄日/原因 for a
--      rolling ~5-week window covering ALL reasons (股東會/除息/除權息/減資/現增).
--      source in ('BFI84U','tpex_term'), is_derived = FALSE.
--   2. HISTORICAL backfill (股東會 only): MOPS t108sb31 股東會日期 -> derived
--      last_cover_date = 開會日 − 60 cal days − 6 trading days (公司登記 60-day
--      book-closure + 融資融券操作辦法 §76 last-cover = 停過戶起日前第6營業日),
--      calibrated once against recent BFI84U windows. source = 'mops_agm_derived',
--      is_derived = TRUE.
--
-- last_cover_date is the tradeable anchor (= 停券起日). meeting_date is set only
-- for 股東會 events. reason is normalised so AGM rows are '股東會'.

CREATE TABLE IF NOT EXISTS tw.short_cover_calendar (
    id              SERIAL PRIMARY KEY,
    stock_id        TEXT NOT NULL,
    last_cover_date DATE NOT NULL,       -- 融券最後回補日 (= 停券起日); the anchor
    suspension_end  DATE,                -- 停券迄日 (NULL for derived AGM rows)
    reason          TEXT,                -- 股東會 / 除息 / 除權息 / 減資 / 現增 / 其他
    meeting_date    DATE,                -- 股東會開會日 (only for 股東會 rows)
    market          TEXT,                -- TWSE / TPEx
    source          TEXT,                -- BFI84U / tpex_term / mops_agm_derived
    is_derived      BOOLEAN DEFAULT FALSE, -- TRUE = last_cover_date derived, not official
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (stock_id, last_cover_date, reason)
);

CREATE INDEX IF NOT EXISTS idx_short_cover_date
    ON tw.short_cover_calendar (last_cover_date DESC);
CREATE INDEX IF NOT EXISTS idx_short_cover_stock
    ON tw.short_cover_calendar (stock_id);
CREATE INDEX IF NOT EXISTS idx_short_cover_reason
    ON tw.short_cover_calendar (reason);
